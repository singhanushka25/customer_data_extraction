import os
import json
import time
import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple
from source_schema_extractor import get_source_schema

# Constants
BASE_URL = "https://{region}-services.hevoapp.com/config/v1.0"
AUTH_HEADER = "Basic aW50ZXJuYWxhcGl1c2VyOjZEQFNPSmYxJDNtZ1dUY3FNUWpIVE5AN2U5WA=="
HEADERS = {
    "Authorization": AUTH_HEADER,
    "Content-Type": "application/json"
}


def check_pipeline_mode(integration_id: int, region: str) -> str:
    """Check if the pipeline is in Custom SQL mode."""
    try:
        url = f"{BASE_URL.format(region=region)}/integrations/{integration_id}/display-details"
        print(url)
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("config", {}).get("Pipeline Mode", "")
        return None
    except Exception:
        time.sleep(1000)
        return check_pipeline_mode(integration_id, region)


def get_active_source_objects(integration_id: int, region: str) -> List[Dict]:
    """Get active source objects for the integration."""
    try:
        url = f"{BASE_URL.format(region=region)}/source-objects/{integration_id}/objects"
        payload = {"statuses": ["ACTIVE"]}
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("source_objects", [])
        return []
    except Exception:
        time.sleep(1000)
        return get_active_source_objects(integration_id, region)


def generate_query_string(full_name: str, source_type: str) -> str:
    """Generate the query string based on source type."""
    if source_type == "MYSQL":
        return f"SHOW CREATE TABLE {full_name};"

    elif source_type == "MS_SQL":
        if '.' in full_name:
            schema, table = full_name.split(".")
        else:
            schema = "dbo"
            table = full_name
        return f"""
        DECLARE @TableName NVARCHAR(MAX) = '{table}';
        DECLARE @SchemaName NVARCHAR(MAX) = '{schema}';
        DECLARE @SQL NVARCHAR(MAX) = '';
        SET @SQL = 'CREATE TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' (' + CHAR(13);

        DECLARE @ColumnList NVARCHAR(MAX) = '';
        SELECT @ColumnList = STRING_AGG(
            CAST(
                '    ' + QUOTENAME(COLUMN_NAME) + ' ' + 
                DATA_TYPE +
                CASE 
                    WHEN DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar') 
                        THEN '(' + CASE WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')'
                    WHEN DATA_TYPE IN ('decimal', 'numeric') 
                        THEN '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')'
                    ELSE ''
                END +
                CASE WHEN IS_NULLABLE = 'NO' THEN ' NOT NULL' ELSE ' NULL' END
            AS NVARCHAR(MAX)), ',' + CHAR(13))
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = @TableName AND TABLE_SCHEMA = @SchemaName;

        SET @SQL = @SQL + @ColumnList + CHAR(13) + ')';

        DECLARE @PKSQL NVARCHAR(MAX) = '';
        SELECT @PKSQL = 'ALTER TABLE ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) +
                        ' ADD CONSTRAINT ' + QUOTENAME(tc.CONSTRAINT_NAME) + 
                        ' PRIMARY KEY (' +
                        STRING_AGG(CAST(QUOTENAME(kcu.COLUMN_NAME) AS NVARCHAR(MAX)), ', ') + ');'
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_NAME = @TableName AND tc.TABLE_SCHEMA = @SchemaName AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        GROUP BY tc.CONSTRAINT_NAME;

        IF @PKSQL IS NOT NULL SET @SQL = @SQL + CHAR(13) + @PKSQL;

        SELECT @SQL AS CreateTableStatement;
        """

    elif source_type == "POSTGRES":
        if '.' in full_name:
            schema, table = full_name.split(".")
        else:
            schema = "public"
            table = full_name
        return f"""
        WITH column_definitions AS (
            SELECT c.table_schema, c.table_name, c.column_name,
                   CASE 
                       WHEN c.data_type IN ('character varying', 'varchar', 'char', 'text') 
                           THEN c.data_type || '(' || c.character_maximum_length || ')'
                       WHEN c.data_type IN ('numeric', 'decimal') 
                           THEN c.data_type || '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
                       ELSE c.data_type 
                   END AS data_type,
                   CASE WHEN c.is_nullable = 'NO' THEN 'NOT NULL' ELSE 'NULL' END AS nullable
            FROM information_schema.columns c
            WHERE c.table_name = '{table}' AND c.table_schema = '{schema}'
        ),
        primary_keys AS (
            SELECT kcu.table_schema, kcu.table_name, kcu.column_name, tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name 
                AND tc.table_schema = kcu.table_schema 
                AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        )
        SELECT 'CREATE TABLE ' || table_schema || '.' || table_name || ' (' || 
               STRING_AGG(column_name || ' ' || data_type || ' ' || nullable, ', ') || ');' ||
               COALESCE((
                   SELECT ' ALTER TABLE ' || table_schema || '.' || table_name || 
                          ' ADD CONSTRAINT ' || constraint_name || 
                          ' PRIMARY KEY (' || STRING_AGG(column_name, ', ') || ');'
                   FROM primary_keys
                   WHERE primary_keys.table_name = column_definitions.table_name
                   GROUP BY table_schema, table_name, constraint_name
               ), '') AS create_table_statement
        FROM column_definitions
        GROUP BY table_schema, table_name;
        """

    elif source_type == "ORACLE":
        if '.' in full_name:
            schema, table = full_name.split(".")
            owner_clause = f"AND owner = UPPER('{schema}')"
        else:
            schema = ""
            table = full_name
            owner_clause = ""
        return f"""
        WITH columns AS (
            SELECT column_name, 
                   data_type || 
                   CASE 
                       WHEN data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2') 
                           THEN '(' || data_length || ')' 
                       WHEN data_type = 'NUMBER' 
                           THEN '(' || COALESCE(TO_CHAR(data_precision), '38') || ',' || COALESCE(TO_CHAR(data_scale), '0') || ')' 
                       ELSE '' 
                   END AS column_type, 
                   CASE WHEN nullable = 'N' THEN ' NOT NULL' ELSE '' END AS nullable 
            FROM ALL_TAB_COLUMNS 
            WHERE table_name = UPPER('{table}') {owner_clause}
        ), 
        primary_keys AS (
            SELECT ac.constraint_name, 
                   LISTAGG(acc.column_name, ', ') WITHIN GROUP (ORDER BY acc.column_name) AS pk_columns 
            FROM ALL_CONS_COLUMNS acc 
            JOIN ALL_CONSTRAINTS ac ON acc.constraint_name = ac.constraint_name 
            WHERE ac.constraint_type = 'P' 
              AND acc.table_name = UPPER('{table}') {owner_clause}
            GROUP BY ac.constraint_name
        ) 
        SELECT 
            (SELECT 'CREATE TABLE {schema}.{table} (' || 
                    LISTAGG(column_name || ' ' || column_type || ' ' || nullable, ', ') 
                    WITHIN GROUP (ORDER BY column_name) || ');' 
             FROM columns) || 
            (SELECT ' ALTER TABLE {schema}.{table} ADD CONSTRAINT ' || constraint_name || 
                    ' PRIMARY KEY (' || pk_columns || ');' 
             FROM primary_keys FETCH FIRST 1 ROW ONLY) 
            AS create_table_statement 
        FROM DUAL;
        """

    else:
        raise ValueError(f"Unsupported source type: {source_type}")



def execute_query(integration_id: int, query: str, region: str) -> Optional[str]:
    """Execute a query and get task ID."""
    try:
        url = f"{BASE_URL.format(region=region)}/integrations/query-execution?override_consent=true"
        payload = {
            "query_params": {"query": query},
            "integration_id": integration_id
        }
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("task_id")
        return None
    except Exception:
        time.sleep(1000)
        return execute_query(integration_id, query, region)


def execute_query_at_destination(destination_id: int, query: str, region: str) -> Optional[str]:
    """Execute a query and get task ID."""
    try:
        url = f"{BASE_URL.format(region=region)}/destinations/execute?override_consent=true"
        payload = {
            "query": query,
            "source_destination_id": destination_id
        }
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("task_id")
        return None
    except Exception:
        time.sleep(1000)
        return execute_query(destination_id, query, region)


def fetch_query_result(task_id: int, region: str) -> Optional[Dict]:
    """Fetch query execution result."""
    try:
        url = f"{BASE_URL.format(region=region)}/integrations/query-execution/{task_id}?current_call_result_fetch=true"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("result", {}).get("data", {})
        return None
    except Exception:
        time.sleep(1000)
        return fetch_query_result(task_id, region)


def fetch_destination_query_result(task_id: int, region: str) -> Optional[Dict]:
    """Fetch query execution result."""
    try:
        url = f"{BASE_URL.format(region=region)}/destinations/execute/{task_id}?current_call_result_fetch=true"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("result", {}).get("data", {})
        return None
    except Exception:
        time.sleep(1000)
        return fetch_query_result(task_id, region)


def get_field_mappings(integration_id: int, source_table: str, region: str = "us") -> Optional[Dict]:
    """Get field mappings for a specific source table."""
    try:
        url = f"{BASE_URL.format(region=region)}/mapper/{integration_id}/mappings/{source_table}"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data.get("data")
        return None
    except Exception:
        time.sleep(1000)
        return get_field_mappings(integration_id, source_table, region)


def get_destination_mappings(integration_id: int, region: str = "us") -> List[Dict]:
    """Get destination mappings for a given integration ID."""
    url = f"{BASE_URL.format(region=region)}/mapper/short/{integration_id}/mappings"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        if data.get("success"):
            return data.get("data", [])
    return []


def get_destination_details(destination_id: int, region: str = "us") -> Optional[Dict]:
    """Get destination details including project ID and dataset."""
    url = f"{BASE_URL.format(region=region)}/destinations/{destination_id}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        if data.get("success"):
            return data.get("data")
    return None


def get_create_table_query(integration_id: int, region: str = "us") -> Dict[str, str]:
    """Get CREATE TABLE queries for all tables in the integration."""
    create_table_queries = {}

    # Check pipeline mode
    pipeline_mode = check_pipeline_mode(integration_id, region)
    if pipeline_mode == "Custom SQL":
        print(f"Custom SQL found for integration id {integration_id}")
        return create_table_queries

    # Get active source objects
    source_objects = get_active_source_objects(integration_id, region)

    # Process each source object
    for source_object in source_objects[:30]:
        full_name = source_object.get("namespace", {}).get("full_name")
        if not full_name:
            continue

        # Generate and execute query
        query = generate_query_string(full_name, "MS_SQL")  # Assuming MySQL for now
        task_id = execute_query(integration_id, query, region)
        if not task_id:
            continue

        time.sleep(1)  # Wait for query execution

        # Fetch query result
        query_result = fetch_query_result(task_id, region)
        if not query_result:
            continue

        # Extract CREATE TABLE query
        rows = query_result.get("rows", [])
        if len(rows) > 0 and len(rows[0]) > 0:
            create_table_query = rows[0][0]  # <-- FIXED
            table_name = full_name.split(".")[-1]
            create_table_queries[table_name] = create_table_query

    return create_table_queries


def generate_mapping_comparison(integration_id: int, region: str) -> pd.DataFrame:
    """Generate the mapping comparison between source and destination schemas."""
    # Get destination mappings
    mappings = get_destination_mappings(integration_id, region)

    # Get CREATE TABLE queries
    create_table_queries = get_create_table_query(integration_id, region)

    results = []

    for mapping in mappings:
        if mapping.get("status") == 'IGNORED':
            continue
        if not mapping.get("destination_id"):
            continue

        destination_id = mapping["destination_id"]

        source_table = mapping["source_schema_name"]
        destination_table = mapping["destination_schema_name"]
        # Get source schema
        if "." in source_table:
            source_table_key = source_table.split(".")[1]
        else:
            source_table_key = source_table

        create_table_query = create_table_queries.get(source_table_key)
        if not create_table_query:
            continue
        print(source_table)
        print(destination_table)

        # Get field mappings
        field_mappings = get_field_mappings(integration_id, source_table, region)
        if not field_mappings:
            continue

        # Get destination details
        dest_details = get_destination_details(destination_id, region)
        if not dest_details:
            continue

        schema_name = dest_details.get("config", {}).get("schema_name", "public")

        # Query destination schema (REDSHIFT)
        query = f"""SELECT column_name, data_type, is_nullable, ordinal_position 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{destination_table}'
        AND table_schema = '{schema_name}'
        """


        task_id = execute_query_at_destination(destination_id, query, region)
        if not task_id:
            continue
        time.sleep(1)
        dest_schema = fetch_destination_query_result(task_id, region)
        if not dest_schema:
            continue

        source_schema = get_source_schema(create_table_query)

        # Create a mapping of destination to source columns
        source_columns = {col['column_name'].lower(): col for col in source_schema}

        # Process the field mappings
        for field_mapping in field_mappings.get("field_mappings", []):
            source_field = field_mapping.get("sf")
            destination_field = field_mapping.get("df")

            if not source_field or not destination_field:
                continue

            # Find source column type
            source_col = source_columns.get(source_field.lower())
            if not source_col:
                continue

            # Find destination column type
            dest_col = next((row for row in dest_schema["rows"] if row[0].lower() == destination_field.lower()), None)
            if not dest_col:
                continue

            results.append({
                "integration_id": integration_id,
                "destination_id": destination_id,
                "source_table_name": source_table,
                "destination_table_name": destination_table,
                "source_field": source_field,
                "destination_field": destination_field,
                "source_type": source_col['data_type'],
                "destination_type": dest_col[1]
            })

        if source_table == 'Lead':
            break

    return pd.DataFrame(results)


def main():
    # Example usage
    integration_ids_dict = {
        # "asia": [15812,17324,18690,18691,19564,19572],
        "us" : [69,2188,2364,464,512,725,8790,12317,14022,32531,40061,40771,38572,38474,38569,38591,38592,40632,40633,44394,7911,7912,7913,7933,7961,7962,7964,7965,7963,7967,7968,7969,7970,7972,7973,7974,7971,8472,8471,8490,8506,8507,9028,13367,14211,14622,19743,21686,27004,26883,43761,43762,44868,44869,44870,45268,45269,45718,46793,47264,47265,47267,47284,47288,47289,47313,47314,47317,47318,47319,47864,47868,47933,48014,48015,48030,48045,50082,50083,45324,53081,47015,48868,48869],
        # "in": [1120,10711,13815,14606,15190,15315,17062,18683,19214,20158,20298,18730,20394,20897,21246],
        "eu":[22694,22695,22696,22697,22698,22699,27311,30950,30956],
        "us2":[12232, 12233, 12134],
        # "au":[5327,5248,5249]
    }

    for region, integration_ids in integration_ids_dict.items():
        for integration_id in integration_ids:
            df = generate_mapping_comparison(integration_id, region)

            # Save to CSV
            output_dir = f"data_mssql/{region}/redshift"
            os.makedirs(output_dir, exist_ok=True)

            output_file = os.path.join(output_dir, f"mapping_comparison_{region}_{integration_id}.csv")
            df.to_csv(output_file, index=False, sep=",")
            print(f"Mapping comparison saved to {output_file}")



if __name__ == "__main__":
    main()