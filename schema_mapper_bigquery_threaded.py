import os
import json
import time
import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading
from source_schema_extractor import get_source_schema

# Constants
BASE_URL = "https://{region}-services.hevoapp.com/config/v1.0"
AUTH_HEADER = "Basic aW50ZXJuYWxhcGl1c2VyOjZEQFNPSmYxJDNtZ1dUY3FNUWpIVE5AN2U5WA=="
HEADERS = {
    "Authorization": AUTH_HEADER,
    "Content-Type": "application/json"
}

# Thread-safe print function
print_lock = Lock()


def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


def check_pipeline_mode(integration_id: int, region: str) -> str:
    """Check if the pipeline is in Custom SQL mode."""
    try:
        url = f"{BASE_URL.format(region=region)}/integrations/{integration_id}/display-details"
        thread_safe_print(f"Checking pipeline mode for {integration_id}: {url}")
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("config", {}).get("Pipeline Mode", "")
        return None
    except Exception as e:
        thread_safe_print(f"Error checking pipeline mode for {integration_id}: {e}")
        time.sleep(1)
        return check_pipeline_mode(integration_id, region)


def get_active_source_objects(integration_id: int, region: str) -> List[Dict]:
    """Get active source objects for the integration."""
    try:
        url = f"{BASE_URL.format(region=region)}/source-objects/{integration_id}/objects"
        payload = {"statuses": ["ACTIVE"]}
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("source_objects", [])
        return []
    except Exception as e:
        thread_safe_print(f"Error getting source objects for {integration_id}: {e}")
        time.sleep(1)
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
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("task_id")
        return None
    except Exception as e:
        thread_safe_print(f"Error executing query for {integration_id}: {e}")
        time.sleep(1)
        return execute_query(integration_id, query, region)


def execute_query_at_destination(destination_id: int, query: str, region: str) -> Optional[str]:
    """Execute a query and get task ID."""
    try:
        url = f"{BASE_URL.format(region=region)}/destinations/execute?override_consent=true"
        payload = {
            "query": query,
            "source_destination_id": destination_id
        }
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
        if response.status_code == 200:
            data = response.json()
            return data.get("data", {}).get("task_id")
        return None
    except Exception as e:
        thread_safe_print(f"Error executing destination query for {destination_id}: {e}")
        time.sleep(1)
        return execute_query_at_destination(destination_id, query, region)


def fetch_query_result(task_id: int, region: str, max_retries: int = 10) -> Optional[Dict]:
    """Fetch query execution result with polling."""
    for attempt in range(max_retries):
        try:
            url = f"{BASE_URL.format(region=region)}/integrations/query-execution/{task_id}?current_call_result_fetch=true"
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code == 200:
                data = response.json()
                result = data.get("data", {}).get("result", {}).get("data", {})
                if result:  # If we have results, return them
                    return result
                # If no results yet, wait and retry
                time.sleep(2)
            else:
                time.sleep(2)
        except Exception as e:
            thread_safe_print(f"Error fetching query result for task {task_id}: {e}")
            time.sleep(2)
    return None


def fetch_destination_query_result(task_id: int, region: str, max_retries: int = 10) -> Optional[Dict]:
    """Fetch query execution result with polling."""
    for attempt in range(max_retries):
        try:
            url = f"{BASE_URL.format(region=region)}/destinations/execute/{task_id}?current_call_result_fetch=true"
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code == 200:
                data = response.json()
                result = data.get("data", {}).get("result", {}).get("data", {})
                if result:  # If we have results, return them
                    return result
                # If no results yet, wait and retry
                time.sleep(2)
            else:
                time.sleep(2)
        except Exception as e:
            thread_safe_print(f"Error fetching destination query result for task {task_id}: {e}")
            time.sleep(2)
    return None


def get_field_mappings(integration_id: int, source_table: str, region: str = "us") -> Optional[Dict]:
    """Get field mappings for a specific source table."""
    try:
        url = f"{BASE_URL.format(region=region)}/mapper/{integration_id}/mappings/{source_table}"
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data.get("data")
        return None
    except Exception as e:
        thread_safe_print(f"Error getting field mappings for {integration_id}/{source_table}: {e}")
        time.sleep(1)
        return get_field_mappings(integration_id, source_table, region)


def get_destination_mappings(integration_id: int, region: str = "us") -> List[Dict]:
    """Get destination mappings for a given integration ID."""
    try:
        url = f"{BASE_URL.format(region=region)}/mapper/short/{integration_id}/mappings"
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data.get("data", [])
        return []
    except Exception as e:
        thread_safe_print(f"Error getting destination mappings for {integration_id}: {e}")
        return []


def get_destination_details(destination_id: int, region: str = "us") -> Optional[Dict]:
    """Get destination details including project ID and dataset."""
    try:
        url = f"{BASE_URL.format(region=region)}/destinations/{destination_id}"
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            data = response.json()
            if data.get("success"):
                return data.get("data")
        return None
    except Exception as e:
        thread_safe_print(f"Error getting destination details for {destination_id}: {e}")
        return None


def process_source_object(source_object: Dict, integration_id: int, region: str) -> Tuple[str, str]:
    """Process a single source object to get CREATE TABLE query."""
    full_name = source_object.get("namespace", {}).get("full_name")
    if not full_name:
        return None, None

    try:
        # Generate and execute query
        query = generate_query_string(full_name, "MS_SQL")
        task_id = execute_query(integration_id, query, region)
        if not task_id:
            return None, None

        # Fetch query result with polling
        query_result = fetch_query_result(task_id, region)
        if not query_result:
            return None, None

        # Extract CREATE TABLE query
        rows = query_result.get("rows", [])
        if len(rows) > 0 and len(rows[0]) > 0:
            create_table_query = rows[0][0]
            table_name = full_name.split(".")[-1]
            return table_name, create_table_query

    except Exception as e:
        thread_safe_print(f"Error processing source object {full_name}: {e}")

    return None, None


def get_create_table_query(integration_id: int, region: str = "us", max_workers: int = 10) -> Dict[str, str]:
    """Get CREATE TABLE queries for all tables in the integration using parallel processing."""
    create_table_queries = {}

    # Check pipeline mode
    pipeline_mode = check_pipeline_mode(integration_id, region)
    if pipeline_mode == "Custom SQL":
        thread_safe_print(f"Custom SQL found for integration id {integration_id}")
        return create_table_queries

    # Get active source objects
    source_objects = get_active_source_objects(integration_id, region)

    # Limit to first 30 objects as in original code
    source_objects = source_objects[:30]

    thread_safe_print(f"Processing {len(source_objects)} source objects for integration {integration_id}")

    # Process source objects in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_object = {
            executor.submit(process_source_object, obj, integration_id, region): obj
            for obj in source_objects
        }

        # Collect results as they complete
        for future in as_completed(future_to_object):
            try:
                table_name, create_table_query = future.result()
                if table_name and create_table_query:
                    create_table_queries[table_name] = create_table_query
                    thread_safe_print(f"✓ Processed table: {table_name}")
            except Exception as e:
                thread_safe_print(f"Error in parallel processing: {e}")

    thread_safe_print(f"Completed processing {len(create_table_queries)} tables for integration {integration_id}")
    return create_table_queries


def process_mapping(mapping: Dict, integration_id: int, region: str, create_table_queries: Dict[str, str]) -> List[
    Dict]:
    """Process a single mapping to generate comparison data."""
    results = []

    if mapping.get("status") == 'IGNORED':
        return results
    if not mapping.get("destination_id"):
        return results

    try:
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
            return results

        thread_safe_print(f"Processing mapping: {source_table} -> {destination_table}")

        # Get field mappings
        field_mappings = get_field_mappings(integration_id, source_table, region)
        if not field_mappings:
            return results

        # Get destination details
        dest_details = get_destination_details(destination_id, region)
        if not dest_details:
            return results

        project_id = dest_details["config"]["project_id"]
        dataset = dest_details["config"]["dataset_name"]

        # Query destination schema
        query = f"""
                SELECT column_name, data_type, is_nullable, ordinal_position 
                FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE table_name = '{destination_table}' 
                ORDER BY ordinal_position
                """

        task_id = execute_query_at_destination(destination_id, query, region)
        if not task_id:
            return results

        dest_schema = fetch_destination_query_result(task_id, region)
        if not dest_schema:
            return results

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

        # Break after processing 'Lead' table as in original code
        if source_table == 'Lead':
            thread_safe_print("Processed Lead table, breaking as per original logic")

    except Exception as e:
        thread_safe_print(f"Error processing mapping {mapping}: {e}")

    return results


def generate_mapping_comparison(integration_id: int, region: str, max_workers: int = 5) -> pd.DataFrame:
    """Generate the mapping comparison between source and destination schemas using parallel processing."""
    # Get destination mappings
    mappings = get_destination_mappings(integration_id, region)
    thread_safe_print(f"Found {len(mappings)} mappings for integration {integration_id}")

    # Get CREATE TABLE queries (this is already parallelized)
    create_table_queries = get_create_table_query(integration_id, region)
    thread_safe_print(f"Retrieved {len(create_table_queries)} CREATE TABLE queries")

    all_results = []

    # Process mappings in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all mapping processing tasks
        future_to_mapping = {
            executor.submit(process_mapping, mapping, integration_id, region, create_table_queries): mapping
            for mapping in mappings
        }

        # Collect results as they complete
        for future in as_completed(future_to_mapping):
            try:
                results = future.result()
                all_results.extend(results)
                if results:
                    thread_safe_print(f"✓ Processed mapping with {len(results)} field mappings")
            except Exception as e:
                thread_safe_print(f"Error in parallel mapping processing: {e}")

    thread_safe_print(f"Completed processing all mappings. Total results: {len(all_results)}")
    return pd.DataFrame(all_results)


def process_integration(integration_id: int, region: str) -> pd.DataFrame:
    """Process a single integration."""
    thread_safe_print(f"🚀 Starting processing for integration {integration_id} in region {region}")
    start_time = time.time()

    try:
        df = generate_mapping_comparison(integration_id, region)

        # Save to CSV
        output_dir = f"data_mssql/{region}/bigquery"
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(output_dir, f"mapping_comparison_{region}_{integration_id}.csv")
        df.to_csv(output_file, index=False, sep=",")

        elapsed_time = time.time() - start_time
        thread_safe_print(f"✅ Completed integration {integration_id} in {elapsed_time:.2f}s. Saved to {output_file}")

        return df

    except Exception as e:
        elapsed_time = time.time() - start_time
        thread_safe_print(f"❌ Error processing integration {integration_id} after {elapsed_time:.2f}s: {e}")
        return pd.DataFrame()


def main():
    """Main function with parallel processing of integrations."""
    integration_ids_dict = {
        "us": [1204,1206,5587,17842,35245,26440,26442,26445,26451,26452,26453,26457,26458,26459,26460,26462,26464,28506,29382,33783,33880,34656,38930,45033,45690,51170,51858,47092,48021,51789,34406,36544,36555,40910,49240,49573,49591,49966,49972,49992,50004,50021,30618,42601,42602,43602,46445,48453,48454,49037,49152,52120,52983,52986,53137,29156,43566,43567,44707,26286,44503,44813,44814,47016,48601,48603,49505,50353,50863,51634,49108,30020,37089,41934,42549,43318,46808,47951,47952,48531,48534,48582,48600,48622,48654,48698,48700,48703,48934,49068,49109,49574,50368,50728,39941,44605,50584],
        "asia": [9884, 9137],
        "in":[19686],
        "eu":[14821,17830,10724,26420,26814,16201,18403,19807,20813,25467,18457,18458,18622,18623,18770,18876,19314,19932,21067,21088,21195,21263,21264,21265,21266,21267,23622,25905,26479,26521,26575,26602,26995,27309,27765,28235,29857,29860,29905,30012,30935,31145,22583,22585,22586,22587,22588,22589,22591,22593,22594,22595,22596,22597,22598,22599,22600,22601,22602,22603,22661,22662,22663,22664,22665,22666,22667,22668,22669,22670,22671,22672,22673,22674,22675,22676,22677,22678,24010,24014,24982,24987,24988,24989,25308,25312,28962,28974,30624,30654,30656,30658,30659,31085,31086,31087,31088,31089,31090,25800],
    }

    # Process each region
    for region, integration_ids in integration_ids_dict.items():
        thread_safe_print(f"🌍 Processing region: {region} with {len(integration_ids)} integrations")

        # Process integrations in parallel (but limit concurrent integrations to avoid overwhelming the API)
        max_concurrent_integrations = 3  # Adjust based on API rate limits

        with ThreadPoolExecutor(max_workers=max_concurrent_integrations) as executor:
            # Submit all integration processing tasks
            future_to_integration = {
                executor.submit(process_integration, integration_id, region): integration_id
                for integration_id in integration_ids
            }

            # Collect results as they complete
            completed_count = 0
            for future in as_completed(future_to_integration):
                integration_id = future_to_integration[future]
                try:
                    df = future.result()
                    completed_count += 1
                    thread_safe_print(
                        f"📊 Progress: {completed_count}/{len(integration_ids)} integrations completed in region {region}")
                except Exception as e:
                    thread_safe_print(f"💥 Failed to process integration {integration_id}: {e}")

        thread_safe_print(f"🎉 Completed all integrations for region: {region}")

    thread_safe_print("🏁 All processing completed!")


if __name__ == "__main__":
    main()