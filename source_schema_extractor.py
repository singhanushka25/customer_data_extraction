import re
from typing import Dict, List, Optional

def extract_mysql_schema(create_table_query: str) -> List[Dict]:
    """Extract schema information from MySQL CREATE TABLE query."""
    schema_info = []

    # Extract column definitions
    column_pattern = r'`(\w+)`\s+([^,]+)(?:,|\))'
    matches = re.finditer(column_pattern, create_table_query)

    for match in matches:
        column_name = match.group(1)
        column_def = match.group(2).strip()

        # Extract data type
        type_pattern = r'(\w+)(?:\([^)]+\))?'
        type_match = re.match(type_pattern, column_def)
        if type_match:
            data_type = type_match.group(1).upper()

            # Map MySQL types to standard types
            type_mapping = {
                'INT': 'int',
                'BIGINT': 'bigint',
                'VARCHAR': 'varchar',
                'TEXT': 'text',
                'LONGTEXT': 'text',
                'TIMESTAMP': 'timestamp',
                'DATETIME': 'datetime',
                'JSON': 'json',
                'FLOAT': 'float',
                'DOUBLE': 'double',
                'DECIMAL': 'decimal',
                'BOOLEAN': 'boolean',
                'TINYINT': 'tinyint',
                'SMALLINT': 'smallint',
                'MEDIUMINT': 'mediumint',
                'CHAR': 'char',
                'ENUM': 'enum',
                'SET': 'set',
                'BINARY': 'binary',
                'VARBINARY': 'varbinary',
                'BLOB': 'blob',
                'LONGBLOB': 'longblob',
                'MEDIUMBLOB': 'mediumblob',
                'TINYBLOB': 'tinyblob',
                'GEOMETRY': 'geometry',
                'POINT': 'point',
                'LINESTRING': 'linestring',
                'POLYGON': 'polygon',
                'MULTIPOINT': 'multipoint',
                'MULTILINESTRING': 'multilinestring',
                'MULTIPOLYGON': 'multipolygon',
                'GEOMETRYCOLLECTION': 'geometrycollection'
            }

            mapped_type = type_mapping.get(data_type, data_type.lower())

            schema_info.append({
                'column_name': column_name,
                'data_type': mapped_type,
                'is_nullable': 'NULL' in column_def.upper(),
                'ordinal_position': len(schema_info) + 1
            })

    return schema_info


def extract_sqlserver_schema(create_table_query: str) -> List[Dict]:
    """Extract schema information from SQL Server CREATE TABLE query."""
    schema_info = []

    # Extract the column definitions section
    # Look for content between CREATE TABLE and the constraints/end
    table_pattern = r'CREATE\s+TABLE\s+.*?\(\s*(.*?)\s*\)'
    table_match = re.search(table_pattern, create_table_query, re.DOTALL | re.IGNORECASE)

    if not table_match:
        return schema_info

    columns_section = table_match.group(1)

    # Split by lines and process each column definition
    lines = columns_section.split('\n')

    for line in lines:
        line = line.strip()
        if not line or line.startswith('CONSTRAINT') or line.startswith('PRIMARY KEY') or line.startswith(
                'FOREIGN KEY'):
            continue

        # Remove trailing comma
        line = line.rstrip(',')

        # Extract column name (in square brackets)
        column_match = re.match(r'\[(\w+)\]\s+(.+)', line)
        if column_match:
            column_name = column_match.group(1)
            column_def = column_match.group(2).strip()

            # Extract data type
            type_pattern = r'(\w+)(?:\([^)]+\))?'
            type_match = re.match(type_pattern, column_def)
            if type_match:
                data_type = type_match.group(1).lower()

                # Map SQL Server types to standard types
                type_mapping = {
                    'int': 'int',
                    'bigint': 'bigint',
                    'smallint': 'smallint',
                    'tinyint': 'tinyint',
                    'bit': 'boolean',
                    'decimal': 'decimal',
                    'numeric': 'decimal',
                    'float': 'float',
                    'real': 'float',
                    'money': 'decimal',
                    'smallmoney': 'decimal',
                    'varchar': 'varchar',
                    'nvarchar': 'nvarchar',
                    'char': 'char',
                    'nchar': 'nchar',
                    'text': 'text',
                    'ntext': 'text',
                    'datetime': 'datetime',
                    'datetime2': 'datetime',
                    'smalldatetime': 'datetime',
                    'date': 'date',
                    'time': 'time',
                    'datetimeoffset': 'datetimeoffset',
                    'timestamp': 'timestamp',
                    'uniqueidentifier': 'uniqueidentifier',
                    'xml': 'xml',
                    'geography': 'geography',
                    'geometry': 'geometry',
                    'hierarchyid': 'hierarchyid',
                    'sql_variant': 'sql_variant',
                    'binary': 'binary',
                    'varbinary': 'varbinary',
                    'image': 'image'
                }

                mapped_type = type_mapping.get(data_type, data_type)

                # Check if nullable (NOT NULL means not nullable)
                is_nullable = 'NOT NULL' not in column_def.upper()

                schema_info.append({
                    'column_name': column_name,
                    'data_type': mapped_type,
                    'is_nullable': is_nullable,
                    'ordinal_position': len(schema_info) + 1
                })

    return schema_info

def get_source_schema(create_table_query: str) -> List[Dict]:
    """Get source schema information from CREATE TABLE query."""
    return extract_sqlserver_schema(create_table_query)