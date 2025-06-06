# Schema Mapping Tool

This tool helps you compare source and destination schema mappings for Hevo pipelines. It generates a CSV file with the mapping comparison between source and destination tables.

## Setup

1. Create a virtual environment (optional but recommended):
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Run the schema mapper script:
```bash
python schema_mapper.py
```

The script will:
1. Fetch destination mappings for the specified integration ID
2. Get destination details including project ID and dataset
3. Query the destination schema
4. Parse the source schema from CREATE TABLE queries
5. Generate a mapping comparison
6. Save the results to a CSV file

## Output

The script generates a CSV file with the following columns:
- integration_id: The Hevo pipeline ID
- destination_id: The destination ID
- source_table_name: The source table name
- destination_table_name: The destination table name
- source_field: The source column name
- destination_field: The destination column name
- source_type: The source data type
- destination_type: The destination data type

## Configuration

You can modify the following constants in `schema_mapper.py`:
- `BASE_URL`: The base URL for the Hevo API
- `AUTH_HEADER`: The authentication header for API requests
- `integration_id`: The pipeline ID to analyze
- `region`: The region for API requests

## Notes

- The script currently supports MySQL source schemas
- The destination schema is queried from BigQuery
- The script includes retry logic for API requests
- The output is saved in pipe-separated CSV format 