import os
import pandas as pd
import xlsxwriter

def combine_per_destination(destination, regions_root="data_mssql"):
    output_excel_path = f'combined_{destination}.xlsx'

    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        for region in os.listdir(regions_root):
            region_path = os.path.join(regions_root, region, destination)
            if not os.path.isdir(region_path):
                continue

            # Gather all CSVs under that region/destination
            csv_files = [f for f in os.listdir(region_path) if f.endswith(".csv")]
            if not csv_files:
                print(f"No CSV files in {region_path}")
                continue

            all_dfs = []
            for csv_file in csv_files:
                csv_path = os.path.join(region_path, csv_file)
                try:
                    df = pd.read_csv(csv_path)
                    all_dfs.append(df)
                except Exception as e:
                    print(f"Failed reading {csv_path}: {e}")

            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                sheet_name = region[:31]  # Excel sheet name limit
                combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"✅ Written {destination} - {region} to sheet.")
            else:
                print(f"No valid dataframes in {region_path}")

    print(f"📄 Created: {output_excel_path}")

def main():
    destinations = ["redshift", "snowflake", "bigquery"]
    for destination in destinations:
        combine_per_destination(destination)

if __name__ == "__main__":
    main()
