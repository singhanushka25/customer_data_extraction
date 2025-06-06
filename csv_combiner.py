import os
import pandas as pd

def combine_to_excel(destination):
    base_dir = f'combined_csvs/{destination}'
    output_excel_path = f'combined_{destination}.xlsx'

    # Get all region folders under the destination
    if not os.path.exists(base_dir):
        print(f"No directory found for destination: {destination}")
        return

    region_folders = [name for name in os.listdir(base_dir)
                      if os.path.isdir(os.path.join(base_dir, name))]

    if not region_folders:
        print(f"No region folders found under {base_dir}")
        return

    with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
        for region in region_folders:
            csv_path = os.path.join(base_dir, region, 'combined.csv')
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                    sheet_name = f"{region}"[:31]  # Excel sheet name limit is 31 characters
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                except Exception as e:
                    print(f"Failed to write sheet for {region}: {e}")
            else:
                print(f"No combined.csv found in {base_dir}/{region}")

    print(f"Combined Excel file created: {output_excel_path}")

# Example usage:
def main():
    regions = {"us", "us2", "asia", "eu", "au","in"}
    destinations = ["redshift", "snowflake", "bigquery"]
    for region in regions:
        base_directories = []
        for destination in destinations:
            base_directories.append(f"data_mssql/{region}/{destination}")
            combine_to_excel(base_directories)

if __name__ == "__main__":
    main()
