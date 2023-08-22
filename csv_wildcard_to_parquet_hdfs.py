import os
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq
import argparse

def csv_to_parquet(csv_path, csv_file_pattern, hdfs_path, delimiter, enclosure):
    # List all CSV files in the local directory
    csv_files = [f for f in os.listdir(csv_path) if fnmatch.fnmatch(f, csv_file_pattern)]

    for csv_file_name in csv_files:
        # Read the CSV file from local disk into a pandas DataFrame
        data_frame = pa.csv.read_csv(f"{csv_path}/{csv_file_name}").to_pandas()

        # Convert the pandas DataFrame to an Arrow table
        table = pa.Table.from_pandas(data_frame)

        # Get the base file name without extension
        base_file_name = os.path.splitext(csv_file_name)[0]

        # Write the Arrow table as a Parquet file to HDFS
        with pa.hdfs.connect() as fs:
            parquet_file_path = f"{hdfs_path}/{base_file_name}.parquet"
            pq.write_table(table, parquet_file_path)

            print(f"Parquet file '{base_file_name}.parquet' written to HDFS successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV files from local disk to Parquet files on HDFS")
    parser.add_argument("--csv_path", required=True, help="Local path of the CSV files")
    parser.add_argument("--csv_file_pattern", required=True, help="Pattern to match CSV file names")
    parser.add_argument("--hdfs_path", required=True, help="Hadoop HDFS directory to save the Parquet files")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")
    parser.add_argument("--enclosure", default='"', help="CSV enclosure character")
    args = parser.parse_args()

    csv_to_parquet(args.csv_path, args.csv_file_pattern, args.hdfs_path, args.delimiter, args.enclosure)
