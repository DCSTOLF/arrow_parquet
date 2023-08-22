import argparse
import os
import fnmatch
import pyarrow as pa
import pyarrow.parquet as pq

def parquet_to_csv(hdfs_path, parquet_file_pattern, csv_path, delimiter, enclosure):
    # List all Parquet files in the Hadoop HDFS directory
    parquet_files = []
    with pa.hdfs.connect() as fs:
        parquet_files = fs.ls(hdfs_path)

    # Filter the Parquet files based on the pattern
    matching_parquet_files = [file for file in parquet_files if fnmatch.fnmatch(file, parquet_file_pattern)]

    for parquet_file in matching_parquet_files:
        # Read the Parquet file from Hadoop HDFS
        parquet_table = pq.read_table(parquet_file)

        # Get the base file name without extension
        base_file_name = os.path.basename(parquet_file)
        base_file_name = os.path.splitext(base_file_name)[0]

        # Construct the full local path for the CSV file
        csv_file_name = f"{base_file_name}.csv"
        full_csv_path = os.path.join(csv_path, csv_file_name)

        # Convert the Parquet table to a pandas DataFrame
        data_frame = parquet_table.to_pandas()

        # Save the DataFrame to a CSV file on the local disk
        data_frame.to_csv(full_csv_path, index=False, sep=delimiter, quotechar=enclosure)

        print(f"CSV file '{csv_file_name}' saved successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Parquet files from Hadoop HDFS to CSV files on local disk")
    parser.add_argument("--hdfs_path", required=True, help="Hadoop HDFS directory containing Parquet files")
    parser.add_argument("--parquet_file_pattern", required=True, help="Pattern to match Parquet file names")
    parser.add_argument("--csv_path", required=True, help="Local path to save the CSV files")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")
    parser.add_argument("--enclosure", default='"', help="CSV enclosure character")
    args = parser.parse_args()

    parquet_to_csv(args.hdfs_path, args.parquet_file_pattern, args.csv_path, args.delimiter, args.enclosure)
