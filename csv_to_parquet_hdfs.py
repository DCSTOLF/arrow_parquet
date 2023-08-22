import pyarrow as pa
import pyarrow.parquet as pq
import argparse

def csv_to_parquet(csv_path, csv_file_name, hdfs_path, parquet_file_name, delimiter, enclosure):
    # Read the CSV file from local disk into a pandas DataFrame
    data_frame = pa.csv.read_csv(f"{csv_path}/{csv_file_name}").to_pandas()

    # Convert the pandas DataFrame to an Arrow table
    table = pa.Table.from_pandas(data_frame)

    # Write the Arrow table as a Parquet file to HDFS
    with pa.hdfs.connect() as fs:
        parquet_file_path = f"{hdfs_path}/{parquet_file_name}"
        pq.write_table(table, parquet_file_path)

        print(f"Parquet file '{parquet_file_name}' written to HDFS successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV file from local disk to Parquet file on HDFS")
    parser.add_argument("--csv_path", required=True, help="Local path of the CSV file")
    parser.add_argument("--csv_file_name", required=True, help="Name of the CSV file")
    parser.add_argument("--hdfs_path", required=True, help="Hadoop HDFS directory to save the Parquet file")
    parser.add_argument("--parquet_file_name", required=True, help="Name of the Parquet file to be created on HDFS")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")
    parser.add_argument("--enclosure", default='"', help="CSV enclosure character")
    args = parser.parse_args()

    csv_to_parquet(args.csv_path, args.csv_file_name, args.hdfs_path, args.parquet_file_name, args.delimiter, args.enclosure)
