import argparse
import pyarrow as pa
import pyarrow.parquet as pq

def parquet_to_csv(hdfs_path, parquet_file_name, csv_path, csv_file_name, delimiter, enclosure):
    # Construct the full Hadoop HDFS path to the Parquet file
    full_hdfs_path = f"{hdfs_path}/{parquet_file_name}"

    # Read the Parquet file from Hadoop HDFS
    parquet_table = pq.read_table(full_hdfs_path)

    # Convert the Parquet table to a pandas DataFrame
    data_frame = parquet_table.to_pandas()

    # Construct the full local path for the CSV file
    full_csv_path = f"{csv_path}/{csv_file_name}"

    # Save the DataFrame to a CSV file on the local disk
    data_frame.to_csv(full_csv_path, index=False, sep=delimiter, quotechar=enclosure)

    print("CSV file saved successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Parquet file from Hadoop HDFS to CSV file on local disk")
    parser.add_argument("--hdfs_path", required=True, help="Hadoop HDFS path to the Parquet file")
    parser.add_argument("--parquet_file_name", required=True, help="Name of the Parquet file")
    parser.add_argument("--csv_path", required=True, help="Local path to save the CSV file")
    parser.add_argument("--csv_file_name", required=True, help="Name of the CSV file")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")
    parser.add_argument("--enclosure", default='"', help="CSV enclosure character")
    args = parser.parse_args()

    parquet_to_csv(args.hdfs_path, args.parquet_file_name, args.csv_path, args.csv_file_name, args.delimiter, args.enclosure)
