import csv
import pyarrow as pa
import pyarrow.parquet as pq
import argparse

def parquet_to_csv(s3_path, parquet_file_name, local_csv_path, csv_file_name, delimiter, enclosure):
    # Construct the full S3 path to the Parquet file
    full_s3_path = f"{s3_path}/{parquet_file_name}"

    # Read the Parquet file from S3
    parquet_table = pq.read_table(full_s3_path)

    # Convert the Parquet table to a pandas DataFrame
    data_frame = parquet_table.to_pandas()

    # Construct the full local path for the CSV file
    full_csv_path = f"{local_csv_path}/{csv_file_name}"

    # Save the DataFrame to a CSV file on the local disk
    print(f"delimiter={delimiter} and enclosure={enclosure}")
    quoting = csv.QUOTE_NONNUMERIC if len(enclosure)>0 else csv.QUOTE_NONE
    data_frame.to_csv(full_csv_path, index=False, sep=delimiter, quotechar=enclosure, quoting=quoting, escapechar="\\")

    print("CSV file saved successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert Parquet file from S3 to CSV file on local disk")
    parser.add_argument("--s3_path", required=True, help="S3 path to the Parquet file")
    parser.add_argument("--parquet_file_name", required=True, help="Name of the Parquet file")
    parser.add_argument("--local_csv_path", required=True, help="Local path to save the CSV file")
    parser.add_argument("--csv_file_name", required=True, help="Name of the CSV file")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")
    parser.add_argument("--enclosure", default='"', help="CSV enclosure character. If empty, will disable quoting")
    args = parser.parse_args()

    parquet_to_csv(args.s3_path, args.parquet_file_name, args.local_csv_path, args.csv_file_name, args.delimiter, args.enclosure)