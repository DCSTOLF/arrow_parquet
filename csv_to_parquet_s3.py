import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
import argparse

def csv_to_parquet(local_csv_path, csv_file_name, s3_path, parquet_file_name, delimiter, enclosure):
    # Read the CSV file from local disk into a pandas DataFrame

    csv_parse_options = pa_csv.ParseOptions( delimiter=delimiter, quote_char=enclosure, escape_char="\\")  if len(enclosure)>0 else pa_csv.ParseOptions( delimiter=delimiter, escape_char="\\")

    data_frame = pa_csv.read_csv(f"{local_csv_path}/{csv_file_name}",  parse_options=csv_parse_options).to_pandas()

    # Convert the pandas DataFrame to an Arrow table
    table = pa.Table.from_pandas(data_frame)

    # Write the Arrow table as a Parquet file to S3
    full_s3_path = f"{s3_path}/{parquet_file_name}"
    pq.write_table(table, full_s3_path)

    print(f"Parquet file '{csv_file_name.replace('.csv', '.parquet')}' written to S3 successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert CSV file from local disk to Parquet file on S3")
    parser.add_argument("--local_csv_path", required=True, help="Local path of the CSV file")
    parser.add_argument("--csv_file_name", required=True, help="Name of the CSV file")
    parser.add_argument("--s3_path", required=True, help="S3 path to the Parquet file")
    parser.add_argument("--parquet_file_name", required=True, help="Name of the Parquet file")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")
    parser.add_argument("--enclosure", default='"', help="CSV enclosure character")
    args = parser.parse_args()

    csv_to_parquet(args.local_csv_path, args.csv_file_name, args.s3_path, args.parquet_file_name, args.delimiter, args.enclosure)