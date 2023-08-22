# Arrow Parquet

This is a set of Python scripts to convert Parquet files (on HDFS and S3) to CSV (on local file system) and vice-versa.

All the S3 related scripts have been tested.

The HDFS related scripts **have not** been tested yet.

# Requiremens

Install dependencies (tecnically, only `pyarrow` is necessary, but the other libraries might come in handy in the future):

`pip install pyarrow boto boto3 pandas parquet-tools dataclasses`

Set your AWS environment variables:
```
export AWS_ACCESS_KEY_ID="<Your Key ID>"
export AWS_SECRET_ACCESS_KEY="<Your Secret Access Key>"
export AWS_SESSION_TOKEN="<Your token>"
```

# Running

All scripts are properly documented. Just run

`python script_name.py -h` 

To get a list of parameters.
