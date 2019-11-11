"""
Convenience for connecting to athena
"""
import sys
#try:
from pyathena import connect
#except:
#    print("Failed to import pyathena, trying to install it")
#    !{sys.executable} -m pip install PyAthena  # this step doesn't work without rewrite unless you do in jupyter
#    from pyathena import connect
#    print("successfully installed")

import boto3
import boto3
import base64
from botocore.exceptions import ClientError


# Settings

event_log_s3_path = "s3://ms-data-pipeline-production/ms-data-stream-production-processed"
event_log_s3_bucket = event_log_s3_path.split("s3://")[1].split("/")[0]
event_log_s3_prefix = event_log_s3_path.split("/")[-1]
athena_bucket_path = "s3://aws-athena-query-results-358002497134-ap-southeast-1/"
#athena_database = "ms_data_processed_production"
athena_database = "ms_data_lake_production"
athena_raw_events_table = "ms_data_stream_production_processed"
#athena_raw_events_table = "ms_data_stream_production_processed_cd5f4696237059a21d780afa83822e6b"
athena_year_partition = "partition_0" #2019
athena_month_partition = "partition_1" #02
athena_day_partition = "partition_2" #0
athena_easy_events_table =  "id_ab_test"
aws_region_name = "ap-southeast-1"




def connect_to_athena():
    athena_conn = connect(s3_staging_dir=athena_bucket_path, region_name=aws_region_name)
    return athena_conn