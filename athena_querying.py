"""
Convenience for connecting to athena.

Note that the default way of connecting for pyathena is crazy slow
and you need to set it to use as_pandas, or to query the S3 bucket 
directly.

Note also that there is some built-in capacity to find recent queries
that are the same as what you're running using cache_size=n where n is
the number of past queries to match against (so ideally timebox queries
well when using it and it might use other people's queries.)
"""
import sys
from pyathena import connect
from pyathena.util import as_pandas
#try:
#    from pyathena import connect
#except:
#    print("Failed to import pyathena, trying to install it")
#    !pip install pyathena
    
    #!{sys.executable} -m pip install PyAthena  # this step doesn't work without rewrite unless you do in jupyter
    # alternative for conda: conda install -c conda-forge pyathena
#    from pyathena import connect
#    print("successfully installed")

import boto3
import boto3
import base64
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime

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



class AthenaQuery(object):
    """
    Create an instance, then use connect() to start everything.
    
    
    """
    def __init__(self):
        pass
    
    def connect(self):
        self.athena_conn, self.athena_cursor = self.connect_to_athena()
        self.boto_s3_client = self.connect_to_s3()

    def connect_to_athena(self):
        athena_conn = connect(s3_staging_dir=athena_bucket_path, region_name=aws_region_name)
        athena_cursor = athena_conn.cursor()
        return athena_conn, athena_cursor


    def connect_to_s3(self):
        session = boto3.session.Session()
        s3_client = boto3.client('s3')
        return s3_client
    
    def query(self, query, print_debug_messages = False):
        """
        Assume that this doesn't respect data types / returns everything as object
        
        Returns a pandas dataframe
        """
        return self.query_athena_via_s3_to_dataframe(query, self.athena_cursor, self.boto_s3_client)



    def query_athena_via_s3_to_dataframe(self, query, athena_cursor, boto_s3_client, print_debug_messages = False):
        """
        This is a lot faster than using the cursors etc of classic python db querying.
        
        Note that this doesn't know the data types TODO: possibly add ability to pass dictionary to the function

        Example for the boto_s3_client:
        session = boto3.session.Session()
        boto_s3_client = boto3.client('s3')

        """
        if print_debug_messages:
            print("Running query")
        ap_start_time = datetime.now()
        
        athena_cursor.execute(query)
        ap_query_done_time = datetime.now()
        query_duration_seconds = (ap_query_done_time - ap_start_time).total_seconds()
        if print_debug_messages:
            print("Query finished after %.2f" % query_duration_seconds)
            print("Now going to get the results from S3")

        s3_start_time = datetime.now()
        bucket, key = athena_cursor.output_location.strip('s3://').split('/', 1)
        response = boto_s3_client.get_object(Bucket=bucket,Key=key)
        file = response["Body"]

        df = pd.read_csv(file)
        s3_end_time = datetime.now()
        s3_read_seconds = (s3_end_time - s3_start_time).total_seconds()
        if print_debug_messages:
            print("Reading the CSV from S3 (not running query) took %.2f"%s3_read_seconds)

        ap_done_time = datetime.now()
        ap_total_seconds = (ap_done_time - ap_start_time).total_seconds()
        if print_debug_messages:
            print("All finished using as_pandas in %.2f"%ap_total_seconds)
        return df