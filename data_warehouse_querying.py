import boto3
import base64
from botocore.exceptions import ClientError
import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

database_name = "data_warehouse"

class DataWarehouseQuery(object):
    """
    Create an instance, then use connect() to start everything.
    
    
    It uses AWS Secret manager to get the credentials
    
    """
    def __init__(self, print_query_times = True):
        self.print_query_times = print_query_times
    
    def connect(self):
        
        # get all the details from Amazon Secret

        secret_name = "production/data-warehouse-production-rs/data_team_notebook"
        region_name = "ap-southeast-1"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        secret_values = client.get_secret_value(SecretId=secret_name)
        connection_details_string = secret_values["SecretString"]
        cd = json.loads(connection_details_string)
        
        #e.g. 'postgresql://scott:tiger@localhost/mydatabase'
        connection_string = "postgresql://"+cd["username"]+":"+cd["password"]\
            + "@"+cd["host"]+":"+str(cd["port"])+ "/"+database_name
        
        #print(connection_string)
        
        self.engine = create_engine(connection_string)
        
        
        
        """
        conn = psycopg2.connect(dbname = database_name,
                         host=cd["host"],
                         port=cd["port"],
                         user= cd["username"],
                         password=cd["password"],
                        
                        )
                        """
        
        #cd["dbClusterIdentifier"]
    
    def query(self, query, print_debug_messages = False):
        """        
        Returns a pandas dataframe
        """
        
        start_time = datetime.now()
        if self.print_query_times:
            print("Starting query at "+start_time.isoformat())
        df =  pd.read_sql(query, self.engine)
        end_time = datetime.now()
        dt_secs = (end_time-start_time).total_seconds()
        
        if self.print_query_times:
            print("Query took %.2f" % dt_secs)
        return df

    #TODO: do I need to explicitly close the redshift connection??