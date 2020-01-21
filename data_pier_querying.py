import psycopg2
import creds
import pandas as pd

_connection = None

class DataPierQuerying():
    
    def connect(self):
        self.conn, self.cursor = self.connect_to_data_pier()
        
    def close(self):
        self.cursor.close()
        self.conn.close()
        
        

    def connect_to_data_pier(self):
        #db_pass = input()
        db_host = "data-pier-production.cl8qfdl47mtr.ap-southeast-1.rds.amazonaws.com"
        db_user = creds.pg_user
        db_pass = creds.pg_pass
        db_database = "data_pier"
        pg_conn = psycopg2.connect(host=db_host, user = db_user, dbname=db_database, password= db_pass)
        pg_conn.set_session(readonly=True, autocommit=True) #without this it can lock tables.


        return pg_conn, pg_conn.cursor()

    def query_to_dataframe(self, query):
        return pd.read_sql(query, self.conn)