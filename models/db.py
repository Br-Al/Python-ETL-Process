import re
import pandas as pd
import pyodbc
import psycopg
import warnings
from datetime import date

class Database():

    def __init__(self, server, user_id, password, driver='{ODBC Driver 17 for SQL Server}', name = None):
        self.server = server
        self.driver = driver
        self.name = name
        self.user_id = user_id
        self.password = password

    def pyodbc_connect(self):
        try:
            self.set_connection_string()
            self.conn = pyodbc.connect(self.connection_string, autocommit=True)
        except ConnectionError as error:
            print(f"Unable to connect to the server: \n {error}")

    def set_connection_string(self, use_psycopg=False):
        if use_psycopg:
            self.connection_string = f"host={self.server} user={self.user_id} password={self.password} sslmode=require dbname={self.name}"
        else:
            self.connection_string = f"DRIVER={self.driver};SERVER={self.server};UID={self.user_id};PWD={self.password};DATABASE={self.name}"

    def psycopg_connect(self):
        try:
            self.set_connection_string(use_psycopg=True)
            self.conn = psycopg.connect(self.connection_string)
        except ConnectionError as error:
            print(f"Unable to connect to the server: \n {error}")

    def get_table_df(self, table):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            query = f"SELECT * FROM {table}"
            data = pd.read_sql(query, self.conn)

            return data

    def get_query_df(self, query):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            data = pd.read_sql(query, self.conn)

            return data

    def data_push(self, query):
        self.cursor.execute(query)

    def insert_many(self, data, query):
        for row_count in range(0, data.shape[0]):
            base = data.iloc[row_count:row_count + 1, :]
            chunk = base.values.tolist()
            tuple_of_tuples = tuple(tuple(x) for x in chunk)
            self.conn.cursor().executemany(query, tuple_of_tuples)

    def get_one(self, query, date_convert=""):
        data = self.get_query_df(query)
        value = data.iat[0, 0]
        if date_convert:
            value = value.strftime(date_convert)
            
        return value

    def clear_table(self, table_name):
        query = f"DELETE FROM {table_name}"
        self.conn.execute(query)

    def get_max_date(self, table, date_column_name):
        query = (f"SELECT MAX({date_column_name})"
                f"FROM {table}")
        try:
            response = self.conn.execute(query).fetchone()
                
            return response[0] if response[0] else None

        except Exception as e:
            print(f"Error: {e}")