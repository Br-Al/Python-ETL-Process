import pandas as pd
import pyodbc
import warnings
from common.config import get_config

DB_CREDS = get_config()['db_credentials']


class Database(object):
    def __init__(self):
        self.conn = pyodbc.connect(**DB_CREDS, autocommit=True)
        self.cursor = self.conn.cursor()

    def get_table_df(self, table):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, self.conn)
            return df

    def get_query_df(self, query):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            df = pd.read_sql(query, self.conn)
            return df

    def data_push(self, query):
        self.cursor.execute(query)

    def insert_many(self, df, query):
        for row_count in range(0, df.shape[0]):
            base = df.iloc[row_count:row_count + 1, :]
            chunk = base.values.tolist()
            tuple_of_tuples = tuple(tuple(x) for x in chunk)
            self.cursor.executemany(query, tuple_of_tuples)

    def get_one(self, query, date_convert=""):
        df = self.get_query_df(query)
        value = df.iat[0, 0]
        if date_convert:
            value = value.strftime(date_convert)
        return value
