import os
from models.db import Database
from utils.date_tools import get_max_date
import pandas as pd
from datetime import datetime

def database_connect():
    host = os.environ.get('GOOGLE_ADS_DBHOST', "")
    user_id = os.environ.get('GOOGLE_ADS_DBUSER', "")
    password = os.environ.get('GOOGLE_ADS_DBPASS', "")
    db_name = os.environ.get("GOOGLE_ADS_DBNAME", "")
    try:
        db = Database(server = host, user_id = user_id, password = password, name = db_name)
        db.pyodbc_connect()
        return db
    except ConnectionError as e:
        print(f'Error: {e}')
  

def get_start_date(account_id, table_name):
    db = database_connect()
    max_date_query = (f"SELECT MAX(Date)as new_date "
                        f"FROM [GoogleAds].[{table_name}] "
                        f"WHERE [CustomerId]={account_id}")
    try:
        max_processed_date_df = db.get_query_df(max_date_query)
        max_date = max_processed_date_df.iat[0, 0]
        start_date = get_max_date(max_date)
        return start_date
    except Exception as e:
        print(f"error: {e}")

