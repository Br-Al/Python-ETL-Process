from datetime import timedelta
from dateutil import rrule
import json

import pandas as pd
import requests
import urllib

from utils.inapp_utils import (
    INDEX_PATTERN, BRAND, BODY, URL, HEADERS,
    GET_QUERY_MAPPING, WHERE_QUERY_MAPPING, INSERT_QUERY_MAPPING)


class Elastic(object):
    def __init__(self, db, event, start_date, end_date):
        self.db = db
        self.event = event
        self.start_date = start_date
        self.end_date = end_date
        self.body = None
        self.run_date = ""

    def set_query(self, date):
        start_date = date.strftime("%Y-%m-%dT%H")
        end_date = (date + timedelta(hours=1)).strftime("%Y-%m-%dT%H")
        month = date.strftime("%Y-%m")

        sql_where = f" package.received >= '{start_date}' and package.received < '{end_date}'"
        sql_from = f" \"{INDEX_PATTERN}{BRAND}-{month}\""

        sql_query = GET_QUERY_MAPPING[self.event]
        where_query = WHERE_QUERY_MAPPING[self.event]
        query = f"SELECT {sql_query} FROM {sql_from} WHERE {sql_where} {where_query} "

        self.body = BODY.copy()
        self.body['query'] = query

    @staticmethod
    def build_dataframe(result):
        df_data = []
        df_columns = []
        for col in result['columns']:
            df_columns.append(col['name'])
        for row in result['rows']:
            df_data.append(row)
        df = pd.DataFrame(df_data, columns=df_columns)
        for col in df.select_dtypes(include=[object]):
            try:
                df[col] = df[col].str.slice(0, 300)
                df[col].fillna("")
            except Exception as e:
                print(e)
        df = df.fillna(value=0)

        if 'prevLaunchTime' in df.columns:
            df['prevLaunchTime'] = df['prevLaunchTime'].astype('datetime64[ns]')
        return df

    def insert_data(self, df):
        insert_query = INSERT_QUERY_MAPPING[self.event]
        self.db.insert_many(df, insert_query)

    def run_import(self):
        for import_hour in rrule.rrule(rrule.HOURLY, dtstart=self.start_date, until=self.end_date):
            self.set_query(import_hour)
            r = requests.post(URL, headers=HEADERS, data=json.dumps(self.body))
            status_code = r.status_code
            if status_code == 200:
                result = r.json()
                date_str = import_hour.strftime('%Y-%m-%d')
                if date_str != self.run_date:
                    self.run_date = date_str
                    print(f"Delete data for date : {self.run_date}")
                    delete_query = f"DELETE FROM [INAPP].[{self.event}] WHERE CAST(packageDate as DATE) = '{self.run_date}'"
                    self.db.data_push(delete_query)
                df = self.build_dataframe(result)
                print(f"Number of rows : {df.shape[0]}")
                self.insert_data(df)
            else:
                raise urllib.error.HTTPError(r.url, status_code, f'Request to {r.url} was unsuccessful.', {}, None)
