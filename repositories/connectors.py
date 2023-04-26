import abc
import pyodbc
import psycopg2
import boto3
from google.analytics.data_v1beta import BetaAnalyticsDataClient
import requests
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from elasticsearch_dsl import connections



class Connector(abc.ABC):
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def disconnect(self):
        pass


class DatabaseConnector(Connector):
    def __init__(self, database_name, host, username, password, driver=None ):
        self.database_name = database_name
        self.host = host
        self.username = username
        self.password = password
        self.driver = driver
        self.conn = None
        self.connection_string = None

    def connect(self, backend):
        if backend == 'psycopg':
            self.psycopg_connect()
        elif backend == 'pyodbc':
            self.pyodbc_connect()
        else:
            raise ValueError(f"Invalid backend type: {backend}")

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def pyodbc_connect(self):
        try:
            self.set_connection_string()
            self.conn = pyodbc.connect(self.connection_string, autocommit=True)
        except ConnectionError as error:
            print(f"Unable to connect to the server: \n {error}")

    def set_connection_string(self, use_psycopg=False):
        if use_psycopg:
            self.connection_string = f"host={self.host} user={self.username} password={self.password} sslmode=require dbname={self.database_name}"
        else:
            self.connection_string = f"DRIVER={self.driver};SERVER={self.host};UID={self.username};PWD={self.password};DATABASE={self.database_name}"
    def psycopg_connect(self):
        try:
            self.set_connection_string(use_psycopg=True)
            self.conn = psycopg2.connect(self.connection_string)
        except ConnectionError as error:
            print(f"Unable to connect to the server: \n {error}")


class APIConnector(Connector):
    def __init__(self, api_provider) -> None:
        self.api_provider = str(api_provider).lower()

    def connect(self, **kwargs):
        if self.api_provider == 's3':
            access_key = kwargs.get('aws_access_key_id')
            secret_key = kwargs.get('aws_secret_access_key')
            self.s3_connect(access_key, secret_key)
        elif self.api_provider == 'googleads':
            self.googleads_connect()
        elif self.api_provider == 'googleanalytics':
            self.ga4_connect()
        else: 
            raise ValueError(f"Invalid backend type: {self.api_provider}")

    def disconnect(self):
        print(f"Disconnecting from {self.api_type} API")


    def googleads_connect(self):
        pass

    def ga4_connect(self):
        self.conn = BetaAnalyticsDataClient()


class OutbrainConnector(Connector):
    def __init__(self, username, password, account_name, url = 'https://api.outbrain.com/amplify/v0.1') -> None:
        self.username = username
        self.password = password
        self.account_name = account_name
        self.url = url
        self.token = None
        self.marketer_id = None

    def connect(self):
        response = requests.get(f'{self.url}/login', auth = requests.auth.HTTPBasicAuth(self.username, self.password))
        self.token = response.json()['OB-TOKEN-V1']
        self.set_marketer_id()
        

    def set_marketer_id(self):
        response = requests.get(self.url + '/marketers', headers={'OB-TOKEN-V1': self.token})
        result = pd.json_normalize(response.json()['marketers'], meta=['id'])
        self.marketer_id = result[result['name'] == self.account_name]['id'].iloc[0]



class MetaApiConnector(Connector):
    def __init__(self, app_id, app_secret, access_token, account_id) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.account_id = account_id
        self.conn = None

    def connect(self):
        FacebookAdsApi.init(self.app_id, self.app_secret, self.access_token)
        self.conn = AdAccount(self.account_id)


class S3Connector(Connector):
    def __init__(self, access_key_id, secret_access_key):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.conn = None

    def connect(self):
        # Create an S3 client
        self.conn = boto3.resource('s3', aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)

    def disconnect(self):
        pass


class ElasticSearchConnector(Connector):
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.conn = None

    def connect(self):
        self.conn = connections.create_connection(hosts=[self.host], http_auth=(self.username, self.password))

    def disconnect(self):
        pass
    