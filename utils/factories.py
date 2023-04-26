from data.extractors import (
    GoogleAdsExtractor, 
    GoogleAnalyticsExtractor,
    DWHExtractor,
    MetaAdsExtractor,
    WebtoolsExtractor
    )
from repositories.connectors import *
from repositories.loaders import S3Loader, DwhLoader
from repositories.query_builders import SelectQueryBuilder, InsertQueryBuilder, DeleteQueryBuilder, RequestBodyBuilder
import os


class ExtractorFactory:
    @staticmethod
    def create_extractor(source, **kwargs):
        connector = ConnectorFactory.create_connector(**kwargs)
        source = str(source).lower()
        if source == 'googleads':

            return GoogleAdsExtractor(connector, query_builder)

        elif source == 'googleanalytics':
            property_id = kwargs.get('property_id')
            dimensions = kwargs.get('dimensions')
            metrics = kwargs.get('metrics')
            start_date = kwargs.get('start_date')
            end_date = kwargs.get('end_date')
            query_builder = RequestBodyBuilder(property_id, dimensions, metrics, start_date, end_date)

            return GoogleAnalyticsExtractor(connector, query_builder)

        elif source == 'metaads':

            return MetaAdsExtractor(connector, query_builder)

        elif source == 'webtools':
            table_name = kwargs.get('table_name')
            query_builder = SqlQueryBuilderFactory.create_sql_query_builder('select', table_name)
            return WebtoolsExtractor(connector, query_builder)

        elif source == 'dwh':
            table_name = kwargs.get('table_name')
            columns = kwargs.get('columns')
            query_builder = SqlQueryBuilderFactory.create_sql_query_builder('select', table_name, columns)

            return DWHExtractor(connector, query_builder)

        else:
            raise ValueError('Invalid source')


class ConnectorFactory:
    @staticmethod
    def create_connector(**kwargs):
        connector_type = kwargs.get('connector_type')
        if connector_type == 'database':
            database_name = kwargs.get('database_name')
            host = kwargs.get('host')
            password = kwargs.get('password')
            username = kwargs.get('username')
            driver = kwargs.get('driver', '{ODBC Driver 17 for SQL Server}')
            return DatabaseConnector(database_name, host, username, password, driver)
        elif str(connector_type).lower() == 'api':
            api_provider = kwargs.get('api_provider')
            return APIConnector(api_provider)
        else:
            raise ValueError(f"Invalid connector type: {connector_type}")

class SqlQueryBuilderFactory:
    @staticmethod
    def create_sql_query_builder(query_type, table_name, columns=None):
        if str(query_type).lower() == "insert":
            return InsertQueryBuilder(table_name, columns)
        elif str(query_type).lower() == 'select':
            return SelectQueryBuilder(table_name, columns)
        elif str(query_type).lower() == 'delete':
            return DeleteQueryBuilder(table_name, columns)
        else:
            raise ValueError(f"Invalid query type: {query_type}")


class LoaderFactory:
    @staticmethod
    def create_loader(target, **kwargs):
        connector = ConnectorFactory.create_connector(**kwargs)
        if str(target).lower() == 's3':

            return S3Loader(connector)

        elif str(target).lower() == 'dwh':
            table_name = kwargs.get('table_name')
            columns = kwargs.get('columns')
            query_builder = SqlQueryBuilderFactory.create_sql_query_builder('insert', table_name, columns)

            return DwhLoader(connector, query_builder)

        else:
            raise ValueError(f"Invalid target: {target}")
