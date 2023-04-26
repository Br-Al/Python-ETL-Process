import os, sys
from datetime import timedelta, date, datetime
from utils.load_env_var import load_env_var
from models.etl import Etl
from repositories.loaders import DwhLoader
from repositories.connectors import APIConnector, DatabaseConnector
from repositories.query_builders import RequestBodyBuilder, SelectMaxQueryBuilder, InsertQueryBuilder
from models.transformers import SimpleTransformer
from data.extractors import GoogleAnalyticsExtractor


def main():
    try:
        # Load environment variables
        load_env_var()
        database_name = os.environ.get("GA_DBNAME")
        host = os.environ.get("GA_DBHOST")
        username = os.environ.get("GA_DBUSER")
        password = os.environ.get("GA_DBPASS")


        # Connect to the DWH and get the last loaded date
        target_connector = DatabaseConnector(database_name, host, username, password, "{ODBC Driver 17 for SQL Server}")
        target_connector.connect("pyodbc")
        ga_property = None # TODO: Add GAProperty object
        account = None # TODO: Add Account object
        # Get the last loaded date
        query_builder = SelectMaxQueryBuilder(table_name=ga_property.dwh_table)
        query = query_builder.build_query('Date')
        max_date = target_connector.conn.execute(query).fetchone()[0]
        start_date = datetime.strftime(max_date, "%Y-%m-%d") if max_date else account.default_start_date
        if datetime.strptime(start_date, "%Y-%m-%d").date() <= (date.today()-timedelta(1)):

            # Create a connector for the source API
            source_connector = APIConnector(api_provider='googleanalytics')

            # Create a query builder for the source API
            query_builder = RequestBodyBuilder(property_id=ga_property.property_id, dimensions=ga_property.dimensions, metrics=ga_property.metrics, start_date=start_date, end_date='yesterday')

            # Create an extractor to extract data from the source
            extractor = GoogleAnalyticsExtractor(connector=source_connector, query_builder=query_builder)

            # Create a transformer to transform the data
            transformer = SimpleTransformer()

            # Create a query builder for the target DWH
            insert_query_builder = InsertQueryBuilder(table_name=ga_property.dwh_table, columns=ga_property.dwh_columns)

            # Create a loader to load the data into the target DWH
            loader = DwhLoader(connector=target_connector, query_builder=insert_query_builder)

            # Create an ETL object to run the ETL process
            etl = Etl(
                extractor=extractor,
                transformer=transformer,
                loader=loader
            )
            etl.run()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
