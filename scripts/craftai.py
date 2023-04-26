import sys
import os
from datetime import datetime
from models.etl import Etl
from utils.load_env_var import load_env_var
from repositories.connectors import S3Connector, DatabaseConnector
from repositories.loaders import S3Loader
from repositories.query_builders import SelectQueryBuilder
from data.extractors import DWHExtractor
from models.transformers import SimpleTransformer


def main():
    try:
        # Load environment variables
        load_env_var()
        aws_access_key_id = os.environ.get("S3_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("S3_SECRET_ACCESS_KEY")
        database_name = os.environ.get("DWH_NAME")
        host = os.environ.get("DWH_HOST")
        username = os.environ.get("DWH_USERNAME")
        password = os.environ.get("DWH_PASSWORD")
        bucket = 'craftai-cs-avanquest-adoptim-prod'
        columns = [] # TODO: Add columns to load
        brand_name = '' # TODO: Add brand name
        brand_table_name = '' # TODO: Add brand table name
        key = f"landing_zone/{datetime.now().date()}/google-{brand_name}.csv"

        # Define connectors for source
        source_connector = DatabaseConnector(database_name=database_name, host=host, username=username, password=password, driver="{ODBC Driver 17 for SQL Server}")

        # Create a query builder for the source
        query_builder = SelectQueryBuilder(table_name=brand_table_name, columns=columns)

        # Create an extractor for the source
        extractor = DWHExtractor(connector=source_connector, query_builder=query_builder)

        # Create a target connector
        target_connector = S3Connector(access_key_id=aws_access_key_id, secret_access_key=aws_secret_access_key)

        # Create a loader for the target
        loader = S3Loader(connector=target_connector, key=key, bucket=bucket)
        
        # Create a transformer
        transformer = SimpleTransformer()
        
        # Create an ETL object 
        etl = Etl(
            extractor,
            transformer,
            loader,
        )
        etl.run()
    except Exception as e:
        print(f"Error:{e}")


if __name__ == "__main__":
    main()