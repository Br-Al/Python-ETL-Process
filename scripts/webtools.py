import os
from utils.load_env_var import load_env_var
from models.etl import Etl
from utils.strategies import SimpleStrategy
from models.transformers import TruncateLongStringTransformer
from repositories.connectors import DatabaseConnector
from repositories.query_builders import SelectMaxQueryBuilder, SelectQueryBuilder, InsertQueryBuilder
from data.extractors import XXXExtractor
from repositories.loaders import DwhLoader


def main():
    try:
        # Load environment variables
        load_env_var()
        host = os.environ.get("DBHOST")
        username = f"{os.environ.get('DBUSER')}@{host}"
        password = os.environ.get("DBPASS")
        dwh_host = os.environ.get("DWH_DBHOST")
        dwh_username = os.environ.get("DWH_DBUSER")
        dwh_password = os.environ.get("DWH_DBPASS")
        database_name = os.environ.get("DBNAME")

        columns = [] # TODO: Add columns to load

        
        src_table = '' # TODO: Add source table name
        target_table = '' # TODO: Add target table name
        
        # Connect to the DWH and get the last loaded date
        connector = DatabaseConnector(database_name, dwh_host, dwh_username, dwh_password, "{ODBC Driver 17 for SQL Server}")
        connector.connect("pyodbc")
        query_builder = SelectMaxQueryBuilder(table_name=target_table)
        query = query_builder.build_query('') # TODO: Add date column name
        max_date = connector.conn.execute(query).fetchone()[0]

        # Define connectors for source
        source_connector = DatabaseConnector(database_name, host, username, password)

        # Create a query builder for the source
        source_query_builder = SelectQueryBuilder(table_name=src_table)
        
        # Create an Extractor to get data from the source
        extractor = XXXExtractor(source_connector, source_query_builder, max_date)

        # Define connectors for target
        target_connector = DatabaseConnector(database_name, dwh_host, dwh_username, dwh_password, "{ODBC Driver 17 for SQL Server}")

        # Create a query builder for the target
        target_query_builder = InsertQueryBuilder(table_name=target_table, columns=columns)

        # Create a Loader to load data into the target
        loader = DwhLoader(target_connector, target_query_builder)

        # Create a Transformer to transform data before loading
        transformer = TruncateLongStringTransformer()

        # Create an ETL object
        etl = Etl(
            extractor=extractor,
            transformer=transformer,
            loader=loader,
        )

        etl.run()
    except Exception as e:
        raise e
        

if __name__ == '__main__':
    main()