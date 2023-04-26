import os
from utils.load_env_var import load_env_var
from models.etl import Etl
from utils.strategies import SimpleStrategy
from models.transformers import TruncateLongStringTransformer
from repositories.connectors import DatabaseConnector
from repositories.query_builders import SelectMaxQueryBuilder, SelectQueryBuilder, InsertQueryBuilder
from data.extractors import WebtoolsExtractor
from repositories.loaders import DwhLoader


def main():
    try:
        # Load environment variables
        load_env_var()
        webtools_host = os.environ.get("WEBTOOLS_DBHOST")
        webtools_username = f"{os.environ.get('WEBTOOLS_DBUSER')}@{webtools_host}"
        webtools_password = os.environ.get("WEBTOOLS_DBPASS")
        dwh_host = os.environ.get("DWH_DBHOST")
        dwh_username = os.environ.get("DWH_DBUSER")
        dwh_password = os.environ.get("DWH_DBPASS")

        columns = [
            'id',
            'date_log',
            'key1',
            'mkey1',
            'key2',
            'cmp',
            'user_id',
            'product_name',
            'exe_id',
            'exe_link',
            'visitor_id',
            'install_id',
            'uid',
            'ref',
        ]

        for src_table in ['webtools_powerbi_install', 'webtools_powerbi_download']:
            if src_table == "webtools_powerbi_install":
                target_table = 'WebTools.install'
            elif src_table == "webtools_powerbi_download":
                target_table = "WebTools.download"
            
            # Connect to the DWH and get the last loaded date
            connector = DatabaseConnector("Acquisition", dwh_host, dwh_username, dwh_password, "{ODBC Driver 17 for SQL Server}")
            connector.connect("pyodbc")
            query_builder = SelectMaxQueryBuilder(table_name=target_table)
            query = query_builder.build_query('date_log')
            max_date = connector.conn.execute(query).fetchone()[0]

            # Define connectors for source
            source_connector = DatabaseConnector("webtoolsbi", webtools_host, webtools_username, webtools_password)

            # Create a query builder for the source
            source_query_builder = SelectQueryBuilder(table_name=src_table)
            
            # Create an Extractor to get data from the source
            extractor = WebtoolsExtractor(source_connector, source_query_builder, max_date)

            # Define connectors for target
            target_connector = DatabaseConnector("Acquisition", dwh_host, dwh_username, dwh_password, "{ODBC Driver 17 for SQL Server}")

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
        print(f"Error:{e}")
        

if __name__ == '__main__':
    main()