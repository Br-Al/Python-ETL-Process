import os
from models.etl import Etl
from models.transformers import SimpleTransformer
from datetime import datetime, timedelta, date
from repositories.connectors import DatabaseConnector, ElasticSearchConnector
from repositories.query_builders import SelectMaxQueryBuilder, InsertQueryBuilder, ElasticSearchBuilder
from repositories.loaders import DwhLoader
from data.extractors import ElasticSearchExtractor
from utils.load_env_var import load_env_var
from elasticsearch import Elasticsearch


def main():
    try:
        # Load environment variables
        load_env_var()
        database_name = os.environ.get("ELASTIC_DBNAME")
        table_name = os.environ.get("ELASTIC_DBTABLE")
        username = os.environ.get("ELASTIC_DBUSER")
        password = os.environ.get("ELASTIC_DBPASS")
        host = os.environ.get("ELASTIC_DBHOST")
        elastic_host = os.environ.get("ELASTIC_HOST")
        elastic_index = os.environ.get("ELASTIC_INDEX")
        elastic_username = os.environ.get("ELASTIC_USERNAME")
        elastic_password = os.environ.get("ELASTIC_PASSWORD")
        fields = [
            "log_timestamp",
            "event.category",
            "http.request.bytes",
            "agent.type",
            "url.query",
            "timetaken",
            "http.response.bytes",
            "source.geo.country_iso_code",
            "http.response.status_code",
            "event.kind",
            "message",
            "site",
            "url.path",
            "url.query_param.productcode"
        ]
        
        columns = [
            'log_timestamp',
            'event_category',
            'http_request_bytes',
            'agent_type',
            'url_query',
            'timetaken',
            'http_response_bytes',
            'source_geo_country_iso_code',
            'http_response_status_code',
            'event_kind',
            'message',
            'site',
            'url_path',
            'url_query_param_productcode',
        ]

        # Create a connector for the source: ElasticSearch
        elastic_search_connector = ElasticSearchConnector(host=elastic_host, username=elastic_username, password=elastic_password)

        # Create a connector for the destination: DWH
        dwh_connector = DatabaseConnector(database_name, host, username, password, "{ODBC Driver 17 for SQL Server}")
        dwh_connector.connect("pyodbc")

        # Get the last loaded date from the DWH
        query_builder = SelectMaxQueryBuilder(table_name)
        query = query_builder.build_query("[DT_INSERTED]")
        max_date = dwh_connector.conn.execute(query).fetchone()[0]
        start_date = datetime.strftime(max_date, "%Y-%m-%d %H:%M:%S") if max_date else "2023-03-22 00:00:00"
        end_date = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

        if start_date <= end_date:
            # Create a query builder for the source: ElasticSearch
            elastic_search_query_builder = ElasticSearchBuilder(
                index='iis-prod', 
                fields=fields, 
                start_date=start_date, 
                end_date=end_date
            )

            # Create an extractor to extract data from the source: ElasticSearch
            elastic_search_extractor = ElasticSearchExtractor(elastic_search_connector, elastic_search_query_builder)

            # Create a transformer to transform the data
            transformer = SimpleTransformer()

            # Create a query builder for the destination: DWH
            insert_query_builder = InsertQueryBuilder(table_name, columns)

            # Create a loader to load the data into the destination: DWH
            dwh_loader = DwhLoader(dwh_connector, insert_query_builder)

            # Create an ETL object to execute the ETL process
            etl = Etl(elastic_search_extractor, transformer, dwh_loader)

            etl.run()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()