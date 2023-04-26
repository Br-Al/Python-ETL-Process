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
        database_name = os.environ.get("CRAFTAI_DWH_NAME")
        host = os.environ.get("CRAFTAI_DWH_HOST")
        username = os.environ.get("CRAFTAI_DWH_USERNAME")
        password = os.environ.get("CRAFTAI_DWH_PASSWORD")
        bucket = 'craftai-cs-avanquest-adoptim-prod'
        columns = [
            "CampaignID",
            "Campaign",
            "Market",
            "DailyBudget",
            "BidStrategyType",
            "Date",
            "BidParameter",
            "AdGroupID",
            "AdGroup",
            "ListOfKeywords",
            "ListOfLandingPages",
            "Clicks",
            "Impressions",
            "Interactions",
            "BounceRate",
            "Cost",
            "ConversionsG",
            "ConversionsSalesG",
            "ConversionsInstallsG",
            "ConversionValueG",
            "Checkouts",
            "Joins",
            "GrossSaleRevenueTaxExclude",
            "NetSaleRevenue",
            "NetSaleRevenueAF",
            "Sales",
            "Downloads",
            "StartedInstalls",
            "CompletedInstalls",
            "Uninstalls"
        ]
        brands = [
            {
                'name': 'sodapdf',
                'table_name': os.environ.get('CRAFTAI_DWH_TABLE_SODA')
            },
            {
                'name': 'pdfsuite',
                'table_name': os.environ.get('CRAFTAI_DWH_TABLE_PDF')
            }
        ]
        for brand in brands:
            key = f"landing_zone/{datetime.now().date()}/google-{brand['name']}.csv"

            # Define connectors for source
            source_connector = DatabaseConnector(database_name=database_name, host=host, username=username, password=password, driver="{ODBC Driver 17 for SQL Server}")

            # Create a query builder for the source
            query_builder = SelectQueryBuilder(table_name=brand['table_name'], columns=columns)

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