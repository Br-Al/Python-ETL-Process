import os
from datetime import datetime, timedelta
from utils.load_env_var import load_env_var
from models.etl import Etl
from repositories.connectors import OutbrainConnector
from data.extractors import OutBrainExtractor
from models.transformers import OutBrainCampaignTransformer, OutBrainPerformanceReportTransformer
from repositories.loaders import DwhLoader
from repositories.connectors import DatabaseConnector
from repositories.query_builders import (
    SelectMaxQueryBuilder, 
    InsertQueryBuilder, 
    OutBrainRequestPerformanceReportBuilder, 
    OutBrainRequestCampaignBuilder,
    DeleteQueryBuilder
)


def main():
    # Load environment variables
    load_env_var()
    username = os.environ.get("OUTBRAIN_USER")
    password = os.environ.get("OUTBRAIN_PASS")
    account_name = os.environ.get("OUTBRAIN_ACCOUNT_NAME")

    dwh_username = os.environ.get("OUTBRAIN_DBUSER")
    dwh_password = os.environ.get("OUTBRAIN_DBPASS")
    host = os.environ.get("OUTBRAIN_DBHOST")
    database_name = os.environ.get("OUTBRAIN_DBNAME")
    accounts = [
        {
            "account_name": 'Avanquest_InPixio_FR_Q121',
            "report_type": ["Campaign", "PerformanceReport"],
        }, 
        {
            "account_name": 'Avanquest_Utilities_FR_Q120',
            "report_type": ["Campaign", "PerformanceReport"],
        },
        {
            "account_name": 'Avanquest_ExpertPDF_Q122',
            "report_type": ["Campaign"],
        }
    ]

    # Connect to the DWH and get the last loaded date
    target_connector = DatabaseConnector(database_name, host, dwh_username, dwh_password, "{ODBC Driver 17 for SQL Server}")
    target_connector.connect("pyodbc")
    del_query = DeleteQueryBuilder(table_name=f'[OUTBRAIN].[Campaigns_V2]').build_query()
    target_connector.execute_query(del_query)
    for account in accounts:
        for report_type in account["report_type"]:
            # Create a connector for the source
            source_connector = OutbrainConnector(username, password, account['account_name'])
            source_connector.connect()
            if report_type == "PerformanceReport":
                columns = [
                  'metrics_impressions',
                  'metrics_clicks',
                  'metrics_totalConversions',
                  'metrics_conversions',
                  'metrics_viewConversions',
                  'metrics_spend',
                  'metrics_ecpc',
                  'metrics_ctr',
                  'metrics_conversionRate',
                  'metrics_viewConversionRate',
                  'metrics_cpa',
                  'metrics_totalCpa',
                  'metrics_totalSumValue',
                  'metrics_sumValue',
                  'metrics_viewSumValue',
                  'metrics_totalAverageValue',
                  'metrics_averageValue',
                  'metrics_viewAverageValue',
                  'campaignId',
                  'date',
                  'AccountName ',
                  'ProcessingDate',
                ]
                
                query = SelectMaxQueryBuilder(table_name=f'[OUTBRAIN].[CampaignPerformanceReport_V2]').build_query(column='[Date]', conditions=f"[AccountName]='{account_name}'")
                last_loaded_date = target_connector.execute_query(query).fetchone()[0]
                start_date = datetime.strftime(last_loaded_date + timedelta(days=1), "%Y-%m-%d") if last_loaded_date else datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")
                end_date = datetime.strftime(datetime.now(), "%Y-%m-%d")

                # Build the query to extract data from the source
                query_builder = OutBrainRequestPerformanceReportBuilder(source_connector.marketer_id, start_date, end_date)

                # Build the query to load the data into the DWH
                insert_query_builder = InsertQueryBuilder(table_name=f'[OUTBRAIN].[CampaignPerformanceReport_V2]', columns=columns)

                # Create a transformer to transform the data
                transformer = OutBrainPerformanceReportTransformer()

            elif report_type == "Campaign":
                columns = [ 'id', 'campaign_name', 'creativeFormat', 'status_enabled', 'AccountName', 'ProcessingDate' ]

                # Build the query to extract data from the source
                query_builder = OutBrainRequestCampaignBuilder(source_connector.marketer_id)

                # Build the query to load the data into the DWH
                insert_query_builder = InsertQueryBuilder(table_name=f'[OUTBRAIN].[Campaigns_V2]', columns=columns)

                # Create a transformer to transform the data
                transformer = OutBrainCampaignTransformer()
                

            # Create an extractor to extract data from the source
            extractor = OutBrainExtractor(source_connector, query_builder)

            # Create a loader to load the data into the DWH
            loader = DwhLoader(target_connector, insert_query_builder)

            # Create an ETL object and run it
            etl = Etl(
                extractor=extractor,
                transformer=transformer,
                loader=loader,
            )
            etl.run()


if __name__ == "__main__":
    main()