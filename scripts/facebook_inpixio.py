from repositories.connectors import MetaApiConnector, DatabaseConnector
from repositories.loaders import DwhLoader
from repositories.query_builders import MetaAdsRequestBuilder, SelectMaxQueryBuilder, InsertQueryBuilder
from data.extractors import MetaAdsExtractor
from models.etl import Etl
from datetime import datetime, timedelta, date
import os
from utils.load_env_var import load_env_var


def inpixio_elt()->None:
    # Load and get environment variables
    load_env_var()
    app_id = os.environ.get('INPIXIO_APP_ID')
    app_secret = os.environ.get('INPIXIO_APP_SECRET')
    access_token = os.environ.get('INPIXIO_ACCESS_TOKEN')
    account_id = os.environ.get('INPIXIO_ACCOUNT_ID')
    database_name = os.environ.get("INPIXIO_DBNAME")
    host = os.environ.get("INPIXIO_DBHOST")
    username = os.environ.get("INPIXIO_DBUSER")
    password = os.environ.get("INPIXIO_DBPASS")

    # Define the fields to extract
    fields = ['account_currency',    
              'account_id',    
              'campaign_id',    
              'account_name',    
              'campaign_name',    
              'clicks',    
              'cost_per_inline_link_click',    
              'cost_per_inline_post_engagement',    
              'cost_per_unique_click',    
              'cost_per_unique_inline_link_click',    
              'impressions',    
              'inline_link_clicks',    
              'reach',    
              'spend',    
              'objective'
            ]
    
    # Define the columns to load
    dwh_columns = ["date_start", "date_stop", "time_increment", "account_currency", 
                       "account_id", "campaign_id", "account_name", "campaign_name", 
                       "clicks", "cost_per_inline_link_click", "cost_per_inline_post_engagement", 
                       "cost_per_unique_click", "cost_per_unique_inline_link_click", "impressions", 
                       "inline_link_clicks", "reach", "spend", "objective", "campaigntype", 
                       "week", "CreateDate"]
    
    # Connect to the DWH and get the last loaded date
    target_connector = DatabaseConnector(database_name, host, username, password, "{ODBC Driver 17 for SQL Server}")
    target_connector.connect("pyodbc")
    query_builder = SelectMaxQueryBuilder(table_name='GA.Sessions_Architect')
    select_max_query = query_builder.build_query('Date')
    max_date =  target_connector.conn.execute(select_max_query).fetchone()[0]
    start_date = datetime.strftime(max_date + timedelta(days=1), "%Y-%m-%d") if max_date else date.today().strftime("%Y-%m-%d")
    end_date = date.today().strftime("%Y-%m-%d")

    # Create a Connector and a RequestBuilder for the source
    source_connector = MetaApiConnector(app_id, app_secret, access_token, account_id)
    request_builder = MetaAdsRequestBuilder(fields, start_date, end_date, 1)

    # Create an Extractor to extract data from the source
    extractor = MetaAdsExtractor(source_connector, request_builder)

    # Build the query to load data into the DWH
    insert_query_builder = InsertQueryBuilder('[Facebook].[AdInsights_V2]', dwh_columns)

    # Create a Loader to load data into the DWH
    loadder = DwhLoader(target_connector, insert_query_builder)

    # Create a Transformer to transform data before loading it into the DWH
    transformer = None

    # Create an ETL to run the ETL process
    etl = Etl(extractor, transformer, loadder)
    etl.run()


if __name__ == '__main__':
    inpixio_elt()