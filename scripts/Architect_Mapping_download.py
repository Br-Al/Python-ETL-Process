import sys
from utils.get_sharepoint_files import authorize,download_file
import pandas as pd
from datetime import datetime
pd.options.mode.chained_assignment = None
from common.db import Database
from common.config import get_api_config
db = Database()

if __name__ == '__main__':
    try:
        MY_THIN_CLIENT_API_PATH = 'C:/thin_client_cred.json'
        cred_json = get_api_config(MY_THIN_CLIENT_API_PATH)
        USERNAME = cred_json["user_name"]
        PASSWORD = cred_json["password"]
        ARCHITECT_SHAREPOINT_SITE = cred_json["architect_sharepoint_url_site"]
        ARCHITECT_SHAREPOINT_SITE_NAME = cred_json["architect_sharepoint_site_name"]
        ARCHITECT_SHAREPOINT_DOC = cred_json["architect_sharepoint_doc_library"]
        FOLDER_NAME = 'External Data Integration/Mapping'
        FILE_NAME = 'GOOGLE - BING MAPPING CAMPAIGN.xlsx'
        sheet_name = 'MAPPING'
        campaign_columns = [
            'TRACKING',
            'CAMPAIGN',
            'COUNTRY',
            'AREA',
            'LANGUAGE',
            'NATURE',
            'BUDGET CATEGORY',
            'PRODUCT CATEGORY',
            'PRODUCT',
            'OS',
            'NETWORK',
            'CAMPAIGN TYPE',
            'SEARCH ENGINE',
            'ACCOUNT',
            'BID STRATEGY TYPE',
             'CUSTOMERCOUNTRY']
        authorize_data = authorize(ARCHITECT_SHAREPOINT_SITE, USERNAME, PASSWORD)
        bytes_file_obj = download_file(authorize_data, ARCHITECT_SHAREPOINT_SITE_NAME, ARCHITECT_SHAREPOINT_DOC, FOLDER_NAME, FILE_NAME)
        campaign_df = pd.read_excel(bytes_file_obj, sheet_name=sheet_name, engine='openpyxl', usecols=campaign_columns)
        campaign_new_columns=['Tracking'
                              ,'Campaign'
                              ,'Country'
                              ,'Area'
                              ,'Language'
                              ,'Nature'
                              ,'BudgetCategory'
                              ,'ProductCategory'
                              ,'Product'
                              ,'OS'
                              ,'Network'
                              ,'CampaignType'
                              ,'SearchEngine'
                              ,'Account'
                              ,'BidStrategyType'
                              ,'CustomerCountry']
        campaign_df.rename(columns={i: j for i, j in zip(campaign_columns, campaign_new_columns)}, inplace=True)
        campaign_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_df['ProcessingDate'] = pd.to_datetime(campaign_df['ProcessingDate'])
        Campaign_data_delete_query = f"DELETE FROM [Acquisition].[XD].[Campaign_Mapping_Architect] "
        db.data_push(Campaign_data_delete_query)

        if not campaign_df.empty:
            insert_query = """INSERT INTO [XD].[Campaign_Mapping_Architect] (
                       [Tracking]
                      ,[Campaign]
                      ,[Country]
                      ,[Area]
                      ,[Language]
                      ,[Nature]
                      ,[BudgetCategory]
                      ,[ProductCategory]
                      ,[Product]
                      ,[OS]
                      ,[Network]
                      ,[CampaignType]
                      ,[SearchEngine]
                      ,[Account]
                      ,[BidStrategyType]
                      ,[CustomerCountry]
                      ,[ProcessingDate]

                     )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_df, insert_query)

    except Exception as e:
        with open('log.txt', 'a') as f:
            f.write(f"{e} \n")


