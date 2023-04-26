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
        INPIXIO_SHAREPOINT_SITE = cred_json["inpixio_sharepoint_url_site"]
        INPIXIO_SHAREPOINT_SITE_NAME = cred_json["inpixio_sharepoint_site_name"]
        INPIXIO_SHAREPOINT_DOC = cred_json["inpixio_sharepoint_doc_library"]
        FOLDER_NAME = 'SP-WEB/UTILITIES/Utilities/Utilities'
        FILE_NAME = 'Mapping.xlsx'
        sheet_name = 'Feuil1'
        campaign_columns = [
            "Tracking Value",
            "Country",
            "Language",
            "Product sold",
            "Type",
            "Channel",
            "Platform",
            "OS Type",
            "BUSINESS",
            "Comment",
            "Order"]
        authorize_data = authorize(INPIXIO_SHAREPOINT_SITE, USERNAME, PASSWORD)
        bytes_file_obj = download_file(authorize_data, INPIXIO_SHAREPOINT_SITE_NAME, INPIXIO_SHAREPOINT_DOC, FOLDER_NAME, FILE_NAME)

        campaign_df = pd.read_excel(bytes_file_obj, sheet_name=sheet_name, engine='openpyxl',
                                         usecols=campaign_columns)
        campaign_new_columns = [ "TrackingValue"
                                  ,"Country"
                                  ,"Language"
                                  ,"ProductSold"
                                  ,"Type"
                                  ,"Channel"
                                  ,"Platform"
                                  ,"OSType"
                                  ,"BUSINESS"
                                  ,"Comment"
                                  ,"Order"]
        campaign_df.rename(columns={i: j for i, j in zip(campaign_columns, campaign_new_columns)}, inplace=True)
        campaign_df["Comment"] = campaign_df["Comment"].fillna(value='').astype('str')
        campaign_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_df['ProcessingDate'] = pd.to_datetime(campaign_df['ProcessingDate'])

        Campaign_data_delete_query = f"DELETE FROM [Acquisition].[XD].[Campaign_Mapping_PCHS] "
        db.data_push(Campaign_data_delete_query)
        if not campaign_df.empty:
            insert_query = """INSERT INTO [XD].[Campaign_Mapping_PCHS] (
                              [TrackingValue]
                              ,[Country]
                              ,[Language]
                              ,[ProductSold]
                              ,[Type]
                              ,[Channel]
                              ,[Platform]
                              ,[OSType]
                              ,[BUSINESS]
                              ,[Comment]
                              ,[Order]
                              ,[ProcessingDate]

                            )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_df, insert_query)
    except:
        print(sys.exc_info())
