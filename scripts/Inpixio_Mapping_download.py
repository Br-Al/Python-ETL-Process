import sys
from utils.get_sharepoint_files import authorize,download_file
import pandas as pd
import numpy as np
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
        FOLDER_NAME = 'SP-WEB/MKT WEB/InPixio/Reporting'
        FILE_NAME = 'Mapping.xlsx'
        sheet_name = 'Feuil1'
        campaign_columns = [
            'Tracking Value',
            'Campaign',
            'Country',
            'Market',
            'Product sold',
            'Product initiating sale',
            'Type',
            'Channel',
            'Platform',
            'OS Type',
            'BUSINESS',
            'Acquisition Type',
            'Comment',
            'Comment 2',
            'Order']
        authorize_data = authorize(INPIXIO_SHAREPOINT_SITE, USERNAME, PASSWORD)
        bytes_file_obj = download_file(authorize_data, INPIXIO_SHAREPOINT_SITE_NAME, INPIXIO_SHAREPOINT_DOC, FOLDER_NAME, FILE_NAME)

        df_campaign_data = pd.read_excel(bytes_file_obj, sheet_name=sheet_name, engine='openpyxl',
                                         usecols=campaign_columns)
        df_campaign_data['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        df_campaign_data['ProcessingDate'] = pd.to_datetime(df_campaign_data['ProcessingDate'])
        df_campaign_data['Order'] = df_campaign_data['Order'].fillna(value=0).astype('Int64')
        df_campaign_data = df_campaign_data.rename(columns={'Comment 2': 'Comment2',
                                                            'Product sold': 'ProductSold',
                                                            'Product initiating sale': 'ProductInitiatingSale',
                                                            'OS Type': 'OSType', 'Acquisition Type': 'AcquisitionType'})

        df_campaign_data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        df_campaign_data = df_campaign_data.fillna(value='')
        Campaign_archive_data_delete_query = f"DELETE FROM [Acquisition].[XD].[Photo_Mapping_archive]  "
        db.data_push(Campaign_archive_data_delete_query)
        get_campaign_table = f"[Acquisition].[XD].[Photo_Mapping]"
        get_campaign_old_df = db.get_table_df(get_campaign_table)
        get_campaign_old_df.drop(['ID'], axis=1, inplace=True)

        if not get_campaign_old_df.empty:
            insert_query = """INSERT INTO [XD].[Photo_Mapping_archive] (
                       [TrackingValue]
                      ,[Campaign]
                      ,[Country]
                      ,[Market]
                      ,[ProductSold]
                      ,[ProductInitiatingSale]
                      ,[Type]
                      ,[Channel]
                      ,[Platform]
                      ,[OSType]
                      ,[BUSINESS]
                      ,[AcquisitionType]
                      ,[Comment]
                      ,[Comment2]
                      ,[ProcessingDate]
                      ,[Order]

                     )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(get_campaign_old_df, insert_query)

        Campaign_data_delete_query = f"DELETE FROM [Acquisition].[XD].[Photo_Mapping] "
        db.data_push(Campaign_data_delete_query)

        if not df_campaign_data.empty:
            insert_query = """INSERT INTO [XD].[Photo_Mapping] (

                      [TrackingValue]
                      ,[Campaign]
                      ,[Country]
                      ,[Market]
                      ,[ProductSold]
                      ,[ProductInitiatingSale]
                      ,[Type]
                      ,[Channel]
                      ,[Platform]
                      ,[OSType]
                      ,[BUSINESS]
                      ,[AcquisitionType]
                      ,[Comment]
                      ,[Comment2]
                      ,[Order]
                      ,[ProcessingDate]
                     )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(df_campaign_data, insert_query)

    except Exception as e:
        with open('log.txt', 'a') as f:
            f.write(f"{e} \n")


