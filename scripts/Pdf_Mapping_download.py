import sys
from utils.get_sharepoint_files import authorize,download_file
import pandas as pd
import numpy as np
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
        SHAREPOINT_SITE = cred_json["pdf_sharepoint_url_site"]
        SHAREPOINT_SITE_NAME = cred_json["pdf_sharepoint_site_name"]
        SHAREPOINT_DOC = cred_json["pdf_sharepoint_doc_library"]

        FOLDER_NAME = 'External Data Integration'
        FILE_NAME = 'Campaign_Mapping.xlsx'
        sheet_name = 'Mapping'
        campaign_columns = [
            'platform',
            'Account_Id',
            'Account',
            'Campaign_ID',
            'Campaign',
            'Sales_Rep',
            'Qrep Line',
            'Currency',
            'Product',
            'Traffic_Type',
            'Ad_Network',
            'Target_Market',
            'Ad_Distribution',
            'Variant',
            'Target_landing',
            'Create_Date']
        authorize_data = authorize(SHAREPOINT_SITE, USERNAME, PASSWORD)
        bytes_file_obj = download_file(authorize_data,SHAREPOINT_SITE_NAME,SHAREPOINT_DOC,FOLDER_NAME,FILE_NAME)
        df_campaign_data = pd.read_excel(bytes_file_obj, sheet_name=sheet_name, engine='openpyxl',
                                         usecols=campaign_columns)

        Campaign_archive_data_delete_query = f"DELETE FROM [Acquisition].[XD].[Campaign_Mapping_archive] "
        db.data_push(Campaign_archive_data_delete_query)
        get_campaign_table = f"[Acquisition].[XD].[Campaign_Mapping]"
        get_campaign_old_df = db.get_table_df(get_campaign_table)
        get_campaign_old_df.drop(['ID', 'DT_INSERTED'], axis=1, inplace=True)
        if not get_campaign_old_df.empty:
            insert_query = """INSERT INTO [XD].[Campaign_Mapping_archive] (
                       [platform]
                     ,[Account_Id]
                     ,[Account]
                     ,[Campaign_ID]
                     ,[Campaign]
                     ,[Sales_Rep]
                     ,[Currency]
                     ,[Qrep Line]
                     ,[Product]
                     ,[Traffic_Type]
                     ,[Ad_Network]
                     ,[Target_Market]
                     ,[Ad_Distribution]
                     ,[Variant]
                     ,[Target_landing]
                     ,[Create_Date]

                     )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(get_campaign_old_df, insert_query)

        Campaign_data_delete_query = f"DELETE FROM [Acquisition].[XD].[Campaign_Mapping] "
        db.data_push(Campaign_data_delete_query)
        columns_fillna = ['platform',
                          'Account_Id',
                          'Account',
                          'Campaign_ID',
                          'Campaign',
                          'Sales_Rep',
                          'Qrep Line',
                          'Currency',
                          'Product',
                          'Traffic_Type',
                          'Ad_Network',
                          'Target_Market',
                          'Ad_Distribution',
                          'Variant',
                          'Target_landing',
                          'Create_Date']
        df_campaign_data['Campaign'] = df_campaign_data['Campaign'].replace(r'\s+', np.nan, regex=True).replace('',np.nan)
        df_campaign_data.dropna(axis=0, how='all', inplace=True)
        df_campaign_data[columns_fillna] = df_campaign_data[columns_fillna].fillna(value=0)
        df_campaign_data[['Qrep Line']] = df_campaign_data[['Qrep Line']].astype('Int64')
        df_campaign_data[['Campaign_ID']] = df_campaign_data[['Campaign_ID']].astype('Int64')

        if not df_campaign_data.empty:
            insert_query = """INSERT INTO [XD].[Campaign_Mapping] (
                           [platform]
                         ,[Account_Id]
                         ,[Account]
                         ,[Campaign_ID]
                         ,[Campaign]
                         ,[Sales_Rep]
                         ,[Qrep Line]
                         ,[Currency]
                         ,[Product]
                         ,[Traffic_Type]
                         ,[Ad_Network]
                         ,[Target_Market]
                         ,[Ad_Distribution]
                         ,[Variant]
                         ,[Target_landing]
                         ,[Create_Date]

                         )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(df_campaign_data, insert_query)

    except Exception as e:
        with open('log.txt', 'a') as f:
            f.write(f"{e} \n")



