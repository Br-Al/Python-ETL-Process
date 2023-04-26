import sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from common.db import Database
from common.config import get_api_config
from common.date_tools import get_max_date
from utils.get_facebook_ads_insights import get_facebook_campaign_data
db = Database()
today = datetime.today()
delta = timedelta(days=1)
yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')

if __name__ == '__main__':
    try:
        timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d : %H:%M')
        e_date = yesterday
        qry_type = "day"
        MY_FACEBOOK_PATH = 'C:/facebook_ads_cred.json'
        MY_FCB_CAMPAIGN_PATH = 'C:/Facebook_campaign_category.json'
        cred_json = get_api_config(MY_FACEBOOK_PATH)
        access_token = cred_json["pdf_access_token"]
        app_id = cred_json["pdf_app_id"]
        app_secret = cred_json["pdf_app_secret"]
        account_id = cred_json["pdf_account_id"]
        camapign_type_json = get_api_config(MY_FCB_CAMPAIGN_PATH)
        max_campaign_date_query = """SELECT MAX(date_stop)as new_date FROM [Facebook].[AdInsights_V2] WHERE [account_id]= '1326073344153929'"""
        max_campaign_processed_date_df = db.get_query_df(max_campaign_date_query)
        max_campaign_date = max_campaign_processed_date_df.iat[0, 0]
        campaign_start_date = get_max_date(max_campaign_date)
        facebook_data_df = get_facebook_campaign_data(app_id, app_secret, access_token, account_id, campaign_start_date,
                                                      e_date, qry_type, camapign_type_json)
        facebook_data_df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        facebook_data_df = facebook_data_df.fillna('')
        facebook_data_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")
        facebook_data_df['CreateDate'] = pd.to_datetime(facebook_data_df['CreateDate'])

        if not facebook_data_df.empty:
            insert_query = """INSERT INTO [Facebook].[AdInsights_V2] (
                       [date_start]
                     ,[date_stop]
                     ,[time_increment]
                     ,[account_currency]
                     ,[account_id]
                     ,[campaign_id]
                     ,[account_name]
                     ,[campaign_name]
                     ,[clicks]
                     ,[cost_per_inline_link_click]
                     ,[cost_per_inline_post_engagement]
                     ,[cost_per_unique_click]
                     ,[cost_per_unique_inline_link_click]
                     ,[impressions]
                     ,[inline_link_clicks]
                     ,[reach]
                     ,[spend]
                     ,[objective]
                     ,[campaigntype]
                     ,[week]
                     ,[CreateDate]
                     )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(facebook_data_df, insert_query)



    except:
        print(sys.exc_info())
