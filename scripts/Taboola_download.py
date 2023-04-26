from utils import get_taboola_reports
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys

from common.db import Database
from common.config import get_api_config
from common.date_tools import get_max_date

db = Database()
today = datetime.today()
delta = timedelta(days=1)
end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
if __name__ == '__main__':
    try:
        MY_TABOOLA_PATH = 'C:/taboola_cred.json'
        cred_json = get_api_config(MY_TABOOLA_PATH)
        inpixio_account_name = cred_json["inpixio_account_name"]
        inpixio_client_id = cred_json["inpixio_client_id"]
        inpixio_client_secret = cred_json["inpixio_client_secret"]
        utility_account_name = cred_json["utility_account_name"]
        utility_client_id = cred_json["utility_client_id"]
        utility_client_secret = cred_json["utility_client_secret"]
        max_summary_date_query = """SELECT MAX(Date)as new_date
                                                       FROM [Acquisition].[TABOOLA].[Summary] WHERE [account_name] = 'claranova-network-sc' """
        max_summary_processed_date = db.get_query_df(max_summary_date_query)
        max_summary_date = max_summary_processed_date.iat[0, 0]
        summary_start_date = get_max_date(max_summary_date)
        max_campaign_content_date_query = """SELECT MAX(Date)as new_date
                                                       FROM [Acquisition].[TABOOLA].[CampaignContent] """
        max_campaign_content_processed_date = db.get_query_df(max_campaign_content_date_query)
        max_campaign_content_date = max_campaign_content_processed_date.iat[0, 0]
        campaign_content_start_date = get_max_date(max_campaign_content_date)
        max_campaign_date_query = """SELECT MAX(Date)as new_date
                                                       FROM [Acquisition].[TABOOLA].[Campaigns] """
        max_campaign_processed_date = db.get_query_df(max_campaign_date_query)
        max_campaign_date = max_campaign_processed_date.iat[0, 0]
        campaign_start_date = get_max_date(max_campaign_date)
        inpixio_account = get_taboola_reports.Taboola(Account_name=inpixio_account_name, client_id=inpixio_client_id,
                                                      client_secret=inpixio_client_secret)
        inpixio_campaign_df = inpixio_account.Campaigns(campaign_start_date)
        inpixio_summary_df = inpixio_account.Summary(summary_start_date, end_date)
        inpixio_campaign_content_df = inpixio_account.campaign_content(campaign_content_start_date)
        utility_account = get_taboola_reports.Taboola(Account_name=utility_account_name, client_id=utility_client_id,
                                                      client_secret=utility_client_secret)
        utility_campaign_df = utility_account.Campaigns(campaign_start_date)
        utility_summary_df = utility_account.Summary(summary_start_date, end_date)
        utility_campaign_content_df = utility_account.campaign_content(campaign_content_start_date)
        campaign_list = [inpixio_campaign_df, utility_campaign_df]
        campaign_df = pd.concat(campaign_list)
        campaign_summary_list = [inpixio_summary_df, utility_summary_df]
        campaign_summary_df = pd.concat(campaign_summary_list)
        campaign_content_list = [inpixio_campaign_content_df, utility_campaign_content_df]
        campaign_content_df = pd.concat(campaign_content_list)
        campaign_columns = ['Date', 'id'
            , 'advertiser_id'
            , 'name'
            , 'branding_text'
            , 'tracking_code'
            , 'pricing_model'
            , 'cpc'
            , 'safety_rating'
            , 'daily_cap'
            , 'daily_ad_delivery_model'
            , 'spending_limit'
            , 'spending_limit_model'
            , 'cpa_goal'
            , 'campaign_profile'
            , 'comments'
            , 'spent'
            , 'bid_type'
            , 'bid_strategy'
            , 'traffic_allocation_mode'
            , 'campaign_groups'
            , 'start_date'
            , 'end_date'
            , 'start_date_in_utc'
            , 'end_date_in_utc'
            , 'approval_state'
            , 'is_active'
            , 'status'
            , 'marketing_objective'
            , 'verification_pixel'
            , 'viewability_tag'
            , 'country_targeting.type'
            , 'country_targeting.value'
            , 'country_targeting.href'
            , 'platform_targeting.type'
            , 'platform_targeting.value'
            , 'platform_targeting.href'
            , 'os_targeting.type'
            , 'os_targeting.value'
            , 'os_targeting.href'
            , 'connection_type_targeting.type'
            , 'connection_type_targeting.value'
            , 'connection_type_targeting.href'
            , 'publisher_bid_modifier.values'
            , 'publisher_bid_strategy_modifiers.values'
            , 'activity_schedule.mode'
            , 'activity_schedule.rules'
            , 'activity_schedule.time_zone'
            , 'policy_review.reject_reason'
            , 'browser_targeting.type'
            , 'browser_targeting.value'
            , 'browser_targeting.href']
        campaign_df = campaign_df[campaign_columns]
        campaign_df['Date'] = pd.to_datetime(campaign_df['Date'])
        campaign_df['start_date'] = pd.to_datetime(campaign_df['end_date'], errors='coerce')
        campaign_df['end_date'] = pd.to_datetime(campaign_df['start_date'], errors='coerce')
        campaign_df['start_date_in_utc'] = pd.to_datetime(campaign_df['start_date_in_utc'])
        campaign_df['end_date_in_utc'] = pd.to_datetime(campaign_df['end_date_in_utc'], errors='coerce')
        campaign_df = campaign_df.astype({'country_targeting.value': 'str',
                                          'platform_targeting.value': 'str',
                                          'os_targeting.value': 'str',
                                          'connection_type_targeting.value': 'str',
                                          'publisher_bid_modifier.values': 'str',
                                          'publisher_bid_strategy_modifiers.values': 'str'
                                             , 'activity_schedule.rules': 'str',
                                          'browser_targeting.value': 'str'})
        campaign_df.drop(['end_date', 'start_date', 'start_date_in_utc', 'end_date_in_utc', 'publisher_bid_modifier.values'], axis=1,inplace=True)
        campaign_df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        campaign_df = campaign_df.fillna('')
        campaign_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_df['ProcessingDate'] = pd.to_datetime(campaign_df['ProcessingDate'])

        if not campaign_df.empty:
            insert_query = """INSERT INTO [TABOOLA].[Campaigns] (
                                [Date]
                                ,[id]
                          ,[advertiser_id]
                          ,[name]
                          ,[branding_text]
                          ,[tracking_code]
                          ,[pricing_model]
                          ,[cpc]
                          ,[safety_rating]
                          ,[daily_cap]
                          ,[daily_ad_delivery_model]
                          ,[spending_limit]
                          ,[spending_limit_model]
                          ,[cpa_goal]
                          ,[campaign_profile]
                          ,[comments]
                          ,[spent]
                          ,[bid_type]
                          ,[bid_strategy]
                          ,[traffic_allocation_mode]
                          ,[campaign_groups]
                          ,[approval_state]
                          ,[is_active]
                          ,[status]
                          ,[marketing_objective]
                          ,[verification_pixel]
                          ,[viewability_tag]
                          ,[country_targeting.type]
                          ,[country_targeting.value]
                          ,[country_targeting.href]
                          ,[platform_targeting.type]
                          ,[platform_targeting.value]
                          ,[platform_targeting.href]
                          ,[os_targeting.type]
                          ,[os_targeting.value]
                          ,[os_targeting.href]
                          ,[connection_type_targeting.type]
                          ,[connection_type_targeting.value]
                          ,[connection_type_targeting.href]
                          ,[publisher_bid_strategy_modifiers.values]
                          ,[activity_schedule.mode]
                          ,[activity_schedule.rules]
                          ,[activity_schedule.time_zone]
                          ,[policy_review.reject_reason]
                          ,[browser_targeting.type]
                          ,[browser_targeting.value]
                          ,[browser_targeting.href]
                          ,[ProcessingDate]
            )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_df, insert_query)

        campaign_summary_df['date'] = pd.to_datetime(campaign_summary_df['date'])
        campaign_summary_df['date_end_period'] = pd.to_datetime(campaign_summary_df['date_end_period'])
        campaign_summary_df = campaign_summary_df.astype({'clicks': 'int64',
                                                          'impressions': 'int64',
                                                          'visible_impressions': 'int64',
                                                          'cpa_actions_num': 'int64',
                                                          'cpa_actions_num_from_clicks': 'int64'
                                                             , 'cpa_actions_num_from_views': 'int64'})
        campaign_summary_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_summary_df['ProcessingDate'] = pd.to_datetime(campaign_summary_df['ProcessingDate'])
        if not campaign_summary_df.empty:
            insert_query = """INSERT INTO [TABOOLA].[Summary] (
                            [account_name]
                                 ,[date]
                              ,[date_end_period]
                              ,[clicks]
                              ,[impressions]
                              ,[visible_impressions]
                              ,[spent]
                              ,[conversions_value]
                              ,[roas]
                              ,[ctr]
                              ,[vctr]
                              ,[cpm]
                              ,[vcpm]
                              ,[cpc]
                              ,[campaigns_num]
                              ,[cpa]
                              ,[cpa_clicks]
                              ,[cpa_views]
                              ,[cpa_actions_num]
                              ,[cpa_actions_num_from_clicks]
                              ,[cpa_actions_num_from_views]
                              ,[cpa_conversion_rate]
                              ,[cpa_conversion_rate_clicks]
                              ,[cpa_conversion_rate_views]
                              ,[currency]
                              ,[ProcessingDate]


            )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_summary_df, insert_query)

        campaign_content_df['date'] = pd.to_datetime(campaign_content_df['date'])
        campaign_content_df['create_time'] = pd.to_datetime(campaign_content_df['create_time'])
        campaign_content_df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        campaign_content_df = campaign_content_df.fillna(0)
        campaign_content_df = campaign_content_df.astype({'item': 'int64',
                                                          'campaign': 'int64',
                                                          'content_provider': 'int64',
                                                          'impressions': 'int64',
                                                          'visible_impressions': 'int64',
                                                          'actions': 'int64',
                                                          'actions_num_from_clicks': 'int64',
                                                          'actions_num_from_views': 'int64'})
        campaign_content_df.drop(['create_time', 'old_item_version_id'], axis=1, inplace=True)
        campaign_content_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_content_df['ProcessingDate'] = pd.to_datetime(campaign_content_df['ProcessingDate'])

        if not campaign_content_df.empty:
            insert_query = """INSERT INTO [TABOOLA].[CampaignContent] (
                                       [date]
                                     ,[item]
                                      ,[item_name]
                                      ,[description]
                                      ,[thumbnail_url]
                                      ,[url]
                                      ,[campaign]
                                      ,[campaign_name]
                                      ,[content_provider]
                                      ,[content_provider_name]
                                      ,[impressions]
                                      ,[visible_impressions]
                                      ,[ctr]
                                      ,[vctr]
                                      ,[clicks]
                                      ,[cpc]
                                      ,[cvr]
                                      ,[cvr_clicks]
                                      ,[cvr_views]
                                      ,[cpa]
                                      ,[cpa_clicks]
                                      ,[cpa_views]
                                      ,[actions]
                                      ,[actions_num_from_clicks]
                                      ,[actions_num_from_views]
                                      ,[cpm]
                                      ,[vcpm]
                                      ,[spent]
                                      ,[conversions_value]
                                      ,[roas]
                                      ,[currency]
                                      ,[learning_display_status]
                                      ,[ProcessingDate]


                )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_content_df, insert_query)
    except:
        print(sys.exc_info())