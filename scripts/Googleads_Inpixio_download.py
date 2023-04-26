from datetime import datetime, timedelta
from common.db import Database
from common.config import get_api_config
from common.date_tools import get_max_date

from utils.Google_ads_authetication import *
from utils.get_google_ads_report import get_campaign_data, get_ads_data, get_geo_performance_data, get_landing_Page_data,get_keyword_view_data,get_bidding_strategy_data,get_ads_with_bid_data,get_campaign_bidding_data

db = Database()
today = datetime.today()
delta = timedelta(days=1)
yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')


if __name__ == '__main__':
    try:
        timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d : %H:%M')

        e_date = yesterday
        MY_GOOGLE_PATH = 'C:/google_ads_cred.json'
        cred_json = get_api_config(MY_GOOGLE_PATH)
        account_id = cred_json["photo_account_id"]


        api_client = google_ads_authentication(cred_json)
        max_campaign_date_query = """SELECT MAX(Date)as new_date
                FROM [GoogleAds].[CampaignPerformance_V2] WHERE [CustomerId]='5449454655'"""

        max_campaign_processed_date_df = db.get_query_df(max_campaign_date_query)
        max_campaign_date = max_campaign_processed_date_df.iat[0, 0]
        campaign_start_date = get_max_date(max_campaign_date)

        campaign_df = get_campaign_data(api_client, account_id, campaign_start_date, e_date)
        campaign_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")


        if not campaign_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[CampaignPerformance_V2] (
                   [Date]
                   ,[CustomerId]
                   ,[CampaignName]
                   ,[CampaignId]
                 ,[Clicks]
                 ,[Impressions]
                 ,[Cost],[CreateDate])  VALUES (?, ?, ?, ?, ?, ?,?,?)"""
            db.insert_many(campaign_df, insert_query)

        max_ad_date_query = """SELECT MAX(Date)as new_date
                               FROM [GoogleAds].[AdgroupPerformance_V2] WHERE [CustomerId]='5449454655'"""
        max_ads_processed_date_df = db.get_query_df(max_ad_date_query)
        max_ads_date = max_ads_processed_date_df.iat[0, 0]
        ad_start_date = get_max_date(max_ads_date)

        ads_df = get_ads_data(api_client, account_id, ad_start_date, e_date)
        ads_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        if not ads_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[AdgroupPerformance_V2] (
                           [Date]
                           ,[CustomerId]
                           ,[CampaignName]
                           ,[AdGroupName]
                           ,[CampaignId]
                           ,[Impressions]
                           ,[Clicks]
                           ,[Cost]
                           ,[SearchImpressionShare]
                           ,[CreateDate]
                           ) VALUES (?, ?, ?, ?, ?, ?,?,?,?,?)"""
            db.insert_many(ads_df, insert_query)

        max_landing_date_query = """SELECT MAX(Date)as new_date
                        FROM [GoogleAds].[LandingPage_V2] WHERE [CustomerId]='5449454655'"""

        max_land_processed_date_df = db.get_query_df(max_landing_date_query)
        max_landing_date = max_land_processed_date_df.iat[0, 0]
        landing_start_date = get_max_date(max_landing_date)

        landingPage_df = get_landing_Page_data(api_client, account_id, landing_start_date, e_date)
        landingPage_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        if not landingPage_df.empty:
             insert_query = """INSERT INTO [GoogleAds].[LandingPage_V2] (
             [Date]
             ,[CustomerId]
             ,[AdGroupId]
             ,[AdGroupName]
             ,[AdGroupStatus]
             ,[CampaignId]
             ,[CampaignName]
             ,[FinalUrl]
             ,[Impressions]
             ,[Conversions]
             ,[Clicks]
             ,[Cost]
             ,[CreateDate]
              )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)"""
             db.insert_many(landingPage_df, insert_query)

        max_keyword_date_query = """SELECT MAX(Date)as new_date
                                       FROM [GoogleAds].[KeywordPerformanceReport_V2] WHERE [CustomerId]='5449454655'"""

        max_keyword_processed_date_df = db.get_query_df(max_keyword_date_query)
        max_keyword_date = max_keyword_processed_date_df.iat[0, 0]
        keyword_start_date = get_max_date(max_keyword_date)
        keyword_view_df = get_keyword_view_data(api_client, account_id, keyword_start_date, e_date)
        keyword_view_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")
        map_bidding_data = {
            0: 'UNSPECIFIED'
            , 1: 'UNKNOWN'
            , 16: 'COMMISSION'
            , 2: 'ENHANCED_CPC'
            , 17: 'INVALID'
            , 18: 'MANUAL_CPA'
            , 3: 'MANUAL_CPC'
            , 4: 'MANUAL_CPM'
            , 13: 'MANUAL_CPV'
            , 10: 'MAXIMIZE_CONVERSIONS'
            , 11: 'MAXIMIZE_CONVERSION_VALUE'
            , 5: 'PAGE_ONE_PROMOTED'
            , 12: 'PERCENT_CPC'
            , 6: 'TARGET_CPA'
            , 14: 'TARGET_CPM'
            , 15: 'TARGET_IMPRESSION_SHARE'
            , 7: 'TARGET_OUTRANK_SHARE'
            , 8: 'TARGET_ROAS'
            , 9: 'TARGET_SPEND'}
        keyword_view_df['BiddingStrategyType'] = keyword_view_df['BiddingStrategyType'].map(map_bidding_data)

        if not keyword_view_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[KeywordPerformanceReport_V2] (
                                   [Date]
                                  ,[CustomerId]
                                  ,[AccountDescriptiveName]
                                  ,[CustomerTimeZone]
                                  ,[AdGroupID]
                                  ,[AdGroupName]
                                  ,[CampaignId]
                                  ,[CampaignName]
                                  ,[AdGroupCriteriaID]
                                  ,[AdGroupCriteriaName]
                                  ,[BiddingStrategyId]
                                  ,[BiddingStrategyName]
                                  ,[BiddingStrategyType]
                                  ,[BiddingStrategyStatus]
                                  ,[ResourceName]
                                  ,[Clicks]
                                  ,[Conversions]
                                  ,[BounceRate]
                                  ,[interactions]
                                  ,[EngagementRate]
                                  ,[Cost]
                                   ,[CreateDate])  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(keyword_view_df, insert_query)

        max_bidding_date_query = """SELECT MAX(Date)as new_date
                                       FROM [GoogleAds].[BiddingStrategyReport_V2] WHERE [CustomerId]='5449454655'"""

        max_bidding_processed_date_df = db.get_query_df(max_bidding_date_query)
        max_bidding_date = max_bidding_processed_date_df.iat[0, 0]
        bidding_start_date = get_max_date(max_bidding_date)
        bid_strategy_df = get_bidding_strategy_data(api_client, account_id, bidding_start_date, e_date)
        bid_strategy_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        if not bid_strategy_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[BiddingStrategyReport_V2] (
                            [Date]
                              ,[CustomerId]
                              ,[AccountDescriptiveName]
                              ,[CustomerCurrencyCode]
                              ,[CustomerTimeZone]
                              ,[StrategyId]
                              ,[StrategyName]
                              ,[ResourceName]
                              ,[TargetRoas]
                              ,[TargetRoasBidCeiling]
                              ,[TargetRoasBidFloor]
                              ,[TargetSpendSpendTarget]
                              ,[ShareImpressionLocation]
                              ,[Conversions]
                              ,[ConversionRate]
                              ,[ConversionValue]
                              ,[Clicks]
                              ,[Cost]
                            ,[CreateDate])  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(bid_strategy_df, insert_query)

        max_geo_date_query = """SELECT MAX(Date)as new_date
                               FROM [GoogleAds].[GeoPerformance_V2] WHERE [CustomerId]='5449454655'"""

        max_geo_processed_date_df = db.get_query_df(max_geo_date_query)
        max_geo_date = max_geo_processed_date_df.iat[0, 0]
        geo_start_date = get_max_date(max_geo_date)
        geo_df = get_geo_performance_data(api_client, account_id, geo_start_date, e_date)
        geo_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        if not geo_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[GeoPerformance_V2] (
            [Date]
            ,[CustomerDescriptiveName]
            ,[CustomerID]
            ,[CustomerTimeZone]
            ,[CampaignId]
            ,[CampaignName]
            ,[AdGroupId]
            ,[AdGroupName]
            ,[CountryCriteriaId]
            ,[CityCriteriaId]
            ,[Clicks]
            ,[Conversions]
            ,[Impressions]
            ,[Cost]
            ,[CreateDate])  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(geo_df, insert_query)

        max_ad_bid_date_query = """SELECT MAX(Date)as new_date
                       FROM [GoogleAds].[AdgroupPerformance_V2Bid] WHERE [CustomerId]='5449454655'"""
        max_ads_bid_processed_date_df = db.get_query_df(max_ad_bid_date_query )
        max_ads_bid_date = max_ads_bid_processed_date_df.iat[0, 0]
        ad__bid_start_date = get_max_date(max_ads_bid_date)

        ads_bid_df = get_ads_with_bid_data(api_client, account_id, ad__bid_start_date, e_date)
        ads_bid_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        if not ads_bid_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[AdgroupPerformance_V2Bid] (
                   [Date]
                   ,[CustomerId]
                   ,[CampaignId]
                   ,[CampaignName]
                   ,[AdGroupId]
                   ,[AdGroupName]
                   ,[BiddingStrategyId]
                   ,[BiddingStrategyType]
                   ,[BiddingStrategyName]
                   ,[Conversions]
                   ,[BounceRate]
                   ,[Interactions]
                   ,[EngagementRate]
                   ,[Impressions]
                   ,[Clicks]
                   ,[Cost]
                   ,[SearchImpressionShare]
                   ,[CreateDate]
                   ) VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(ads_bid_df, insert_query)

        max_campaign_bid_date_query = """SELECT MAX(Date)as new_date
                                                               FROM [GoogleAds].[CampaignPerformance_V2Bid] WHERE [CustomerId]='5449454655'"""
        max_campaign_bid_processed_date_df = db.get_query_df(max_campaign_bid_date_query)
        max_campaign_bid_date = max_campaign_bid_processed_date_df.iat[0, 0]
        campaign_bid_start_date = get_max_date(max_campaign_bid_date)
        campaign_bid_df = get_campaign_bidding_data(api_client, account_id, campaign_bid_start_date, e_date)
        campaign_bid_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        if not campaign_bid_df.empty:
            insert_query = """INSERT INTO [GoogleAds].[CampaignPerformance_V2Bid] (
                       [Date]
                       ,[CustomerId]
                       ,[CampaignName]
                       ,[CampaignId]
                       ,[BiddingStrategyId]
                       ,[BiddingStrategyName]
                       ,[BiddingStrategyType]
                       ,[Conversions]
                       ,[BounceRate]
                       ,[Interactions]
                       ,[EngagementRate]
                       ,[Clicks]
                       ,[Impressions]
                       ,[Cost]
                       ,[CreateDate]
                       ) VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_bid_df, insert_query)


    except:
        print(sys.exc_info())


