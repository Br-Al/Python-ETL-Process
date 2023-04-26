import numpy as np
from common.db import Database
from common.config import get_api_config
from utils.Bing_ads_authentication import *
from common.date_tools import get_max_date

from utils.get_bings_ads_performance_reports import get_campaign_report, get_adgroup_report, \
    get_geo_performance_report, get_desturi_report, download_campaign_report, download_adgroup_report, \
    download_geo_performance_report, download_desturi_report,get_keyword_report,download_get_keyword_report,get_keyword_craftai_report,download_get_keyword_craft_ai_report

db = Database()
today = datetime.today()
delta = timedelta(days=1)
yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')

if __name__ == '__main__':
    try:
        timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d : %H:%M')
        e_date = yesterday
        qry_type = "day"

        MY_BING_PATH = 'C:/bing_ads_cred.json'
        cred_json = get_api_config(MY_BING_PATH)
        client_id = cred_json["client_id"]
        developer_token = cred_json["developer_token"]
        refresh_token = cred_json["refresh_token"]
        account_id = cred_json["Soda_pdf_account_id"]

        authorization_data = ms_auth(refresh_token, client_id, developer_token)
        max_campaign_date_query = """SELECT MAX(TimePeriod)as new_date
                FROM [BingAds].[CampaignPerformanceReport_V2] WHERE [AccountName] = 'LULU Software'"""

        max_campaign_processed_date_df = db.get_query_df(max_campaign_date_query)
        max_campaign_date = max_campaign_processed_date_df.iat[0, 0]
        campaign_start_date = get_max_date(max_campaign_date)

        report_request_campaign = get_campaign_report(authorization_data, account_id, campaign_start_date , e_date, qry_type)
        campaign_analytics_data = download_campaign_report(report_request_campaign, authorization_data, campaign_start_date ,
                                                           e_date,
                                                           qry_type)
        campaign_analytics_data['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        campaign_analytics_data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        campaign_analytics_data = campaign_analytics_data.fillna(0)

        if not campaign_analytics_data.empty:
            insert_query = """INSERT INTO [BingAds].[CampaignPerformanceReport_V2] (
            [TimePeriod]
          ,[AccountName]
          ,[AccountNumber]
          ,[AccountId]
          ,[AccountStatus]
          ,[CampaignName]
          ,[CampaignId]
          ,[CampaignLabels]
          ,[CampaignStatus]
          ,[CurrencyCode]
          ,[AdDistribution]
          ,[Network]
          ,[DeliveredMatchType]
          ,[Impressions]
          ,[AbsoluteTopImpressionSharePercent]
          ,[Clicks]
          ,[Ctr]
          ,[AverageCpc]
          ,[Spend]
          ,[AveragePosition]
          ,[Conversions]
          ,[ConversionRate]
          ,[ExactMatchImpressionSharePercent]
          ,[ImpressionSharePercent]
          ,[QualityScore]
          ,[Week]
          ,[CreateDate]
          )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_analytics_data, insert_query)

        max_ad_date_query = """SELECT MAX(TimePeriod)as new_date
                        FROM [BingAds].[AdPerformanceReport_V2] WHERE [AccountName] = 'LULU Software'"""

        max_ads_processed_date_df = db.get_query_df(max_ad_date_query)
        max_ads_date = max_ads_processed_date_df.iat[0, 0]
        ad_start_date = get_max_date(max_ads_date)
        report_request_adgroup = get_adgroup_report(authorization_data, account_id, ad_start_date, e_date, qry_type)
        ads_analytics_data = download_adgroup_report(report_request_adgroup, authorization_data, ad_start_date, e_date,
                                                     qry_type)

        ads_analytics_data['CreateDate'] = datetime.now().strftime("%Y-%m-%d")
        ads_analytics_data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        ads_analytics_data = ads_analytics_data.fillna(0)


        if not ads_analytics_data.empty:
            insert_query = """INSERT INTO [BingAds].[AdPerformanceReport_V2] (
            [TimePeriod]
          ,[AccountName]
          ,[AccountNumber]
          ,[AccountId]
          ,[Status]
          ,[CampaignName]
          ,[CampaignId]
          ,[AdGroupName]
          ,[AdGroupId]
          ,[CurrencyCode]
          ,[AdDistribution]
          ,[Network]
          ,[AccountStatus]
          ,[CampaignStatus]
          ,[Language]
          ,[DeliveredMatchType]
          ,[Impressions]
          ,[AbsoluteTopImpressionSharePercent]
          ,[Clicks]
          ,[Ctr]
          ,[Spend]
          ,[AveragePosition]
          ,[Conversions]
          ,[ConversionRate]
          ,[ExactMatchImpressionSharePercent]
          ,[ImpressionSharePercent]
          ,[QualityScore]
          ,[AdRelevance]
          ,[Week]
          ,[CreateDate]

          )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(ads_analytics_data, insert_query)

        max_geo_date_query = """SELECT MAX(TimePeriod)as new_date
                                FROM [BingAds].[GeographicPerformanceReport_V2] WHERE [AccountName] = 'LULU Software'"""

        max_geo_processed_date_df = db.get_query_df(max_geo_date_query)
        max_geo_date = max_geo_processed_date_df.iat[0, 0]
        geo_start_date = get_max_date(max_geo_date)

        report_request = get_geo_performance_report(authorization_data, account_id, geo_start_date, e_date, qry_type)
        geo_analytics_data = download_geo_performance_report(report_request, authorization_data, geo_start_date, e_date,
                                                             qry_type)
        geo_analytics_data['CreateDate'] = datetime.now().strftime("%Y-%m-%d")
        geo_analytics_data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        geo_analytics_data = geo_analytics_data.fillna(0)

        if not geo_analytics_data.empty:
            insert_query = """INSERT INTO [BingAds].[GeographicPerformanceReport_V2] (
            [TimePeriod]
          ,[AccountName]
          ,[AccountNumber]
          ,[AccountId]
          ,[AdGroupId]
          ,[CampaignId]
          ,[AdGroupName]
          ,[CampaignName]
          ,[Language]
          ,[AccountStatus]
          ,[CampaignStatus]
          ,[AdGroupStatus]
          ,[Country]
          ,[CurrencyCode]
          ,[AdDistribution]
          ,[ProximityTargetLocation]
          ,[Radius]
          ,[BidMatchType]
          ,[DeliveredMatchType]
          ,[Network]
          ,[TopVsOther]
          ,[DeviceType]
          ,[LocationType]
          ,[DeviceOS]
          ,[Impressions]
          ,[Clicks]
          ,[Ctr]
          ,[AverageCpc]
          ,[Spend]
          ,[AveragePosition]
          ,[Assists]
          ,[Conversions]
          ,[ConversionRate]
          ,[CostPerConversion]
          ,[Revenue]
          ,[ReturnOnAdSpend]
          ,[CostPerAssist]
          ,[RevenuePerConversion]
          ,[RevenuePerAssist]
          ,[Week]
          ,[CreateDate]
          )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(geo_analytics_data, insert_query)

        max_urli_date_query = """SELECT MAX(TimePeriod)as new_date
                                        FROM [BingAds].[DestinationUrlPerformanceReport_V2] WHERE [AccountName] = 'LULU Software'"""

        max_urli_processed_date_df = db.get_query_df(max_urli_date_query)
        max_urli_date = max_urli_processed_date_df.iat[0, 0]
        urli_start_date = get_max_date(max_urli_date)

        report_request_url = get_desturi_report(authorization_data, account_id, urli_start_date, e_date, qry_type)
        desti_URL_analytics_data = download_desturi_report(report_request_url, authorization_data, urli_start_date, e_date,
                                                           qry_type)
        desti_URL_analytics_data['CreateDate'] = datetime.now().strftime("%Y-%m-%d")
        desti_URL_analytics_data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        desti_URL_analytics_data = desti_URL_analytics_data.fillna(0)

        if not desti_URL_analytics_data.empty:
            insert_query = """INSERT INTO [BingAds].[DestinationUrlPerformanceReport_V2] (
            [TimePeriod]
          ,[AccountName]
          ,[AccountNumber]
          ,[AccountId]
          ,[CampaignName]
          ,[CampaignId]
          ,[AdGroupName]
          ,[AdGroupId]
          ,[CurrencyCode]
          ,[AdDistribution]
          ,[Network]
          ,[AccountStatus]
          ,[CampaignStatus]
          ,[Language]
          ,[DeliveredMatchType]
          ,[DestinationUrl]
          ,[FinalUrl]
          ,[Impressions]
          ,[Clicks]
          ,[Ctr]
          ,[Spend]
          ,[AveragePosition]
          ,[Conversions]
          ,[ConversionRate]
          ,[Week]
          ,[CreateDate]

          )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(desti_URL_analytics_data, insert_query)

        max_keyword_date_query = """SELECT MAX(TimePeriod)as new_date
                                        FROM [BingAds].[KeywordPerformanceReport_V2] WHERE [AccountName] = 'LULU Software'"""

        max_keyword_processed_date_df = db.get_query_df(max_keyword_date_query)
        max_keyword_date = max_keyword_processed_date_df.iat[0, 0]
        keyword_start_date = get_max_date(max_keyword_date)
        report_request_keyword = get_keyword_craftai_report(authorization_data, account_id, keyword_start_date, e_date,
                                                    qry_type)
        keyword_analytics_data = download_get_keyword_craft_ai_report(report_request_keyword, authorization_data,
                                                             keyword_start_date,
                                                             e_date,
                                                             qry_type)

        keyword_analytics_data['CreateDate'] = datetime.now().strftime("%Y-%m-%d")
        keyword_analytics_data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        keyword_analytics_data = keyword_analytics_data.fillna(0)

        if not keyword_analytics_data.empty:
            insert_query = """INSERT INTO [BingAds].[KeywordPerformanceReport_V2] (
                            [TimePeriod]
                          ,[AccountName]
                          ,[AccountNumber]
                          ,[AccountId]
                          ,[CampaignName]
                          ,[CampaignId]
                          ,[AdGroupName]
                          ,[AdGroupId]
                          ,[Keyword]
                          ,[KeywordId]
                          ,[FirstPageBid]
                          ,[DeviceType]
                          ,[CurrentMaxCpc]
                          ,[Clicks]
                          ,[Impressions]
                          ,[Conversions]
                          ,[ConversionRate]
                          ,[AverageCpc]
                          ,[Spend]
                          ,[week]
                          ,[CreateDate]

                          )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(keyword_analytics_data, insert_query)


    except:
        print(sys.exc_info())


