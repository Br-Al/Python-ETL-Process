#!/usr/bin/python3
import sys
import pandas as pd
from datetime import datetime, timedelta

def date_validation(date_text):
    """
    Gets input date in text format and return  date in Year-Month-Day format

    Parameters
    ----------
    date_text : date
        The date in year,month and date format
    Returns
    -------
    The date in year-month-day format

    """
    try:
        while date_text != datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d'):
            Exception('*** Input Date does not match format yyyy-mm-dd ***')
        else:
            return datetime.strptime(date_text, '%Y-%m-%d').date()
    except:
        raise Exception('\n *** Function (date_validation) Failed ***')


def get_campaign_data(client, account_id, start_date, end_date):
    """
       Query and download campaign performance report for an Google ads account in a time range.

       Parameters
       ----------
       client: Boolean
           The right to access the Google ads account after providing user's credentials
       account_id : int
            An identification of an ads account
      s_date : date
           The starting date.
      e_date : date
           The end date

       Returns
       -------
        dictionary data in table and columns for campaign

    """
    try:

        start_date = date_validation(start_date)

        end_date = date_validation(end_date)

        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
          segments.date,
          customer.id, 
          campaign.id,
          campaign.name,
          metrics.cost_micros,
          metrics.clicks,
          metrics.impressions 
        FROM campaign
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000

                data_df = pd.concat(
                    [data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)


        return data_df
    except:
        print(sys.exc_info())


def get_ads_data(client, account_id, start_date, end_date):
    """
   Query and download ads group performance report for an Google ads account in a time range.

   Parameters
   ----------
   client: Boolean
       The right to access the Google ads account after providing user's credentials
   account_id : int
        An identification of an ads account
  s_date : date
       The starting date.
  e_date : date
       The end date

   Returns
   -------
    dictionary data in tables and columns of ads group report

"""
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)

        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
        segments.date,
        customer.id, 
        campaign.id,
        campaign.name,
        ad_group.name,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.search_impression_share
        FROM ad_group
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """' AND ad_group.status != 'REMOVED'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000
                tmp_dict['SearchImpressionShare'] = row.metrics.search_impression_share

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())

def get_geo_performance_data(client, account_id, start_date, end_date):
    """
      Query and download geographic performance report for an Google ads account in a time range.

      Parameters
      ----------
      client: Boolean
          The right to access the Google ads account after providing user's credentials
      account_id : int
           An identification of an ads account
      s_date : date
          The starting date.
      e_date : date
          The end date

      Returns
      -------
       dictionary data in table and columns of geographic report

    """
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)
        ga_service = client.get_service("GoogleAdsService")
        query = """
       SELECT 
       segments.date,
       geographic_view.country_criterion_id,
        customer.descriptive_name, 
        customer.id, 
        customer.time_zone,
         campaign.id,
          campaign.name,
         ad_group.id, 
         ad_group.name, 
          segments.geo_target_city,
            metrics.clicks,
             metrics.conversions, 
             metrics.impressions, 
             metrics.cost_micros 
               FROM geographic_view
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date

                tmp_dict['CustomerDescriptiveName'] = row.customer.descriptive_name
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CustomerTimeZone'] = row.customer.time_zone
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['AdGroupId'] = row.ad_group.id
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['CountryCriteriaId'] = row.geographic_view.country_criterion_id
                tmp_dict['CityCriteriaId'] = row.segments.geo_target_city

                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000
                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())

def get_countries_data(client, account_id):
    """
      Query and download list of countries for Google ads.

      Parameters
      ----------
      client: Boolean
          The right to access the Google ads account after providing user's credentials
      account_id : int
           An identification of an ads account
      Returns
      -------
       dictionary data in table and columns for countries

    """
    try:
        ga_service = client.get_service("GoogleAdsService")

        query = """
       SELECT 
       geo_target_constant.canonical_name,
       geo_target_constant.country_code,
       geo_target_constant.id
       
        FROM geo_target_constant """

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['CountryName'] = row.geo_target_constant.canonical_name
                tmp_dict['CountryCode'] = row.geo_target_constant.country_code
                tmp_dict['CountryCriteriaId'] = row.geo_target_constant.id
                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())

def get_landing_Page_data(client, account_id, start_date, end_date):
    """
      Query and download landing page report for an Google ads account in a time range.

      Parameters
      ----------
      client: Boolean
          The right to access the Google ads account after providing user's credentials
      account_id : int
           An identification of an ads account
      s_date : date
          The starting date.
      e_date : date
          The end date

      Returns
      -------
       dictionary data in tables and columns of landing page report

    """
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)
        ga_service = client.get_service("GoogleAdsService")

        query = """
       SELECT  
       segments.date, 
       customer.id, 
        campaign.name,
          campaign.id,
           ad_group.id, 
           ad_group.name, 
           ad_group.status,
           landing_page_view.unexpanded_final_url,
       metrics.conversions,
        metrics.clicks, 
        metrics.impressions, 
        metrics.cost_micros

        FROM landing_page_view
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['AdGroupId'] = row.ad_group.id
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['AdGroupStatus'] = row.ad_group.status
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['FinalUrl'] = row.landing_page_view.unexpanded_final_url

                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())


def get_keyword_view_data(client, account_id, start_date, end_date):
    """
      Query and download keyword performance report for an Google ads account in a time range.

      Parameters
      ----------
      client: Boolean
          The right to access the Google ads account after providing user's credentials
      account_id : int
           An identification of an ads account
      s_date : date
          The starting date.
      e_date : date
          The end date

      Returns
      -------
       dictionary data in tables and columns of keyword performance report

    """
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)
        ga_service = client.get_service("GoogleAdsService")

        query = """
       SELECT  
       segments.date,
       customer.id,
       customer.descriptive_name,
        customer.time_zone,
        keyword_view.resource_name,
        ad_group.id,
         ad_group.name,
        ad_group.type,
        campaign.id, 
        campaign.name,
        ad_group_criterion.display_name, 
        ad_group_criterion.criterion_id,
        bidding_strategy.id,
        bidding_strategy.name,
        campaign.bidding_strategy_type,
        bidding_strategy.status,
        bidding_strategy.resource_name,

        metrics.conversions,
        metrics.clicks,
        metrics.bounce_rate,
        metrics.interactions,
        metrics.engagement_rate,
        metrics.cost_micros
        FROM keyword_view
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['AccountDescriptiveName'] = row.customer.descriptive_name
                tmp_dict['CustomerTimeZone'] = row.customer.time_zone
                tmp_dict['AdGroupId'] = row.ad_group.id
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['AdGroupCriteriaId'] = row.ad_group_criterion.criterion_id
                tmp_dict['AdGroupCriteriaName'] = row.ad_group_criterion.display_name
                tmp_dict['BiddingStrategyId'] = row.bidding_strategy.id
                tmp_dict['BiddingtrategyName'] = row.bidding_strategy.name
                tmp_dict['BiddingStrategyType'] = row.campaign.bidding_strategy_type
                tmp_dict['BiddingStrageyStatus'] = row.bidding_strategy.status
                tmp_dict['ResourceName'] = row.bidding_strategy.resource_name

                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['BounceRate'] = row.metrics.bounce_rate
                tmp_dict['Interactions'] = row.metrics.interactions
                tmp_dict['EngagementRate'] = row.metrics.engagement_rate
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())


def get_bidding_strategy_data(client, account_id, start_date, end_date):
    """
      Query and download bidding strategy report for an Google ads account in a time range.

      Parameters
      ----------
      client: Boolean
          The right to access the Google ads account after providing user's credentials
      account_id : int
           An identification of an ads account
      s_date : date
          The starting date.
      e_date : date
          The end date

      Returns
      -------
       dictionary data in tables and columns of bidding strategy report

    """
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)
        ga_service = client.get_service("GoogleAdsService")

        query = """
       SELECT  
        segments.date,
        customer.id,
        customer.descriptive_name,
        customer.currency_code,
        customer.time_zone,
        bidding_strategy.id,
        bidding_strategy.name,
        bidding_strategy.resource_name,
        bidding_strategy.target_roas.target_roas,
        bidding_strategy.target_roas.cpc_bid_ceiling_micros,
        bidding_strategy.target_roas.cpc_bid_floor_micros,

        bidding_strategy.target_spend.cpc_bid_ceiling_micros,
        bidding_strategy.target_spend.target_spend_micros,
        bidding_strategy.target_impression_share.location,
        metrics.conversions,
        metrics.conversions_from_interactions_rate,
        metrics.all_conversions_value,
        metrics.clicks,
        metrics.impressions

        FROM bidding_strategy
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY customer.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['AccountDescriptiveName'] = row.customer.descriptive_name
                tmp_dict['CustomerCurrencyCode'] = row.customer.currency_code
                tmp_dict['CustomerTimeZone'] = row.customer.time_zone
                tmp_dict['StrategyId'] = row.bidding_strategy.id
                tmp_dict['StrategyName'] = row.bidding_strategy.name
                tmp_dict['ResourceName'] = row.bidding_strategy.resource_name
                tmp_dict['TargetRoas'] = row.bidding_strategy.target_roas.target_roas
                tmp_dict['TargetRoasBidCeiling'] = row.bidding_strategy.target_roas.cpc_bid_ceiling_micros
                tmp_dict['TargetRoasBidFloor'] = row.bidding_strategy.target_roas.cpc_bid_floor_micros
                tmp_dict['TargetSpendSpendTarget'] = row.bidding_strategy.target_spend.target_spend_micros
                tmp_dict['ShareImpressionLocation'] = row.bidding_strategy.target_impression_share.location

                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['ConversionRate'] = row.metrics.conversions_from_interactions_rate
                tmp_dict['ConversionValue'] = row.metrics.conversions_value
                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())

def get_ads_with_bid_data(client, account_id, start_date, end_date):
    """
   Query and download ads group with bid performance report for an Google ads account in a time range.

   Parameters
   ----------
   client: Boolean
       The right to access the Google ads account after providing user's credentials
   account_id : int
        An identification of an ads account
  s_date : date
       The starting date.
  e_date : date
       The end date

   Returns
   -------
    dictionary data in tables and columns of ads group report

"""
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)

        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
        segments.date,
        customer.id, 
        campaign.id,
        campaign.name,
        ad_group.id,
        ad_group.name,
        bidding_strategy.id,
        campaign.bidding_strategy_type,
        bidding_strategy.name, 
        metrics.conversions,
        metrics.bounce_rate,
        metrics.interactions,
        metrics.engagement_rate,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.search_impression_share
        FROM ad_group
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """' AND ad_group.status != 'REMOVED'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['AdGroupId'] = row.ad_group.id
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['BiddingStrategyId'] = row.bidding_strategy.id
                tmp_dict['BiddingStrategyType'] = row.campaign.bidding_strategy_type
                tmp_dict['BiddingStrategyName'] = row.bidding_strategy.name
                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['BounceRate'] = row.metrics.bounce_rate
                tmp_dict['Interactions'] = row.metrics.interactions
                tmp_dict['EngagementRate'] = row.metrics.engagement_rate

                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000
                tmp_dict['SearchImpressionShare'] = row.metrics.search_impression_share

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())


def get_campaign_bidding_data(client, account_id, start_date, end_date):
    """
       Query and download campaign with bid performance report for an Google ads account in a time range.

       Parameters
       ----------
       client: Boolean
           The right to access the Google ads account after providing user's credentials
       account_id : int
            An identification of an ads account
      s_date : date
           The starting date.
      e_date : date
           The end date

       Returns
       -------
        dictionary data in table and columns for campaign with bid

    """
    try:

        start_date = date_validation(start_date)

        end_date = date_validation(end_date)

        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
          segments.date,
          customer.id, 
          campaign.id,
          campaign.name,
          bidding_strategy.id, 
          bidding_strategy.name,
          campaign.bidding_strategy_type,
          metrics.conversions,
          metrics.bounce_rate,
        metrics.interactions,
        metrics.engagement_rate,
          metrics.cost_micros,
          metrics.clicks,
          metrics.impressions 
        FROM campaign
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['BiddingStrategyId'] = row.bidding_strategy.id
                tmp_dict['BiddingStrategyName'] = row.bidding_strategy.name
                tmp_dict['BiddingStrategyType'] = row.campaign.bidding_strategy_type
                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['BounceRate'] = row.metrics.bounce_rate
                tmp_dict['Interactions'] = row.metrics.interactions
                tmp_dict['EngagementRate'] = row.metrics.engagement_rate
                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000

                data_df = pd.concat(
                    [data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)


        return data_df
    except:
        print(sys.exc_info())


def get_ads_bid_craft_AI_data(client, account_id, start_date, end_date):
    """
   Query and download ads group performance report for an Google ads account in a time range.

   Parameters
   ----------
   client: Boolean
       The right to access the Google ads account after providing user's credentials
   account_id : int
        An identification of an ads account
  s_date : date
       The starting date.
  e_date : date
       The end date

   Returns
   -------
    dictionary data in tables and columns of ads group report

"""
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)

        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
         segments.date
         , customer.id
         , customer.descriptive_name
         , customer.currency_code
         , campaign.id
         , campaign.name
         , campaign.bidding_strategy
         , campaign.bidding_strategy_type
         , ad_group.id
         , ad_group.name
         , ad_group.effective_target_roas
         , ad_group.effective_target_cpa_micros
         , ad_group.cpc_bid_micros
         , ad_group.cpv_bid_micros
         , ad_group.target_cpa_micros
         , ad_group.target_roas
         , bidding_strategy.id
         , bidding_strategy.name
         ,bidding_strategy.type
         , metrics.clicks
         , metrics.impressions
         , metrics.conversions
         , metrics.conversions_value
         , metrics.interactions
         , metrics.cost_micros
         , metrics.engagement_rate
         , metrics.bounce_rate

         FROM ad_group
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """' AND ad_group.status != 'REMOVED'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CustomerDescriptiveName'] = row.customer.descriptive_name
                tmp_dict['CurrencyCode'] = row.customer.currency_code
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['CampaignBiddingStrategy'] = row.campaign.bidding_strategy
                tmp_dict['CampaignBiddingStrategyType'] = row.campaign.bidding_strategy_type
                tmp_dict['AdGroupId'] = row.ad_group.id
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['AdGroup_effective_target_roas'] = row.ad_group.effective_target_roas
                tmp_dict['AdGroup_effective_target_cpa_micros'] = row.ad_group.effective_target_cpa_micros / 1000000
                tmp_dict['AdGroup_cpc_bid_micros'] = row.ad_group.cpc_bid_micros / 1000000
                tmp_dict['AdGroup_cpv_bid_micros'] = row.ad_group.cpv_bid_micros / 1000000
                tmp_dict['AdGroup_target_cpa_micros'] = row.ad_group.target_cpa_micros / 1000000
                tmp_dict['AdGroup_target_roas'] = row.ad_group.target_roas
                tmp_dict['BiddingStrategyId'] = row.bidding_strategy.id
                tmp_dict['BiddingStrategyName'] = row.bidding_strategy.name
                tmp_dict['BiddingStrategyType'] = row.bidding_strategy.type_

                tmp_dict['Clicks'] = row.metrics.clicks
                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Conversions'] = row.metrics.conversions
                tmp_dict['Conversions_value'] = row.metrics.conversions_value
                tmp_dict['Interactions'] = row.metrics.interactions
                tmp_dict['Cost_micros'] = row.metrics.cost_micros / 1000000
                tmp_dict['EngagementRate'] = row.metrics.engagement_rate
                tmp_dict['BounceRate'] = row.metrics.bounce_rate

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())


def get_ads_bid_craft_ai_conv_data(client, account_id, start_date, end_date):
    """
   Query and download ads group performance report for an Google ads account in a time range.

   Parameters
   ----------
   client: Boolean
       The right to access the Google ads account after providing user's credentials
   account_id : int
        An identification of an ads account
  s_date : date
       The starting date.
  e_date : date
       The end date

   Returns
   -------
    dictionary data in tables and columns of ads group report

"""
    try:
        start_date = date_validation(start_date)
        end_date = date_validation(end_date)

        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
         segments.date
         ,customer.id
         , campaign.id
         , campaign.name
         , ad_group.id
         , ad_group.name
         , metrics.all_conversions
         ,segments.conversion_action_name
         , metrics.conversions_value

         FROM ad_group
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """' AND ad_group.status != 'REMOVED'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)

        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['AdGroupId'] = row.ad_group.id
                tmp_dict['AdGroupName'] = row.ad_group.name
                tmp_dict['ConversionAction'] = row.segments.conversion_action_name
                tmp_dict['Conversions'] = row. metrics.all_conversions
                tmp_dict['Conversions_value'] = row.metrics.conversions_value

                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())

def get_campaign_budget_data(client, account_id, start_date, end_date):
    """
       Query and download campaign performance report for an Google ads account in a time range.

       Parameters
       ----------
       client: Boolean
           The right to access the Google ads account after providing user's credentials
       account_id : int
            An identification of an ads account
      s_date : date
           The starting date.
      e_date : date
           The end date

       Returns
       -------
        dictionary data in table and columns for campaign

    """
    try:

        start_date = date_validation(start_date)
        end_date = date_validation(end_date)
        ga_service = client.get_service("GoogleAdsService")

        query = """
        SELECT
          segments.date,
          customer.id, 
          campaign.id,
          campaign.name,
          campaign_budget.amount_micros,
            metrics.search_impression_share,
           metrics.search_budget_lost_impression_share,
         metrics.search_exact_match_impression_share,
          metrics.absolute_top_impression_percentage,
          metrics.impressions,
          metrics.cost_micros 
        FROM campaign
        WHERE segments.date BETWEEN '""" + str(start_date) + """' AND '""" + str(end_date) + """'
        ORDER BY campaign.id"""

        search_request = client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        data_df = pd.DataFrame()

        for batch in response:
            for row in batch.results:
                tmp_dict = {}
                tmp_dict['Date'] = row.segments.date
                tmp_dict['CustomerId'] = row.customer.id
                tmp_dict['CampaignId'] = row.campaign.id
                tmp_dict['CampaignName'] = row.campaign.name
                tmp_dict['CampaignBudget'] = row.campaign_budget.amount_micros / 1000000
                tmp_dict['SearchImpressionShare'] = row.metrics.search_impression_share
                tmp_dict['SearchBudgetLostImpressionShare'] = row.metrics.search_budget_lost_impression_share
                tmp_dict['SearchExactMatchImpressionShare'] = row.metrics.search_exact_match_impression_share
                tmp_dict['AbsoluteTopImpression'] = row.metrics.absolute_top_impression_percentage
                tmp_dict['Impressions'] = row.metrics.impressions
                tmp_dict['Cost'] = row.metrics.cost_micros / 1000000
                data_df = pd.concat([data_df, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

        return data_df
    except:
        print(sys.exc_info())
