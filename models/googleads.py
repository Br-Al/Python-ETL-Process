import sys
import os
from xmlrpc.client import boolean
from google.ads.googleads.client import GoogleAdsClient
from datetime import datetime
import pandas as pd
from models.db import Database
import utils.googleads as ga

class GoogleAds():
    def __init__(self, resource_name=None, account_id=None, start_date=None, end_date=None, fields:list=None) -> None:
        if resource_name:
            self.resource_name = resource_name
        if account_id:
            self.account_id = account_id
        if start_date:
            self.start_date = self.date_validation(start_date)
        if end_date:
            self.end_date = self.date_validation(end_date)
        if fields:
            self.fields = fields
        self.client = self.authenticate()
        self.dwh_db = self.dwh_connect()

    def dwh_connect(self):
        host = os.environ.get('GOOGLE_ADS_DBHOST', "")
        user_id = os.environ.get('GOOGLE_ADS_DBUSER', "")
        password = os.environ.get('GOOGLE_ADS_DBPASS', "")
        db_name = os.environ.get("GOOGLE_ADS_DBNAME", "")
        db = Database(server = host, user_id = user_id, password = password, name = db_name)
        db.pyodbc_connect()

        return db

    def authenticate(self):
        """
        Gets user's credentials and grant access to data of Google ads.

        Parameters
        ----------
        None : 

        Returns
        -------
        Access to dictionary data.

        """
        try:
            client = GoogleAdsClient.load_from_env()
            return client

        except Exception as e:
            print(sys.exc_info())
    
    def date_validation(self, date_text):
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

    def load_to_dwh(self, data, query)->None:
        if not data.empty:
            try:
                self.dwh_db.insert_many(data, query)
            except Exception as e:
                print(f"Error:{self.resource_name} {e}")

    def get_campaign_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}'")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                    'Date' : row.segments.date,
                    'CustomerId' : row.customer.id,
                    'CampaignName' : row.campaign.name,
                    'CampaignId' : row.campaign.id,
                    'Impressions' : row.metrics.impressions,
                    'Clicks' : row.metrics.clicks,
                    'Cost' : row.metrics.cost_micros / 1000000,} 
                    for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_ads_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' "
                        f"AND ad_group.status != 'REMOVED'")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [
                {
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'CampaignId': row.campaign.id,
                'CampaignName': row.campaign.name,
                'AdGroupName': row.ad_group.name,
                'Impressions': row.metrics.impressions,
                'Cost': row.metrics.cost_micros/1000000,
                'Clicks': row.metrics.clicks,
                'SearchImpressionShare':row.metrics.search_impression_share
                } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        return data_df

    def get_countries_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} ")
        
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'CountryName': row.geo_target_constant.canonical_name,
                'CountryCode': row.geo_target_constant.country_code,
                'CountryCriteriaId': row.geo_target_constant.id
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")
        
        return data_df

    def get_ads_bid_craft_ai_conv_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' "
                        f"AND ad_group.status != 'REMOVED'")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'CampaignId': row.campaign.id,
                'CampaignName': row.campaign.name,
                'AdGroupId': row.ad_group.id,
                'AdGroupName': row.ad_group.name,
                'ConversionAction': row.segments.conversion_action_name,
                'Conversions': row. metrics.all_conversions,
                'Conversions_value': row.metrics.conversions_value,
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_ads_bid_craft_AI_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' "
                        f"AND ad_group.status != 'REMOVED'")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'CustomerDescriptiveName': row.customer.descriptive_name,
                'CurrencyCode': row.customer.currency_code,
                'CampaignId': row.campaign.id,
                'CampaignName': row.campaign.name,
                'CampaignBiddingStrategy': row.campaign.bidding_strategy,
                'CampaignBiddingStrategyType': row.campaign.bidding_strategy_type,
                'AdGroupId': row.ad_group.id,
                'AdGroupName': row.ad_group.name,
                'AdGroup_effective_target_roas': row.ad_group.effective_target_roas,
                'AdGroup_effective_target_cpa_micros': row.ad_group.effective_target_cpa_micros / 1000000,
                'AdGroup_cpc_bid_micros': row.ad_group.cpc_bid_micros / 1000000,
                'AdGroup_cpv_bid_micros': row.ad_group.cpv_bid_micros / 1000000,
                'AdGroup_target_cpa_micros': row.ad_group.target_cpa_micros / 1000000,
                'AdGroup_target_roas': row.ad_group.target_roas,
                'BiddingStrategyId': row.bidding_strategy.id,
                'BiddingStrategyName': row.bidding_strategy.name,
                'BiddingStrategyType': row.bidding_strategy.type_,
                'Clicks': row.metrics.clicks,
                'Impressions': row.metrics.impressions,
                'Conversions': row.metrics.conversions,
                'Conversions_value': row.metrics.conversions_value,
                'Interactions': row.metrics.interactions,
                'Cost_micros': row.metrics.cost_micros / 1000000,
                'EngagementRate': row.metrics.engagement_rate,
                'BounceRate': row.metrics.bounce_rate
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_campaign_bidding_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}'")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'CampaignName': row.campaign.name,
                'CampaignId': row.campaign.id,
                'BiddingStrategyId': row.bidding_strategy.id,
                'BiddingStrategyName': row.bidding_strategy.name,
                'BiddingStrategyType': row.campaign.bidding_strategy_type,
                'Conversions': row.metrics.conversions,
                'BounceRate': row.metrics.bounce_rate,
                'Interactions': row.metrics.interactions,
                'EngagementRate': row.metrics.engagement_rate,
                'Clicks': row.metrics.clicks,
                'Impressions': row.metrics.impressions,
                'Cost': row.metrics.cost_micros / 1000000
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_ads_with_bid_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' "
                        f"AND ad_group.status != 'REMOVED'")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'CampaignId': row.campaign.id,
                'CampaignName': row.campaign.name,
                'AdGroupId': row.ad_group.id,
                'AdGroupName': row.ad_group.name,
                'BiddingStrategyId': row.bidding_strategy.id,
                'BiddingStrategyType': row.campaign.bidding_strategy_type,
                'BiddingStrategyName': row.bidding_strategy.name,
                'Conversions': row.metrics.conversions,
                'BounceRate': row.metrics.bounce_rate,
                'Interactions': row.metrics.interactions,
                'EngagementRate': row.metrics.engagement_rate,
                'Impressions': row.metrics.impressions,
                'Clicks': row.metrics.clicks,
                'Cost': row.metrics.cost_micros / 1000000,
                'SearchImpressionShare': row.metrics.search_impression_share
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_bidding_strategy_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' ")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY customer.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'AccountDescriptiveName': row.customer.descriptive_name,
                'CustomerCurrencyCode': row.customer.currency_code,
                'CustomerTimeZone': row.customer.time_zone,
                'StrategyId': row.bidding_strategy.id,
                'StrategyName': row.bidding_strategy.name,
                'ResourceName': row.bidding_strategy.resource_name,
                'TargetRoas': row.bidding_strategy.target_roas.target_roas,
                'TargetRoasBidCeiling': row.bidding_strategy.target_roas.cpc_bid_ceiling_micros,
                'TargetRoasBidFloor': row.bidding_strategy.target_roas.cpc_bid_floor_micros,
                'TargetSpendSpendTarget': row.bidding_strategy.target_spend.target_spend_micros,
                'ShareImpressionLocation': row.bidding_strategy.target_impression_share.location,
                'Conversions': row.metrics.conversions,
                'ConversionRate': row.metrics.conversions_from_interactions_rate,
                'ConversionValue': row.metrics.conversions_value,
                'Clicks': row.metrics.clicks,
                'Cost': row.metrics.cost_micros / 1000000
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_keyword_view_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' ")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
            'Date': row.segments.date,
            'CustomerId': row.customer.id,
            'AccountDescriptiveName': row.customer.descriptive_name,
            'CustomerTimeZone': row.customer.time_zone,
            'AdGroupId': row.ad_group.id,
            'AdGroupName': row.ad_group.name,
            'CampaignId': row.campaign.id,
            'CampaignName': row.campaign.name,
            'AdGroupCriteriaId': row.ad_group_criterion.criterion_id,
            'AdGroupCriteriaName': row.ad_group_criterion.display_name,
            'BiddingStrategyId': row.bidding_strategy.id,
            'BiddingtrategyName': row.bidding_strategy.name,
            'BiddingStrategyType': row.campaign.bidding_strategy_type,
            'BiddingStrageyStatus': row.bidding_strategy.status,
            'ResourceName': row.bidding_strategy.resource_name,
            'Clicks': row.metrics.clicks,
            'Conversions': row.metrics.conversions,
            'BounceRate': row.metrics.bounce_rate,
            'Interactions': row.metrics.interactions,
            'EngagementRate': row.metrics.engagement_rate,
            'Cost': row.metrics.cost_micros / 1000000 
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")
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
            data_df['BiddingStrategyType'] = data_df['BiddingStrategyType'].map(map_bidding_data)
        
        return data_df

    def get_landing_Page_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' ")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerId': row.customer.id,
                'AdGroupId': row.ad_group.id,
                'AdGroupName': row.ad_group.name,
                'AdGroupStatus': row.ad_group.status,
                'CampaignId': row.campaign.id,
                'CampaignName': row.campaign.name,
                'FinalUrl': row.landing_page_view.unexpanded_final_url,
                'Impressions': row.metrics.impressions,
                'Conversions': row.metrics.conversions,
                'Clicks': row.metrics.clicks,
                'Cost': row.metrics.cost_micros / 1000000
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")

        
        return data_df

    def get_geo_performance_data(self):
        data_df = pd.DataFrame()
        ga_service = self.client.get_service("GoogleAdsService")
        conditions =  (f"segments.date BETWEEN '{str(self.start_date)}' AND '{str(self.end_date)}' ")
        query = (f"SELECT {','.join(self.fields)} " 
                f"FROM {self.resource_name} "
                f"WHERE {conditions} "
                f"ORDER BY campaign.id")
        search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
        search_request.customer_id = self.account_id
        search_request.query = query
        response = ga_service.search_stream(search_request)
        if response:
            data = [{
                'Date': row.segments.date,
                'CustomerDescriptiveName': row.customer.descriptive_name,
                'CustomerId': row.customer.id,
                'CustomerTimeZone': row.customer.time_zone,
                'CampaignId': row.campaign.id,
                'CampaignName': row.campaign.name,
                'AdGroupId': row.ad_group.id,
                'AdGroupName': row.ad_group.name,
                'CountryCriteriaId': row.geographic_view.country_criterion_id,
                'CityCriteriaId': row.segments.geo_target_city,
                'Clicks': row.metrics.clicks,
                'Conversions': row.metrics.conversions,
                'Impressions': row.metrics.impressions,
                'Cost': row.metrics.cost_micros / 1000000
            } for batch in response for row in batch.results]
            data_df = pd.DataFrame(data)
            data_df['create_date'] = datetime.now().strftime("%Y-%m-%d")
 
        return data_df

    


        
