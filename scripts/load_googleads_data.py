from datetime import datetime, timedelta
import sys, os
from dotenv import load_dotenv

sys.path.append("C:\\Users\\bill.somen\\avanquest\\load-to-dwh")
load_dotenv(os.path.join(os.environ.get("DOT_ENV_FILE_PATH", "./.env")))
from models.googleads import GoogleAds
import os
import sys
from utils.googleads import get_start_date

def ETL(account_id):
    today = datetime.today()
    end_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    if not account_id:
        return
    
    try:
        os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"] = account_id
        # Load campaign data to DWH
        campaign_start_date = get_start_date(account_id, "campaignPerformance_V2")
        if campaign_start_date:
            campaign_fields = [
                "segments.date",
                "customer.id", 
                "campaign.id",
                "campaign.name",
                "metrics.cost_micros",
                "metrics.clicks",
                "metrics.impressions"]
            campaign = GoogleAds('campaign', account_id, campaign_start_date, end_date, campaign_fields)
            query = (f"INSERT INTO [GoogleAds].[CampaignPerformance_V2] ([Date], [CustomerId],"
                    f"[CampaignName], [CampaignId], [Clicks], [Impressions], [Cost], [CreateDate]) "
                    f"VALUES (?, ?, ?, ?, ?, ?,?,?)")
            campaign_data = campaign.get_campaign_data()
            campaign.load_to_dwh(campaign_data, query)
        # Load ad_group data to DWH
        ad_start_date = get_start_date(account_id, "AdgroupPerformance_V2")
        if ad_start_date:
            ads_fields = [
                "segments.date",
                "customer.id", 
                "campaign.id",
                "campaign.name",
                "ad_group.name",
                "metrics.impressions",
                "metrics.clicks",
                "metrics.cost_micros",
                "metrics.search_impression_share"]
            ads = GoogleAds('ad_group', account_id, ad_start_date, end_date, ads_fields)
            query = (f"INSERT INTO [GoogleAds].[AdgroupPerformance_V2] ([Date],[CustomerId],"
                    f"[CampaignId],[CampaignName],[AdGroupName],[Impressions],[Clicks],[Cost],"
                    f"[SearchImpressionShare],[CreateDate]) "
                    f"VALUES (?, ?, ?, ?, ?, ?,?,?,?,?)")
            ads_data = ads.get_ads_data()
            ads.load_to_dwh(ads_data, query)

        # Load landing_page_view data to DWH
        landing_start_date = get_start_date(account_id, "LandingPage_V2")
        if landing_start_date:
            landing_page_fields = [
                "segments.date", 
                "customer.id", 
                "campaign.name",
                "campaign.id",
                "ad_group.id", 
                "ad_group.name", 
                "ad_group.status",
                "landing_page_view.unexpanded_final_url",
                "metrics.conversions",
                "metrics.clicks", 
                "metrics.impressions", 
                "metrics.cost_micros",
            ]
            landing_page = GoogleAds('landing_page_view', account_id, landing_start_date, end_date, landing_page_fields)
            query = (f"INSERT INTO [GoogleAds].[LandingPage_V2] ([Date],[CustomerId],[AdGroupId],"
                    f"[AdGroupName],[AdGroupStatus],[CampaignId],[CampaignName],[FinalUrl],"
                    f"[Impressions],[Conversions],[Clicks],[Cost],[CreateDate]) "
                    f"VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?)")
            landing_page_data = landing_page.get_landing_Page_data()
            landing_page.load_to_dwh(landing_page_data, query)
            
        # Load geographic_view data to DWH
        geo_start_date = get_start_date(account_id, "GeoPerformance_V2")
        if geo_start_date:
            geo_fields = [
                "segments.date",
                "geographic_view.country_criterion_id",
                "customer.descriptive_name", 
                "customer.id", 
                "customer.time_zone",
                "campaign.id",
                "campaign.name",
                "ad_group.id", 
                "ad_group.name", 
                "segments.geo_target_city",
                "metrics.clicks",
                "metrics.conversions", 
                "metrics.impressions", 
                "metrics.cost_micros",
            ]
            geo = GoogleAds('geographic_view', account_id, geo_start_date, end_date, geo_fields)
            query = (f"INSERT INTO [GoogleAds].[GeoPerformance_V2] ([Date],[CustomerDescriptiveName],"
                            f"[CustomerID],[CustomerTimeZone],[CampaignId],[CampaignName],[AdGroupId],[AdGroupName],"
                            f"[CountryCriteriaId],[CityCriteriaId],[Clicks],[Conversions],[Impressions],[Cost],[CreateDate]) "
                            f"VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?)") 
            geo_data = geo.get_geo_performance_data()
            geo.load_to_dwh(geo_data, query)

        # Load campaign bidding data to DWH
        campaign_bid_start_date = get_start_date(account_id, "CampaignPerformance_V2Bid")
        if campaign_bid_start_date:
            campaign_bidding_fields = [
                "segments.date",
                "customer.id", 
                "campaign.id",
                "campaign.name",
                "bidding_strategy.id", 
                "bidding_strategy.name",
                "campaign.bidding_strategy_type",
                "metrics.conversions",
                "metrics.bounce_rate",
                "metrics.interactions",
                "metrics.engagement_rate",
                "metrics.cost_micros",
                "metrics.clicks",
                "metrics.impressions"
            ]
            campaign_bidding = GoogleAds("campaign", account_id, campaign_bid_start_date, end_date, campaign_bidding_fields)
            query = (f"INSERT INTO [GoogleAds].[CampaignPerformance_V2Bid] ([Date],[CustomerId],"
                            f"[CampaignName],[CampaignId],[BiddingStrategyId],[BiddingStrategyName],[BiddingStrategyType],"
                            f"[Conversions],[BounceRate],[Interactions],[EngagementRate],[Clicks],[Impressions],[Cost],[CreateDate]) "
                            f"VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?)")
            campaign_bidding_data = campaign_bidding.get_campaign_bidding_data()
            campaign_bidding.load_to_dwh(campaign_bidding_data, query)

        # Load keyword_view data to DWH
        keyword_start_date = get_start_date(account_id, "KeywordPerformanceReport_V2")
        if keyword_start_date:
            keyword_view_fields = [
                "segments.date",
                "customer.id",
                "customer.descriptive_name",
                "customer.time_zone",
                "keyword_view.resource_name",
                "ad_group.id",
                "ad_group.name",
                "ad_group.type",
                "campaign.id", 
                "campaign.name",
                "ad_group_criterion.display_name", 
                "ad_group_criterion.criterion_id",
                "bidding_strategy.id",
                "bidding_strategy.name",
                "campaign.bidding_strategy_type",
                "bidding_strategy.status",
                "bidding_strategy.resource_name",
                "metrics.conversions",
                "metrics.clicks",
                "metrics.bounce_rate",
                "metrics.interactions",
                "metrics.engagement_rate",
                "metrics.cost_micros"
            ]
            keyword_view = GoogleAds('keyword_view', account_id, keyword_start_date, end_date, keyword_view_fields)
            query = (f"INSERT INTO [GoogleAds].[KeywordPerformanceReport_V2] ("
                    f"[Date],[CustomerId],[AccountDescriptiveName],[CustomerTimeZone],"
                    f"[AdGroupID],[AdGroupName],[CampaignId],[CampaignName],[AdGroupCriteriaID],"
                    f"[AdGroupCriteriaName],[BiddingStrategyId],[BiddingStrategyName],[BiddingStrategyType],"
                    f"[BiddingStrategyStatus],[ResourceName],[Clicks],[Conversions],[BounceRate],[interactions],[EngagementRate],[Cost],[CreateDate]) "
                    f"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)") 
            keyword_view_data = keyword_view.get_keyword_view_data()
            keyword_view.load_to_dwh(keyword_view_data, query)

        # Load bidding strategy data to DWH
        ads_craft_ai_start_date = get_start_date(account_id, "AdgroupPerformance_V2CraftAI")
        if ads_craft_ai_start_date:
            craft_ai_fields = [
                "segments.date",
                "customer.id",
                "customer.descriptive_name",
                "customer.currency_code",
                "campaign.id",
                "campaign.name",
                "campaign.bidding_strategy",
                "campaign.bidding_strategy_type",
                "ad_group.id",
                "ad_group.name",
                "ad_group.effective_target_roas",
                "ad_group.effective_target_cpa_micros",
                "ad_group.cpc_bid_micros",
                "ad_group.cpv_bid_micros",
                "ad_group.target_cpa_micros",
                "ad_group.target_roas",
                "bidding_strategy.id",
                "bidding_strategy.name",
                "bidding_strategy.type",
                "metrics.clicks",
                "metrics.impressions",
                "metrics.conversions",
                "metrics.conversions_value",
                "metrics.interactions",
                "metrics.cost_micros",
                "metrics.engagement_rate",
                "metrics.bounce_rate",
            ]
            craft_ai = GoogleAds("ad_group", account_id, ads_craft_ai_start_date, end_date, craft_ai_fields)
            craft_ai_data = craft_ai.get_ads_bid_craft_AI_data()
            query = (f"INSERT INTO [GoogleAds].[AdgroupPerformance_V2CraftAI] ("
                    f"[Date],[CustomerID],[CustomerDescriptiveName],[CurrencyCode],"
                    f"[CampaignId],[CampaignName],[CampaignBiddingStrategy],[CampaignBiddingStrategyType],"
                    f"[AdGroupID],[AdGroupName],[AdGroup_effective_target_roas],[AdGroup_effective_target_cpa_micros],"
                    f"[AdGroup_cpc_bid_micros],[AdGroup_cpv_bid_micros],[AdGroup_target_cpa_micros],"
                    f"[AdGroup_target_roas],[BiddingStrategyId],[BiddingStrategyName],[BiddingStrategyType],"
                    f"[Clicks],[Impressions],[Conversions],[Conversions_value],[Interactions],[Cost_micros],"
                    f"[Engagement_rate],[Bounce_rate],[CreateDate]) "
                    f"VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            craft_ai.load_to_dwh(craft_ai_data, query)

        # Load bidding strategy data to DWH
        bidding_start_date = get_start_date(account_id, "BiddingStrategyReport_V2")
        if bidding_start_date:
            bid_strategy_fields = [
                "segments.date",
                "customer.id",
                "customer.descriptive_name",
                "customer.currency_code",
                "customer.time_zone",
                "bidding_strategy.id",
                "bidding_strategy.name",
                "bidding_strategy.resource_name",
                "bidding_strategy.target_roas.target_roas",
                "bidding_strategy.target_roas.cpc_bid_ceiling_micros",
                "bidding_strategy.target_roas.cpc_bid_floor_micros",
                "bidding_strategy.target_spend.cpc_bid_ceiling_micros",
                "bidding_strategy.target_spend.target_spend_micros",
                "bidding_strategy.target_impression_share.location",
                "metrics.conversions",
                "metrics.conversions_from_interactions_rate",
                "metrics.all_conversions_value",
                "metrics.clicks",
                "metrics.impressions",
            ]
            bid_strategy = GoogleAds('bidding_strategy', account_id, bidding_start_date, end_date, bid_strategy_fields)
            query = (f"INSERT INTO [GoogleAds].[BiddingStrategyReport_V2] ("
                            f"[Date],[CustomerId],[AccountDescriptiveName],[CustomerCurrencyCode],"
                            f"[CustomerTimeZone],[StrategyId],[StrategyName],[ResourceName],"
                            f"[TargetRoas],[TargetRoasBidCeiling],[TargetRoasBidFloor],[TargetSpendSpendTarget],"
                            f"[ShareImpressionLocation],[Conversions],[ConversionRate],"
                            f"[ConversionValue],[Clicks],[Cost],[CreateDate]) "
                            f"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            bid_strategy_data = bid_strategy.get_bidding_strategy_data()
            bid_strategy.load_to_dwh(bid_strategy_data, query)

        # Load ad_group bidding data to DWH
        ad__bid_start_date = get_start_date(account_id, "AdgroupPerformance_V2Bid")
        if ad__bid_start_date:
            ads_bid_fields = [
                "segments.date",
                "customer.id", 
                "campaign.id",
                "campaign.name",
                "ad_group.id",
                "ad_group.name",
                "bidding_strategy.id",
                "campaign.bidding_strategy_type",
                "bidding_strategy.name", 
                "metrics.conversions",
                "metrics.bounce_rate",
                "metrics.interactions",
                "metrics.engagement_rate",
                "metrics.impressions",
                "metrics.clicks",
                "metrics.cost_micros",
                "metrics.search_impression_share"
            ]
            ads_bid = GoogleAds("ad_group", account_id, ad__bid_start_date, end_date, ads_bid_fields)
            query = (f"INSERT INTO [GoogleAds].[AdgroupPerformance_V2Bid] ("
                    f"[Date],[CustomerId],[CampaignId],[CampaignName],"
                    f"[AdGroupId],[AdGroupName],[BiddingStrategyId],[BiddingStrategyType],"
                    f"[BiddingStrategyName],[Conversions],[BounceRate],[Interactions],"
                    f"[EngagementRate],[Impressions],[Clicks],[Cost],[SearchImpressionShare],[CreateDate]) "
                    f"VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?)")
            ads_bid_data = ads_bid.get_ads_with_bid_data()
            ads_bid.load_to_dwh(ads_bid_data, query)

        # Load ads_bid_craft_ai_conv data to DWH
        craft_ai_conv_start_date = get_start_date(account_id, "AdgroupPerformance_V2CraftAIConv")
        if craft_ai_conv_start_date:
            craft_ai_conv_fields = [
                "segments.date",
                "customer.id",
                "campaign.id",
                "campaign.name",
                "ad_group.id",
                "ad_group.name",
                "metrics.all_conversions",
                "segments.conversion_action_name",
                " metrics.conversions_value"
            ]
            craft_ai_conv = GoogleAds("ad_group", account_id, craft_ai_conv_start_date, end_date, craft_ai_conv_fields)
            query = (f"INSERT INTO [GoogleAds].[AdgroupPerformance_V2CraftAIConv] ("
                    f"[Date],[CustomerID],[CampaignId],[CampaignName],[AdGroupID],"
                    f"[AdGroupName],[ConversionAction],[Conversions],[Conversions_value],[CreateDate]) "
                    f"VALUES (?,?,?,?,?,?,?,?,?,?)")
            craft_ai_conv_data = craft_ai_conv.get_ads_bid_craft_ai_conv_data()
            craft_ai_conv.load_to_dwh(craft_ai_conv_data, query)
    except Exception as e:
        print(e)

if __name__ == '__main__':
    account_ids = os.environ.get("GOOGLE_ADS_ACCOUNT_IDS")
    if account_ids:
        account_ids = account_ids.replace(" ", "")
        account_ids = account_ids.split(",")
        for account_id in account_ids:
            ETL(account_id)