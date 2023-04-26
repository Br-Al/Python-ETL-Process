from utils import get_outbrain_reports
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
        MY_OUTBRAIN_PATH = 'C:/outbrain_cred.json'
        cred_json = get_api_config(MY_OUTBRAIN_PATH)
        username = cred_json["username"]
        password = cred_json["password"]
        inpixio_account_name = cred_json["inpixio_account_name"]
        utility_account_name = cred_json["utility_account_name"]
        pdf_account_name = cred_json["expert_pdf-account_name"]

        max_campaign_inpixio_date_query = """SELECT MAX(Date)as new_date
                                                       FROM [OUTBRAIN].[CampaignPerformanceReport_V2] WHERE [AccountName] = 'Inpixio' """
        max_campaign_inpixio_processed_date = db.get_query_df(max_campaign_inpixio_date_query)
        max_campaign_inpixio_date = max_campaign_inpixio_processed_date.iat[0, 0]
        campaign_inpixio_start_date = get_max_date(max_campaign_inpixio_date)
        inpixio_account = get_outbrain_reports.OutBrain(username=username, password=password, account_name=inpixio_account_name)
        inpixio_campaign_df = inpixio_account.Campaigns()
        inpixio_campaign_df['AccountName'] = 'Inpixio'
        inpixio_campaign_performance_df = inpixio_account.Campaign_periodic(campaign_inpixio_start_date, end_date)
        inpixio_campaign_performance_df['AccountName'] = 'Inpixio'

        max_campaign_utility_date_query = """SELECT MAX(Date)as new_date
                                                       FROM [OUTBRAIN].[CampaignPerformanceReport_V2]WHERE [AccountName] = 'Utility' """
        max_campaign_utility_processed_date = db.get_query_df(max_campaign_utility_date_query)
        max_campaign_utility_date = max_campaign_utility_processed_date.iat[0, 0]
        campaign_utility_start_date = get_max_date(max_campaign_utility_date)
        utility_account = get_outbrain_reports.OutBrain(username=username, password=password,account_name=utility_account_name)
        utility_campaign_df = utility_account.Campaigns()
        utility_campaign_df['AccountName'] = 'Utility'
        utility_campaign_performance_df = utility_account.Campaign_periodic(campaign_utility_start_date, end_date)
        utility_campaign_performance_df['AccountName'] = 'Utility'
        pdf_account = get_outbrain_reports.OutBrain(username=username, password=password, account_name=pdf_account_name)
        pdf_campaign_df = pdf_account.Campaigns()
        pdf_campaign_df['AccountName'] = 'PDF'
        campaign_list = [inpixio_campaign_df, utility_campaign_df, pdf_campaign_df]
        campaign_df = pd.concat(campaign_list)
        campaign_performance_list = [inpixio_campaign_performance_df, utility_campaign_performance_df]
        campaign_performance_df = pd.concat(campaign_performance_list)
        camapign_old_columns = ["id", "name", "enabled", "creationTime",
                                "lastModified", "cpc", "autoArchived", "minimumCpc",
                                "currency", "marketerId", "autoExpirationOfAds", "contentType",
                                "suffixTrackingCode", "readonly", "startHour", "onAirType",
                                "objective", "creativeFormat", "dynamicRetargeting", "targeting.platform",
                                "targeting.language",
                                "targeting.excludeAdBlockUsers", "targeting.nativePlacements.enabled",
                                "targeting.includeCellularNetwork",
                                "targeting.nativePlacementsEnabled", "budget.id", "budget.name",
                                "budget.shared", "budget.amount", "budget.currency", "budget.creationTime",
                                "budget.lastModified", "budget.startDate", "budget.runForever",
                                "budget.type", "budget.pacing", "prefixTrackingCode.prefix",
                                "prefixTrackingCode.encode",
                                "liveStatus.onAirReason", "liveStatus.campaignOnAir", "liveStatus.amountSpent",
                                "liveStatus.onAirModificationTime", "trackingPixels.enabled", "trackingPixels.urls"]
        campaign_new_columns = ["id", "campaign_name", "status_enabled", "creationTime",
                                "lastModified", "cpc", "autoArchived", "minimumCpc",
                                "currency", "marketerId", "autoExpirationOfAds", "contentType",
                                "suffixTrackingCode", "readonly", "startHour", "onAirType",
                                "objective", "creativeFormat", "dynamicRetargeting", "targeting_platform",
                                "targeting_language",
                                "targeting_excludeAdBlockUsers", "targeting_nativePlacements_enabled",
                                "targeting_includeCellularNetwork",
                                "targeting_nativePlacementsEnabled", "budget_id", "budget_name",
                                "budget_shared", "budget_amount", "budget_currency", "budget_creationTime",
                                "budget_lastModified", "budget_startDate", "budget_runForever",
                                "budget_type", "budget_pacing", "prefixTrackingCode_prefix",
                                "prefixTrackingCode_encode",
                                "liveStatus_onAirReason", "liveStatus_campaignOnAir", "liveStatus_amountSpent",
                                "liveStatus_onAirModificationTime", "trackingPixels_enabled", "trackingPixels_urls"]
        campaign_df.rename(columns={i: j for i, j in zip(camapign_old_columns, campaign_new_columns)}, inplace=True)
        campaign_df = campaign_df[["id", "campaign_name", "creativeFormat", "status_enabled", "AccountName"]]
        campaign_performance_old_columns = ["metrics.impressions", "metrics.clicks",
                                            "metrics.totalConversions", "metrics.conversions",
                                            "metrics.viewConversions",
                                            "metrics.spend", "metrics.ecpc", "metrics.ctr", "metrics.conversionRate",
                                            "metrics.viewConversionRate", "metrics.cpa", "metrics.totalCpa",
                                            "metrics.totalSumValue", "metrics.sumValue", "metrics.viewSumValue",
                                            "metrics.totalAverageValue", "metrics.averageValue",
                                            "metrics.viewAverageValue",
                                            "campaignId", "Date", "AccountName"]
        campaign_performance_new_columns = ["metrics_impressions", "metrics_clicks",
                                            "metrics_totalConversions", "metrics_conversions",
                                            "metrics_viewConversions",
                                            "metrics_spend", "metrics_ecpc", "metrics_ctr", "metrics_conversionRate",
                                            "metrics_viewConversionRate", "metrics_cpa", "metrics_totalCpa",
                                            "metrics_totalSumValue", "metrics_sumValue", "metrics_viewSumValue",
                                            "metrics_totalAverageValue", "metrics_averageValue",
                                            "metrics_viewAverageValue",
                                            "campaignId", "date", "AccountName"]
        campaign_performance_df.drop(['metrics.totalRoas', 'metrics.roas'], axis=1, inplace=True)
        campaign_performance_df = campaign_performance_df[campaign_performance_old_columns]
        campaign_performance_df.rename(
            columns={i: j for i, j in zip(campaign_performance_old_columns, campaign_performance_new_columns)},
            inplace=True)
        campaign_performance_df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        campaign_performance_df = campaign_performance_df.fillna(0)
        campaign_performance_df['date'] = pd.to_datetime(campaign_performance_df['date'])
        campaign_performance_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_performance_df['ProcessingDate'] = pd.to_datetime(campaign_performance_df['ProcessingDate'])
        campaign_df['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        campaign_df['ProcessingDate'] = pd.to_datetime(campaign_df['ProcessingDate'])
        Campaign_data_delete_query = f"DELETE FROM [Acquisition].[OUTBRAIN].[Campaigns_V2] "
        db.data_push(Campaign_data_delete_query)
        if not campaign_df.empty:
            insert_query = """INSERT INTO [OUTBRAIN].[Campaigns_V2] (

              [id ]
              ,[campaign_name]
              ,[creativeFormat]
              ,[status_enabled]
              ,[AccountName ]
              ,[ProcessingDate]

            )  VALUES (?, ?, ?, ?, ?, ?)"""
            db.insert_many(campaign_df, insert_query)

        if not campaign_performance_df.empty:
            insert_query = """INSERT INTO [OUTBRAIN].[CampaignPerformanceReport_V2] (

              [metrics_impressions]
              ,[metrics_clicks]
              ,[metrics_totalConversions]
              ,[metrics_conversions]
              ,[metrics_viewConversions]
              ,[metrics_spend]
              ,[metrics_ecpc]
              ,[metrics_ctr]
              ,[metrics_conversionRate]
              ,[metrics_viewConversionRate]
              ,[metrics_cpa]
              ,[metrics_totalCpa]
              ,[metrics_totalSumValue]
              ,[metrics_sumValue]
              ,[metrics_viewSumValue]
              ,[metrics_totalAverageValue]
              ,[metrics_averageValue]
              ,[metrics_viewAverageValue]
              ,[campaignId]
              ,[date]
              ,[AccountName ]
              ,[ProcessingDate]

                             )  VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(campaign_performance_df, insert_query)
    except:
        print(sys.exc_info())

