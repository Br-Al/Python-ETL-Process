import pandas as pd
from datetime import datetime
import numpy as np

class Transformer:
    def transform(self, data):
        pass


class SimpleTransformer(Transformer):
    def transform(self, data):
        """
            Convert the input data to DataFrame

            Args:
            data (list): an arry of dictionaries

            Return:
            pandas.DataFrame: a Dataframe of the input data
        """
        
        data = pd.DataFrame(data)
        
        return data


class ComplexTransformer(Transformer):
    def transform(self, data):
        pass


class TruncateLongStringTransformer(Transformer):
    def transform(self, data):
        data[data.select_dtypes(object).columns.values] = data[data.select_dtypes(object).columns.values].apply(
            lambda x: x.str.slice(0,255)
            )
            
        return data


class SubscriptionsPerformanceTransformer(Transformer):
    def transform(self, data, asof=None):
        # Set the 'Division' column of 'data' to 'OTHERS' for all rows where the 'OrderBrand' column equals 'Ecomm'
        data[data['OrderBrand'] == 'Ecomm', 'Division'] = 'OTHERS'

        # Filter on asof date if passed in
        if asof is not None:
            data = data[data['SubscriptionDate'] < asof]

        # Set 'UserId' column to 1
        data['UserId'] = 1 
        
        # Create new dataframe with sum of 'Rebilled' column by date
        dt_cut = data.groupby('Date').agg({'Rebilled': 'sum'}).reset_index()

        # Sort descending by date
        dt_cut = dt_cut.sort_values(by='Date',ascending=False)
        
        # Create 'idx' column initially set to 0
        dt_cut['idx'] = 0

        # Set 'idx' column to 1 for rows where 'Rebilled' is greater than 0
        dt_cut.loc[dt_cut['Rebilled'] > 0, 'idx'] = 1

        # Calculate cumulative sum of 'idx' column
        dt_cut['idx_sum'] = dt_cut['idx'].cumsum()

        # Create new dataframe from 'data' with rows where 'Date' is greater than last value in 'idx_sum' 
        dt_backlog_org = data[data['Date'] > dt_cut[dt_cut['idx_sum'] == 1]['Date'].iloc[0]].copy()

        # Create new columns for 'RenewalMonth', 'RenewalDate', 'RenewalsCnt', 'RenewalAmountUSD'
        dt_backlog_org['RenewalMonth'] = dt_backlog_org['Date'].str[:7]
        dt_backlog_org['RenewalDate'] = dt_backlog_org['Date']
        dt_backlog_org['RenewalsCnt'] = dt_backlog_org['Scheduled']
        dt_backlog_org['RenewalAmountUSD'] = dt_backlog_org['ScheduledUSD']

        # Select necessary columns
        dt_backlog_org = dt_backlog_org[
            [
                'SubscriptionDate',
                'RenewalMonth',
                'RenewalDate',
                'CountryISO2',
                'OrderBrand',
                'Division',
                'UserID',
                'Interval',
                'Cycle',
                'RenewalsCnt',
                'RenewalAmountUSD'
            ]
        ]

        # Create new dataframe from 'data' with rows where Date is less than or equal to last value in 'idx_sum'
        dt_performance = data[data['Date'] <= dt_cut[dt_cut['idx_sum'] == 1]['Date'].iloc[0]].copy() 

        return ([dt_performance, dt_backlog_org, dt_cut['idx_sum' == 1]['Date']])
        

class OutBrainCampaignTransformer(Transformer):
    def transform(self, data: pd.DataFrame):
        old_columns = data.columns.values
        new_columns = [
            "id", "campaign_name", "status_enabled", "creationTime",                       
            "lastModified", "cpc", "autoArchived", "minimumCpc",                       
            "currency", "marketerId", "autoExpirationOfAds", "contentType",                        
            "suffixTrackingCode", "readonly", "startHour", "onAirType",                        
            "objective", "creativeFormat", "dynamicRetargeting", "targeting_platform",                        
            "targeting_language", "targeting_excludeAdBlockUsers", 
            "targeting_nativePlacements_enabled",                        
            "targeting_includeCellularNetwork",                       
            "targeting_nativePlacementsEnabled", "budget_id", "budget_name",                        
            "budget_shared", "budget_amount", "budget_currency", "budget_creationTime",                        
            "budget_lastModified", "budget_startDate", "budget_runForever",                        
            "budget_type", "budget_pacing", "prefixTrackingCode_prefix",                        
            "prefixTrackingCode_encode",                        
            "liveStatus_onAirReason", "liveStatus_campaignOnAir", "liveStatus_amountSpent",                        
            "liveStatus_onAirModificationTime", "trackingPixels_enabled", "trackingPixels_urls"
        ]

        data.rename(columns=dict(zip(old_columns, new_columns)), inplace=True)
        data = data[["id", "campaign_name", "creativeFormat", "status_enabled", "AccountName"]]
        data['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        data['ProcessingDate'] = pd.to_datetime(data['ProcessingDate'])


class OutBrainPerformanceReportTransformer(Transformer):
    def transform(self, data: pd.DataFrame):
        old_columns = [
            "metrics.impressions", "metrics.clicks",                                    
            "metrics.totalConversions", "metrics.conversions",                                    
            "metrics.viewConversions",                                    
            "metrics.spend", "metrics.ecpc", "metrics.ctr", "metrics.conversionRate",                                    
            "metrics.viewConversionRate", "metrics.cpa", "metrics.totalCpa",                                    
            "metrics.totalSumValue", "metrics.sumValue", "metrics.viewSumValue",                                    
            "metrics.totalAverageValue", "metrics.averageValue",                                    
            "metrics.viewAverageValue",                                    
            "campaignId", "Date", "AccountName"

        ]
        new_columns = [
            "metrics_impressions", "metrics_clicks",                                    
            "metrics_totalConversions", "metrics_conversions",                                    
            "metrics_viewConversions",                                    
            "metrics_spend", "metrics_ecpc", "metrics_ctr", "metrics_conversionRate",                                    
            "metrics_viewConversionRate", "metrics_cpa", "metrics_totalCpa",                                    
            "metrics_totalSumValue", "metrics_sumValue", "metrics_viewSumValue",                                    
            "metrics_totalAverageValue", "metrics_averageValue",                                    
            "metrics_viewAverageValue",                                    
            "campaignId", "date", "AccountName"
        ]
        data = data[[old_columns]]
        data.rename(columns=dict(zip(old_columns, new_columns)), inplace=True)
        data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        data = data.fillna(0)
        data['date'] = pd.to_datetime(data['date'])
        data['ProcessingDate'] = datetime.now().strftime("%Y-%m-%d")
        data['ProcessingDate'] = pd.to_datetime(data['ProcessingDate'])

        return data
