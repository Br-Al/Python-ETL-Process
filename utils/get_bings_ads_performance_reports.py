#!/usr/bin/python3
import sys
import io
import pandas as pd
import  numpy as np
from urllib import parse
from datetime import datetime, timedelta
from bingads.service_client import ServiceClient
from bingads.v13 import *
from bingads.v13.reporting import *
from suds import WebFault
from suds.client import Client
#Function for date validation
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
            date_text = input('Please Enter the date in YYYY-MM-DD format\t')
        else:
            return datetime.strptime(date_text,'%Y-%m-%d').date()
    except:
        raise Exception('linkedin_campaign_processing : year does not match format yyyy-mm-dd')

def get_campaign_report(authorization_data, account_id, s_date, e_date, qry_type):
    """
        Request campaign performance report for an ads account in a time range.

        Parameters
        ----------
        authorization_data: dictionary: table
            access to data after getting user's credentials
        account_id : int
            An identification of an ads account
       s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

        Returns
        -------
        table and columns that are in request

        """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment='production',
        )
        if qry_type in ["day", "daily"]:
            aggregation = 'Daily'
        elif qry_type in ["week", "weekly"]:
            aggregation = 'Weekly'

        exclude_column_headers = False
        exclude_report_footer = False
        exclude_report_header = False
        time = reporting_service.factory.create('ReportTime')
        start_date = reporting_service.factory.create('Date')
        start_date.Day = startDate.day
        start_date.Month = startDate.month
        start_date.Year = startDate.year
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create('Date')
        end_date.Day = endDate.day
        end_date.Month = endDate.month
        end_date.Year = endDate.year
        time.CustomDateRangeEnd = end_date
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        report_request = reporting_service.factory.create('CampaignPerformanceReportRequest')
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = 'Csv'
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "Campaign Performance Report"
        scope = reporting_service.factory.create('AccountThroughCampaignReportScope')
        scope.AccountIds = {'long': [account_id]}
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = reporting_service.factory.create('ArrayOfCampaignPerformanceReportColumn')
        report_columns.CampaignPerformanceReportColumn.append(['AccountName',
                                                               'TimePeriod',
                                                               'AccountNumber',
                                                               'AccountId',
                                                               'AccountStatus',
                                                               'CampaignName',
                                                               'CampaignId',
                                                               'CampaignLabels',
                                                               'CampaignStatus',
                                                               'CurrencyCode',
                                                               'AdDistribution',
                                                               'Network',
                                                               'DeliveredMatchType',
                                                               'Impressions',
                                                               'AbsoluteTopImpressionSharePercent',
                                                               'Clicks',
                                                               'Ctr',
                                                               'AverageCpc',
                                                               'Spend',
                                                               'AveragePosition',
                                                               'Conversions',
                                                               'ConversionRate',
                                                               'ExactMatchImpressionSharePercent',
                                                               'ImpressionSharePercent',
                                                               'QualityScore'])
        report_request.Columns = report_columns

        return report_request
    except:
        print(sys.exc_info())


def download_campaign_report(report_request, authorization_data, s_date, e_date, qry_type):
    """
        Download campaign performance report that has been requested.

        Parameters
        ----------
        report_request: dictionary: table
            Tables and columns that are in request
        authorization_data: dictionary: table
            access to data after getting user's credentials
       s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

        Returns
        -------
        dictionary data: table for campaign

        """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            overwrite_result_file=True,
            timeout_in_milliseconds=3600000
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            sys.exit(0)

        campaign_analytics_data = pd.DataFrame(
            columns=['TimePeriod',
                     'AccountName',
                     'AccountNumber',
                     'AccountId',
                     'AccountStatus',
                     'CampaignName',
                     'CampaignId',
                     'CampaignLabels',
                     'CampaignStatus',
                     'CurrencyCode',
                     'AdDistribution',
                     'Network',
                     'DeliveredMatchType',
                     'Impressions',
                     'AbsoluteTopImpressionSharePercent',
                     'Clicks',
                     'Ctr',
                     'AverageCpc',
                     'Spend',
                     'AveragePosition',
                     'Conversions',
                     'ConversionRate',
                     'ExactMatchImpressionSharePercent',
                     'ImpressionSharePercent',
                     'QualityScore'])
        if "Impressions" in report_container.report_columns and \
                "Clicks" in report_container.report_columns and \
                "Spend" in report_container.report_columns and \
                "CampaignLabels" in report_container.report_columns and \
                "CampaignStatus" in report_container.report_columns and \
                "CurrencyCode" in report_container.report_columns and \
                "AdDistribution" in report_container.report_columns and \
                "Network" in report_container.report_columns and \
                "CampaignId" in report_container.report_columns:

            report_record_iterable = report_container.report_records

            for record in report_record_iterable:
                tmp_dict = {}
                tmp_dict["AccountName"] = record.value("AccountName")
                tmp_dict["AccountNumber"] = record.value("AccountNumber")
                tmp_dict["TimePeriod"] = record.value("TimePeriod")
                tmp_dict["AccountId"] = record.int_value("AccountId")
                tmp_dict["AccountStatus"] = record.value("AccountStatus")
                tmp_dict["CampaignName"] = record.value("CampaignName")
                tmp_dict["CampaignId"] = record.int_value("CampaignId")
                tmp_dict["CampaignLabels"] = record.value("CampaignLabels")
                tmp_dict["CampaignStatus"] = record.value("CampaignStatus")
                tmp_dict["CurrencyCode"] = record.value("CurrencyCode")
                tmp_dict["AdDistribution"] = record.value("AdDistribution")
                tmp_dict["Network"] = record.value("Network")
                tmp_dict["DeliveredMatchType"] = record.value("DeliveredMatchType")

                tmp_dict["Impressions"] = record.int_value("Impressions")
                tmp_dict["AbsoluteTopImpressionSharePercent"] = pd.to_numeric(record.value("AbsoluteTopImpressionSharePercent"), errors='coerce')
                tmp_dict["Clicks"] = record.int_value("Clicks")
                tmp_dict['Ctr'] = pd.to_numeric(record.value("Ctr"), errors='coerce')
                tmp_dict["AverageCpc"] = pd.to_numeric(record.value("AverageCpc"), errors='coerce')
                tmp_dict["Spend"] = float(record.value("Spend"))
                tmp_dict["AveragePosition"] = pd.to_numeric(record.value("AveragePosition"), errors='coerce')
                tmp_dict["Conversions"] = pd.to_numeric(record.value("Conversions"), errors='coerce')
                tmp_dict["ConversionRate"] = pd.to_numeric(record.value("ConversionRate"), errors='coerce')
                tmp_dict["ExactMatchImpressionSharePercent"] = pd.to_numeric(record.value("ExactMatchImpressionSharePercent"), errors='coerce')
                tmp_dict["ImpressionSharePercent"] = pd.to_numeric(record.value("ImpressionSharePercent"), errors='coerce')
                tmp_dict["QualityScore"] = pd.to_numeric(record.value("QualityScore"), errors='coerce')

                campaign_analytics_data = pd.concat([campaign_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)


                if qry_type in ["week", "weekly"]:
                    campaign_analytics_data["week"] = week_number
                elif qry_type in ["month", "monthly"]:
                    campaign_analytics_data["month"] = startDate.month
                elif qry_type in ["day", "daily"]:
                    campaign_analytics_data["week"] = week_number

        report_container.close()

        return campaign_analytics_data
    except:
        print(sys.exc_info())

def get_adgroup_report(authorization_data, account_id, s_date, e_date, qry_type):
    """
       Request ads group performance report for an ads account in a time range.

       Parameters
       ----------
       authorization_data: dictionary: table
           access to data after getting user's credentials
       account_id : int
           An identification of an ads account
      s_date : date
           The starting date
      e_date : date
           The end date
      qry_type : string
           The group or aggregation type:The data is aggregated daily or weekly

       Returns
       -------
       table and columns of ads group report that are in request

       """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment='production',
        )
        if qry_type in ["day", "daily"]:
            aggregation = 'Daily'
        elif qry_type in ["week", "weekly"]:
            aggregation = 'Weekly'

        exclude_column_headers = False
        exclude_report_footer = False
        exclude_report_header = False
        time = reporting_service.factory.create('ReportTime')

        start_date = reporting_service.factory.create('Date')
        start_date.Day = startDate.day
        start_date.Month = startDate.month
        start_date.Year = startDate.year
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create('Date')
        end_date.Day = endDate.day
        end_date.Month = endDate.month
        end_date.Year = endDate.year
        time.CustomDateRangeEnd = end_date
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        report_request = reporting_service.factory.create('AdGroupPerformanceReportRequest')
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = 'Csv'
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "AdGroup Performance Report"
        scope = reporting_service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': [account_id]}
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = reporting_service.factory.create('ArrayOfAdGroupPerformanceReportColumn')
        report_columns.AdGroupPerformanceReportColumn.append(['AccountName',
                                                              'AccountNumber',
                                                              'AccountId',
                                                              'TimePeriod',
                                                              'Status',
                                                              'CampaignName',
                                                              'CampaignId',
                                                              'AdGroupName',
                                                              'AdGroupId',
                                                              'CurrencyCode',
                                                              'AdDistribution',
                                                              'Network',
                                                              'AccountStatus',
                                                              'CampaignStatus',
                                                              'Language',
                                                              'DeliveredMatchType',
                                                              'Impressions',
                                                              'AbsoluteTopImpressionSharePercent',
                                                              'Clicks',
                                                              'Ctr',
                                                              'Spend',
                                                              'AveragePosition',
                                                              'Conversions',
                                                              'ConversionRate',
                                                              'ExactMatchImpressionSharePercent',
                                                              'ImpressionSharePercent',
                                                              'QualityScore',
                                                              'AdRelevance'])
        report_request.Columns = report_columns

        return report_request
    except:
        print(sys.exc_info())


def download_adgroup_report(report_request, authorization_data, s_date, e_date, qry_type):
    """
        Download Ads group performance report that has been request.

        Parameters
        ----------
        report_request: dictionary: table
            Tables and columns that are in request
        authorization_data: dictionary: table
            access to data after getting user's credentials
       s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

        Returns
        -------
        table with ads group performance

        """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,

            overwrite_result_file=True,
            timeout_in_milliseconds=3600000
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            sys.exit(0)

        adgroup_analytics_data = pd.DataFrame(
            columns=['TimePeriod',
                     'AccountName',
                     'AccountNumber',
                     'AccountId',
                     'Status',
                     'CampaignName',
                     'CampaignId',
                     'AdGroupName',
                     'AdGroupId',
                     'CurrencyCode',
                     'AdDistribution',
                     'Network',
                     'AccountStatus',
                     'CampaignStatus',
                     'Language',
                     'DeliveredMatchType',
                     'Impressions',
                     'AbsoluteTopImpressionRatePercent',
                     'Clicks',
                     'Ctr',
                     'Spend',
                     'AveragePosition',
                     'Conversions',
                     'ConversionRate',
                     'ExactMatchImpressionSharePercent',
                     'ImpressionSharePercent',
                     'QualityScore',
                     'AdRelevance'
                     ])
        if "Impressions" in report_container.report_columns and \
                "Clicks" in report_container.report_columns and \
                "Spend" in report_container.report_columns and \
                "CampaignStatus" in report_container.report_columns and \
                "CurrencyCode" in report_container.report_columns and \
                "AdDistribution" in report_container.report_columns and \
                "Network" in report_container.report_columns and \
                "CampaignId" in report_container.report_columns:

            report_record_iterable = report_container.report_records

            for record in report_record_iterable:
                tmp_dict = {}
                tmp_dict["AccountName"] = record.value("AccountName")
                tmp_dict["TimePeriod"] = record.value("TimePeriod")
                tmp_dict["AccountNumber"] = record.value("AccountNumber")
                tmp_dict["AccountId"] = record.int_value("AccountId")
                tmp_dict["Status"] = record.value("Status")
                tmp_dict["CampaignName"] = record.value("CampaignName")
                tmp_dict["CampaignId"] = record.int_value("CampaignId")
                tmp_dict["AdGroupName"] = record.value("AdGroupName")
                tmp_dict["AdGroupId"] = record.value("AdGroupId")
                tmp_dict["CurrencyCode"] = record.value("CurrencyCode")
                tmp_dict["AdDistribution"] = record.value("AdDistribution")
                tmp_dict["Network"] = record.value("Network")
                tmp_dict["AccountStatus"] = record.value("AccountStatus")
                tmp_dict["CampaignStatus"] = record.value("CampaignStatus")
                tmp_dict["Language"] = record.value("Language")
                tmp_dict["DeliveredMatchType"] = record.value("DeliveredMatchType")

                tmp_dict["Impressions"] = pd.to_numeric(record.value("Impressions"), errors='coerce')
                tmp_dict["Clicks"] = record.int_value("Clicks")
                tmp_dict["Ctr"] = pd.to_numeric(record.value("Ctr"), errors='coerce')
                tmp_dict["Spend"] = float(record.value("Spend"))
                tmp_dict["AveragePosition"] = pd.to_numeric(record.value("AveragePosition"), errors='coerce')
                tmp_dict["Conversions"] = pd.to_numeric(record.value("Conversions"), errors='coerce')
                tmp_dict["ConversionRate"] = pd.to_numeric(record.value("ConversionRate"), errors='coerce')
                tmp_dict["ExactMatchImpressionSharePercent"] = pd.to_numeric(record.value("ExactMatchImpressionSharePercent"), errors='coerce')
                tmp_dict["ImpressionSharePercent"] = pd.to_numeric(record.value("ImpressionSharePercent"), errors='coerce')
                tmp_dict["QualityScore"] = pd.to_numeric(record.value("QualityScore"), errors='coerce')
                tmp_dict["AdRelevance"] = pd.to_numeric(record.value("AdRelevance"), errors='coerce')

                adgroup_analytics_data = pd.concat([adgroup_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)


                if qry_type in ["week", "weekly"]:
                    adgroup_analytics_data["week"] = week_number
                elif qry_type in ["month", "monthly"]:
                    adgroup_analytics_data["month"] = startDate.month
                elif qry_type in ["day", "daily"]:
                    adgroup_analytics_data["week"] = week_number


        report_container.close()

        return adgroup_analytics_data
    except:
        print(sys.exc_info())

def get_geo_performance_report(authorization_data, account_id, s_date, e_date, qry_type):
    """
      Request geographic performance report for an ads account in a time range.

      Parameters
      ----------
      authorization_data: dictionary: table
          access to data after getting user's credentials
      account_id : int
          An identification of an ads account
     s_date : date
          The starting date
     e_date : date
          The end date
     qry_type : string
          The group or aggregation type:The data is aggregated daily or weekly

      Returns
      -------
      table and columns of geo-performance report that are in request

      """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment='production',
        )
        if qry_type in ["day", "daily"]:
            aggregation = 'Daily'
        elif qry_type in ["week", "weekly"]:
            aggregation = 'Weekly'

        exclude_column_headers = False
        exclude_report_footer = False
        exclude_report_header = False
        time = reporting_service.factory.create('ReportTime')
        start_date = reporting_service.factory.create('Date')
        start_date.Day = startDate.day
        start_date.Month = startDate.month
        start_date.Year = startDate.year
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create('Date')
        end_date.Day = endDate.day
        end_date.Month = endDate.month
        end_date.Year = endDate.year
        time.CustomDateRangeEnd = end_date
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        report_request = reporting_service.factory.create('GeographicPerformanceReportRequest')
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = 'Csv'
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "Geographic Performance Report"
        scope = reporting_service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': [account_id]}
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = reporting_service.factory.create('ArrayOfGeographicPerformanceReportColumn')
        report_columns.GeographicPerformanceReportColumn.append([
                                                                 'AccountName',
                                                                 'AccountNumber',
                                                                  'TimePeriod',
                                                                 'AccountId',
                                                                 'AdGroupId',
                                                                 'CampaignId',
                                                                 'AdGroupName',
                                                                 'CampaignName',
                                                                 'Language',
                                                                 'AccountStatus',
                                                                 'CampaignStatus',
                                                                 'AdGroupStatus',
                                                                 'Country',
                                                                 'CurrencyCode',
                                                                 'AdDistribution',
                                                                 'ProximityTargetLocation',
                                                                 'Radius',
                                                                 'BidMatchType',
                                                                 'DeliveredMatchType',
                                                                 'Network',
                                                                 'TopVsOther',
                                                                 'DeviceType',
                                                                 'LocationType',
                                                                 'DeviceOS',
                                                                 'Impressions',
                                                                 'Clicks',
                                                                 'Ctr',
                                                                 'AverageCpc',
                                                                 'Spend',
                                                                 'AveragePosition',
                                                                 'Assists',
                                                                 'Conversions',
                                                                 'ConversionRate',
                                                                 'CostPerConversion',
                                                                 'Revenue',
                                                                 'ReturnOnAdSpend',
                                                                 'CostPerAssist',
                                                                 'RevenuePerConversion',
                                                                 'RevenuePerAssist'
                                                                 ])
        report_request.Columns = report_columns

        return report_request
    except:
        print("\nMS_GEOGRAPHIC_PERFORMANCE_REPORT : report processing Failed : ", sys.exc_info())


def download_geo_performance_report(report_request, authorization_data, s_date, e_date, qry_type):
    """
        Download geographic performance report that has been requested.

        Parameters
        ----------
        report_request: dictionary: table
            Tables and columns that are in request
        authorization_data: dictionary: table
            access to data after getting user's credentials
        s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

       Returns
        -------
        table with geo-graphic performance.

    """

    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            overwrite_result_file=True,
            timeout_in_milliseconds=120000
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            sys.exit(0)

        geographic_analytics_data = pd.DataFrame(columns=[
            'TimePeriod',
            'AccountName',
            'AccountNumber',
            'AccountId',
            'AdGroupId',
            'CampaignId',
            'AdGroupName',
            'CampaignName',
            'Language',
            'AccountStatus',
            'CampaignStatus',
            'AdGroupStatus',
            'Country',
            'CurrencyCode',
            'AdDistribution',
            'ProximityTargetLocation',
            'Radius',
            'BidMatchType',
            'DeliveredMatchType',
            'Network',
            'TopVsOther',
            'DeviceType',
            'LocationType',
            'DeviceOS',
            'Impressions',
            'Clicks',
            'Ctr',
            'AverageCpc',
            'Spend',
            'AveragePosition',
            'Assists',
            'Conversions',
            'ConversionRate',
            'CostPerConversion',
            'Revenue',
            'ReturnOnAdSpend',
            'CostPerAssist',
            'RevenuePerConversion',
            'RevenuePerAssist'
        ])

        if "Impressions" in report_container.report_columns and \
                "Clicks" in report_container.report_columns and \
                "Spend" in report_container.report_columns and \
                "CampaignId" in report_container.report_columns:

            report_record_iterable = report_container.report_records

            for record in report_record_iterable:
                tmp_dict = {}
                tmp_dict["AccountName"] = record.value("AccountName")
                tmp_dict["TimePeriod"] = record.value("TimePeriod")
                tmp_dict["AccountNumber"] = record.value("AccountNumber")
                tmp_dict["AccountId"] = record.int_value("AccountId")
                tmp_dict["AdGroupId"] = record.value("AdGroupId")
                tmp_dict["CampaignId"] = record.int_value("CampaignId")
                tmp_dict["AdGroupName"] = record.value("AdGroupName")
                tmp_dict["CampaignName"] = record.value("CampaignName")
                tmp_dict["Language"] = record.value("Language")
                tmp_dict["AccountStatus"] = record.value("AccountStatus")
                tmp_dict["CampaignStatus"] = record.value("CampaignStatus")
                tmp_dict["AdGroupStatus"] = record.value("AdGroupStatus")
                tmp_dict["Country"] = record.value("Country")
                tmp_dict["CurrencyCode"] = record.value("CurrencyCode")
                tmp_dict["AdDistribution"] = record.value("AdDistribution")
                tmp_dict["ProximityTargetLocation"] = record.value("ProximityTargetLocation")
                tmp_dict["Radius"] = record.value("Radius")
                tmp_dict["BidMatchType"] = record.value("BidMatchType")
                tmp_dict["DeliveredMatchType"] = record.value("DeliveredMatchType")
                tmp_dict["Network"] = record.value("Network")
                tmp_dict["TopVsOther"] = record.value("TopVsOther")
                tmp_dict["DeviceType"] = record.value("DeviceType")
                tmp_dict["LocationType"] = record.value("LocationType")
                tmp_dict["DeviceOS"] = record.value("DeviceOS")
                tmp_dict["Impressions"] = pd.to_numeric(record.value("Impressions"), errors='coerce')
                tmp_dict["Clicks"] = record.int_value("Clicks")
                tmp_dict["Ctr"] = pd.to_numeric(record.value("Ctr"), errors='coerce')
                tmp_dict["AverageCpc"] = pd.to_numeric(record.value("AverageCpc"), errors='coerce')
                tmp_dict["Spend"] = float(record.value("Spend"))
                tmp_dict["AveragePosition"] = pd.to_numeric(record.value("AveragePosition"), errors='coerce')
                tmp_dict["Assists"] = pd.to_numeric(record.value("Assists"), errors='coerce')
                tmp_dict["Conversions"] = pd.to_numeric(record.value("Conversions"), errors='coerce')
                tmp_dict["ConversionRate"] = pd.to_numeric(record.value("ConversionRate"), errors='coerce')
                tmp_dict["CostPerConversion"] = pd.to_numeric(record.value("CostPerConversion"), errors='coerce')
                tmp_dict["Revenue"] = pd.to_numeric(record.value("Revenue"), errors='coerce')
                tmp_dict["ReturnOnAdSpend"] = pd.to_numeric(record.value("ReturnOnAdSpend"), errors='coerce')
                tmp_dict["CostPerAssist"] = pd.to_numeric(record.value("CostPerAssist"), errors='coerce')
                tmp_dict["RevenuePerConversion"] = pd.to_numeric(record.value("RevenuePerConversion"), errors='coerce')
                tmp_dict["RevenuePerAssist"] = pd.to_numeric(record.value("RevenuePerAssist"), errors='coerce')

                geographic_analytics_data = pd.concat([geographic_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)


                if qry_type in ["week", "weekly"]:
                    geographic_analytics_data["week"] = week_number
                elif qry_type in ["month", "monthly"]:
                    geographic_analytics_data["month"] = startDate.month
                elif qry_type in ["day", "daily"]:
                    geographic_analytics_data["week"] = week_number

        report_container.close()

        return geographic_analytics_data
    except:
        print(sys.exc_info())


def get_desturi_report(authorization_data,account_id, s_date, e_date, qry_type):
    """
      Request destination URL report for an ads account in a time range.

      Parameters
      ----------
      authorization_data: dictionary: table
          access to data after getting user's credentials
      account_id : int
          An identification of an ads account
     s_date : date
          The starting date
     e_date : date
          The end date
     qry_type : string
          The group or aggregation type:The data is aggregated daily or weekly

      Returns
      -------
      table and columns of destination url report that are in request

    """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment='production',
        )
        if qry_type in ["day", "daily"]:
            aggregation = 'Daily'
        elif qry_type in ["week", "weekly"]:
            aggregation = 'Weekly'

        exclude_column_headers = False
        exclude_report_footer = False
        exclude_report_header = False
        time = reporting_service.factory.create('ReportTime')
        start_date = reporting_service.factory.create('Date')
        start_date.Day = startDate.day
        start_date.Month = startDate.month
        start_date.Year = startDate.year
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create('Date')
        end_date.Day = endDate.day
        end_date.Month = endDate.month
        end_date.Year = endDate.year
        time.CustomDateRangeEnd = end_date
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        report_request = reporting_service.factory.create('DestinationUrlPerformanceReportRequest')
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = 'Csv'
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "DestinationUrl Performance Report"
        scope = reporting_service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': [account_id]}
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = reporting_service.factory.create('ArrayOfDestinationUrlPerformanceReportColumn')
        report_columns.DestinationUrlPerformanceReportColumn.append(['AccountName',
                                                                     'AccountNumber',
                                                                     'TimePeriod',
                                                                     'AccountId',
                                                                     'CampaignName',
                                                                     'CampaignId',
                                                                     'AdGroupName',
                                                                     'AdGroupId',
                                                                     'CurrencyCode',
                                                                     'AdDistribution',
                                                                     'Network',
                                                                     'AccountStatus',
                                                                     'CampaignStatus',
                                                                     'Language',
                                                                     'DeliveredMatchType',
                                                                     'DestinationUrl',
                                                                     'FinalUrl',
                                                                     'Impressions',
                                                                     'Clicks',
                                                                     'Ctr',
                                                                     'Spend',
                                                                     'AveragePosition',
                                                                     'Conversions',
                                                                     'ConversionRate'

                                                                     ])
        report_request.Columns = report_columns

        return report_request
    except:
        print( sys.exc_info())


def download_desturi_report(report_request, authorization_data, s_date, e_date, qry_type):
    """
        Download destination URL report that has been requested.

        Parameters
        ----------
        report_request: dictionary: table
            Tables and columns that are in request
        authorization_data: dictionary: table
            access to data after getting user's credentials
       s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

        Returns
        -------
        table with destination URL report.

    """

    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,

            overwrite_result_file=True,
            timeout_in_milliseconds=120000
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            print("There is no report data for the submitted report request parameters.")
            sys.exit(0)

        dest_analytics_data = pd.DataFrame(
            columns=['TimePeriod',
                     'AccountName',
                     'AccountNumber',
                     'AccountId',
                     'CampaignName',
                     'CampaignId',
                     'AdGroupName',
                     'AdGroupId',
                     'CurrencyCode',
                     'AdDistribution',
                     'Network',
                     'AccountStatus',
                     'CampaignStatus',
                     'Language',
                     'DeliveredMatchType',
                     'DestinationUrl',
                     'FinalUrl',
                     'Impressions',
                     'Clicks',
                     'Ctr',
                     'Spend',
                     'AveragePosition',
                     'Conversions',
                     'ConversionRate' ])

        if "Impressions" in report_container.report_columns and \
                "Clicks" in report_container.report_columns and \
                "Spend" in report_container.report_columns and \
                "AdGroupId" in report_container.report_columns:

            report_record_iterable = report_container.report_records

            total_impressions = 0
            total_clicks = 0
            distinct_devices = set()
            distinct_networks = set()
            for record in report_record_iterable:
                tmp_dict = {}
                tmp_dict["AccountName"] = record.value("AccountName")
                tmp_dict["TimePeriod"] = record.value("TimePeriod")
                tmp_dict["AccountNumber"] = record.value("AccountNumber")
                tmp_dict["AccountId"] = record.value("AccountId")
                tmp_dict["CampaignName"] = record.value("CampaignName")
                tmp_dict["CampaignId"] = record.value("CampaignId")
                tmp_dict["AdGroupName"] = record.value("AdGroupName")
                tmp_dict["AdGroupId"] = record.value("AdGroupId")
                tmp_dict["CurrencyCode"] = record.value("CurrencyCode")
                tmp_dict["AdDistribution"] = record.value("AdDistribution")
                tmp_dict["Network"] = record.value("Network")
                tmp_dict["AccountStatus"] = record.value("AccountStatus")
                tmp_dict["CampaignStatus"] = record.value("CampaignStatus")
                tmp_dict["Language"] = record.value("Language")
                tmp_dict["DeliveredMatchType"] = record.value("DeliveredMatchType")
                tmp_dict["DestinationUrl"] = record.value("DestinationUrl")
                tmp_dict["FinalUrl"] = record.value("FinalUrl")

                tmp_dict["Impressions"] = record.int_value("Impressions")
                tmp_dict["Clicks"] = record.int_value("Clicks")
                tmp_dict["Ctr"] = pd.to_numeric(record.value("Ctr"), errors='coerce')
                tmp_dict["Spend"] = float(record.value("Spend"))
                tmp_dict["Conversions"] = record.int_value("Conversions")
                tmp_dict["ConversionRate"] = pd.to_numeric(record.value("ConversionRate"), errors='coerce')

                try:
                    utm_campaign = None
                    utm_source = 'bing'
                    o = parse.urlparse(record.value("FinalUrl"))
                    query_url = parse.parse_qs(o.query)
                    url = o._replace(query=None).geturl()

                    utm_campaign = "MS " + record.value("CampaignName")
                except:
                    print("\n***UTM data extraction Failed: ", sys.exc_info())
                    pass

                tmp_dict["FinalUrl"] = url

                dest_analytics_data = pd.concat([dest_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

            dest_analytics_data = pd.concat([dest_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

            if qry_type in ["week", "weekly"]:
                dest_analytics_data["week"] = week_number
            elif qry_type in ["month", "monthly"]:
                dest_analytics_data["month"] = startDate.month
            elif qry_type in ["day", "daily"]:
                dest_analytics_data["week"] = week_number


        report_container.close()
        return dest_analytics_data
    except:
        print(sys.exc_info())


def get_keyword_report(authorization_data, account_id, s_date, e_date, qry_type):
    """
       Request keyword performance report for an ads account in a time range.

       Parameters
       ----------
       authorization_data: dictionary: table
           access to data after getting user's credentials
       account_id : int
           An identification of an ads account
      s_date : date
           The starting date
      e_date : date
           The end date
      qry_type : string
           The group or aggregation type:The data is aggregated daily or weekly

       Returns
       -------
       table and columns of keywords report that are in request

       """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment='production',
        )
        if qry_type in ["day", "daily"]:
            aggregation = 'Daily'
        elif qry_type in ["week", "weekly"]:
            aggregation = 'Weekly'

        exclude_column_headers = False
        exclude_report_footer = False
        exclude_report_header = False
        time = reporting_service.factory.create('ReportTime')

        start_date = reporting_service.factory.create('Date')
        start_date.Day = startDate.day
        start_date.Month = startDate.month
        start_date.Year = startDate.year
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create('Date')
        end_date.Day = endDate.day
        end_date.Month = endDate.month
        end_date.Year = endDate.year
        time.CustomDateRangeEnd = end_date
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        report_request = reporting_service.factory.create('KeywordPerformanceReportRequest')
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = 'Csv'
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "Keyword Performance Report"
        scope = reporting_service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': [account_id]}
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = reporting_service.factory.create('ArrayOfKeywordPerformanceReportColumn')
        report_columns.KeywordPerformanceReportColumn.append(['AccountName',
                                                              'AccountNumber',
                                                              'AccountId',
                                                              'TimePeriod',
                                                              'CampaignName',
                                                              'CampaignId',
                                                              'AdGroupName',
                                                              'AdGroupId',
                                                              'Keyword',
                                                              'KeywordId',
                                                              'AdId',
                                                              'AdType',
                                                              'AdDistribution',
                                                              'BidMatchType',
                                                              'BidStrategyType',
                                                              'MainlineBid',
                                                              'FirstPageBid',
                                                              'DeviceType',
                                                              'CurrentMaxCpc',
                                                              'AdRelevance',
                                                              'Goal',
                                                              'GoalType',
                                                              'Clicks',
                                                              'Impressions',
                                                              'Conversions',
                                                              'ConversionRate',
                                                              'AverageCpc',
                                                              'Spend'
                                                              ])
        report_request.Columns = report_columns

        return report_request
    except:
        print(sys.exc_info())


def download_get_keyword_report(report_request, authorization_data, s_date, e_date, qry_type):
    """
        Download keyword performance report that has been request.

        Parameters
        ----------
        report_request: dictionary: table
            Tables and columns that are in request
        authorization_data: dictionary: table
            access to data after getting user's credentials
       s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

        Returns
        -------
        table with keyword performance

        """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,

            overwrite_result_file=True,
            timeout_in_milliseconds=3600000
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            sys.exit(0)

        keyword_analytics_data = pd.DataFrame(
            columns=[
                'TimePeriod',
                'AccountName',
                  'AccountNumber',
                  'AccountId',
                  'CampaignName',
                  'CampaignId',
                  'AdGroupName',
                  'AdGroupId',
                  'Keyword',
                  'KeywordId',
                  'AdId',
                  'AdType',
                  'AdDistribution',
                  'BidMatchType',
                  'BidStrategyType',
                  'MainlineBid',
                  'FirstPageBid',
                  'DeviceType',
                  'CurrentMaxCpc',
                  'AdRelevance',
                  'Goal',
                  'GoalType',
                  'Clicks',
                  'Impressions',
                  'Conversions',
                  'ConversionRate',
                  'AverageCpc',
                  'Spend'
                     ])
        if "Impressions" in report_container.report_columns and \
                "Clicks" in report_container.report_columns and \
                "Spend" in report_container.report_columns and \
                "AdDistribution" in report_container.report_columns and \
                "CampaignId" in report_container.report_columns:

            report_record_iterable = report_container.report_records

            for record in report_record_iterable:
                tmp_dict = {}
                tmp_dict["AccountName"] = record.value("AccountName")
                tmp_dict["TimePeriod"] = record.value("TimePeriod")
                tmp_dict["AccountNumber"] = record.value("AccountNumber")
                tmp_dict["AccountId"] = record.int_value("AccountId")
                tmp_dict["CampaignName"] = record.value("CampaignName")
                tmp_dict["CampaignId"] = record.int_value("CampaignId")
                tmp_dict["AdGroupName"] = record.value("AdGroupName")
                tmp_dict["AdGroupId"] = record.int_value("AdGroupId")
                tmp_dict["Keyword"] = record.value("Keyword")
                tmp_dict["KeywordId"] = record.int_value("KeywordId")
                tmp_dict["AdId"] = record.int_value("AdId")
                tmp_dict["AdType"] = record.value("AdType")
                tmp_dict["AdDistribution"] = record.value("AdDistribution")
                tmp_dict["BidMatchType"] = record.value("BidMatchType")
                tmp_dict["BidStrategyType"] = record.value("BidStrategyType")
                tmp_dict["MainlineBid"] = record.value("MainlineBid")
                tmp_dict["FirstPageBid"] = record.value("FirstPageBid")
                tmp_dict["DeviceType"] = record.value("DeviceType")
                tmp_dict["CurrentMaxCpc"] = record.value("CurrentMaxCpc")
                tmp_dict["AdRelevance"] = record.int_value("AdRelevance")
                tmp_dict["Goal"] = record.value("Goal")
                tmp_dict["GoalType"] = record.value("GoalType")

                tmp_dict["Clicks"] = record.int_value("Clicks")
                tmp_dict["Impressions"] = pd.to_numeric(record.value("Impressions"), errors='coerce')
                tmp_dict["Conversions"] = pd.to_numeric(record.value("Conversions"), errors='coerce')
                tmp_dict["ConversionRate"] = pd.to_numeric(record.value("ConversionRate"), errors='coerce')
                tmp_dict["AverageCpc"] = pd.to_numeric(record.value("AverageCpc"), errors='coerce')
                tmp_dict["Spend"] = float(record.value("Spend"))


                keyword_analytics_data = pd.concat([keyword_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)


                if qry_type in ["week", "weekly"]:
                    keyword_analytics_data["week"] = week_number
                elif qry_type in ["month", "monthly"]:
                    keyword_analytics_data["month"] = startDate.month
                elif qry_type in ["day", "daily"]:
                    keyword_analytics_data["week"] = week_number


        report_container.close()

        return keyword_analytics_data
    except:
        print(sys.exc_info())

def get_keyword_craftai_report(authorization_data, account_id, s_date, e_date, qry_type):
    """
       Request keyword performance report for an ads account in a time range.

       Parameters
       ----------
       authorization_data: dictionary: table
           access to data after getting user's credentials
       account_id : int
           An identification of an ads account
      s_date : date
           The starting date
      e_date : date
           The end date
      qry_type : string
           The group or aggregation type:The data is aggregated daily or weekly

       Returns
       -------
       table and columns of keywords report that are in request

       """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_service = ServiceClient(
            service='ReportingService',
            version=13,
            authorization_data=authorization_data,
            environment='production',
        )
        if qry_type in ["day", "daily"]:
            aggregation = 'Daily'
        elif qry_type in ["week", "weekly"]:
            aggregation = 'Weekly'

        exclude_column_headers = False
        exclude_report_footer = False
        exclude_report_header = False
        time = reporting_service.factory.create('ReportTime')

        start_date = reporting_service.factory.create('Date')
        start_date.Day = startDate.day
        start_date.Month = startDate.month
        start_date.Year = startDate.year
        time.CustomDateRangeStart = start_date

        end_date = reporting_service.factory.create('Date')
        end_date.Day = endDate.day
        end_date.Month = endDate.month
        end_date.Year = endDate.year
        time.CustomDateRangeEnd = end_date
        time.ReportTimeZone = 'PacificTimeUSCanadaTijuana'
        return_only_complete_data = False

        report_request = reporting_service.factory.create('KeywordPerformanceReportRequest')
        report_request.Aggregation = aggregation
        report_request.ExcludeColumnHeaders = exclude_column_headers
        report_request.ExcludeReportFooter = exclude_report_footer
        report_request.ExcludeReportHeader = exclude_report_header
        report_request.Format = 'Csv'
        report_request.ReturnOnlyCompleteData = return_only_complete_data
        report_request.Time = time
        report_request.ReportName = "Keyword Performance Report"
        scope = reporting_service.factory.create('AccountThroughAdGroupReportScope')
        scope.AccountIds = {'long': [account_id]}
        scope.Campaigns = None
        report_request.Scope = scope

        report_columns = reporting_service.factory.create('ArrayOfKeywordPerformanceReportColumn')
        report_columns.KeywordPerformanceReportColumn.append(['AccountName',
                                                              'AccountNumber',
                                                              'AccountId',
                                                              'TimePeriod',
                                                              'CampaignName',
                                                              'CampaignId',
                                                              'AdGroupName',
                                                              'AdGroupId',
                                                              'Keyword',
                                                              'KeywordId',
                                                              'FirstPageBid',
                                                              'DeviceType',
                                                              'CurrentMaxCpc',
                                                              'Clicks',
                                                              'Impressions',
                                                              'Conversions',
                                                              'ConversionRate',
                                                              'AverageCpc',
                                                              'Spend'
                                                              ])
        report_request.Columns = report_columns

        return report_request
    except:
        print(sys.exc_info())


def download_get_keyword_craft_ai_report(report_request, authorization_data, s_date, e_date, qry_type):
    """
        Download keyword performance report that has been request.

        Parameters
        ----------
        report_request: dictionary: table
            Tables and columns that are in request
        authorization_data: dictionary: table
            access to data after getting user's credentials
       s_date : date
            The starting date
       e_date : date
            The end date
       qry_type : string
            The group or aggregation type:The data is aggregated daily or weekly

        Returns
        -------
        table with keyword performance

        """
    try:
        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]
        endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,

            overwrite_result_file=True,
            timeout_in_milliseconds=3600000
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            sys.exit(0)

        keyword_analytics_data = pd.DataFrame(
            columns=[
                'TimePeriod',
                'AccountName',
                  'AccountNumber',
                  'AccountId',
                  'CampaignName',
                  'CampaignId',
                  'AdGroupName',
                  'AdGroupId',
                  'Keyword',
                  'KeywordId',
                  'FirstPageBid',
                  'DeviceType',
                  'CurrentMaxCpc',
                  'Clicks',
                  'Impressions',
                  'Conversions',
                  'ConversionRate',
                  'AverageCpc',
                  'Spend'
                     ])
        if "Impressions" in report_container.report_columns and \
                "Clicks" in report_container.report_columns and \
                "Spend" in report_container.report_columns and \
                "CampaignId" in report_container.report_columns:

            report_record_iterable = report_container.report_records

            for record in report_record_iterable:
                tmp_dict = {}
                tmp_dict["AccountName"] = record.value("AccountName")
                tmp_dict["TimePeriod"] = record.value("TimePeriod")
                tmp_dict["AccountNumber"] = record.value("AccountNumber")
                tmp_dict["AccountId"] = record.int_value("AccountId")
                tmp_dict["CampaignName"] = record.value("CampaignName")
                tmp_dict["CampaignId"] = record.int_value("CampaignId")
                tmp_dict["AdGroupName"] = record.value("AdGroupName")
                tmp_dict["AdGroupId"] = record.int_value("AdGroupId")
                tmp_dict["Keyword"] = record.value("Keyword")
                tmp_dict["KeywordId"] = record.int_value("KeywordId")
                tmp_dict["FirstPageBid"] = record.value("FirstPageBid")
                tmp_dict["DeviceType"] = record.value("DeviceType")
                tmp_dict["CurrentMaxCpc"] = record.value("CurrentMaxCpc")
                tmp_dict["Clicks"] = record.int_value("Clicks")
                tmp_dict["Impressions"] = pd.to_numeric(record.value("Impressions"), errors='coerce')
                tmp_dict["Conversions"] = pd.to_numeric(record.value("Conversions"), errors='coerce')
                tmp_dict["ConversionRate"] = pd.to_numeric(record.value("ConversionRate"), errors='coerce')
                tmp_dict["AverageCpc"] = pd.to_numeric(record.value("AverageCpc"), errors='coerce')
                tmp_dict["Spend"] = float(record.value("Spend"))

                keyword_analytics_data = pd.concat([keyword_analytics_data, pd.DataFrame.from_records([dict(tmp_dict)])], ignore_index=True)

                if qry_type in ["week", "weekly"]:
                    keyword_analytics_data["week"] = week_number
                elif qry_type in ["month", "monthly"]:
                    keyword_analytics_data["month"] = startDate.month
                elif qry_type in ["day", "daily"]:
                    keyword_analytics_data["week"] = week_number


        report_container.close()

        return keyword_analytics_data
    except:
        print(sys.exc_info())

