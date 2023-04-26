#!/usr/bin/python3
import sys
import pandas
import json
from datetime import datetime, timedelta
import datetime
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

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
        while date_text != datetime.datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d'):
            date_text = input('Please Enter the date in YYYY-MM-DD format\t')
        else:
            return datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except:
        raise Exception('get_fb_data_query : year does not match format yyyy-mm-dd')


def get_facebook_campaign_data(app_id, app_secret, access_token, account, s_date, e_date, qry_type,
                               camapign_type_json):
    """
    Request and download campaign performance report for an facebook ads account in a time range.

    Parameters
    ----------
    app_id: int
        The id of an application that will access ads account on behalf of user.
    app_secret : int
        The secret key of application that will access ads account on behalf of user.
    access_token : str
        This allows an application to connect to Facebook API.
   s_date : date
        The starting date.
   e_date : date
        The end date
   qry_type : string
        The group or aggregation type:The data is aggregated daily or weekly.
   camapign_type_json : dictionary
        The keys and values for different campaign types.

    Returns
    -------
     dictionary data in table and columns for campaign.

    """
    try:
        FacebookAdsApi.init(app_id, app_secret, access_token)
        account = AdAccount("act_" + account)
        campaigns = account.get_campaigns(fields=['id', 'name', 'account_id'])

        startDate = date_validation(s_date)
        dt = startDate + timedelta(1)
        week_number = dt.isocalendar()[1]

        endDate = date_validation(e_date)

        campaign_data_df = pandas.DataFrame(columns=[

            'date_start',
            'date_stop',
            'time_increment',
            'account_currency',
            'account_id',
            'campaign_id',
            'account_name',
            'campaign_name',
            'clicks',
            'cost_per_inline_link_click',
            'cost_per_inline_post_engagement',
            'cost_per_unique_click',
            'cost_per_unique_inline_link_click',
            'impressions',
            'inline_link_clicks',
            'reach',
            'spend',
            'objective',
            'campaignType'])

        fields = [
            'account_currency',
            'account_id',
            'campaign_id',
            'account_name',
            'campaign_name',
            'clicks',
            'cost_per_inline_link_click',
            'cost_per_inline_post_engagement',
            'cost_per_unique_click',
            'cost_per_unique_inline_link_click',
            'impressions',
            'inline_link_clicks',
            'reach',
            'spend',
            'objective'
        ]

        for campaign in campaigns:
            for camp_insight in campaign.get_insights(fields=fields,
                                                      params={'time_range': {'since': s_date, 'until': e_date},'time_increment': 1}):

                campaign_obj = camp_insight["objective"]
                if campaign_obj in camapign_type_json["off_site"]:
                    camp_insight.update({"campaignType": "off_site"})
                elif campaign_obj in camapign_type_json["on_site"]:
                    camp_insight.update({"campaignType": "on_site"})
                else:
                    None

                campaign_data_df = pandas.concat([campaign_data_df, pandas.DataFrame.from_records([dict(camp_insight)])],ignore_index=True)


        campaign_data_df["clicks"] = pandas.to_numeric(campaign_data_df["clicks"])
        campaign_data_df["cost_per_inline_link_click"] = pandas.to_numeric(campaign_data_df["cost_per_inline_link_click"])
        campaign_data_df["cost_per_unique_click"] = pandas.to_numeric(campaign_data_df["cost_per_unique_click"])
        campaign_data_df["cost_per_unique_inline_link_click"] = pandas.to_numeric(campaign_data_df["cost_per_unique_inline_link_click"])


        campaign_data_df["impressions"] = pandas.to_numeric(campaign_data_df["impressions"])
        campaign_data_df["inline_link_clicks"] = pandas.to_numeric(campaign_data_df["inline_link_clicks"])
        campaign_data_df["reach"] = pandas.to_numeric(campaign_data_df["reach"])
        campaign_data_df["spend"] = pandas.to_numeric(campaign_data_df["spend"])
        campaign_data_df["date_start"] = pandas.to_datetime(campaign_data_df["date_start"])
        campaign_data_df["date_stop"] = pandas.to_datetime(campaign_data_df["date_stop"])
        campaign_data_df["time_increment"] = 1


        if qry_type in ["week", "weekly"]:
            campaign_data_df["week"] = week_number
        elif qry_type in ["day", "daily"]:
            campaign_data_df["week"] = week_number
        elif qry_type in ["month", "monthly"]:
            campaign_data_df["month"] = startDate.month



        return campaign_data_df
    except:
        print(sys.exc_info())