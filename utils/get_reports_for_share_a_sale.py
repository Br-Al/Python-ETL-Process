import sys
from urllib import parse, request
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import time
import datetime
from datetime import date, timedelta
from time import strftime, gmtime
from common.config import get_api_config
from pandas import DataFrame
from io import StringIO


def get_affiliate_report(merchant_id, api_token, api_secret_key, action_verb, date_start, date_end):
    """
        Request and download affiliates performance report for a share a sale account in a time range.

        Parameters
        ----------
        merchant_id: int
            The id of an account in a share a sale.
        api_token : str
            This allows a developer  to connect to Share a sale API.
        api_secret_key : str.
             the key associated to share a sale API
        action_verb : str
            The action or type of report to be downloaded:affiliatetimespan
       date_start : date
            The starting date.
       date_end : date
            The end date
        Returns
        -------
         dictionary data in table and columns for affiliates.

        """
    try:
        my_timestamp = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        api_version = 3.0

        export_type = 'pipe'
        sortcol = 'affiliate ID'
        sortdir = 'desc'
        data = parse.urlencode({'merchantId': merchant_id, 'token': api_token,
                                'version': api_version, 'action': action_verb,
                                'sortCol': sortcol, 'sortDir': sortdir,
                                'dateStart': date_start, 'dateEnd': date_end,
                                'format': export_type})

        sig = api_token + ':' + my_timestamp + ':' + action_verb + ':' + api_secret_key
        sig_hash = hashlib.sha256(sig.encode('utf-8')).hexdigest()
        my_headers = {'x-ShareASale-Date': my_timestamp,
                      'x-ShareASale-Authentication': sig_hash}
        call = request.Request('https://shareasale.com/w.cfm?%s' % data, headers=my_headers)
        response = request.urlopen(call).read()
        output = response.decode("utf-8")
        affiliate_columns = ['Affiliate', 'Clicks', 'GrossSales', 'Voids', 'NetSales', 'NumberofSales', 'ManualCredits',
                   'Commissions', 'Conversion',  'AverageOrder', 'AffiliateID', 'Organization', 'Website',
                   'NumbSales', 'NumbLeads', 'NumbTwoTier', 'NumbBonuses', 'NumbPayPerCall', 'NumbLeapFrog',
                   'SaleCommissions', 'LeadCommissions', 'TwoTierCommissions', 'BonusCommissions',
                   'PayPerCallCommissions', 'TransactionFees']

        affiliate_df = pd.read_csv(StringIO(output), sep='|', engine='python',usecols= affiliate_columns)
        affiliate_df['Date'] = date_start

        affiliate_df['GrossSales'] = affiliate_df['GrossSales'].str.replace(',', '').replace('\$', '', regex=True).astype(float)
        affiliate_df['NetSales'] = affiliate_df['NetSales'].str.replace(',', '').replace('\$', '', regex=True).astype(float)
        affiliate_df['Commissions'] = affiliate_df['Commissions'].str.replace(',', '').replace('\$', '', regex=True).astype(float)
        affiliate_df['AverageOrder'] = affiliate_df['AverageOrder'].str.replace(',', '').replace('\$', '', regex=True).astype(float)
        affiliate_df['SaleCommissions'] = affiliate_df['SaleCommissions'].str.replace(',', '').replace('\$', '', regex=True).astype(float)
        affiliate_df['LeadCommissions'] = affiliate_df['LeadCommissions'].str.replace(',', '').replace('\$', '',regex=True).astype(float)
        affiliate_df['TwoTierCommissions'] = affiliate_df['TwoTierCommissions'].str.replace(',', '').replace('\$', '',regex=True).astype(float)
        affiliate_df['BonusCommissions'] = affiliate_df['BonusCommissions'].str.replace(',', '').replace('\$', '',regex=True).astype(float)
        affiliate_df['PayPerCallCommissions'] = affiliate_df['PayPerCallCommissions'].str.replace(',', '').replace('\$', '',regex=True).astype(float)
        affiliate_df['TransactionFees'] = affiliate_df['TransactionFees'].str.replace(',', '').replace('\$', '',regex=True).astype(float)

        return affiliate_df
    except:
        print(sys.exc_info())


def get_transaction_report(merchant_id, api_token, api_secret_key, action_verb, date_start, date_end):
    """
            Request and download transaction report for a share a sale account in a time range.

            Parameters
            ----------
            merchant_id: int
                The id of an account in a share a sale.
            api_token : str
                This allows a developer  to connect to Share a sale API.
            api_secret_key : str.
                 the key associated to share a sale API
            action_verb : str
                The action or type of report to be downloaded:transactiondetail
           date_start : date
                The starting date.
           date_end : date
                The end date
            Returns
            -------
             dictionary data in table and columns for transaction.

            """
    try:
        my_timestamp = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        api_version = 3.0

        export_type = 'pipe'
        sortcol = 'affiliate ID'
        sortdir = 'desc'
        data = parse.urlencode({'merchantId': merchant_id, 'token': api_token,
                                'version': api_version, 'action': action_verb,
                                'sortCol': sortcol, 'sortDir': sortdir,
                                'dateStart': date_start, 'dateEnd': date_end,
                                'format': export_type})

        sig = api_token + ':' + my_timestamp + ':' + action_verb + ':' + api_secret_key
        sig_hash = hashlib.sha256(sig.encode('utf-8')).hexdigest()
        my_headers = {'x-ShareASale-Date': my_timestamp,
                      'x-ShareASale-Authentication': sig_hash}
        call = request.Request('https://shareasale.com/w.cfm?%s' % data, headers=my_headers)
        response = request.urlopen(call).read()
        output = response.decode("utf-8")
        transaction_Columns= ['transID',
                       'userID',
                       'transdate',
                       'transamount',
                       'commission',
                       'ssamount',
                       'comment',
                       'voided',
                       'lastip',
                       'lastreferer',
                       'orderNumber',
                       'transactionType',
                       'CommissionType',
                       'quantityList',
                       'bannerType',
                       'bannerName',
                       'usedACoupon'


                       ]

        transaction_df = pd.read_csv(StringIO(output), sep='|', engine='python',usecols= transaction_Columns)

        transaction_df = transaction_df.rename(columns={'transID': 'TransID',
                                                        'userID':'UserID',
                                                        'transdate':'TransDate',
                                                        'transamount':'TransAmount',
                                                        'commission':'Commission',
                                                        'ssamount':'SsAmount',
                                                        'comment':'Comment',
                                                        'voided':'Voided',
                                                        'lastip':'LastIP',
                                                        'lastreferer':'LastReferer',
                                                        'orderNumber':'OrderNumber',
                                                        'transactionType':'TransactionType',
                                                        'CommissionType':'CommissionType',
                                                        'quantityList':'QuantityList',
                                                        'bannerName':'BannerName',
                                                        'bannernumber':'BannerNumber',

                                                        'bannerType':'BannerType',
                                                        'usedACoupon':'UsedACoupon'})

        transaction_df['TransDate'] = pd.to_datetime(transaction_df['TransDate']).dt.date
        transaction_df['TransAmount'] = transaction_df['TransAmount'].astype(float)
        transaction_df['Commission'] = transaction_df['Commission'].astype(float)
        transaction_df['SsAmount'] = transaction_df['SsAmount'].astype(float)

        return transaction_df
    except:
        print(sys.exc_info())


