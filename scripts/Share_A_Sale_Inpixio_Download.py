import sys
from urllib import parse, request
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import datetime
from datetime import date,datetime, timedelta
from time import strftime, gmtime
from common.config import get_api_config
from common.date_tools import get_max_date,generate_date_list
from common.db import Database
from utils.get_reports_for_share_a_sale import get_transaction_report,get_affiliate_report
from pandas import DataFrame
from io import StringIO
db = Database()
transact_action_verb = 'transactiondetail'

if __name__ == '__main__':
    try:

        MY_SHARE_A_SALE_API_PATH = 'C:/share_a_sale_cred.json'

        cred_json = get_api_config(MY_SHARE_A_SALE_API_PATH)
        merchant_id = cred_json["merchant_id"]
        api_token = cred_json["api_token"]
        api_secret_key = cred_json["api_secret_key"]
        account_name = cred_json["account_name"]

        date_end_limit = date.today() - timedelta(days=1)

        max_transaction_date_query = """SELECT MAX(TransDate)as new_date
                                       FROM [ShareASale].[Transaction_Report]WHERE MerchantID= '68774'"""

        max_transaction_processed_date_df = db.get_query_df(max_transaction_date_query)
        max_transaction_date = max_transaction_processed_date_df.iat[0, 0]
        transaction_start_date = get_max_date(max_transaction_date)

        transaction_df = get_transaction_report(merchant_id, api_token, api_secret_key, transact_action_verb ,
                                              transaction_start_date,
                                              date_end_limit)

        transaction_df['MerchantID'] = merchant_id
        transaction_df['AccountName'] = account_name
        transaction_df['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        transaction_df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        transaction_df = transaction_df.fillna(0)

        if not transaction_df.empty:
            insert_query = """INSERT INTO [ShareASale].[Transaction_Report](
                       [TransID]
                      ,[UserID]
                      ,[TransDate]
                      ,[TransAmount]
                      ,[Commission]
                      ,[SsAmount]
                      ,[Comment]
                      ,[Voided]
                      ,[LastIP]
                      ,[LastReferer]
                      ,[TransactionType]
                      ,[CommissionType]
                      ,[QuantityList]
                      ,[OrderNumber]
                      ,[BannerName]
                      ,[BannerType]
                      ,[UsedACoupon]
                      , [MerchantID]
                      , [AccountName]
                      ,[CreateDate]
                       )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(transaction_df, insert_query)

        max_affiliate_date_query = """SELECT MAX(Date)as new_date
                                               FROM [ShareASale].[Affiliate_Report] WHERE MerchantID = '68774'"""

        max_affiliate_processed_date_df = db.get_query_df(max_affiliate_date_query)
        max_affiliate_date = max_affiliate_processed_date_df.iat[0, 0]
        affiliate_start_date = get_max_date(max_affiliate_date)
        date_end_limit = date.today()

        date_start = date.today() - timedelta(days=1)
        dfs = []
        next_day = date_start

        start_dates = generate_date_list(affiliate_start_date, date_end_limit)
        for date_n in start_dates:
            next_day = date_n
            date_end = next_day
            affiliate_action_verb = action_verb = 'affiliatetimespan'
            affiliate_df = get_affiliate_report(merchant_id, api_token, api_secret_key, affiliate_action_verb,
                                                  next_day,
                                                  date_end)

            dfs.append(affiliate_df)

        affiliate_columns = ['Affiliate', 'Clicks', 'GrossSales', 'Voids', 'NetSales', 'NumberofSales', 'ManualCredits',
                             'Commissions', 'Conversion', 'AverageOrder', 'AffiliateID', 'Organization', 'Website',
                             'NumbSales', 'NumbLeads', 'NumbTwoTier', 'NumbBonuses', 'NumbPayPerCall', 'NumbLeapFrog',
                             'SaleCommissions', 'LeadCommissions', 'TwoTierCommissions', 'BonusCommissions',
                             'PayPerCallCommissions', 'TransactionFees']

        combined_affiliate = pd.concat(dfs)if len(dfs) > 0 else pd.DataFrame(columns=affiliate_columns)
        combined_affiliate['MerchantID'] = merchant_id
        combined_affiliate['AccountName'] = account_name
        combined_affiliate['CreateDate'] = datetime.now().strftime("%Y-%m-%d")

        combined_affiliate.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        transaction_df = combined_affiliate.fillna(0)

        if not combined_affiliate.empty:
            insert_query = """INSERT INTO [ShareASale].[Affiliate_Report](
                      [Affiliate]
                      ,[Clicks]
                      ,[GrossSales]
                      ,[Voids]
                      ,[NetSales]
                      ,[NumberofSales]
                      ,[ManualCredits]
                      ,[Commissions]
                      ,[Conversion]
                      ,[AverageOrder]
                      ,[AffiliateID]
                      ,[Organization]
                      ,[Website]
                      ,[NumbSales]
                      ,[NumbLeads]
                      ,[NumbTwoTier]
                      ,[NumbBonuses]
                      ,[NumbPayPerCall]
                      ,[NumbLeapFrog]
                      ,[SaleCommissions]
                      ,[LeadCommissions]
                      ,[TwoTierCommissions]
                      ,[BonusCommissions]
                      ,[PayPerCallCommissions]
                      ,[TransactionFees]
                      ,[Date]
                      ,[MerchantID]
                      ,[AccountName]
                      ,[CreateDate]
                       )  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            db.insert_many(combined_affiliate, insert_query)


    except:
        print(sys.exc_info())