import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import stripe
from common.db import Database
from common.config import get_api_config
from common.date_tools import get_max_date,generate_date_list
today = datetime.today()
delta = timedelta(days=1)
yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
db = Database()
MY_STRIPE_PATH = 'C:/stripe_cred.json'
cred_json = get_api_config(MY_STRIPE_PATH)
stripe.api_key = cred_json["api_key"]
delete_query = f"DELETE FROM [Acquisition].[STRIPE].[Charges] WHERE TransactionDate = (select max(TransactionDate) from [Acquisition].[STRIPE].[Charges])"
db.data_push(delete_query)
max_charge_date_query = """SELECT MAX(TransactionDate) FROM Acquisition.[STRIPE].[Charges]"""
max_charge_processed_date = db.get_query_df(max_charge_date_query)
max_charge_date = max_charge_processed_date.iat[0, 0]
max_date = get_max_date(max_charge_date)
today = str(today)

if max_date < today:
    start_date = str(max_date)
    try:
        for i in range(23):
            hour1 = str(i).zfill(2)
            hour2 = str(i + 1).zfill(2)
            start = str(round(
                time.mktime(datetime.strptime(start_date + " " + hour1 + ":00:00", "%Y-%m-%d %H:%M:%S").timetuple())))
            end = str(round(
                time.mktime(datetime.strptime(start_date + " " + hour2 + ":00:00", "%Y-%m-%d %H:%M:%S").timetuple())))
            ch_list = stripe.Charge.list(created={'gt': start, 'lte': end}, limit=1000)
            r_list = stripe.Refund.list(created={'gt': start, 'lte': end}, limit=1000)

            df_temp = pd.DataFrame(pd.json_normalize(ch_list)['data'][0])
            df_r_temp = pd.DataFrame(pd.json_normalize(r_list)['data'][0])

            if i == 0:
                charge_df = pd.DataFrame(pd.json_normalize(ch_list)['data'][0])
                refund_df = pd.DataFrame(pd.json_normalize(r_list)['data'][0])

            else:
                charge_df = pd.concat([charge_df, df_temp], ignore_index=True)
                refund_df = pd.concat([refund_df, df_r_temp], ignore_index=True)

        charge_df.drop(['failure_balance_transaction', 'transfer_data', 'transfer_group'], axis=1, inplace=True)
        charge_df['fraud_details'] = charge_df['fraud_details'].astype(str)
        charge_df['metadata'] = charge_df['metadata'].astype(str)
        charge_df['outcome'] = charge_df['outcome'].astype(str)
        charge_df['payment_method_details'] = charge_df['payment_method_details'].astype(str)
        charge_df['refunds'] = charge_df['refunds'].astype(str)
        charge_df['source'] = charge_df['source'].astype(str)
        charge_df['billing_details'] = charge_df['billing_details'].astype(str)
        charge_df.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        charge_df = charge_df.fillna('')
        charge_df['TransactionDate'] = charge_df['created']
        charge_df['TransactionDate'] = pd.to_datetime(charge_df['TransactionDate'], unit='s')
        charge_df['ProcessingDate'] = start_date

        if not charge_df.empty:
            insert_query = """INSERT INTO [Acquisition].[STRIPE].[Charges] ([id], [object], [amount], [amount_captured], [amount_refunded],
                                [application], [application_fee], [application_fee_amount],[balance_transaction],[billing_details],
                                [calculated_statement_descriptor], [captured], [created], [currency], [customer], [description], [destination],
                                [dispute], [disputed], [failure_code], [failure_message], [fraud_details], [invoice], [livemode], [metadata],
                                [on_behalf_of], [order], [outcome], [paid], [payment_intent],[payment_method], [payment_method_details],
                                [receipt_email], [receipt_number], [receipt_url], [refunded], [refunds], [review], [shipping], [source],
                                [source_transfer], [statement_descriptor], [statement_descriptor_suffix], [status],[TransactionDate], [ProcessingDate])
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)"""
            db.insert_many(charge_df, insert_query)

        if not refund_df.empty:
            refund_df['metadata'] = refund_df['metadata'].astype(str)
            refund_df['TransactionDate'] = refund_df['created']
            refund_df['TransactionDate'] = pd.to_datetime(charge_df['TransactionDate'], unit='s')
            refund_df['ProcessingDate'] = start_date

            insert_query = """INSERT INTO [STRIPE].[Refunds] ([id], [object], [amount], [balance_transaction], [charge],
                                       [created], [currency], [metadata], [payment_intent], [reason], [receipt_number],
                                       [source_transfer_reversal], [status], [transfer_reversal],[TransactionDate],[ProcessingDate])
                                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"""
            db.insert_many(refund_df, insert_query)
    except Exception as e:
        raise e