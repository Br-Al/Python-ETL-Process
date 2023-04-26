from datetime import date, datetime, timedelta

from common.db import Database
from utils.inapp_es import Elastic
from utils.inapp_utils import EVENTS


DB = Database()

for e in EVENTS:
    table = f"[INAPP].[{e}]"
    select_query = f"SELECT MAX(CONVERT(date, packageDate)) FROM {table}"
    max_date = DB.get_one(select_query)

    if max_date is not None:
        start_date = max_date + timedelta(days=1)
    else:
        start_date = date.today() - timedelta(days=1)

    start_date = datetime(
        year=start_date.year,
        month=7,
        day=9)
    end_date = date.today() - timedelta(days=1)
    end_date = datetime(
        year=end_date.year,
        month=end_date.month,
        day=end_date.day,
        hour=23)

    print(f"Event : {e} | Max Date : {max_date}")
    print(f"Start date : {start_date} | End date : {end_date}")

    if start_date > end_date:
        print("Already ran")
        continue

    es = Elastic(db=DB, event=e, start_date=start_date, end_date=end_date)
    es.run_import()
