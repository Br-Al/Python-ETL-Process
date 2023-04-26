
from datetime import date, datetime, timedelta

import pandas as pd


def generate_date_list(start_date, end_date):
    """
    Generates a list of dates ranged from start date, up to an end date (included)

    :param start_date: The start date of the range
    :param end_date: The end date of the range
    :return list: The list of dates
    """
    date_range = pd.date_range(start_date, end_date - timedelta(days=1), freq='d')

    return date_range


def generate_hour_list(start_date, end_date):
    hour_range = pd.date_range(start_date, end_date, freq='H')

    return hour_range
def get_max_date(max_date):
    today = datetime.today()
    delta = timedelta(days=1)
    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    max_date = str(max_date)
    max_date = datetime.strptime(max_date, '%Y-%m-%d')
    max_date_extended = max_date + delta
    max_date_extended = max_date_extended.date()
    max_date_extended = max_date_extended.strftime('%Y-%m-%d')
    if max_date_extended > yesterday:
        return 'already done'
    if max_date_extended <= yesterday:
        return max_date_extended
    return max_date_extended

