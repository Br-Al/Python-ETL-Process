#!/usr/local/bin/python3
import sys
from google.ads.googleads.client import GoogleAdsClient


def google_ads_authentication(cred_dict):
    """
    Gets user's credentials and grant access to data of Google ads.

    Parameters
    ----------
    cred_dict : dictionary
        The user's credentials that authorize an application to access ads' data on behalf of user.

    Returns
    -------
    Access to dictionary data.

    """
    try:
        client = GoogleAdsClient.load_from_dict(cred_dict)
        return client

    except Exception as e:
        print(sys.exc_info())
