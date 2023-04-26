#!/usr/local/bin/python3
import sys
from bingads.authorization import *
from bingads.service_client import ServiceClient
def ms_auth(refresh_token, client_id, developer_token):
    """
    Gets user's credentials and grant access to data.

    Parameters
    ----------
    refresh_token : str
        This helps an application to get access without login again.
    client_id : int
        An id of the application that is accessing the ads account.
   developer_token : int
        This universal token that allows an application to connect to Bing API.

    Returns
    -------
    Access to dictionary data: tables

    """
    try:
        authorization_data = AuthorizationData(
            account_id=None,
            customer_id=None,
            developer_token=developer_token,
            authentication=None,
        )
        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=client_id,
            env='production'
        )

        authentication.state = 'bld@bingads_amp'

        authorization_data.authentication = authentication

        authorization_data.authentication.request_oauth_tokens_by_refresh_token(refresh_token)

        return authorization_data
    except:
        print(sys.exc_info())