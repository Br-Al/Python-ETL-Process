import requests
import pandas as pd
import json
from common.config import get_api_config
MY_OUTBRAIN_PATH = 'C:/outbrain_cred.json'
cred_json = get_api_config(MY_OUTBRAIN_PATH)
api_token = cred_json["API_TOKEN"]

class OutBrain:
    """
        A class to represent an Outbrain web traffic.

        ...

        Attributes
        ----------
        username : str
            the username of an account accessing outbrain
        password : str
            the password of an account accessing outbrain
        account_name : str
            the name of advertising account
        """
    def __init__(self, username, password, account_name):
        """
       Constructs all the necessary attributes for the outbrain object.

       Parameters
       ----------
       username : str
        the username of a login account accessing outbrain
        password : str
            the password of a login account accessing outbrain
        account_name : str
            the name of advertising account
       """
        self.url = 'https://api.outbrain.com/amplify/v0.1'
        self.password = password
        self.username = username
        self.account_name = account_name
        self.token = api_token
        self.marketer_id = self.get_marketer_id()

    def get_token(self):
        """
            This allows a developer to connect to outbrain API
        """
        print("get token")
        response = requests.get(self.url + '/login',
                                auth=requests.auth.HTTPBasicAuth(self.username,
                                                                 self.password))

        return response.json()['OB-TOKEN-V1']

    def get_marketer_id(self):
        """
            This return the marketer ID of each advertising account.
        """
        print("get marketerID")

        response = requests.get(self.url + '/marketers', headers={'OB-TOKEN-V1': self.token})
        result = pd.json_normalize(response.json()['marketers'], meta=['id'])
        return result[result['name'] == self.account_name]['id'].iloc[0]

    def Campaigns(self):
        """
            This return the campaigns associated to an advertising account.
        """
        print("get camp")
        response = requests.get(
            self.url + '/marketers/' + self.marketer_id + "/campaigns?includeArchived=true&fetch=basic",
            headers={'OB-TOKEN-V1': self.token})
        campaigns = pd.json_normalize(response.json()['campaigns'])
        return campaigns

    def Campaign_periodic(self, start, end):
        """
       Downloads campaign performance report for an Outbrain ads account in a time range.

       Parameters
       ----------
      start : date
           The starting date.
      end : date
           The end date
       Returns
       -------
        dictionary data in table and columns for campaign performance

        """
        print("get camp periodic")
        response = requests.get(
            self.url + '/reports/marketers/' + self.marketer_id + "/campaigns/periodic?from=" + start + "&to=" + end + "&breakdown=daily&limit=500",
            headers={'OB-TOKEN-V1': self.token})

        Campaign_periodic_report = pd.json_normalize(response.json()['campaignResults'], meta=['campaignId'],
                                                     record_path=['results'])

        if (not Campaign_periodic_report.empty):
            Campaign_periodic_report['Date'] = Campaign_periodic_report['metadata.id']

            Campaign_periodic_report = Campaign_periodic_report.drop(
                ['metadata.id', 'metadata.fromDate', 'metadata.toDate'], axis=1)
        return Campaign_periodic_report