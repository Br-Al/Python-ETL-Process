import requests
import pandas as pd

class Taboola:
    def __init__(self, Account_name, client_id, client_secret):
        self.url = 'https://backstage.taboola.com/backstage'
        self.api = '/api/1.0'
        self.account_name = '/' + Account_name
        self.AN = Account_name
        self.payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }
        access_token = self.check_conection()
        self.headers = {'Authorization': f'Bearer {access_token}'}

    def check_conection(self):
        response = requests.post(self.url + '/oauth/token', data=self.payload)
        return response.json()['access_token']

    def allowed_accounts(self):
        response = requests.get(self.url + self.api + '/users/current/allowed-accounts/', headers=self.headers)
        return response.json()

    def Summary(self, start_date, end_date):
        response = requests.get(
            self.url + self.api + self.account_name + '/reports/campaign-summary/dimensions/day?start_date=' + start_date + '&end_date=' + end_date,
            headers=self.headers)
        summary = pd.json_normalize(response.json()['results'], meta=['item'])
        summary['account_name'] = self.AN
        return summary

    def campaign_content(self, Date):
        response = requests.get(
            self.url + self.api + self.account_name + '/reports/top-campaign-content/dimensions/item_breakdown?start_date=' + Date + '&end_date=' + Date,
            headers=self.headers)
        Campagins = pd.json_normalize(response.json()['results'], meta=['item'])
        Campagins['date'] = Date
        return Campagins

    def Campaigns(self, Date):
        url = self.url + self.api + self.account_name + '/campaigns/?fetch_level=R'
        response = requests.get(url, headers=self.headers)
        Campain_info = pd.json_normalize(response.json()['results'], meta=['id'])
        Campain_info['Date'] = Date
        return Campain_info
