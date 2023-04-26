import abc
import warnings
import pandas as pd
import requests
from repositories.connectors import Connector
from repositories.query_builders import (
    OutBrainRequestCampaignBuilder, 
    OutBrainRequestPerformanceReportBuilder,
)


class Extractor(abc.ABC):
    @abc.abstractmethod
    def extract_data(self):
        pass

    def __init__(self, connector:Connector, query_builder):
        self.connector = connector
        self.query_builder = query_builder


class GoogleAdsExtractor(Extractor):
    def extract_data(self):
        self.connector.connect()
        print("Extracting data from Google Ads")
        self.connector.disconnect()


class GoogleAnalyticsExtractor(Extractor):
    def extract_data(self):
        self.connector.connect()
        # Execute the request
        query = self.query_builder.build_query()
        response = self.connector.conn.run_report(query)
          
        # Extract the dimension headers and metric headers from the response
        dimension_headers = response.dimension_headers
        metric_headers = response.metric_headers

        # Extract the dimension names and metric names from the headers
        dimension_names = [header.name for header in dimension_headers]
        metric_names = [header.name for header in metric_headers]

        # Extract the rows from the response
        rows = response.rows

        # Create an empty list to store the data for the DataFrame
        data = []

        # Iterate through the rows
        for row in rows:
            # Extract the dimension values and metric values from the row
            dimension_values = row.dimension_values
            metric_values = row.metric_values

            # Extract the values from the dimension values and metric values
            dimension_values = [value.value for value in dimension_values]
            metric_values = [value.value for value in metric_values]

            # Create a dictionary to store the data for the row
            row_data = dict(zip(dimension_names + metric_names, dimension_values + metric_values))

            # Add the row data to the list of data
            data.append(row_data)

        # Create the DataFrame from the list of data
        data = pd.DataFrame(data)
        
        # Convert the date Column to datetime
        data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d')

        # Return the DataFrame
        return data


class MetaAdsExtractor(Extractor):
    def extract_data(self):
        campaign_types = {} # TODO: Add campaign types
        campaign_data = pd.DataFrame(
            columns=[] # TODO: Add columns
        )

        self.connector.connect()
        campaigns = self.connector.conn.get_campaigns(
            fields = [] # TODO: Add fields
            )
        fields, params = self.query_builder.build_query()
        for campaign in campaigns:
            for campaign_insight in campaign.get_insights(fields=fields, params=params):
                campaign_objectives = campaign_insight.get('campaign_objective', None)
                campaign_insight.update({'campaignType': 'off-site' if campaign_objectives in campaign_types['off_site'] else 'on-site'})
                campaign_data_df = pd.concat([campaign_data_df, pd.DataFrame.from_records([dict(campaign_insight)])],ignore_index=True)

        campaign_data['clicks'] = campaign_data['clicks'].astype(int)
        campaign_data['impressions'] = campaign_data['impressions'].astype(int)
        campaign_data['reach'] = campaign_data['reach'].astype(int)
        campaign_data['spend'] = campaign_data['spend'].astype(float)
        campaign_data['cost_per_inline_link_click'] = campaign_data['cost_per_inline_link_click'].astype(float)
        campaign_data['cost_per_inline_post_engagement'] = campaign_data['cost_per_inline_post_engagement'].astype(float)
        campaign_data['cost_per_unique_click'] = campaign_data['cost_per_unique_click'].astype(float)
        campaign_data['cost_per_unique_inline_link_click'] = campaign_data['cost_per_unique_inline_link_click'].astype(float)
        campaign_data['inline_link_clicks'] = campaign_data['inline_link_clicks'].astype(int)
        campaign_data['date_start'] = pd.to_datetime(campaign_data['date_start'], format='%Y-%m-%d')
        campaign_data['date_stop'] = pd.to_datetime(campaign_data['date_stop'], format='%Y-%m-%d')
        campaign_data['time_increment'] = 1
        if self.query_builder.frequency == 'week':
            campaign_data['week'] = campaign_data['date_start'].dt.isocalendar().week
        elif self.query_builder.frequency == 'month':
            campaign_data['month'] = self.query_builder.start_date.month
        elif self.query_builder.frequency == 'day':
            campaign_data['day'] = self.query_builder.start_date.day

        return campaign_data


class XXXExtractor(Extractor):
    def __init__(self, connector: Connector, query_builder, from_date=None):
        super().__init__(connector, query_builder)
        self.from_date = from_date

    def extract_data(self):
        conditions = None
        if self.from_date:
            conditions = f"date_log > '{self.from_date}'"
        query = self.query_builder.build_query(conditions=conditions, all=True, order_by="date_log")
        self.connector.connect('psycopg')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            data = pd.read_sql(query, self.connector.conn)
        self.connector.disconnect()

        return data


class DWHExtractor(Extractor):
    def extract_data(self):
        self.connector.connect(backend='pyodbc')
        query = self.query_builder.build_query()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            data = pd.read_sql(query, self.connector.conn)
        self.connector.disconnect()

        return data
    

class OutBrainExtractor(Extractor):
        
    def extract_data(self):
        self.connector.connect()
        query = self.query_builder.build_query()
        if isinstance(self.query_builder, OutBrainRequestPerformanceReportBuilder):
            response = requests.get(
                query,    
                headers={'OB-TOKEN-V1': self.connector.token}
            )
            data = pd.json_normalize(response.json()['campaignResults'], meta=['campaignId'], record_path=['results'])
            if (not data.empty):    data['Date'] = data['metadata.id']    
            data = data.drop(['metadata.id', 'metadata.fromDate', 'metadata.toDate'], axis=1)

        elif isinstance(self.query_builder, OutBrainRequestCampaignBuilder):
            response = requests.get(
                query, 
                headers={'OB-TOKEN-V1': self.token}
            )
            data = pd.json_normalize(response.json()['campaigns'])

        return data
    

class ElasticSearchExtractor(Extractor):
    def extract_data(self):
        self.connector.connect()
        search = self.query_builder.build_query()
        response = search.using(self.connector.conn).execute()
        data = []
        for hit in response:
            hit = hit.to_dict()
            row = {} # TODO: Extract data from hit
            data.append(row)
        data = pd.DataFrame(data)

        return data
    