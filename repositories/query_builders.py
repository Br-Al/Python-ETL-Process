import abc
from elasticsearch_dsl import Q, Search
from elasticsearch import Elasticsearch
from google.analytics.data_v1beta.types import (
    Dimension,
    Metric,
    DateRange,
    RunReportRequest
)


class SqlQueryBuilder(abc.ABC):
    @abc.abstractmethod
    def build_query(self):
        pass

    def __init__(self, table_name, columns=None) -> None:
        self.table_name = table_name
        self.columns = columns


class InsertQueryBuilder(SqlQueryBuilder):
    def build_query(self):
        values = ['?' for i in range(len(self.columns))]
        query = (
            f"INSERT INTO {self.table_name} ({','.join(self.columns)}) "
            f"VALUES ({','.join(values)})"
        )

        return query


class SelectQueryBuilder(SqlQueryBuilder):
    def build_query(self, conditions: str = None, order_by: str = None, all: bool = False, limit: int = None):
        if all:
            self.columns = ["*"]
        query = (
            f"SELECT {','.join(self.columns)} "
            f"FROM {self.table_name} "
        ) 

        if conditions:
            query += f"WHERE {conditions} "
        if order_by:
            query += f"ORDER BY {order_by} "
        if limit:
            query += f"LIMIT {limit}"

        return query


class DeleteQueryBuilder(SelectQueryBuilder):
    def build_query(self, conditions: str = None):
        if conditions:
            query = (
                f"DELETE "
                f"FROM {self.table_name} "
                f"WHERE {conditions} "
            )   
        else:
            query = (
                f"DELETE "
                f"FROM {self.table_name} "
            )
        return query


class SelectMaxQueryBuilder(SqlQueryBuilder):
    def build_query(self, column, conditions: str = None):
        query = (f"SELECT MAX({column}) "
                f"FROM {self.table_name} ")
        if conditions:
            query += f"WHERE {conditions} "

        return query


class RequestBuilder(abc.ABC):
    @abc.abstractmethod
    def build_query(self):
        pass

    def __init__(self, property_id, dimensions, metrics, start_date, end_date) -> None:
        self.property_id = property_id
        self.dimensions = [Dimension(name=dimension) for dimension in dimensions if dimensions]
        self.metrics = [Metric(name=metric) for metric in metrics if metrics]
        self.date_ranges = [DateRange(start_date=start_date, end_date=end_date)]

    
class RequestBodyBuilder(RequestBuilder):
    def build_query(self):
        # Define the request body
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=self.dimensions,
            metrics=self.metrics,
            date_ranges=self.date_ranges,
        )

        return request

class MetaAdsQueryBuilder(abc.ABC):
    @abc.abstractmethod
    def build_query(self):
        pass

    def __init__(self, fields, start_date, end_date, date_step) -> None:
        self.fields = fields
        self.start_date = start_date
        self.end_date = end_date
        self.date_step = date_step


class MetaAdsRequestBuilder(MetaAdsQueryBuilder):
    def build_query(self):
        # Define the request body
        request = {'fields': self.fields, 'params': {'time_range': {'since': self.start_date, 'until': self.end_date}, 'time_increment': self.date_step}}
        
        return request
    

class ElasticQueryBuilder(abc.ABC):
    @abc.abstractmethod
    def build_query(self):
        pass

    def __init__(self, index, fields, start_date, end_date) -> None:
        self.index = index
        self.fields = fields
        self.start_date = start_date
        self.end_date = end_date


class ElasticSearchBuilder(ElasticQueryBuilder):
    def build_query(self)-> Search:
        # Define the search object and index pattern  
        s = Search(index=self.index)  
        
        # Define the query filter using Q function  
        q = Q('wildcard', site='')   # TODO: add the site filter

        # Add the date range filters to the search object  
        s = s.filter('range', log_timestamp={'gte': self.start_date, 'lte': self.end_date})  

        # Add the filters to the search object  
        s = s.query(q)  
        
        # Define the fields to be included in the search results  
        s = s.source(self.fields)

        return s