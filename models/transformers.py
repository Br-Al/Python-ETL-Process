import pandas as pd
from datetime import datetime
import numpy as np

class Transformer:
    def transform(self, data):
        pass


class SimpleTransformer(Transformer):
    def transform(self, data):
        """
            Convert the input data to DataFrame

            Args:
            data (list): an arry of dictionaries

            Return:
            pandas.DataFrame: a Dataframe of the input data
        """
        
        data = pd.DataFrame(data)
        
        return data


class ComplexTransformer(Transformer):
    def transform(self, data):
        pass


class TruncateLongStringTransformer(Transformer):
    def transform(self, data):
        data[data.select_dtypes(object).columns.values] = data[data.select_dtypes(object).columns.values].apply(
            lambda x: x.str.slice(0,255)
            )
            
        return data




