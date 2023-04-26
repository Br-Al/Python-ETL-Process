from repositories.connectors import Connector
from repositories.query_builders import SqlQueryBuilder


class Loader:
    def load(self, data):
        pass

    def __init__(self, connector: Connector, query_builder: SqlQueryBuilder = None):
        self.connector = connector
        if query_builder:
            self.query = query_builder.build_query()


class DwhLoader(Loader):
    def load(self, data, **kwargs):
        if not data.empty:
            try:
                self.connector.connect(backend='pyodbc')
                for row_count in range(0, data.shape[0]):
                    base = data.iloc[row_count:row_count + 1, :]
                    chunk = base.values.tolist()
                    tuple_of_tuples = tuple(tuple(x) for x in chunk)
                    self.connector.conn.cursor().executemany(self.query, tuple_of_tuples)
            except Exception as e:
                print(f"Error:{e}")

class S3Loader(Loader):
    def __init__(self, connector: Connector, key, bucket, query_builder: SqlQueryBuilder = None):
        super().__init__(connector, query_builder)
        self.key = key
        self.bucket = bucket

    def load(self, data):
        self.connector.connect()
        self.connector.conn.Bucket(self.bucket).put_object(Key=self.key, Body=data.to_csv(index=False)) 

