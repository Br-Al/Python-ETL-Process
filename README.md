The following documentation is intended to outline the steps required to build an efficient ETL (Extraction, Transformation, and Loading) flow using Python in order to help fellow developers quickly understand and use the same process in their own scripts.

# Step 1: Data Extraction
The first step in any ETL process is data extraction. This is the process of retrieving data from its source, often including collection via API calls, CSV files, web scraping scripts, etc. Once the data has been extracted, it is important to keep track of where each data set came from and how it was retrieved for future reference.

To do complete this step you can create an Extractor Object from data.extractors a set of classes from the abstract class called Extractor . A class is an object-oriented programming (OOP) construct that allows you to group related data and functions under one name. The Extractor class consists of two methods : 

the extract_data method, which is abstract and so must be implemented by a subclass 

the constructor, which takes two arguments, a Connector and a QueryBuilder.

## Connector
The connector class Connector that is inherited from abstract base class abc.

The @abc.abstractmethod decorator marks two methods in the class, one called connect, and the other one called discponnect(). This makes it so that these methods must be defined in any class that inherits from Connector, in order to be able to use the class.

In other words, if another class is created that extends Connector, then those classes will need to define both connect and disconnect methods in order to successfully use the class.

The repositories.connectors.py has connections to various kinds of data sources: databases, APIs and S3 buckets.

### The DatabaseConnector 
class is used to create connections to databases using either pyodbc or psycopg as the default connection type. It takes in parameters like host, username, password, and driver required to establish a connection to the database. It also has two methods connect(), which takes in a parameter backend, that determines whether pyodbc should be used or psycopg. And disconnect() method, which closes the current connection.

### The APIConnector 
class is used to create connections to APIs. The connect() method takes in keyword arguments that are peculiar to the API endpoints being accessed. For instance, while implementing googleads and s3, it requires aws_access_key_id and aws_secret_access_key.

### The OutbrainConnector 
class is a connector for OutBrain's Amplify API. It has attributes for api_provider and four other variables - 3 for authentication purposes and 1 for URL for the endpoint.

### The MetaApiConnector 
class is used for creating connections to Facebook's Ads API. Its constructor takes in 4 parameters namely - app_id, app_secret, accesstoken, and account_id.

### Finally, the S3Connector 
class is used to interface with Amazon S3. It takes in two parameters i.e. access_key_id and secret_access_key.

## QueryBuilder
There are several classes that extend an abstract base class called SqlQueryBuilder. The abstract build_query() method is overridden by each class to construct a different type of SQL query.

### The InsertQueryBuilder 
creates an INSERT query with one or more columns as parameters. The SelectQueryBuilder creates a SELECT query, either returning all columns if all is set to True or with columns provided in the constructor. This SELECT query can also include a WHERE condition and order by clause. The DeleteQueryBuilder is like the SelectQueryBuilder but only includes a WHERE condition to filter out records that should be deleted. Finally, the SelectMaxQueryBuilder creates a SELECT MAX(<column>) query, which can include a WHERE condition.

### The RequestBuilder 
abstract base class defines a constructor for a request body that contains property, dimension, metric, and date range information. The RequestBodyBuilder extends this abstract class to provide a build_query that constructs the request body from the provided parameters.

### The OutBrainQueryBuilder 
provides an abstract base class for Outbrain queries. It has two concrete subclasses - the OutBrainRequestCampaignBuilder and the OutBrainRequestPerformanceReportBuilder. Both have their own build_query methods which create URLs to make corresponding API requests to Outbrain.

### Finally, the MetaAdsQueryBuilder 
provides an abstract base class for MetaAds queries. Its build_query method builds a request body containing fields, time range, and time increment information.

# Step 2: Data Transformation
Data transformation follows data extraction in the ETL process. During data transformation, data is manipulated and/or converted into a different format for easier use or analysis. Any changes made to the data should be documented so that others can follow the same process when using this data in the future.

The models.transformers.py contains operations which are used to extract certain information from a DataFrame in Python:

## SimpleTransformer
This class creates a DataFrame from an array of dictionaries (data), which contains the input data.

## ComplexTransformer
The complex transformer does not have any operations yet, as this is still to be defined.

## TruncateLongStringTransformer
The truncate long string transformer slices strings in a DataFrame to a maximum of 255 characters.

It should be noted that the classes are all subclasses of the main Transformer class, which provides some basic operations for these classes.

# Step 3: Data Loading
Once the data has been extracted and transformed, the next step is loading it into a database or other storage system. Depending on the structure of the final data set, this may include writing SQL code to load the data into the necessary format, or creating a connection between the data transformed in the second step and the storage solution.

The Loader class has an init method which takes in the connector and sql query builder as parameters, with the sql query builder being optional.

## The DwhLoader 
class then inherits from the Loader class and overrides the load method, enabling it to take in data and pass it to the connector's connection cursor object - with the data being converted into an iterable item using values.tolist. This is used to execute the query stored within the query_builder object.

### The S3Loader 
class also inherits from the 'Loader' class and overrides the load method. This methods takes in data and uses the connector's connection method to create a bucket and save the data as a csv file.

# Conclusion
By following the above steps, one should be able to build an efficient ETL flow and provide fellow developers with clear and concise documentation to allow them to easily replicate the same process following the coding guidelines and philosophy established. 
