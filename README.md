# CSPipeline

CSPipeline is for use as a simple ETL pipeline for moving data from an API into an internal database.
The main reason for this package is to allow for Daily updates of Court Opinion data from [CourtListener](www.courtlistener.com).
Currently we only support a backend for legal data (more specifically court opinion data) for processing.

CourtListener's API allows a user to search for recently updated files. In this search, a list of API IDs are returned. 
The results are also displayed in pages, which means multiple requests need to be made to retrieve the entire list. 
For the purpose of this library we call this "paging". 

After the list of IDs is retrieved, we place the IDs into a MongoDB collection while they wait for processing.
We utilize a batching system for the actual transformation and processing of data.

## Installation

The current installation path is to clone this repository, and run `python setup.py install` in the `base-cspipeline` directory.

## Internal Data Model

Our internal data model is designed to capture court opinion documents in plain text along with it's metadata.

We store the following information in a MongoDB:

- **\_id**: Our generic internal ID. Used by MongoDB as a unique ID.
- **metadata**: Data pertaining to source documents
  - **api\_id**: ID assigned by the external API
  - **api\_resource\_url**: Direct link to the source document
  - **api\_date\_modified**: Time of last update.
  - **court\_from**: Issuing Court.
  - **citations**: Certain sources provide a list of citations used in the document.
  - **created**: Initial creation date.
  - **retrieved**: Date the data was retrieved. 
  - **pages**: Number of pages in the document.
  - **document\_type**: What kind of document that is stored.
  - **case\_type**: What type of case the document was created for.
- **content**: Actual content of the document.
  - **document\_title**
  - **document\_text**

## Generic Pipeline Structure

The generic pipeline works as follows:

1. Make a call to the main API Endpoint, using either the APIPagingOperator or SinglePageOperator.
  - These operators are required to receive certain functions for manipulating API responses.
2. Optional: Assert there is data to process, otherwise skip the following steps (Using the NoDataOperator)
3. After the main API call is done, spin up a variable number of Extract and Transform tasks.
  - There should be the same number of Extract and Transform tasks.
  - An Extract task should have a downstream Transform task.

Visually we can see this structure from our basic Daily running CourtListener DAG:

![Court Listener Example DAG](https://github.com/drsooch/CS410-Pipeline/blob/master/images/CourtListenerGraph.png) 


This pipeline can be constructed in two ways.

1. A user can directly write out the Operators and DAG construction directly similar to tutorials found in the Airflow Documentation. See an example [here](https://github.com/drsooch/CS410-Pipeline/blob/master/examples/court_listener_static.py).
2. Or, the user can defer to functions provided to construct the DAG for them. Using the functions under `cspipeline.dag_creation` the user can provide arguments directly that get passed onto the internal DAG creation. The final DAG ends up similar to the example shown in 1. There is one change the user must make to get the DAG recognized by airflow. The best way we've found to do this is simply add the line: 

```python
globals()['UNIQUE_DAG_NAME'] = construct_paging_dag(args)
```

This places the DAG in global scope where Airflow will find the DAG visible.

## Operators

This section provides a brief outline of the Operators used in the Pipeline and their arguments.

### APIOperator

The APIOperator is the Base class for our two main API "Start" operators. It encapsulates the necessary information for sending API requests including:

- HTTP Conn ID: The HTTP string id with the main URL to query (stored as an Airflow connection)
- Mongo Conn ID: The connection to MongoDB (stored as an Airflow Connection)
- The API endpoint to use
- Request headers/options
- A Query Builder: a function that can generate a query string for the request

The APIOperator also encapsulates any functionality needed to interpret API response:

- Parser: a function that parses a valid API response to produce information
- Response Counter: a function that determines the number of items produced by an API call
- Response Validator: a function that determines what Status Codes are valid

The main goal of the APIOperator was to extend the functionality of Airflow's SimpleHTTPOperator (where much of it's design is derived). The SimpleHTTPOperator only runs a single request. Our initial design was to query [CourtListener](https://courtlistener.com), however due to the structuring of their API we needed the ability to send multiple requests from the same task.
The APIOperator still allows the option to make a single request (APISinglePageOperator).

### APIPagingOperator

The term "paging" is derived from the idea that certain API requests return only a portion of the total request. The remainder of which can be accessed by querying the next "page".

The APIPagingOperator builds off our internal APIOperator, however it requires a few extra paramaters.

- Next Page: a function that returns the next page in the request (could be a full URL or just the endpoint)
- Check More: a function that returns a Boolean signalling if there are more pages to request.

The functionality of the APIPagingOperator is as follows:

1. We make the first API call to the endpoint request WITH the query builder passed in.
2. Check for available data (response_count) skipping to the end if no data
3. Otherwise it parses the first page and it's data pushing to our persistence layer (MongoDB)
4. Check to see if there are more "pages" (check_more) setting the endpoint to the proper location.
5. If there is no more data we finish

### APISinglePageOperator

The APISingleOperator is essentially a drop in replacement for SimpleHTTPOperator, with a few extra features outlined in APIOperator.

### NoDataOperator

The NoDataOperator is a re-implementation of the ShortCircuitOperator. Upstream in our APIOperators we push data to XCOM that indicates no data was received. We explicitly check for this key which indicates we can skip our downstream tasks.

### Extract Function

The Extract function is designed to query a single API endpoint, extract the necessary data from the response, and store it in our persistence layer. Technically, the extract function operates in a loop, where it processes a batch of endpoints. 

This is not explicitly an Operator as of now. It should be wrapped in a PythonOperator.
The extract function requires:

- Batch ID: Should be the DAG id name tagged with the batch number i.e. `DAG_ID1`
- Method: Defaults to "GET" operation (should always be a GET anyways)
- HTTP Conn ID: The HTTP string id with the main URL to query (stored as an Airflow connection)
- Mongo Conn ID: The connection string ID to MongoDB (stored as an Airflow Connection)

### Transform Function

The Transform function is designed to transform API response data into our internal data model.
We do this requiring a Map of our Internal Keys to Keys found in the API response. This function extracts the fields it needs and constructs a document for consumption.

Like the Extract Function, we use the PythonOperator and pass this function as a callable.
The transform function requires:

- Batch ID: Should be the DAG id name tagged with the batch number i.e. `DAG_ID1`
- Mapping: A dict of string key value pairs mapping our internal data model keys to keys found in the API response. The values can be arbitrarily nested dictionaries to indicate fields nested inside other fields. 
- Unique Keys: A list of unique keys found in the API response should be provided, we use this to hash our own internal unique IDs.

### Constructing Dags

We offer a single option as of now for constructing a DAG using our Operators, `construct_paging_dag`.

This function requires:

- DAG ID: we need this explicitly for use in our other operators, so we require it here
- Paging Kwargs: Keyword arguments to pass to APIPagingOperator.
- Key Map: Mapping for use in the Transform stage
- Key List: Unique Keys for use in Transform stage
- Start Date: Start date of the DAG
- Schedule: Schedule for the DAG
- Number of Batches: Number of batches to create (defaults to 5)
- Default Args: This is the Default Args dict that is used by the decorator `@apply_defaults`. There is a generic fallback default args, but is largely useless.
- DAG Kwargs: Keyword arguments to pass to DAG

There is also the main CourtListener DAG builder. All of it's defaults are structured to run Daily, however replacing the schedule argument allows the user to specify their preferred time frame.


