# Pipeline

## Internal Data Model:

Our internal data representation - needs a good name :)

We can deconstruct this model into many Objects for use in MongoDB:
```JSON
{
  "metadata": {
      "api_id": 1234,
      "api_url": "https://google.com/api/v1/1234",
      "court_from": "Wyoming District Court",
      "document_type": "opinion",
      ...
  },
  "content": {
      "title": "Foo vs Bar et. al",
      "body": "LONG STRING",
      ...
  },
  "id": "ABCD"
}
```

We also have the opportunity construct nested objects for instance:

```json
{
  "metadata": {
      "api_info": {
        "api_id": 1234,
        "api_url": "https://google.com/api/v1/1234",
        "api_modified_date": 2021-03-09
      }
      ...
  }
  ...
}
```

We can also look to add some more identifying metadata, to allow for ease of filtering, searching etc.


#### Required Fields:

- `id`:
  - Our internal ID structure
  - Take unique identifiers for an API: state, court, api name and combined/convert into a UUID.
- `case_type`:
  - What kind of case are we dealing with? Tax? Bankruptcy? Criminal? Civil? etc.
- `precedence`:
  - Not sure if this is encoded elsewhere, but is the document a precedent?
- `document_text`:
  - full slug of the actual document text
- `document_date`:
  - when the document was originally created
  - this could be expanded out: date created, date submitted to court, date approved by court etc.
- `document_type`:
  - What type of document this is.
- `pages`:
  - How many pages the document actually is.
- `modified`:
  - date we last updated or modified entry
- `created`:
  - date we originally created entry
- `citations`:
  - Possible cases that cited in this case or cited by this case
  - this may have to be stored in `api_id` form, if we can't recompute internal id
- `court_from`:
  - What court the document was produced for.
- `api_id`:
  - The APIs unique id
- `api_resource_url`:
  - URL for the endpoint the data was retrieved from.
- `api_date_modified`:
  - The date this document was modified on the APIs server

-------------------------------------------------------------------------------

## Pipeline Manager:

Encapsulates entire RabbitMQ pipeline.
Constructs internal pipeline structure (setting up queues, exchange mechanism, etc.).

#### Fields:
- `connection`:
  - `PikaConnection` object. Instantiates the actual RabbitMQ system. The rest of the system is built off this.
- `channel`:
  - `PikaChannel` object. Is derived directly from a `PikaConnection`, a channel is an individual pipeline.
- `queue_exchange`:
  - Could be an actual exchange object or may just be a string name. This is for dispatch message objects through out Channel. 
- `producer_queue`:
  - The queue for `ProducerLeader` to `enqueue` and `ProducerWorker` to `dequeue`
- `retry_queue`:
  - The queue for `ProducerWorker` to `enqueue` and `DataConsumer` to `dequeue`
- `database_queue`:
  - The queue for `DataConsumer` to `enqueue` and `DatabaseConsumer` to `dequeue`

#### Methods:
- `setup()`:
  - Monolithic initialization function. Constructs connection -> channel -> exchange -> queues. Provides proper error checking for an improper setup.
  - Probably use a YAML or JSON file as a global config to setup pipeline.
- `shutdown()`:
  - Possibly a simple shutdown function to release our resources.

-------------------------------------------------------------------------------
  
## PipelineProducer:

Constructs the Producer portion of pipeline.

#### Fields:
- `manager`:
  - The current pipelines instance of a `RequestManager` class
  - Only one per pipeline
- `num_workers`:
  - Number of workers we want to use to produce information
  - most likely found in global config
- `queue_exchange`:
  - Name of the exchange for passing messages.
- `producer_queue`:
  - Name of the queue for Leader->Worker
- `consumer_queue`:
  - Name of the queue for Producer->Consumer
- `retry_queue`:
  - Name of the retry queue.

#### Methods:
- `setup()`:
  - Generic setup method that calls into the `RequestManager`
- `shutdown()`:
  - Cleans up resources
  

### RequestManager Interface:

This could be our own internal Class that we use rather than making the user define a Manager.
Essentially sets up the producer system.

#### Fields:
- `HTTP_Conn_Manager`:
  - Most likely derived from the `Requests` library, used to manage HTTP connections for the pipeline.
- `worker_type`:
  - This is the `ProducerWorker` that the user provides.

#### Methods:
- `setup_leader(HTTPConn)`:
  - Most APIs have single list of IDs or sub-endpoints that we can hit to gather data. For instance the `/opinions` endpoint lists all the case IDs we want to grab.
  - This "leader" will hit the main endpoint and pass along these sub-endpoints.
- `setup_workers(HTTPConn)`:
  - This is the where we setup the`ProducerWorker` that the user provides.
  - We will setup some sort of Threading/Parallel action to have multiple workers running
- `setup_retry(HTTPConn, Int)`:
  - This is the where we setup the`ProducerWorker` that the user provides.
  - This queue must operate under the assumption that after a certain amount of retries we fail the message and log it.
  - This retry logic could be built into our Global Config.
- `shutdown()`:
  - Cleans up resources

### ProducerWorker Interface:

An interface for the overall construction of the `ProducerWorker`. These Workers will generate the actual `ResponseMessage` that we send along the pipeline.
This may be abstracted away from the user, as we could potentially do this internally.

#### Fields:
- `queue_exchange`:
  - The basic exchange for RabbitMQ
- `work_queue`:
  - Which queue they pull from.
  
Note: These fields are probably abstracted from the client Class.

#### Methods:
- `produce()`:
  - Continually polls the `work_queue` to produce data.
- `shutdown()`:
  - resource cleanup.
  
-------------------------------------------------------------------------------

## PipelineDataConsumer:

Constructs the Data Consumer portion of pipeline.

#### Fields:
- `manager`:
  - The current pipelines instance of a `DataConsumerManager` class
  - Only one per pipeline
- `num_workers`:
  - Number of workers we want to use to produce information
  - most likely found in global config
- `queue_exchange`:
  - Name of the exchange for passing messages.
- `consumer_queue`:
  - Name of the queue for Producer->DataConsumer.
- `database_queue`:
  - Name of the DataConsumer->DatabaseConsumer queue.

#### Methods:
- `setup()`:
  - Generic setup method that calls into the `DataConsumerManager`
- `shutdown()`:
  - Cleans up resources
  

### DataConsumerManager Interface:

This could be our own internal Class that we use rather than making the user define a Manager.
Essentially sets up the DataConsumer system.
We could require the user to pass in a translation function that maps the API->InternalDataModel fields.

#### Fields:
- `worker_type`:
  - This is the `DataConsumerWorker` that the user provides.

#### Methods:
- `setup_workers()`:
  - This is the where we setup the`DataConsumerWorker` that the user may provide.
  - We will setup some sort of Threading/Parallel action to have multiple workers running
- `shutdown()`:
  - Cleans up resources

### DataConsumerWorker Interface:

An interface for the overall construction of the `DataConsumerWorker`. These Workers will generate our InternalDataModel to pass along to the DB.
This may be abstracted away from the user, as we could potentially do this internally.

#### Fields:
- `queue_exchange`:
  - The basic exchange for RabbitMQ
- `work_queue`:
  - Which queue they pull from.
  
Note: These fields are probably abstracted from the client Class.

#### Methods:
- `consume()`:
  - Continually polls the `work_queue` to translate data.
- `translate()`:
  - As mentioned before this could a function provided by the user as a way to convert between the abstract API model to our internal model.
- `shutdown()`:
  - resource cleanup.
  
-------------------------------------------------------------------------------

## PipelineDatabaseConsumer:

Constructs the Database Consumer portion of pipeline.

#### Fields:
- `manager`:
  - The current pipelines instance of a `DatabaseConsumerManager` class
  - Only one per pipeline
- `num_workers`:
  - Number of workers we want to use to produce information
  - most likely found in global config
- `queue_exchange`:
  - Name of the exchange for passing messages.
- `database_queue`:
  - Name of the queue for DataConsumer->DatabaseConsumer.

#### Methods:
- `setup()`:
  - Generic setup method that calls into the `DatabaseConsumerManager`
- `shutdown()`:
  - Cleans up resources
  

### DatabaseConsumerManager Interface:

This could be our own internal Class that we use rather than making the user define a Manager.
Essentially sets up the DatabaseConsumer system.

#### Fields:
- `worker_type`:
  - This is the `DatabaseConsumerWorker` that the user provides.

#### Methods:
- `setup_workers()`:
  - This is the where we setup the`DatabaseConsumerWorker` that the user may provide.
  - We will setup some sort of Threading/Parallel action to have multiple workers running
- `shutdown()`:
  - Cleans up resources

### DatabaseConsumerWorker Interface:

An interface for the overall construction of the `DatabaseConsumerWorker`. These Workers will generate our InternalDataModel to pass along to the DB.
This may be abstracted away from the user, as we could potentially do this internally.

#### Fields:
- `queue_exchange`:
  - The basic exchange for RabbitMQ
- `work_queue`:
  - Which queue they pull from.
  
Note: These fields are probably abstracted from the client Class.

#### Methods:
- `consume()`:
  - Continually polls the `work_queue` to get data to the database.
- `push_to_db()`:
  - Push a finalized InternalDataModel object to the database.
- `shutdown()`:
  - resource cleanup.
  
-------------------------------------------------------------------------------

## Messages

Internal Message Interface for passing data across the pipeline.

#### Fields:
- `data`:
  - simple wrapper around whatever data we are passing through.
- `routing_key`:
  - May not be explicitly needed as the `resolve_key()` method will generate the key we need to pass to the exchange

#### Methods:
- `resolve_key()`:
  - Certain Messages may need some runtime checks to make sure they are valid (ex: If we don't get a proper `Response` from our API call, we want to send to `retry_queue` rather than `consumer_queue`).
  - The `ConsumerMessages`, may just return a constant value.
  
### ResponseMessage

Takes an "wrapped" API request (whatever `Requests` library passes us) and constructs a message out of it. It should keep track of Retry state to make sure we drop it after a certain amount of retries.

#### Fields:
- `data`:
  - `Response` objects from the `Requests` library.
- `routing_key`:
  - see note in interface.

#### Methods:
- `resolve_key()`:
  - If we don't get a proper `Response` from our API call, we want to send to `retry_queue` rather than `consumer_queue`

### ConsumerMessage

Takes our InternalDataModel and wraps it into a Message.

#### Fields:
- `data`:
  - InternalDataModel
- `routing_key`:
  - see note in interface.

#### Methods:
- `resolve_key()`:
  - constant value.

-------------------------------------------------------------------------------

## Logging/Reporting:

Throughout the pipeline there should be some logging and reporting operations running concurrent with each Worker.
This provides us an avenue of showcasing results to the user when the pipeline has finished.
We can track dropped API requests, malformed data, failed conversion attempts etc.
Our reporting should be somewhat legible to the user, as it may be useful in their debugging attempts if for whatever reason their API source changes their model.

## RabbitMQ:

Also not explicitly stated are our internal choices for the how the queues will operate. 
There is a multitude of functionality that we can utilize in our pipeline. 
There are many flags in the `Pika` library (our library for interacting with RabbitMQ) that we can explore to change how our pipeline reacts to unforeseen or even expected deviations from the norm.

## Global Config:
In order to make the system more Generic, the global configuration file would be an option for users to tweak options, change the API source, provide credentials etc. 

For example:

```JSON
{
    "username": "admin",
    "password": "password",
    "producer": {
        "num_workers": 5,
        "leader_endpoint": "https://www.example.com/leader",
        "worker_endpoint": "https://www.example.com/worker/{ID}"
    }
    ...
}

```

## MongoDB:

The `DatabaseConsumer` will depend on the MongoDB interface that we construct. It should have all the necessities for pushing new data, checking for duplicates, checking for similar cases etc.
