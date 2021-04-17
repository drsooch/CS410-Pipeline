# Basic Mongo Collection Structure

This document outlines the basic structure for our MongoDB.
The collection schema is subject to change but for planning purposes,
this document provides some semblance of an outline.

## Collection for Extract Functionality

The extract function/operator will make a request to a specific endpoint.
This endpoint can be found in the `ids_to_update` collection.
The basic outline schema is as follows:

```json
{
    "api_id": 123456,
    "batch_id": "random_string{batch_number}",
    "api_reference": "api_name"
}
```

The above schema has three items.
The API ID corresponds to the external API's id field. This will be used in the
API request to get the required data.
Batch ID should be the variable passed to the extraction function so it knows
which items it needs to complete.
The API reference field contains the API's formal name. If the pipeline fails
at some point, and it needs to be restarted. We have the attached information
to know which API this ID corresponds to.

## Collection for Transformation Functionality

The transform function/operator will operate in a similar manner to the
extract functionality. The collection `data_to_transform` will hold the
necessary data for transformation. The basic outline schema is as follows:

```json
{
    "batch_id": "random_string{batch_number}",
    "api_reference": "api_name",
    "data": "actual JSON data"
}

```

Similarly, the Batch ID will be given to the extract function/operator so that
it knows which data to consume.
The data field will hold the actual JSON from our API request so that it can be
transformed into our internal model.
Again, the API reference field may not be necessary, but it should be beneficial
to have in case of unforeseen errors.
