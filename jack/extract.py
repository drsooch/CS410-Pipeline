from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook

def extract(batch_id, 
    method = "GET",
    http_conn_id = "default_api",
    mongo_conn_id = "default_mongo"):

    http = HttpHook(method, http_conn_id=http_conn_id)

    mongo_conn = MongoHook(mongo_conn_id)
    collection = mongo_conn.get_collection("ids_to_update")


    #Note/TODO: because we add endpoints back that we couldn't handle, we may
    #get stuck in an infinite loop. Another solution is exiting whenever an 
    #exception occurs, but this isn't ideal either
    while (mongo_conn.find("ids_to_update", { "batch_id": str(batch_id)}) != None):

        #find a job to work on
        result = collection.find_one_and_delete({ "batch_id": str(batch_id)})
        api_id = result['api_id']
        try: 

            #transform to get a valid link
            #TODO: this needs to be generalized to any website
            endpoint = "https://www.courtlistener.com/api/rest/v3/opinions/?id=" + str(api_id)

            #pull data in
            response = http.run(endpoint)

            if response.status_code == 200:

                #store our result into mongo
                mongo_conn.insert_one("results_to_transform", 
                    response.json()['results'],
                    mongo_db="courts")

            else:
                #TODO: throw a more specific exception
                raise Exception(f"Received {response.status_code} code from {endpoint}.")

        except Exception as error:

            #something went wrong. Log it and return this endpoint to mongoDB so we can try again

             print(f'An exception occured while processing batch {batch_id}:\n{error}')

            mongo_conn.insert_one("ids_to_update", 
               {"api_id": str(api_id), "batch_id": str(batch_id)},
                mongo_db="courts")