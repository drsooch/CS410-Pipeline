from airflow import DAG
from airflow.utils.dates import datetime, days_ago, timedelta
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import hashlib
import json
import uuid
import os

from cspipeline.courtlistener import (
    # check_more,
    # daily_query,
    # next_page,
    # parser,
    # response_count,
    # response_valid,
    COURT_LISTENER_KEY_LIST,
    COURT_LISTENER_MAPPING,
)

# from cspipeline.operators import (
#     # APIPagingOperator,
#     # transform,
#     # extract,
#     local_transform,
# )

# just some globals to keep around
# START_OP_ID = "start_op"
# EXTRACT_OP_ID = "extract_op_{}"
# TRANSFORM_OP_ID = "transform_op_{}"
# DAG_ID = "CourtListener_Daily"
# NUMBER_OF_BATCHES = 5

# SKIP_KEY = "skip"

batch_name = "local_batch_"
max_batch_size = 10
number_of_batches = 5
path = '/opt/airflow/initial-dataset'
TRANSFORM_OP_ID = "transform_op_{}"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=25),
    "number_of_batches": 5,
    "log_response": True
   # 'execution_timeout': timedelta(seconds=300),
}

# daily_paging_args = {
#     "parser": parser,
#     "check_more": check_more,
#     "next_page": next_page,
#     "response_count": response_count,
#     "response_valid": response_valid,
#     "query_builder": daily_query,
#     "endpoint": "opinions",
#     "http_conn_id": "default_api",
#     "mongo_conn_id": "default_mongo",
#     "batch_name": DAG_ID
# }


def _continue(**kwargs):
    """For use with ShortCircuitOperator. Checks to see if the StartOp returns a SKIP Message."""
    ti = kwargs["ti"]
    should_continue = ti.xcom_pull(task_ids=START_OP_ID, key=SKIP_KEY)

    # if there are updates to be done, we won't find a key
    return should_continue is None

def batcher(
    batch_name: str,
    max_batch_size: int,
    number_of_batches: int,
    root_path: str
    ) -> None:

    print('ENTERING BATCHER')

    curr_item_num = 0
    batch_id = 1
    id_list = []
    mongo_conn = MongoHook(conn_id="default_mongo")

    for root, directories, files in os.walk(root_path, topdown=False):
        print('INSIDE OF THE RIGHT DIRECTORY')
        for name in files:
            fp = os.path.join(root, name)
            if curr_item_num == max_batch_size:
                # pprint(id_list)
                mongo_conn.insert_many("local_results_to_transform", id_list, mongo_db="courts")
                print(id_list)
                curr_item_num = 0
                batch_id %= number_of_batches
                batch_id += 1
                id_list = []
            if curr_item_num < max_batch_size:
                id_list.append(
                    {
                        "batch_id" : f"{batch_name}{batch_id}",
                        "file_path" : fp
                    }
                )
                curr_item_num += 1
    if (curr_item_num > 0):
        # pprint(id_list)
        mongo_conn.insert_many(
            "local_results_to_transform", id_list, mongo_db="courts"
        )
        id_list = []

def local_transform(batch_name: str, mapping: dict, keys: list) -> None:
    mongo_conn = MongoHook(conn_id="default_mongo")
    results_to_transform_coll = mongo_conn.get_collection(
        "local_results_to_transform", "courts"
    )
    court_documents_coll = mongo_conn.get_collection("court_documents", "courts")

    while results_to_transform_coll.find_one({"batch_id": batch_name}):

        batch_data = results_to_transform_coll.find_one_and_delete({"batch_id": batch_name})

        fp = batch_data['file_path']
        # print(fp)
        file = open(fp, 'r')
        input_string = file.read()
        data = json.loads(input_string, strict=False)

        try:
            output_data = build_data(data, mapping)
            unique_id = generate_id(build_seed(data, keys))
            output_data["_id"] = unique_id
            output_data["retrieved"] = datetime.utcnow().isoformat()

            # Replace the document with new information if it exists
            # Upsert will insert if no document found
            court_documents_coll.replace_one(
                {"_id": unique_id}, output_data, upsert=True
            )

        except Exception as error:
            print(f"An exception occured while processing batch {batch_name}:\n{error}")
            results_to_transform_coll.insert_one(data)


def generate_id(seed: str) -> str:
    m = hashlib.md5()
    m.update(seed.encode("utf-8"))
    return str(uuid.UUID(m.hexdigest()))


def build_seed(data: dict, keys: list) -> str:
    seed = ""
    for key in keys:
        seed += str(find_by_key(key, data))
    return seed


def build_data(data: dict, mapping: dict) -> dict:
    """
    Returns the transformed Python dictionary from a key mapping.
    """
    output = dict()
    for key1, key2 in mapping.items():
        if isinstance(key2, dict):
            output[key1] = build_data(data, key2)
        elif key2 != "":
            output[key1] = find_by_key(key2, data)
        else:
            output[key1] = ""
    return output


def find_by_key(target: str, data: dict) -> str:
    """
    Returns the value of the target key from a nested Python dictionary.
    """
    for key, value in data.items():
        if isinstance(value, dict):
            return find_by_key(target, value)
        elif key == target:
            return value
    return ""


with DAG(
    dag_id='CourtListener_BulkInit',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
) as dag:

    local_start_op = PythonOperator(
        task_id="batching_op",
        python_callable=batcher,
        op_args=[
            batch_name,
            max_batch_size,
            number_of_batches,
            path
        ],
        dag=dag
    )

    # start_op = APIPagingOperator(
    #     task_id=START_OP_ID,
    #     **daily_paging_args,
    #     dag=dag,
    # )

    # no_results = ShortCircuitOperator(task_id="no_results", python_callable=_continue)

    local_start_op

    for batch_id in range(1, number_of_batches + 1):
        # extract_op = PythonOperator(
        #     task_id=EXTRACT_OP_ID.format(batch_id),
        #     python_callable=extract,
        #     op_args=[DAG_ID + str(batch_id)],
        # )

        local_transform_op = PythonOperator(
            task_id=TRANSFORM_OP_ID.format(batch_id),
            python_callable=local_transform,
            op_args=[
                batch_name + str(batch_id),
                COURT_LISTENER_MAPPING,
                COURT_LISTENER_KEY_LIST,
            ],
        )

        local_start_op >> local_transform_op
