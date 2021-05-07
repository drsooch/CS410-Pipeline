# transform_utilities.py
#
# NOTE: Unique keys are required within the nested source data model.
#
# Internal Data Model
# {
#     'id': '',
#     'metadata': {
#         'api_id': '',
#         'api_resource_url': '',
#         'api_date_modified': '',
#         'court_from': '',
#         'citations': [],
#         'created': '',
#         'retrieved': '',
#         'pages': '',
#         'document_type': '',
#         'case_type': ''
#     },
#     'content': {
#         'document_title': '',
#         'document_text': ''
#     }
# }

import hashlib
import json
import uuid

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import datetime
from airflow.exceptions import AirflowException


def transform(batch_id: str, mapping: dict, keys: list) -> None:

    if len(mapping) == 0:
        raise AirflowException("Mapping provided is empty")

    if len(keys) == 0:
        raise AirflowException("Unique Keys provided is empty")

    mongo_conn = MongoHook(conn_id="default_mongo")
    results_to_transform_coll = mongo_conn.get_collection(
        "results_to_transform", "courts"
    )
    court_documents_coll = mongo_conn.get_collection("court_documents", "courts")

    while results_to_transform_coll.find_one({"batch_id": batch_id}):

        data = results_to_transform_coll.find_one_and_delete({"batch_id": batch_id})

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
            print(f"An exception occured while processing batch {batch_id}:\n{error}")
            results_to_transform_coll.insert_one(data)


def local_transform(batch_id: str, mapping: dict, keys: list) -> None:

    if len(mapping) == 0:
        raise AirflowException("Mapping provided is empty")

    if len(keys) == 0:
        raise AirflowException("Unique Keys provided is empty")

    mongo_conn = MongoHook(conn_id="default_mongo")
    local_results_to_transform_coll = mongo_conn.get_collection(
        "local_results_to_transform", "courts"
    )
    court_documents_coll = mongo_conn.get_collection("court_documents", "courts")

    while local_results_to_transform_coll.find_one({"batch_id": batch_id}):

        batch_data = local_results_to_transform_coll.find_one_and_delete({"batch_id": batch_id})

        try:
            fp = batch_data['file_path']
            file = open(fp, 'r')
            input_string = file.read()
            data = json.loads(input_string, strict=False)
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
            print(f"An exception occured while processing batch {batch_id}:\n{error}")
            local_results_to_transform_coll.insert_one(batch_id)


def generate_id(seed: str) -> str:
    """
    Returns a UUID from an input seed string.
    """
    m = hashlib.md5()
    m.update(seed.encode("utf-8"))
    return str(uuid.UUID(m.hexdigest()))


def build_seed(data: dict, keys: list) -> str:
    """
    Returns a string built from a list of keys from the source data model.
    """
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
