# transform_utilities.py
#
# Unique keys are required within the nested source data model.
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
#         'modified': '',
#         'pages': '',
#         'document_type': '',
#         'document_date': '',
#         'precedence': '',
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

# Key mapping from internal data model to source model
COURT_LISTENER_MAPPING = {
    "id": "",
    "metadata": {
        "api_id": "id",
        "api_resource_url": "resource_uri",
        "api_date_modified": "date_modified",
        "court_from": "",
        "citations": "opinions_cited",
        "created": "date_created",
        "modified": "date_modified",
        "pages": "page_count",
        "document_type": "type",
        "document_date": "",
        "precedence": "",
        "case_type": "",
    },
    "content": {"document_title": "", "document_text": "plain_text"},
}

# List of keys from source data model to use to generate unique id
COURT_LISTENER_KEY_LIST = ["resource_uri", "date_modified"]


def _transform(input_string: str, mapping: dict, keys: list) -> str:
    """
    Takes a JSON formatted string and mapping.
    """
    data = json.loads(input_string, strict=False)
    output_data = build_data(data, mapping)
    uuid = generate_id(build_seed(data, keys))
    output_data["id"] = uuid
    return json.dumps(output_data, indent=2)


def transform(batch_name: str, mapping: dict, keys: list) -> None:
    mongo_conn = MongoHook(conn_id="default_mongo")
    results_to_transform_coll = mongo_conn.get_collection("results_to_transform")
    court_documents_coll = mongo_conn.get_collection("court_documents")

    while results_to_transform_coll.find({"batch_id": batch_name}):

        data = results_to_transform_coll.find_one_and_delete({"batch_id": batch_name})

        try:
            output_data = build_data(data, mapping)
            unique_id = generate_id(build_seed(data, keys))
            output_data["_id"] = unique_id

            court_documents_coll.insert_one(output_data)
        except Exception as error:
            print(f"An exception occured while processing batch {batch_name}:\n{error}")
            results_to_transform_coll.insert_one(data)


def generate_id(seed: str) -> str:
    m = hashlib.md5()
    m.update(seed.encode("utf-8"))
    id = str(uuid.UUID(m.hexdigest()))
    return id


def build_seed(data: dict, keys: list) -> str:
    seed = ""
    for key in keys:
        seed += find_by_key(key, data)
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


def main():
    """
    Example run of the function with a JSON document and mapping.
    """

    # Court document JSON file
    file = open("4582560.json", "r")
    data_string = file.read()

    # Key mapping from internal data model to source model
    MAPPING = {
        "id": "",
        "metadata": {
            "api_id": "id",
            "api_resource_url": "resource_uri",
            "api_date_modified": "date_modified",
            "court_from": "",
            "citations": "opinions_cited",
            "created": "date_created",
            "modified": "date_modified",
            "pages": "page_count",
            "document_type": "type",
            "document_date": "",
            "precedence": "",
            "case_type": "",
        },
        "content": {"document_title": "", "document_text": "plain_text"},
    }

    # List of keys from source data model to use to generate unique id
    KEY_LIST = ["resource_uri", "date_modified"]

    # JSON formatted string output
    output = transform(data_string, MAPPING, KEY_LIST)
    print(output)


if __name__ == "__main__":
    main()
