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
#

import json
import uuid


def transform(input_string, mapping):
    """
    Takes a JSON formatted string and mapping.
    """
    data = json.loads(input_string, strict=False)
    output_data = build_data(data, mapping)
    output_data = add_uuid(output_data)
    return json.dumps(output_data, indent=2)

def add_uuid(data):
    """
    Adds a UUID to the Python dictionary.
    """
    data['id']= str(uuid.uuid4())
    return data


def build_data(data, mapping):
    """
    Returns the transformed Python dictionary from a key mapping.
    """
    output = dict()
    for key1, key2 in mapping.items():
        if isinstance(key2, dict):
            output[key1] = build_data(data, key2)
        elif key2 != '':
            output[key1] = find_by_key(key2, data)
        else: output[key1] = ''
    return output


def find_by_key(target, data):
    """
    Returns the value of the target key from a nested Python dictionary.
    """
    for key, value in data.items():
        if isinstance(value, dict):
            return find_by_key(target, value)
        elif key==target:
            return value
    return ''


def main():
    """
    Example run of the function with a JSON document and mapping.
    """

    # Court document JSON file
    file = open('4582560.json', 'r')
    data_string = file.read()

    # Key mapping from internal data model to source model
    MAPPING = {
        'id': '',
        'metadata':
        {
            'api_id': 'id',
            'api_resource_url': 'resource_uri',
            'api_date_modified': 'date_modified',
            'court_from': '',
            'citations': 'opinions_cited',
            'created': 'date_created',
            'modified': 'date_modified',
            'pages': 'page_count',
            'document_type': 'type',
            'document_date': '',
            'precedence': '',
            'case_type': ''
        },
        'content':
        {
            'document_title': '',
            'document_text': 'plain_text'
        }
    }

    # JSON formatted string output
    output = transform(data_string, MAPPING)
    print(output)


if __name__ == "__main__":
    main()
