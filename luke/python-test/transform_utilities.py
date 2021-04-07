# transform_utilities.py
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
from jsonbender import bend, K, S


def transform(input_string, mapping):
    """
    Takes a JSON formatted string and mapping.
    """
    input_data = json.loads(input_string, strict=False)
    result = bend(mapping, input_data)
    result['id'] = str(uuid.uuid4())
    return json.dumps(result, indent=2)

def main():
    file = open('4582560.json', 'r')
    data_string = file.read()

    MAPPING = {
        'id': K(''),
        'metadata': {
            'api_id': S('id'),
            'api_resource_url': S('resource_uri'),
            'api_date_modified': S('date_modified'),
            'court_from': K(''),
            'citations': S('opinions_cited'),
            'created': S('date_created'),
            'modified': S('date_modified'),
            'pages': S('page_count'),
            'document_type': S('type'),
            'document_date': K(''),
            'precedence': K(''),
            'case_type': K('')
        },
        'content': {
            'document_title': K(''),
            'document_text': S('plain_text')
        }
    }

    # JSON formatted string
    output = transform(data_string, MAPPING)
    print(output)


if __name__ == "__main__":
    main()
