# transform_utilities.py
#
#
# Internal Data Model
# {
#     'id': '',
#     'metadata': {
#         'api_id': '',
#         'api_resource_url': '',
#         'api_date_modified': '',
#         'court_from': '',
#         'citations': '',
#         'created': '',
#         'modified': '[]',
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

# Takes JSON formatted input string and transforms it into internal data model
# and returns a JSON formatted string
def transform_cl_opinion(input_json):

    # Deserializes string containing JSON document to Python object
    data = json.loads(input_json, strict=False)

    # Initialize internal data model object
    output_data = dict()
    output_data['metadata'] = dict()
    output_data['content'] = dict()

    # Transform input json data into IDM object

    # Generate a UUID from the resource_uri
    output_data['id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, data.get('resource_uri')))

    output_data['metadata']['api_id'] = data.get('id', '')
    output_data['metadata']['api_resource_url'] = data.get('resource_uri', '')
    output_data['metadata']['api_date_modified'] = data.get('date_modified', '')
    output_data['metadata']['court_from'] = ''
    output_data['metadata']['citations'] = list()
    for citation in data.get('opinions_cited', list()):
        output_data['metadata']['citations'].append(citation)
    output_data['metadata']['created'] = data.get('date_created', '')
    output_data['metadata']['modified'] = data.get('date_modified', '')
    output_data['metadata']['pages'] = data.get('page_count', '')
    output_data['metadata']['document_type'] = data.get('type', '')
    output_data['metadata']['document_date'] = ''
    output_data['metadata']['precedence'] = ''
    output_data['metadata']['case_type'] = ''

    output_data['content']['document_title'] = ''
    output_data['content']['document_text'] = data.get('plain_text', '')

    # Serialize object as a JSON formatted string
    output_json = json.dumps(output_data, indent=2)

    return output_json

def main():
    file = open('4582560.json', 'r')
    data_string = file.read()
    output = transform_cl_opinion(data_string)
    print(output)

if __name__ == "__main__":
    main()
