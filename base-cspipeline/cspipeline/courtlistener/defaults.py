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
        # "retrieved": "date_modified",
        "pages": "page_count",
        "document_type": "type",
        "document_date": "",
        "case_type": "",
    },
    "content": {"document_title": "", "document_text": "plain_text"},
}

# List of keys from source data model to use to generate unique id
COURT_LISTENER_KEY_LIST = ["resource_uri", "id"]
