from airflow.exceptions import AirflowException
from airflow.utils.dates import datetime, days_ago, timedelta, timezone
from cspipeline.courtlistener.functions import (
    check_more,
    daily_query,
    next_page,
    parser,
    response_count,
    response_valid,
)

from .generic_dag import construct_paging_dag

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


def daily_query():
    """
    Constructs query parameters that get endpoints starting from a Day prior to the Date it's run.
    """
    return {
        "date_modified__gte": datetime.isoformat(
            datetime.now(timezone.utc) - timedelta(days=1)
        )
    }


def parser(response: dict):
    """Parses the json response from CourtListener /opinions endpoint."""
    results = response.get("results")

    if not results:
        return []

    ids = []

    for result in results:
        _id = result.get("id", None)
        if _id is not None:
            ids.append(_id)

    return ids


def response_count(response: dict):
    """Parses the count field from the json response from CourtListener /opinions endpoint"""
    count = response.get("count", None)

    if count is None:
        raise AirflowException("Count field in opinions endpoint was not found.")

    return count


def check_more(response):
    """Reads the next page field to check if there are more results."""
    return response.get("next", None) is not None


def next_page(response):
    """Returns the actual endpoint for the next page of results."""
    next_page = response.get("next", None)

    if next_page is None:
        raise AirflowException(
            "Next Page field in opinions endpoint was null or missing."
        )

    return next_page


def response_valid(response):
    """Checks API Response, returns True for 2XX status codes."""
    return response.status_code >= 200 and response.status_code < 300


# Generic Defaults for DAILY run.
default_paging_args = {
    "parser": parser,
    "check_more": check_more,
    "next_page": next_page,
    "response_count": response_count,
    "response_valid": response_valid,
    "query_builder": daily_query,
    "endpoint": "opinions",
    "http_conn_id": "default_api",
    "mongo_conn_id": "default_mongo",
}


def court_listener_dag(
    paging_args=default_paging_args,
    start_date=days_ago(0),
    schedule="@daily",
    number_of_batches=5,
    default_dag_args=None,
):
    """
    Construct Court Listener DAG.

    :param paging_args: Kwargs for APIPagingOperator
    :type paging_args: dict
    :param start_date: Date for DAG to start
    :type start_date: str
    :param schedule: Scheduling for DAG, see Airflow Docs.
    :type str: str
    :param number_of_batches: Number of Batches to use in the DAG.
    :type number_of_batches: int
    :param default_dag_args: Default Arguments to pass to DAG (and its operators)
    :type default_dag_args: dict
    """

    return construct_paging_dag(
        dag_id="CourtListener_Daily",
        paging_args=paging_args,
        key_map=1,
        extract_fn=1,
        transform_fn=1,
        start_date=start_date,
        schedule=schedule,
        number_of_batches=number_of_batches,
        default_dag_args=default_dag_args,
    )
