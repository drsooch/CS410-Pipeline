from airflow.utils.dates import datetime, timezone, timedelta
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook

__all__ = [
    "daily_query",
    "parser",
    "response_count",
    "check_more",
    "next_page",
    "response_valid",
]


def daily_query():
    """
    Constructs query parameters that get endpoints starting from a Day prior to the Date it's run.
    """
    return {
        "date_modified__gte": datetime.isoformat(
            datetime.now(timezone.utc) - timedelta(days=1)
        )
    }


def parser(response):
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


def response_count(response):
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
