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

from cspipeline.courtlistener.defaults import (
    COURT_LISTENER_KEY_LIST,
    COURT_LISTENER_MAPPING,
)

from .generic_dag import construct_paging_dag


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
        default_args=default_dag_args,
    )
