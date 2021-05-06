from airflow import DAG
from airflow.utils.dates import datetime, days_ago, timedelta
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from cspipeline.courtlistener import (
    check_more,
    daily_query,
    next_page,
    parser,
    response_count,
    response_valid,
    COURT_LISTENER_KEY_LIST,
    COURT_LISTENER_MAPPING,
)

from cspipeline.operators import APIPagingOperator, transform, extract, NoDataOperator

# just some globals to keep around
START_OP_ID = "start_op"
EXTRACT_OP_ID = "extract_op_{}"
TRANSFORM_OP_ID = "transform_op_{}"
DAG_ID = "CourtListener_Daily"
NUMBER_OF_BATCHES = 5

SKIP_KEY = "skip"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=25),
    "number_of_batches": 5,
    "log_response": True
    #    'execution_timeout': timedelta(seconds=300),
}

daily_paging_args = {
    "parser": parser,
    "check_more": check_more,
    "next_page": next_page,
    "response_count": response_count,
    "response_valid": response_valid,
    "query_builder": daily_query,
    "endpoint": "opinions",
    "http_conn_id": "default_api",
    "mongo_conn_id": "default_mongo",
    "batch_name": DAG_ID,
}


def _continue(**kwargs):
    """For use with ShortCircuitOperator. Checks to see if the StartOp returns a SKIP Message."""
    ti = kwargs["ti"]
    should_continue = ti.xcom_pull(task_ids=START_OP_ID, key=SKIP_KEY)

    # if there are updates to be done, we won't find a key
    return should_continue is None


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
) as dag:

    start_op = APIPagingOperator(
        task_id=START_OP_ID,
        **daily_paging_args,
        dag=dag,
    )

    no_data = NoDataOperator(task_id="no_data")

    start_op >> no_data

    for batch_id in range(1, NUMBER_OF_BATCHES + 1):
        extract_op = PythonOperator(
            task_id=EXTRACT_OP_ID.format(batch_id),
            python_callable=extract,
            op_args=[DAG_ID + str(batch_id)],
        )

        transform_op = PythonOperator(
            task_id=TRANSFORM_OP_ID.format(batch_id),
            python_callable=transform,
            op_args=[
                DAG_ID + str(batch_id),
                COURT_LISTENER_MAPPING,
                COURT_LISTENER_KEY_LIST,
            ],
        )

        no_data >> extract_op >> transform_op
