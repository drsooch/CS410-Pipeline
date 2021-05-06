import random
from string import ascii_letters
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago, timedelta
from cspipeline.operators import APIPagingOperator, extract, NoDataOperator, transform

apply_default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=25),
    "number_of_batches": 5,
    "log_response": True
    # 'execution_timeout': timedelta(seconds=300),
}

# just some globals to keep around
START_OP_ID = "start_op"
EXTRACT_OP_ID = "extract_op_{}"
TRANSFORM_OP_ID = "transform_op_{}"

SKIP_KEY = "skip"


# Not used as of now.
# Python callable arguments for some reason get clobbered.
# Even if the batch_name is set before the loop,
# the PythonOperator seems to call this function again.
def _generate_random_batch_name() -> str:
    """Generates a random 10 letter batch string."""
    return "".join(random.choices(ascii_letters, k=10))


def construct_paging_dag(
    dag_id: str,
    paging_kwargs: Dict[str, Any],
    key_map: Dict[str, Any],
    key_list: List[str],
    start_date=days_ago(1),
    schedule=None,
    number_of_batches=5,
    default_args=None,
    dag_kwargs=None,
):
    """
    Construct a DAG with an endpoint that responds with pages.

    :param dag_id: DAG ID to name the DAG
    :type dag_id: str
    :param paging_args: Kwargs for APIPagingOperator
    :type paging_args: dict
    :param key_map: Map from API Response to Internal Data Model
    :type key_map: dict of internal data model fields to fields from API response
    :param key_list: List of API Response fields to create Unique ID
    :type key_list: list of field names
    :param start_date: Date for DAG to start
    :type start_date: str
    :param schedule: Scheduling for DAG, see Airflow Docs.
    :type str: str
    :param number_of_batches: Number of Batches to use in the DAG.
    :type number_of_batches: int
    :param default_dag_args: Default Arguments to pass through @apply_defaults
    :type default_dag_args: dict
    :param dag_kwargs: Keyword Arguments to pass to DAG
    :type dag_kwargs: dict
    """

    if default_args is None:
        default_args = apply_default_args

    if dag_kwargs is None:
        dag_kwargs = dict()

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=start_date,
        schedule_interval=schedule,
        **dag_kwargs,
    )

    start_op = APIPagingOperator(
        task_id=START_OP_ID,
        batch_name=dag_id,
        dag=dag,
        **paging_kwargs,
    )

    no_data = NoDataOperator(task_id="no_data")

    start_op >> no_data

    for batch_id in range(1, number_of_batches + 1):
        extract = PythonOperator(
            task_id=EXTRACT_OP_ID.format(batch_id),
            python_callable=extract,
            op_args=[dag_id + str(batch_id)],
        )

        ## FIXME: add Key and Mapping
        transform = PythonOperator(
            task_id=TRANSFORM_OP_ID.format(batch_id),
            python_callable=transform,
            op_args=[dag_id + str(batch_id), key_map, key_list],
        )

        no_data >> extract >> transform

    return dag
