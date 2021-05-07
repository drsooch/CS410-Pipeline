from airflow import DAG
from airflow.utils.dates import datetime, days_ago, timedelta
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


from cspipeline.courtlistener import (
    COURT_LISTENER_KEY_LIST,
    COURT_LISTENER_MAPPING,
)

from cspipeline.operators import (
    local_batch,
    local_transform
)

batch_name = "local_batch_"
max_batch_size = 25
number_of_batches = 12
path = '/opt/airflow/initial-dataset'
TRANSFORM_OP_ID = "transform_op_{}"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=25),
    # "number_of_batches": 5,
    "log_response": True
   # 'execution_timeout': timedelta(seconds=300),
}


with DAG(
    dag_id='CourtListener_BulkInit',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
) as dag:

    local_start_op = PythonOperator(
        task_id="batching_op",
        python_callable=local_batch,
        op_args=[
            batch_name,
            max_batch_size,
            number_of_batches,
            path
        ],
        dag=dag
    )

    local_start_op

    for batch_id in range(1, number_of_batches + 1):

        local_transform_op = PythonOperator(
            task_id=TRANSFORM_OP_ID.format(batch_id),
            python_callable=local_transform,
            op_args=[
                batch_name + str(batch_id),
                COURT_LISTENER_MAPPING,
                COURT_LISTENER_KEY_LIST,
            ],
        )

        local_start_op >> local_transform_op
