from airflow.models.skipmixin import SkipMixin
from airflow.models.baseoperator import BaseOperator


class NoDataOperator(BaseOperator, SkipMixin):
    """
    Looks for Skip Message in XCOM of Parent Tasks.
    If we find the Skip message, we will cancel all downstream tasks.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        parent_tasks = self.upstream_task_ids
        ti = context["ti"]

        list_of_results = ti.xcom_pull(task_ids=parent_tasks, key="skip")
        self.log.info(f"Found XCOMS: {list_of_results}")

        # if any parent task sent an error skip downstream
        # xcom pull returns a list of results
        if any(list_of_results):
            # generate list of downstream task instances
            downstream_tasks = context["task"].get_flat_relatives(upstream=False)

            self.log.info("Skipping downstream tasks...")

            dag_run = context["dag_run"]

            self.skip(
                dag_run=dag_run,
                execution_date=ti.execution_date,
                tasks=downstream_tasks,
            )
