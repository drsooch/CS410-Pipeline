from typing import Any, Dict

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class ExtractOperator(BaseOperator):
    """
    ExtractOperator
    """

    @apply_defaults
    def __init__(
        self,
        batch_name,
        batch_id,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)

        self.batch_name = batch_name
        self.batch_id = batch_id

    def execute(self, context: Dict[str, Any]) -> Any:

        self.log.info(f"Extracting {self.batch_name}{self.batch_id}")
