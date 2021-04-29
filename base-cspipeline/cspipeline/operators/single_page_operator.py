import math
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from .api_operator import BaseAPIOperator


class APISinglePageOperator(BaseAPIOperator):
    """
    Operator designed to make API queries to an endpoint, which provides subendpoints for further requests.
    This operator is designed for single page results.

    """

    @apply_defaults
    def __init__(
        self,
        **kwargs,
    ) -> None:

        # delegate to BaseAPIOperator, we don't need to do anything else
        super().__init__(**kwargs)

    # Must overload this internal execute function, the base class takes care of
    # setup and teardown
    def _execute(self, context: Dict[str, Any]) -> Any:

        # call main endpoint with starting query data
        response_json = self._call_once(use_query=True)

        if response_json is None:
            raise AirflowException(f"Failed request on Connection: {self.http_conn_id}")

        resp_count = self.response_count(response_json)

        if resp_count == 0:
            self.log.info("No results found, skipping downstream")
            context["ti"].xcom_push(key="skip", value=True)
            return

        items_in_batch = math.ceil(resp_count / self.number_of_batches)
        curr_item_num = 0
        batch_id = 1

        if self.log_response:
            self.log.info(f"Processing {self.response_count(response_json)} items.")

        id_list = []
        # parse the incoming response
        # transform each subendpoint into a document
        # for insertion into MongoDB
        for subendpoint in self.parser(response_json):
            # if we've processed the amount of items in a batch
            # reset counter and decrement batch_id
            if curr_item_num == items_in_batch:
                curr_item_num = 0
                batch_id += 1

                if self.log_response:
                    self.log.info(
                        f"Processed Batch {batch_id} out of {self.number_of_batches}...\n{batch_id * items_in_batch} items so far"
                    )

            id_list.append(
                self._api_id_to_document(subendpoint, self.batch_name, batch_id)
            )

            curr_item_num += 1

        # insert ids into DB
        self.mongo_conn.insert_many("ids_to_update", id_list, mongo_db="courts")

        return None
