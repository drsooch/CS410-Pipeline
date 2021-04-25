import math
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from api_operator import BaseAPIOperator


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

        # generate random batch string, and get the number of batches
        batch_name = self._generate_random_batch_name()
        batch_id = self.number_of_batches

        # call main endpoint with starting query data
        resp = self._call_once(self.query)

        if resp:
            items_in_batch = math.ceil(
                self.response_count(resp) / self.number_of_batches
            )
            curr_item_num = 0
        else:
            raise AirflowException(f"Failed request on Connection: {self.http_conn_id}")

        if self.log_response:
            self.log.info(f"Processing {self.response_count(resp)} items.")

        id_list = []
        # parse the incoming response
        # transform each subendpoint into a document
        # for insertion into MongoDB
        for subendpoint in self.parser(resp):
            # if we've processed the amount of items in a batch
            # reset counter and decrement batch_id
            if curr_item_num == items_in_batch:
                curr_item_num = 0
                batch_id -= 1

                if self.log_response:
                    self.log.info(
                        f"Processed Batch {batch_id} out of {self.number_of_batches}...\n{batch_id * items_in_batch} items so far"
                    )

            id_list.append(self._api_id_to_document(subendpoint, batch_name, batch_id))

            curr_item_num += 1

        # insert ids into DB
        self.mongo_conn.insert_many("ids_to_update", id_list, mongo_db="courts")

        self._shutdown()

        # for now just return the batch_name and number of batches created
        return (batch_name, batch_id)
