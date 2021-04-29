import math
from typing import Any, Callable, Dict

import requests
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from .api_operator import BaseAPIOperator

# Much of the design of this Operator is based off the Airflow SimpleHTTPOperator.
# It's been extended to allow for multiple requests in the event an API has "pages"
# of data.


class APIPagingOperator(BaseAPIOperator):
    """
    Operator designed to make API queries to an endpoint, which provides subendpoints for further requests.
    This Operator allows for Paging. Builds on top of BaseAPIOperator

    """

    @apply_defaults
    def __init__(
        self,
        next_page: Callable[
            [requests.Response], str
        ],  # Function shifts endpoint to next page
        check_more: Callable[
            [requests.Response], bool
        ] = None,  # Function that parses a response to determine if there are more "pages" to request
        **kwargs,
    ) -> None:

        # delegate to BaseAPIOperator, we don't need to do anything else
        super().__init__(**kwargs)

        # Functions for operating on response data
        self.next_page = next_page
        self.check_more = check_more or self._default_check_more

    # Must overload this internal execute function, the base class takes care of
    # setup and teardown
    def _execute(self, context: Dict[str, Any]) -> Any:

        # call main endpoint with starting query data
        response_json = self._call_once(use_query=True)

        # error out on failure
        if response_json is None:
            raise AirflowException(f"Failed request on Connection: {self.http_conn_id}")

        resp_count = self.response_count(response_json)

        if resp_count == 0:
            self.log.info("No results found, skipping downstream")
            context["ti"].xcom_push(key="skip", value=True)
            return

        if self.log_response:
            self.log.info(f"Processing {self.response_count(response_json)} items.")

        # generate batch lengths
        items_in_batch = math.ceil(resp_count / self.number_of_batches)
        curr_item_num = 0
        batch_id = 1

        while True:

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

                # add to list for insert_many
                id_list.append(
                    self._api_id_to_document(subendpoint, self.batch_name, batch_id)
                )

                curr_item_num += 1

            # insert ids into DB
            self.mongo_conn.insert_many("ids_to_update", id_list, mongo_db="courts")

            # do we need to continue?
            if not self.check_more(response_json):
                break

            # update response with next page
            self._update_endpoint(response_json)

            # this time we don't need query data
            response_json = self._call_once()

        return None

    def _default_check_more(self) -> bool:
        """Default check_more() function. Always returns False."""
        return False

    def _update_endpoint(self, response: dict):
        """Update endpoint the HTTPHook is requesting from."""
        next_page = self.next_page(response)

        # if the next page is a fully qualified url
        # swap out the base url
        # otherwise just update the endpoint

        # FIXME: This is crap.
        # CourtListener API returns the next page as the full url
        # Since HttpHook uses a base connection id which derives from Airflow UI
        # we cannot auto-update the base connection
        # Something like may be better:
        # for ch, ix in enumerate(next_page):
        #   if self.http.base_url[ix] == ch:
        #       continue
        #   else
        #       self.endpoint = self.next_page[ix:]
        #       break
        # This way we explicitly make sure the base url is the same
        if "://" in next_page:
            self.endpoint = next_page[len(self.http.base_url) :]
        else:
            self.endpoint = next_page
