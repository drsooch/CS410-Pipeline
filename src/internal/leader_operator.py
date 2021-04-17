from typing import Any, Union, Callable, Optional, Dict

from airflow.utils.dates import timedelta, datetime
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
from airflow.providers.mongo.hooks.mongo import MongoHook

import requests
import random
from string import ascii_letters

request_options = {"timeout": 5}

# Much of the design of this Operator is based off the Airflow SimpleHTTPOperator.
# It's been extended to allow for multiple requests in the event an API has "pages"
# of data. 

class LeaderHTTPOperator(BaseOperator):
    """
    Leader API Endpoint Operator

    :param endpoint: The API endpoint to query.
    :type endpoint: str
    :param parser: Function that parses the endpoint response, into a list of sub-endpoints.
                   Should return a list of strings.
    :type parser: function that takes a requests.Response object and returns a list of sub-endpoints
    :param next_page: If check_more() returns True, this function returns the endpoint the operator 
                      should request endpoint.
    :type next_page: Callable[[requests.Response], None]
    :param check_more: Function that determines if the endpoint has "paging", i.e. 
                       results are more than one page. Should return boolean value.
                       Defaults to function that returns False.
    :type check_more: Callable[[requests.Response], bool]
    :param response_valid: Function that checks if status code is valid. Defaults to 200 status only.
    :type response_valid: Callable[[requests.Response], bool]
    :param http_conn_id: Airflow Connection variable name for the base API URL.
    :type http_conn_id: str
    :param mongo_conn_id: Airflow Connection variable name for the MongoDB.
    :type mongo_conn_id: str
    :param header: Headers to be added to API request.
    :type header: dict of string key-value pairs
    :param options: Optional keyword arguments for the Requests library get function.
    :type options: dict of string key-value pairs
    :param log_response: Flag to allow for logging Request response. Defaults to False.
    :type options: bool
    """

    @apply_defaults
    def __init__(
        self,
        endpoint: str,
        parser: Callable[
            [requests.Response], list
        ],  # Function that parses a response to gather specific endpoints
        next_page: Callable[
            [requests.Response], str
        ],  # Function shifts endpoint to next page
        check_more: Callable[
            [requests.Response], bool
        ] = default_check_more,  # Function that parses a response to determine if there are more "pages" to request
        response_valid: Callable[[requests.Response], bool] = default_response_valid,
        http_conn_id: str = "default_api",
        mongo_conn_id: str = "default_mongo",
        header: Optional[Dict[str, str]] = None,
        options: Optional[Dict[str, Any]] = None,
        log_response: bool = False,
        **kwargs,
    ) -> None:

        # delegate to BaseOperator, we don't need to do anything else
        super().__init__(**kwargs)

        # API endpoint information, we should only be making GET requests from here
        # Header is most likely unneccessary
        self.endpoint = endpoint
        self.method = "GET"
        self.params = params or {}
        self.header = header or {}
        self.http_conn_id = http_conn_id
        self.mongo_conn_id = mongo_conn_id

        # Functions for operating on response data
        self.parser = parser
        self.check_more = check_more
        self.next_page = next_page
        self.response_valid = response_valid

        # Options is for Requests library functions
        self.options = options or {}

        self.log_response = log_response

        self.queue = "default"

    # here we override BaseOperator
    # We can safely ignore context for now (and possibly never need it)
    # TODO: Needs to Batch data if there is no paging. 
    # Courtlistener pages its data making it easy for batching.
    # If all data is on one page and there's a lot of it, there
    # will be only one giant batch.
    def execute(self, context: Dict[str, Any]) -> Any:

        batch_name = self._generate_random_batch_name()
        batch_id = 1

        self.http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.mongo_conn = MongoHook(self.mongo_conn_id)

        self.log.info(f"Connecting to: {self.http_conn_id}")

        while resp := self._call_once():
            # push all new sub-endpoints to the database
            # first transform into dict for pushing into DB
            subendpoints = list(
                map(
                    lambda se: {"api_id": se, "batch_id": batch_name + str(batch_id)},
                    self.parser(resp),
                )
            )

            self.mongo_conn.insert_many(
                "ids_to_update", subendpoints, mongo_db="courts"
            )

            print(f"Pushing {subendpoints[0]} to Database")

            # do we need to continue?
            if not self.check_more(resp):
                return "Finished Leader Endpoint"

            batch_id += 1

            # mutate connection endpoint to point to next page
            self.endpoint = self.next_page(resp)


        self._shutdown()

        # for now just return the batch_name and number of batches created
        return (batch_name, batch_id)

    def _call_once(self) -> Union[requests.Response, None]:
        """
        Execute a single API call to the leader endpoint.
        """
        response = self.http.run(
            self.endpoint,
            {
                "date_modified__gte": datetime.isoformat(
                    datetime.now() - timedelta(days=1)
                )  # need to make this programmable
            },
            self.header,
            self.options,
        )

        print(response.url)

        if self.log_response:
            self.log.info(response.text)

        if self.response_valid(response):
            return response

        return None

    def _default_check_more(self) -> bool:
        '''Default check_more() function. Always returns False.'''
        return False

    def _default_response_valid(self, response: requests.Response) -> bool:
        '''Default response_valid() function. Returns True only on 200.'''
        return response.status_code == 200

    def _generate_random_batch_name(self) -> str:
        '''Generates a random 10 letter batch string.'''
        return "".join(random.choices(ascii_letters, k=10))

    def _shutdown(self) -> None:
        '''Explicitly close MongoDB connection'''
        if self.mongo_conn:
            self.mongo_conn.close_conn()
