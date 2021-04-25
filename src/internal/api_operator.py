import random
from string import ascii_letters
from typing import Any, Callable, Dict, Optional, Union

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.decorators import apply_defaults

import batch_workers as batching

##########################################################
# Derived Classes should implement the _execute function #
##########################################################


class BaseAPIOperator(BaseOperator):
    """
    Base Operator for API Requests to a main endpoint that generates subendpoints for futher requests.

    :param endpoint: The API endpoint to query.
    :type endpoint: str
    :param parser: Function that parses the endpoint response, into a list of sub-endpoints.
                   Should return a list of strings.
    :type parser: function that takes a requests.Response object and returns a list of sub-endpoints
    :param response_count: Function that returns number of items in API response
    :type response_count_more: Callable[[requests.Response], int]
    :param response_valid: Function that checks if status code is valid. Defaults to 200 status only.
    :type response_valid: Callable[[requests.Response], bool]
    :param http_conn_id: Airflow Connection variable name for the base API URL.
    :type http_conn_id: str
    :param mongo_conn_id: Airflow Connection variable name for the MongoDB.
    :type mongo_conn_id: str
    :param query: Query string to add to API request.
    :type query: dict of string key-value pairs
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
        response_count: Callable[
            [requests.Response], int
        ],  # Determines the number of items from query
        response_valid: Callable[[requests.Response], bool] = None,
        http_conn_id: str = "default_api",
        mongo_conn_id: str = "default_mongo",
        number_of_batches: int = None,
        query: Optional[Dict[str, str]] = None,
        header: Optional[Dict[str, str]] = None,
        options: Optional[Dict[str, Any]] = None,
        log_response: bool = False,
        **kwargs,
    ) -> None:

        # delegate to BaseOperator, we don't need to do anything else
        super().__init__(**kwargs)

        # Default to a batch number from batching module
        self.number_of_batches = number_of_batches or batching.get_batch_number()

        # API endpoint information, we should only be making GET requests from here
        # Header is most likely unneccessary
        self.endpoint = endpoint
        self.method = "GET"
        self.query = query or {}
        self.header = header or {}

        self.http_conn_id = http_conn_id
        self.mongo_conn_id = mongo_conn_id

        # Functions for operating on response data
        self.parser = parser
        self.response_count = response_count
        self.response_valid = response_valid or self._default_response_valid

        # Options is for Requests library functions
        self.options = options or {}

        self.log_response = log_response

        # # these get instantiated on execute
        # these get instantiated on execute
        self.http = None
        self.mongo_conn = None

    # Override the execute method, we want any derived classes to override
    # _execute()
    def execute(self, context: Dict[str, Any]) -> Any:

        self.http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.mongo_conn = MongoHook(self.mongo_conn_id)

        self.log.info(f"Connecting to: {self.http_conn_id}")

        return_val = self._execute(context)

        self._shutdown()

        return return_val

    def _execute(self, context: Dict[str, Any]) -> Any:
        raise NotImplementedError("_execute() needs to be defined for subclasses.")

    def _call_once(self, use_query: bool = False) -> Union[requests.Response, None]:
        """
        Execute a single API call.

        :param query: If use_query is true, we use the internal query string provided in our request.
        :type query: bool (defaults to False)
        """
        response = self.http.run(
            self.endpoint,
            self.query if use_query else {},
            self.header,
            self.options,
        )

        if self.log_response:
            self.log.info(response.url)

        if self.response_valid(response):
            return response

        return None

    def _api_id_to_document(self, _id: str, batch_name: str, batch_id: int):
        return {"api_id": str(_id), "batch_id": f"{batch_name}{batch_id}"}

    def _default_response_valid(self, response: requests.Response) -> bool:
        """Default response_valid() function. Returns True only on 200."""
        return response.status_code == 200

    def _generate_random_batch_name(self) -> str:
        """Generates a random 10 letter batch string."""
        return "".join(random.choices(ascii_letters, k=10))

    def _shutdown(self) -> None:
        """Explicitly close MongoDB connection"""
        if self.mongo_conn:
            self.mongo_conn.close_conn()
