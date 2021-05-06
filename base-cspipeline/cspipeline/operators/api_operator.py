import random
from json import JSONDecodeError
from string import ascii_letters
from typing import Any, Callable, Dict, Optional, Union

import requests
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.decorators import apply_defaults

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
    :type response_count: Callable[[requests.Response], int]
    :param number_of_batches: Number of batches used in the DAG Run.
    :type number_of_batches: int
    :param http_conn_id: Airflow Connection variable name for the base API URL.
    :type http_conn_id: str
    :param mongo_conn_id: Airflow Connection variable name for the MongoDB.
    :type mongo_conn_id: str
    :param response_valid: Function that checks if status code is valid. Defaults to 200 status only.
    :type response_valid: Callable[[requests.Response], bool]
    :param query_builder: Function that returns a Dictionary of query parameters.
    :type query_builder: Callable[[None], Dict[str, str]]
    :param header: Headers to be added to API request.
    :type header: dict of string key-value pairs
    :param options: Optional keyword arguments for the Requests library get function.
    :type options: dict of string key-value pairs
    :param log_response: Flag to allow for logging Request response. Defaults to False.
    :type log_response: bool
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
        number_of_batches: int,
        http_conn_id: str,
        mongo_conn_id: str,
        batch_name: str,
        response_valid: Callable[[requests.Response], bool] = None,
        query_builder: Callable[[None], str] = None,
        header: Optional[Dict[str, str]] = None,
        options: Optional[Dict[str, Any]] = None,
        log_response: bool = False,
        **kwargs,
    ) -> None:

        # delegate to BaseOperator, we don't need to do anything else
        super().__init__(**kwargs)

        self.number_of_batches = number_of_batches

        # API endpoint information, we should only be making GET requests from here
        # Header is most likely unneccessary
        self.endpoint = endpoint
        self.method = "GET"
        self.query_builder = query_builder or self._default_query_builder
        self.header = header or {}

        self.http_conn_id = http_conn_id
        self.mongo_conn_id = mongo_conn_id
        self.batch_name = batch_name

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

        # generate query parameters
        self.query = self.query_builder()

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

        if not self.response_valid(response):
            return None

        return self._to_json(response)

    def _to_json(self, response: requests.Response):
        try:
            return response.json()
        except JSONDecodeError:
            self.log.error(f"Failed to convert response to JSON: {response.url}")
            return None

    def _api_id_to_document(self, _id: str, name: str, batch_id: int):
        return {"api_id": str(_id), "batch_id": f"{name}{batch_id}"}

    def _default_query_builder(self) -> dict:
        return {}

    def _default_response_valid(self, response: requests.Response) -> bool:
        """Default response_valid() function. Returns True only on 200."""
        return response.status_code == 200

    def _shutdown(self) -> None:
        """Explicitly close MongoDB connection"""
        if self.mongo_conn:
            self.mongo_conn.close_conn()
