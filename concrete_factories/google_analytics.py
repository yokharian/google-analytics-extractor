"""TODO
https://developers.google.com/analytics/devguides/reporting/core/v3/errors"""
import logging
from datetime import datetime as DaTe
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from core.generic_credential import GenericCredential
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ...core.utils import parse_date_range
from ..connector_factory import ConnectorFactory
from ..decorators import mapping_alias


# noinspection PyMissingConstructor
@mapping_alias("google-ads-universal")
class GoogleUniversalFactory(ConnectorFactory):
    API_NAME, API_VERSION = "analytics", "v3"
    API_DATE_FORMAT = "%Y-%m-%d"

    def __init__(
        self, credential: GenericCredential, date_range, **kwargs
    ) -> None:
        """function called by factory manager"""
        # https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
        # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#dimensions
        DIMENSIONS = (
            "campaign",
            "source",
            "medium",
            "date",
            "deviceCategory",
            "operatingSystem",
            "region",
        )
        # You can supply a maximum of 7 dimensions in any query.
        assert len(DIMENSIONS) <= 7
        self.DIMENSIONS = self.parse_ga_args(DIMENSIONS)

        # https://developers.google.com/analytics/devguides/reporting/core/v3/reference#metrics
        METRICS = (
            "bounces",
            "pageviews",
            "sessionDuration",
            "sessions",
            "newUsers",
            "transactions",
        )
        # You can supply a maximum of 10 metrics for any query.
        assert len(METRICS) <= 10
        self.METRICS = self.parse_ga_args(METRICS)

        self.token = credential.metadata["token"]
        self.scopes = credential.metadata["scopes"]
        self.client_id = credential.metadata["client_id"]
        self.token_uri = credential.metadata["token_uri"]
        self.client_secret = credential.metadata["client_secret"]
        self.refresh_token = credential.metadata["refresh_token"]
        self.accounts = credential.metadata["accounts"]

        self.date_range = date_range
        self.service = None
        self.profiles = []
        self.set_up()
        self.extract()

    def set_up(self) -> None:
        """custom implementation here"""
        # Authenticate and construct service.
        self.accounts = [i["account"] for i in self.accounts]
        self.service = self.build_service()
        self.profiles = list(self.generate_profiles())

    def extract(self) -> None:
        """custom implementation here"""
        for profile in self.profiles:
            for date_range in self.date_range:
                date_from, date_to = parse_date_range(date_range)

                query = self.query_to_extract_data(profile, date_from, date_to)
                for response in self.paginate_through(query):
                    self.send(response)

    @staticmethod
    def parse_ga_args(ga_args: Union[List[str], Tuple]) -> str:
        return ",".join(["ga:" + i for i in ga_args])

    def generate_profiles(self):
        """simple wrapper to generate each entity with data
        Args:

        Returns:
            generate each entity with data
        """
        for id_ in self.accounts:
            profile_ids = self._obtain_profiles(id_)
            for profile in profile_ids:
                yield profile

    def build_service(self):
        """Get a service that communicates to a Google API.
        Returns:
            A service that is connected to the specified API.
        """
        credentials = Credentials(
            token=self.token,
            refresh_token=self.refresh_token,
            id_token="",
            token_uri=self.token_uri,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=self.scopes,
        )
        # Build the service object.
        service = build(
            serviceName=self.API_NAME,
            version=self.API_VERSION,
            credentials=credentials,
        )
        return service

    def _obtain_profiles(self, account_id: str) -> List[str]:
        """Use the Analytics service object to get all the profile ids
        based on the account_id.
        Args:
            account_id:
        Returns:
            a list of all profile_ids obtained.
        """
        try:
            # Get a list of all the properties for the account_id.
            query = (
                self.service.management()
                .webproperties()
                .list(accountId=account_id)
            )
            response = query.execute()

            if not response.get("items"):
                logging.warning("no Google Analytics profiles detected\n" * 5)
                return []
            else:
                # Get the first property id.
                assert len(response.get("items")) == 1  # api constant (?
                property_ = response.get("items")[0].get("id")

                # Get a list of all views (profiles) for the first property.
                query = (
                    self.service.management()
                    .profiles()
                    .list(accountId=account_id, webPropertyId=property_)
                )
                response = query.execute()
        except TypeError as error:
            # Handle errors in constructing a query.
            logging.error(
                f"There was an error in constructing your query : {error}"
            )
            raise TypeError(error)
        except HttpError as error:
            # Handle API errors.
            logging.error(
                "Arg, there was an API error : {} : {}".format(
                    error.resp.status, error._get_reason()
                )
            )
        except RefreshError:
            # Handle Auth errors.
            logging.error(
                "The credentials have been revoked or expired, please re-run "
                "the application to re-authorize"
            )
        except Exception as e:
            # Handle all other errors.
            raise Exception(e)
        else:
            if response.get("items"):
                return [i["id"] for i in response["items"]]
            logging.warning("no Google Analytics profiles detected\n" * 5)
            return []

    @staticmethod
    def paginate_through(query):
        """Get a service that communicates to a Google API.
        Args:
            query: The constructed query to request from.
        Returns:
            all contents paginating through query response.
        """
        cursor = None

        do_once = True
        while cursor is not None or do_once:
            do_once = False
            # Try to make a request to the API. yield results or handle errors.
            try:
                response = query.execute()
                cursor = response.get("nextLink")
                query.uri = cursor
                yield response
            except HttpError as error:
                # Handle API errors.
                logging.error(
                    "Arg, there was an API error : {} : {}".format(
                        error.resp.status, error._get_reason()
                    )
                )
            except RefreshError:
                # Handle Auth errors.
                logging.error(
                    "The credentials have been revoked or expired,"
                    "please re-run the application to re-authorize"
                )
            except Exception as e:
                # Handle all other errors.
                raise Exception(e)

    def query_to_extract_data(
        self, profile_id: str, date_from: DaTe, date_to: DaTe
    ):
        """Use the Analytics Service Object to query the Core Reporting API
        Args:
            :param date_from:
            :param date_to:
            :param profile_id: specific profile_id to query from
        Returns:
            constructed query.
        """
        try:
            query = (
                self.service.data()
                .ga()
                .get(
                    ids="ga:" + profile_id,
                    start_date=date_from.strftime(self.API_DATE_FORMAT),
                    end_date=date_to.strftime(self.API_DATE_FORMAT),
                    metrics=self.METRICS,
                    dimensions=self.DIMENSIONS,
                    include_empty_rows=True,
                    start_index=1,
                    max_results=1000,
                )
            )
        except TypeError as error:
            # Handle errors in constructing a query.
            logging.error(
                f"There was an error in constructing your query : {error}"
            )
            raise TypeError(error)
        return query

    @staticmethod
    def _obtain_accounts(service) -> List[str]:
        """Get a list of all Google Analytics accounts for this user
        Args:
            service: A service that is connected to the specified API.
        Returns:
            a list of all account_ids obtained
        """
        try:
            query = service.management().accounts().list()
            response = query.execute()
        except TypeError as error:
            # Handle errors in constructing a query.
            logging.error(
                f"There was an error in constructing your query : {error}"
            )
            raise TypeError(error)
        except HttpError as error:
            # Handle API errors.
            logging.error(
                "Arg, there was an API error : {} : {}".format(
                    error.resp.status, error._get_reason()
                )
            )
            raise HttpError(error)
        except RefreshError:
            # Handle Auth errors.
            message = (
                "The credentials have been revoked or expired, please re-run "
                "the application to re-authorize"
            )
            logging.error(message)
            raise RefreshError(message)
        except Exception as e:
            # Handle all other errors.
            raise Exception(e)
        else:
            if response.get("items"):
                # Get all Google Analytics account.
                return [i["id"] for i in response["items"]]
            logging.warning("no Google Analytics accounts detected\n" * 5)
            return []

    @staticmethod
    def parse_response(response: dict) -> Optional[List[dict]]:
        if not response.get("columnHeaders") and response.get("rows"):
            return None
        print(response["totalsForAllResults"])
        rows = response["rows"]
        column_headers = [i["name"] for i in response["columnHeaders"]]

        def zip_a_row(a_row):
            return dict(zip(column_headers, a_row))

        return list(map(zip_a_row, rows))

    def __del__(self):
        if self.service:
            self.service.close()
