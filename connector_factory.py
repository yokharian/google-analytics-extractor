import abc
import json
import os
import uuid
from typing import NoReturn
import logging
import requests


class ConnectorFactory(abc.ABC):
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "__init__")
            and callable(subclass.__init__)
            and hasattr(subclass, "set_up")
            and callable(subclass.set_up)
            and hasattr(subclass, "extract")
            and callable(subclass.extract)
        )

    @abc.abstractmethod
    def __init__(self) -> NoReturn:
        """function called by factory manager
        :return: None
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_up(self) -> NoReturn:
        """
        Sets up all the parameters for the api connector request
        in self.extract function
        :return: None
        """
        raise NotImplementedError

    @abc.abstractmethod
    def extract(self) -> dict:
        """
        Gets the data from the connector api
        :return: dictionary:
            {
                source_name: string,
                client_id: string,
                data: List[dict]
            }
        """
        raise NotImplementedError

    @staticmethod
    def send(payload: dict) -> bool:
        """
        :param payload: dictionary data from self.extract
        to send to data_bridge middleware data_bridge
        {
            data: dict, None if no data
            client_id: str,

            source_name: Optional[str],
            store_name: Optional[str],
            internal_id: Optional[str]
        }
        :return: bool if everything is fine
        """
        all_ok = True
        if not payload.get("data"):
            logging.info(
                "no orders for {}-{}".format(
                    payload.get("client_id"), payload.get("source_name")
                )
            )
            return not all_ok

        logging.info(f"Requesting to {os.getenv('CM_URL')}")
        request = requests.post(os.getenv("CM_URL"), json=payload)

        if request.status_code != 200:
            logging.error(f"Request error: {request.status_code}")
            filename = str(uuid.uuid4())
            logging.info(f"[-] filename: {filename}")
            with open(f"{filename}.json", "w") as file_:
                data = json.dumps(payload)
                file_.write(data)
            return not all_ok

        response = request.json()
        if response.get("status") != "Success":
            logging.error(f"Response error: {response}")
            filename = str(uuid.uuid4())
            logging.info(f"[-] filename: {filename}")
            with open(f"{filename}.json", "w") as file_:
                data = json.dumps(payload)
                file_.write(data)
            return not all_ok
        else:
            logging.info("Success response from Data Bridge")
        return all_ok
