import json
import logging
from typing import Dict

import django.db
from confluent_kafka.cimpl import Consumer
from pydantic import ValidationError

from ..kafka.exceptions import KafkaConsumerMessageSerializationFailed, \
    KafkaConsumerPayloadSerializationFailed

logger = logging.getLogger(__name__)


class BaseConsumer:
    """
    raises:
        KafkaConsumerMessageSerializationFailed
        KafkaConsumerPayloadSerializationFailed
    """
    config = None
    topics: list = []
    payload_dto = None
    message_dto = None

    def __init__(self, *args, **kwargs):
        self.consumer = Consumer(self.config)
        self.consumer.subscribe(self.topics)
        super().__init__(*args, **kwargs)

    def get_payload_mapper(self, *args, **kwargs):
        assert hasattr(self, "payload_dto"), (
            f"Please specify the `payload_dto` in {self.__class__.__name__}."
        )
        return getattr(self, "payload_dto")

    def get_message_mapper(self, *args, **kwargs):
        assert hasattr(self, "message_dto"), (
            f"Please specify the `message_dto` in {self.__class__.__name__}"
        )
        return getattr(self, "message_dto")

    def get_payload(self, payload_dict: Dict, payload_dto=None):
        if payload_dto is None:
            payload_dto = self.get_payload_mapper()

        try:
            payload = payload_dto(**payload_dict)
            return payload
        except ValidationError as e:
            logger.error(msg="serialization failed on incoming message 'payload' through kafka",
                         exc_info=True, extra={"data": e.json()})
            raise KafkaConsumerPayloadSerializationFailed(
                "serialization failed on incoming message through 'payload' kafka") from e

    def get_message(self, message_dto, message_dict: Dict):
        if message_dto is None:
            message_dto = self.get_message_mapper()

        try:
            message = message_dto(**message_dict)
            return message
        except ValidationError as e:
            logger.error(msg="Serialization Failed on incoming message through kafka",
                         exc_info=True, extra={"data": e.json()})
            raise KafkaConsumerMessageSerializationFailed(
                "Serialization Failed on incoming message through kafka") from e

    def process_message(self, msg):
        message_str = json.loads(msg.value())
        message_dict = json.loads(message_str)

        payload = message_dict["payload"]
        payload = self.get_payload(payload_dto=self.get_payload_mapper(), payload_dict=payload)

        message_dict["payload"] = payload
        message = self.get_message(message_dto=self.get_message_mapper(), message_dict=message_dict)
        return message

    def handle_message(self, message):
        raise NotImplementedError("Please Implement `handle_message` method.")

    def handle_operational_errors(self, message):
        """Code to handle operational_errors goes here."""
        logger.error(f"OperationalError in "
                     f" {self.__class__.__name__}.", exc_info=True)

    def close_old_db_connections(self):
        django.db.close_old_connections()

    def consume(self):
        logger.info(f"{self.__class__.__name__} --- {str(self.config)}", extra={
            "data": {
                "config": self.config
            }
        })
        while True:
            try:
                msg = self.consumer.poll(1.0)
                if msg:
                    message = self.process_message(msg)
                    # close_old_db_connections() is maintenance code that is run
                    # to keep the consumer up and running on k8s
                    # it closes old db connections(set by MAX_AGE in db settings).
                    # Django closes connections by default on every new request
                    # since we do not have any api request here, we will have to manually run the code
                    self.close_old_db_connections()
                    self.handle_message(message=message)
            except django.db.OperationalError:
                # this is a db related error and not a kafka related error
                # owing to the fact that it is a db related error, it is assumed that msg has
                # been successfully polled from the kafka-broker
                self.handle_operational_errors(message=message)
                # re-raise the error therefore terminating the application
                raise

            except Exception:
                logger.error(f"Error in Consume of"
                            f" {self.__class__.__name__}.", exc_info=True)