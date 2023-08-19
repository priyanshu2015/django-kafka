import logging
from datetime import datetime
from typing import Optional
from django.utils import timezone
from pydantic import ValidationError

from ..kafka.exceptions import KafkaProducerMessageSerializationFailed, \
    KafkaProducerPayloadSerializationFailed

logger = logging.getLogger(__name__)


class BaseProducerAdapter:
    kafka_producer_class = None
    topics = []
    payload_dto = None
    message_dto = None

    def get_kafka_producer(self):
        """Override this method to get the kafka_producer."""
        return self.kafka_producer_class()

    def get_topics(self):
        """You can override this method to set which topic to write"""
        return self.topics

    def get_partition(self, topic) -> Optional[int]:
        """You can override this method to set which partition to write based on the topic"""
        return None

    def get_payload_mapper(self):
        """You can override this method to set custom logic for fetching payload_dto"""
        assert self.payload_dto is not None, (
            "Please provide a `payload_dto` or override the `get_payload_mapper()` method"
        )
        return self.payload_dto

    def get_payload(self, payload_dto, data):
        """You can override this method to set custom logic to get the payload.
        If you send the payload object as data then it'll return the same object
        """
        if payload_dto is None:
            payload_dto = self.get_payload_mapper()

        try:
            payload = payload_dto(**data)
            return payload
        except ValidationError as e:
            logger.error(msg="Serialization Failed on outgoing message payload to kafka",
                         exc_info=True, extra={"data": e.json()})
            raise KafkaProducerPayloadSerializationFailed(
                "Serialization Failed on outgoing message payload to kafka") from e

    def get_message_mapper(self):
        """Override this method to set custom logic for retrieving the message class"""
        assert self.message_dto is not None, (
            "Please provide a `message_dto` or override the `get_message_mapper` method"
        )
        return self.message_dto

    def get_message(self, message_dto, payload_dto, message_type: str = "", timestamp: datetime = None):
        """Override this method to change the way in which the message is created.
        @param message_dto
        @param payload_dto
        @param message_type
        @param timestamp
        """
        data = {
            "message_type": message_type,
            "payload": payload_dto,
            "timestamp": timestamp or timezone.now().timestamp(),
        }

        try:
            message = message_dto(**data)
            return message
        except ValidationError as e:
            logger.error(msg="Serialization Failed on outgoing message to kafka",
                         exc_info=True, extra={"data": e.json()})
            raise KafkaProducerMessageSerializationFailed(
                "Serialization Failed on outgoing message to kafka") from e

    def sync_produce(self, kafka_producer, topic: str, message, partition: Optional[int] = None):
        if partition is not None and isinstance(partition, int):
            kafka_producer.sync_produce_to_partition(
                topic=topic,
                message=message,
                partition=partition
            )
        else:
            kafka_producer.sync_produce(
                topic=topic,
                message=message
            )

    def async_produce(self, kafka_producer, topic: str, message, partition: Optional[int] = None):
        if partition is not None and isinstance(partition, int):
            kafka_producer.produce_to_partition(topic=topic, message=message, partition=partition)
        else:
            kafka_producer.produce(topic=topic, message=message)

    def produce(self, payload, message_type: str = "", timestamp: datetime = None, sync: bool = False):
        logger.info(f"{self.__class__.__name__}", extra={
            "data": {
                "config": str(self.kafka_producer_class)
            }
        })
        topics = self.get_topics()
        payload = self.get_payload(self.get_payload_mapper(), payload)
        message = self.get_message(
            message_dto=self.get_message_mapper(),
            payload_dto=payload,
            message_type=message_type,
            timestamp=timestamp
        )
        kafka_producer = self.get_kafka_producer()

        for topic in topics:
            partition = self.get_partition(topic)
            if sync:
                self.sync_produce(kafka_producer, topic, message.json(), partition)
            else:
                self.async_produce(kafka_producer, topic, message.json(), partition)
