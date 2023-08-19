import logging
from django.conf import settings
from ..kafka.base_consumer import BaseConsumer
from ..kafka.dto import KafkaMessageDTO, SampleEventDTO
from ..kafka.kafka_topics import KafkaTopic

logger = logging.getLogger(__name__)


class KafkaSampleConsumer(BaseConsumer):
    config = {
        'bootstrap.servers': settings.KAFKA_CLUSTER["brokers"],
        'group.id': 'main_kafka_sample_consumer_group',
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'earliest'}
    }

    topics: list = [KafkaTopic.SAMPLE_EVENTS]
    message_dto = KafkaMessageDTO
    payload_dto = SampleEventDTO

    def handle_message(self, message):
        logger.info(msg="KafkaSampleConsumer:: Message received on kafka with data", extra={"data": message.dict()})
        # perform actions
