from ..kafka.base_producer import BaseProducer
from ..kafka.base_producer_adapter import BaseProducerAdapter
from ..kafka.dto import KafkaMessageDTO, SampleEventDTO
from ..kafka.kafka_topics import KafkaTopic
from django.conf import settings

import logging

logger = logging.getLogger(__name__)


class KafkaSampleProducer(BaseProducer):
    producer_conf = {
        "bootstrap.servers": settings.KAFKA_CLUSTER["brokers"]
    }


class KafkaSampleProducerAdapter(BaseProducerAdapter):
    """
    """
    kafka_producer_class = KafkaSampleProducer
    topics = [KafkaTopic.SAMPLE_EVENT]
    payload_dto = SampleEventDTO
    message_dto = KafkaMessageDTO


# Use this command to push message to Kafka
# KafkaSampleProducerAdapter().produce(message_type=KafkaEventMessages.SAMPLE_EVENT, payload=serializer.data)