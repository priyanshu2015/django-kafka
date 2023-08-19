import logging
from django.core.management.base import BaseCommand
from main.consumers.kafka_sample_consumer import KafkaSampleConsumer

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Start kafka sample consumer and perform ... operations. "
        "Usage: python3 manage.py start_kafka_sample_consumer"
    )

    def handle(self, *args, **options):
        logger.info("Received start kafka sample consumer command line argument")
        logger.info(f"Starting kafka sample consumer with config: {KafkaSampleConsumer.config}")
        KafkaSampleConsumer().consume()
