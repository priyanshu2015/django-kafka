import json
import logging
from typing import Callable

from confluent_kafka import Producer
from django.conf import settings

logger = logging.getLogger("krpt.event.kafka.log")


class BaseProducer:
    """Can be initialized only once.
    Subsequent calls will just return the previously created instance. [Side-effect of Singleton]
    Only one BaseProducer connection to Kafka is maintained.
    Eg:
    In [2]: b1 = BaseProducer()
    In [3]: b2 = BaseProducer()
    In [4]: b1 is b2
    Out[4]: True
    In [5]: b1.producer is b2.producer
    Out[5]: True
    Usage:
        producer = BaseProducer()
        producer.produce_to_partition(......)
    """
    producer_conf = None

    def __init__(self):
        assert self.producer_conf is not None, (
            f"Please specify `producer_conf` in class: `{self.__class__.__name__}`."
        )
        self.producer = Producer(self.producer_conf)

    def on_delivery(self, error, msg):
        if error:
            """Sample Error Data when error was logged
            error = KafkaError{code=_UNKNOWN_PARTITION,val=-190,str="Local: Unknown partition"}
            msg = b'{"payload": {"candidate_job_action_id": 10}, "message_type": "add", "timestamp": "2020-04-10 13:45:37.341868+00:00"}'
            The above lines were logged for this piece of snippet
                logger.error(f'Failed to push data to Kafka: \n'
                 f'error = {error}\n'
                 f'msg = {json.loads(msg.value())}')
            """
            logger.warning(f"------- Fail -----------\n"
                           f"Failed to push data to Kafka: \n"
                           f"error = {error}\n"
                           f"topic = {msg.topic()};"
                           f"partition = {msg.partition()}; "
                           f"value = {msg.value()}\n"
                           f"---------------------------")
        else:
            logger.debug(f"----------Success--------\n"
                         f"Successfully pushed data: "
                         f"Topic: {msg.topic()}; "
                         f"Partition: {msg.partition()}; "
                         f"Value: {json.loads(msg.value())}\n"
                         f"---------------------")

    def produce_to_partition(self, topic: str, message, partition: int, on_delivery: Callable = None):
        content = json.dumps(message.to_dict())
        self.producer.produce(
            topic=topic,
            value=content,
            partition=partition,
            on_delivery=on_delivery or self.on_delivery
        )
        self.producer.poll(0.2)

    def produce(self, topic: str, message, on_delivery: Callable = None):
        content = json.dumps(message)
        self.producer.produce(
            topic=topic,
            value=content,
            on_delivery=on_delivery or self.on_delivery
        )
        self.producer.poll(0.2)

    def sync_produce_to_partition(self, topic: str, message, partition: int, on_delivery: Callable = None):
        """Makes the produce call synchronous.
        Has major performance implication. You typically don't need this.
        Please make sure you know what you are doing, when calling this method.
        Do this if delivery guarantees are required and if the number of
        messages to ingested is low.
        """
        content = json.dumps(message.to_dict())
        self.producer.produce(
            topic=topic,
            value=content,
            partition=partition,
            on_delivery=on_delivery or self.on_delivery
        )

        self.producer.flush()

    def sync_produce(
            self,
            topic: str,
            message,
            on_delivery: Callable = None
    ):
        content = json.dumps(message.to_dict())
        self.producer.produce(
            topic=topic,
            value=content,
            on_delivery=on_delivery or self.on_delivery
        )
        self.producer.flush()


class GenericProducer(BaseProducer):
    producer_conf = {
        "bootstrap.servers": settings.KAFKA_CLUSTER["brokers"]
    }