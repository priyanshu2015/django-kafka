
class KafkaBaseException(BaseException):
    """
    This is base of all Kafka Related Exceptions
    """
    pass


class KafkaProducerMessageSerializationFailed(KafkaBaseException):
    """
    This exception is raised on serialization failure of "outgoing message"
    """


class KafkaProducerPayloadSerializationFailed(KafkaBaseException):
    """
    This exception is raised on serialization failure of "outgoing message payload"
    """


class KafkaConsumerMessageSerializationFailed(KafkaBaseException):
    """
    This exception is raised on serialization failure of "incoming message"
    """


class KafkaConsumerPayloadSerializationFailed(KafkaBaseException):
    """
    This exception is raised on serialization failure of "incoming message payload"
    """
