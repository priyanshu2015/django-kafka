import datetime
import typing
from pydantic import BaseModel


class KafkaMessageDTO(BaseModel):
    message_type: str = ...
    timestamp: datetime.datetime = ...
    payload: typing.Any = ...


class SampleEventDTO(BaseModel):
    name: str = ...
    email: str = ...
    phone_number: str = ...