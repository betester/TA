import asyncio

from fogverse.util import calc_datetime, get_timestamp

from .consumer_producer import (
    AIOKafkaConsumer, AIOKafkaProducer, OpenCVConsumer
)
from .base import AbstractConsumer, AbstractProducer
from .general import Runnable
from .profiling import Profiling
from .manager import Manager

class Producer(AbstractConsumer, AIOKafkaProducer, Runnable):
    pass

class Consumer(AIOKafkaConsumer, AbstractProducer, Runnable):
    pass

class ConsumerStorage(AbstractConsumer, AbstractProducer, Runnable):
    def __init__(self, keep_messages=False):
        self.keep_messages = keep_messages
        self.q = asyncio.Queue()

    def _before_receive(self):
        self._start = get_timestamp()

    def _after_receive(self, _):
        self._consume_time = calc_datetime(self._start)

    def _get_send_extra(self, *args, **kwargs):
        return {'consume time': getattr(self, '_consume_time', None)}

    async def send(self, data):
        extras = None
        extra_func = getattr(self, '_get_send_extra', None)
        if callable(extra_func):
            extras = self._get_send_extra(data)
        obj = {
            'message': self.message,
            'data': data,
            'extra': extras
        }
        if not self.keep_messages:
            # remove the last message before putting the new one
            if not self.q.empty():
                self.get_nowait()
        await self.q.put(obj)

    async def get(self):
        return await self.q.get()

    def get_nowait(self):
        try:
            return self.q.get_nowait()
        except asyncio.QueueEmpty:
            return None
