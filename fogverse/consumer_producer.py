import asyncio
import os
import socket
import sys
import uuid

from aiokafka import (
    AIOKafkaConsumer as _AIOKafkaConsumer,
    AIOKafkaProducer as _AIOKafkaProducer
)
from .util import get_config
from .fogverse_logging import FogVerseLogging
from .base import AbstractConsumer, AbstractProducer

class AIOKafkaConsumer(AbstractConsumer):
    '''
    consumer_servers has to be url to kafka servers, e.g [localhost:9092]
    '''
    def __init__(self, 
                 consumer_servers: list[str], 
                 group_id: str,
                 consumer_topic: list[str],
                 client_id: str="",
                 consumer_conf={}, 
                 topic_pattern=None, 
                 loop=None
                 ):
        self._loop = loop or asyncio.get_event_loop()
        self._topic_pattern = topic_pattern
        self._consumer_topic = consumer_topic
        self._consumer_servers = consumer_servers
        self.consumer_conf = {
            'loop': self._loop,
            'bootstrap_servers': self._consumer_servers,
            'group_id': get_config('GROUP_ID', self, group_id),
            'client_id': client_id or socket.gethostname(),
            **consumer_conf
        }
        if self._consumer_topic:
            self.consumer = _AIOKafkaConsumer(*self._consumer_topic, **self.consumer_conf)
        else:
            self.consumer = _AIOKafkaConsumer(**self.consumer_conf)
        self.seeking_end = None

    async def start_consumer(self):
        logger = getattr(self, '_log', None)
        if logger is not None and isinstance(logger, FogVerseLogging):
            logger.std_log('Topic: %s', self._consumer_topic)
            logger.std_log('Config: %s', self.consumer_conf)
        await self.consumer.start()
        if self._topic_pattern:
            self.consumer.subscribe(pattern=self._topic_pattern)
        await asyncio.sleep(5) # wait until assigned to partition
        if getattr(self, 'read_last', True):
            await self.consumer.seek_to_end()

    async def receive(self):
        if getattr(self, 'always_read_last', False):
            if asyncio.isfuture(self.seeking_end):
                await self.seeking_end
            self.seeking_end = \
                asyncio.ensure_future(self.consumer.seek_to_end())
            if getattr(self, 'always_await_seek_end', False):
                await self.seeking_end
                self.seeking_end = None
        return await self.consumer.getone()

    async def close_consumer(self):
        await self.consumer.stop()
        logger = getattr(self, '_log', None)
        if isinstance(logger, FogVerseLogging):
            logger.std_log('Consumer has closed.')

class AIOKafkaProducer(AbstractProducer):
    def __init__(self, producer_servers, producer_topic: str, producer_conf={}, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self.producer_topic = producer_topic
        self._producer_servers = producer_servers
        self._producer_conf = {
            'loop': self._loop,
            'bootstrap_servers': self._producer_servers,
            'client_id': get_config('CLIENT_ID', self, socket.gethostname()),
            **producer_conf
        }
        self.producer = _AIOKafkaProducer(**self._producer_conf)

    async def start_producer(self):
        logger = getattr(self, '_log', None)
        if isinstance(logger, FogVerseLogging):
            logger.std_log('Config: %s', self._producer_conf)
            if getattr(self, 'producer_topic', None) is not None:
                logger.std_log('Topic: %s', self.producer_topic)
        await self.producer.start()

    async def _send(self, data, *args, topic=None, headers=None, key=None,
                    **kwargs):
        if topic is None: 
            raise ValueError('Topic should not be None.')
        return await self.producer.send(topic,
                                        *args,
                                        value=data,
                                        key=key,
                                        headers=headers,
                                        **kwargs)

    async def close_producer(self):
        await self.producer.stop()
        logger = getattr(self, '_log', None)
        if isinstance(logger, FogVerseLogging):
            logger.std_log('Producer has closed.')

def _get_cv_video_capture(device=0):
    import cv2
    return cv2.VideoCapture(device)

class OpenCVConsumer(AbstractConsumer):
    def __init__(self, loop=None, executor=None):
        self._device = get_config('DEVICE', self, 0)
        self.consumer = getattr(self, 'consumer', None) or \
                            _get_cv_video_capture(self._device)
        self._loop = loop or asyncio.get_event_loop()
        self._executor = executor

    def close_consumer(self):
        self.consumer.release()
        logger = getattr(self, '_log', None)
        if isinstance(logger, FogVerseLogging):
            logger.std_log('OpenCVConsumer has closed.')

    async def receive_error(self, frame=None):
        logger = getattr(self, '_log', None)
        if isinstance(logger, FogVerseLogging):
            logger.std_log('Done OpenCVConsumer consuming the camera.')
        sys.exit(0)

    async def receive(self):
        success, frame = self.consumer.read()
        if not success:
            return await self.receive_error(frame)
        return frame
