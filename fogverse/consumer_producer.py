
import uuid
import queue
import asyncio
import socket
import sys
import multiprocessing as mp


from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from threading import Event
from traceback import print_exc

from aiokafka import (
    AIOKafkaConsumer as _AIOKafkaConsumer,
    AIOKafkaProducer as _AIOKafkaProducer
)
from confluent_kafka import Consumer, Message, Producer

from .util import get_config
from .fogverse_logging import FogVerseLogging, get_logger
from .base import AbstractConsumer, AbstractProducer, Processor

class AIOKafkaConsumer(AbstractConsumer):
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._topic_pattern = get_config('TOPIC_PATTERN', self)

        self._consumer_topic = get_config('CONSUMER_TOPIC', self, [])
        if isinstance(self._consumer_topic, str):
            self._consumer_topic = self._consumer_topic.split(',')

        self._consumer_servers = get_config('CONSUMER_SERVERS', self,
                                             'localhost')
        self.consumer_conf = getattr(self, 'consumer_conf', {})
        self.consumer_conf = {
            'loop': self._loop,
            'bootstrap_servers': self._consumer_servers,
            'group_id': get_config('GROUP_ID', self, str(uuid.uuid4())),
            'client_id': get_config('CLIENT_ID', self, socket.gethostname()),
            **self.consumer_conf}
        if self._consumer_topic:
            self.consumer = _AIOKafkaConsumer(*self._consumer_topic,
                                            **self.consumer_conf)
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
        if getattr(self, 'read_last', False):
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
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self.producer_topic = get_config('PRODUCER_TOPIC', self)
        self._producer_servers = get_config('PRODUCER_SERVERS', self)
        self._producer_conf = getattr(self, 'producer_conf', {})
        self._producer_conf = {
            'loop': self._loop,
            'bootstrap_servers': self._producer_servers,
            'client_id': get_config('CLIENT_ID', self, socket.gethostname()),
            **self._producer_conf}
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
        if topic is None: raise ValueError('Topic should not be None.')
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


class ConfluentConsumer:

    def __init__(self,
                 topics: str,
                 kafka_server: str,
                 group_id: str,
                 consumer_id : str,
                 consumer_auto_scaler,
                 producer_observer,
                 consumer_extra_config: dict={},
                 poll_time=1.0,
                 batch_size: int = 1,
                 ):

        self.topics = topics
        self.group_id = group_id
        self.poll_time = poll_time
        self.batch_size = batch_size
        self.consumer_auto_scaler = consumer_auto_scaler
        self.consumer_id = consumer_id
        self.consumed_messages = None

        self.current_assigned_partition = 0

        self.consumer = Consumer({
            **consumer_extra_config,
            "bootstrap.servers": kafka_server,
            "group.id": group_id
        })
        self.producer_observer = producer_observer


        self.log = get_logger(name=self.__class__.__name__)

    def on_partition_assigned(self, _, partition):

        if self.current_assigned_partition == 0 and len(partition) != 0:
            self.producer_observer.send_consumer_join_event(len(partition))
        if self.current_assigned_partition != 0 and len(partition) == 0:
            self.producer_observer.send_consumer_leave_event(len(partition))
        
        self.current_assigned_partition = len(partition)
        self.log.info(f"{self.consumer_id} assigned {len(partition)} partition")

    def on_partition_lost(self, _, partition):
        self.producer_observer.send_consumer_leave_event(len(partition))

        self.current_assigned_partition = len(partition)
        self.log.info(f"{self.consumer_id} disconnected")
        

    def start_consume(self, queue: queue.Queue, stop_event: Event):

        if self.consumer_auto_scaler is not None:
            self.consumed_messages = self.consumer_auto_scaler.start_with_distributed_lock(
                self.consumer,
                self.topics,
                self.group_id,
                self.consumer_id,
                self.on_partition_assigned,
                self.on_partition_lost,
            )

        else:
            self.consumer.subscribe([self.topics], 
                                    on_assign=self.on_partition_assigned,
                                    on_lost=self.on_partition_lost)

        try:
            while not stop_event.is_set():
                messages: list[Message] = self.consumer.consume(self.batch_size, self.poll_time)

                if self.current_assigned_partition > 0:
                    for message in messages:
                        queue.put(message)

        except Exception:
            print_exc()


class ConfluentProducer:

    def __init__(self, 
                 topic: str,
                 kafka_server: str,
                 processor : Processor,
                 producer_observer,
                 max_process_size=1,
                 producer_extra_config: dict={},
                 batch_size: int=1):
        
        self.producer = Producer({
            **producer_extra_config,
            "bootstrap.servers": kafka_server
        })
        self.processor = processor
        self.producer_observer = producer_observer

        self.topic = topic
        self.batch_size = batch_size
        self.max_process_size = max_process_size
        self.processing_pool = ProcessPoolExecutor(max_process_size, mp_context=mp.get_context('spawn'))
        self.thread_pool = ThreadPoolExecutor(max_process_size)

        self.log = get_logger(name=self.__class__.__name__)

    def __offload_to_process(self, message_batch: list[Message], queue):
        try:
            message_values = [m.value() for m in message_batch]
            future = self.processing_pool.submit(self.processor.process, message_values)
            results = future.result()

            self.__send_data(results)

        except Exception as e:
            self.log.error(e)
        finally:
            queue.task_done()
            message_batch.clear()

    def __send_data(self, results):
        for result in results:
            self.producer.produce(topic=self.topic, value=result)

        self.producer_observer.send_total_successful_messages(self.topic, len(results))
        self.producer.flush()

    def start_produce(self, queue: queue.Queue, stop_event: Event):
        try:
            message_batch: list[Message] = []
            while not stop_event.is_set():
                message: Message = queue.get()

                if message is None or message.error():
                    self.log.error(f"Error in message: {message.error()}")
                    queue.task_done()
                    continue

                message_batch.append(message)
                if len(message_batch) >= self.batch_size:
                    if self.max_process_size != 1:
                        self.thread_pool.submit(self.__offload_to_process, message_batch, queue)
                    else:
                        message_values = [m.value() for m in message_batch]
                        result = self.processor.process(message_values)
                        self.__send_data(result)

                    message_batch = []

        except Exception:
            print_exc()
        finally:
            self.processing_pool.shutdown(wait=True)
            self.thread_pool.shutdown(wait=True)
            self.producer.flush()


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
