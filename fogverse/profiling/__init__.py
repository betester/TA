import asyncio
import json
import logging
import os
import secrets
import socket
import time

from .base import AbstractProfiling

from fogverse.util import (calc_datetime, get_header, get_timestamp, size_kb,
                           get_timestamp_str)
from fogverse.fogverse_logging import FogVerseLogging

from aiokafka import ConsumerRecord, AIOKafkaProducer as _AIOKafkaProducer

def _calc_delay(start, end=None, decimals=2):
    end = end or time.time()
    delay = (end - start) * 1E3
    return round(delay, decimals)

class BaseProfiling(AbstractProfiling):
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._log_data = {}

    def finalize_data(self, data=None, default_none=None):
        if data is None:
            data = self._log_data

        res = []
        headers = self._df_header
        for header in headers:
            _data = data.get(header, default_none)
            res.append(_data)

        return headers, res

class Profiling(BaseProfiling):
    def __init__(self,
                 name=None,
                 dirname=None,
                 df_header=['topic from','topic to','frame','offset received',
                            'frame delay (ms)','msg creation delay (ms)',
                            'consume time (ms)','size data received (KB)',
                            'decode time (ms)','size data decoded (KB)',
                            'process time (ms)','size data processed (KB)',
                            'encode time (ms)','size data encoded (KB)',
                            'send time (ms)','size data sent (KB)','offset sent'],
                 remote_logging=False,
                 remote_logging_name=None,
                 logger=None,
                 app_id=None,
                 loop=None):
        super().__init__(loop=loop)
        self._unique_id = secrets.token_hex(3)
        self.app_id = app_id or self._unique_id
        self._profiling_name = name or \
            f'{self.__class__.__name__}_{self._unique_id}'
        self._remote_logging_name = remote_logging_name or self._profiling_name
        self._logging_producer = None
        self._df_header = df_header
        if remote_logging:
            # self._bg_tasks = set()
            self._logging_topic = os.getenv('LOGGING_TOPIC') or \
                getattr(self, 'logging_topic', None) or \
                'fogverse-profiling'
            assert self._logging_topic is not None, \
                'Setting remote_log=True for Profiling means there should\n'\
                'be topic for logger to produce. Please set LOGGING_TOPIC env,\n'\
                'or self.logging_topic var.'

            self._logging_producer_servers = \
                os.getenv('LOGGING_PRODUCER_SERVERS') or \
                getattr(self, 'logging_producer_servers', None) or \
                os.getenv('PRODUCER_SERVERS') or \
                getattr(self, 'producer_servers', None)
            assert self._logging_producer_servers is not None, \
                'Setting remote_log=True for Profiling means there should\n'\
                'be kafka server address for logger to produce. Please set\n'\
                'LOGGING_PRODUCER_SERVERS env, or self.logging_producer_servers var,\n'\
                'or PRODUCER_SERVERS env, or self.producer_servers var.'

            self._logging_producer_conf = getattr(self, 'logging_producer_conf', {})
            _logging_producer_conf = {
                'loop': self._loop,
                'bootstrap_servers': self._logging_producer_servers,
                'client_id': os.getenv('CLIENT_ID', socket.gethostname()),
                **self._logging_producer_conf}
            self._logging_producer = _AIOKafkaProducer(**_logging_producer_conf)

        self._log = logger or FogVerseLogging(name=self._profiling_name,
                                              dirname=dirname,
                                              csv_header=self._df_header,
                                              level=logging.FOGV_CSV)

    async def start_producer(self):
        if hasattr(super(), 'start_producer'):
            await super().start_producer()
        if self._logging_producer:
            await self._logging_producer.start()

    async def stop_producer(self):
        if hasattr(super(), 'stop_producer'):
            await super().stop_producer()
        if self._logging_producer:
            await self._logging_producer.start()

    def _before_receive(self):
        self._log_data.clear()
        self._start = get_timestamp()

    def _after_receive(self, data):
        delay_consume = calc_datetime(self._start)
        self._log_data['consume time (ms)'] = delay_consume

        if isinstance(data, dict) and data.get('data') != None:
            _size = size_kb(data['data'])
        else:
            _size = size_kb(data)
        self._log_data['size data received (KB)'] = _size

    def _before_decode(self, _):
        self._before_decode_time = get_timestamp()

    def _after_decode(self, data):
        decoding_time = calc_datetime(self._before_decode_time)
        self._log_data['decode time (ms)'] = decoding_time

        if isinstance(self.message, ConsumerRecord):
            now = get_timestamp()
            frame_creation_time = get_header(self.message.headers,
                                               'timestamp')
            if frame_creation_time == None:
                frame_delay = -1
            else:
                frame_delay = calc_datetime(frame_creation_time, end=now)
            creation_delay = _calc_delay(self.message.timestamp/1e3)
            offset_received = self.message.offset
            topic_from = self.message.topic
        else:
            frame_delay = -1
            creation_delay = self._log_data['consume time (ms)']
            offset_received = -1
            topic_from = None

        extras = getattr(self, '_message_extra', None)
        if extras:
            consume_time = extras.get('consume time (ms)')
            if consume_time:
                self._log_data['consume time (ms)'] = consume_time
        self._log_data['frame delay (ms)'] = frame_delay
        self._log_data['msg creation delay (ms)'] = creation_delay
        self._log_data['offset received'] = offset_received
        self._log_data['topic from'] = topic_from

        self._log_data['size data decoded (KB)'] = size_kb(data)

    def _before_process(self, _):
        self._before_process_time = get_timestamp()

    def _after_process(self, result):
        delay_process = calc_datetime(self._before_process_time)
        self._log_data['process time (ms)'] = delay_process
        self._log_data['size data processed (KB)'] = size_kb(result)

    def _before_encode(self, _):
        self._before_encode_time = get_timestamp()

    def _after_encode(self, data):
        delay_encode = calc_datetime(self._before_encode_time)
        self._log_data['encode time (ms)'] = delay_encode
        self._log_data['size data encoded (KB)'] = size_kb(data)

    def _before_send(self, data):
        self._log_data['size data sent (KB)'] = size_kb(data)
        self._datetime_before_send = get_timestamp()

    def _get_extra_callback_args(self):
        args, kwargs = [] , {
            'log_data': self._log_data.copy(),
            'headers': getattr(self,'_headers',None),
            'topic': getattr(self,'_topic',None),
            'timestamp': getattr(self, '_datetime_before_send', None),
        }
        self._log_data.clear()
        return args, kwargs

    async def _send_logging_data(self, log_headers, log_data) -> asyncio.Future:
        if self._logging_producer is None: return
        send_data = {
            'app_id': self.app_id,
            'log headers': ['timestamp', *log_headers],
            'log data': [get_timestamp_str(), *log_data],
            'extras': getattr(self, 'extra_remote_data', {}),
        }
        _edit_remote_data = getattr(self, '_edit_remote_data', None)
        if callable(_edit_remote_data):
            self._edit_remote_data(send_data)
        send_data = json.dumps(send_data, default=vars).encode()
        await self._logging_producer.send(topic=self._logging_topic,
                                                   value=send_data)

    async def callback(self, record_metadata, *args,
                 log_data=None, headers=None, topic=None,
                    timestamp=None, **kwargs):
        frame = int(get_header(headers,'frame',default=-1))
        offset = getattr(record_metadata, 'offset', -1)
        log_data['offset sent'] = offset
        log_data['frame'] = frame
        log_data['topic to'] = topic
        log_data['send time (ms)'] = calc_datetime(timestamp)

        log_headers, res_data = self.finalize_data(log_data)
        await self._send_logging_data(log_headers, res_data)
        self._log.csv_log(res_data)

    def _after_send(self, data):
        if len(self._log_data) == 0: return
        send_time = calc_datetime(self._datetime_before_send)
        self._log_data['send time (ms)'] = send_time

        size_sent = size_kb(data)
        self._log_data['size data sent (KB)'] = size_sent

        log_headers, res_data = self.finalize_data()
        self._send_logging_data(log_headers, res_data)
        self._log.csv_log(res_data)
