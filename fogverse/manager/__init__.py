import aiokafka
import asyncio
import json
import logging
import os
import re
import secrets
import socket
import sys
import traceback
import uuid

from confluent_kafka import admin
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fogverse.admin import create_topics, read_topic_yaml
from fogverse.constants import *
from fogverse.fogverse_logging import FogVerseLogging
from pathlib import Path

_default_topic_config = {
    'retention.ms': 30000,
    'segment.bytes': 1000000,
    'segment.ms': 10000,
}

_default_partitions = 1

class Manager:
    def __init__(self,
                 components=[],
                 loop=None,
                 kafka_servers='localhost',
                 app_id=None,
                 component_id='',
                 topic='fogverse-commands',
                 log_dir='logs',
                 to_deploy={},
                 manager_topic_config={},
                 topic_str_format=_default_topic_config,
                 manager_topic_partitions=_default_partitions,
        ):
        self.app_id = os.getenv('APP_ID', app_id) or str(uuid.uuid4())
        self.component_id = component_id.title() or secrets.token_hex(3)
        self.manager_id = f'Manager_{self.component_id}_{self.app_id}'
        self.loop = loop or asyncio.get_event_loop()
        self.components = components
        self.logger = FogVerseLogging(self.manager_id,level=logging.FOGV_FILE,
                                      dirname=log_dir)
        self.manager_kafka_servers = os.getenv('KAFKA_SERVERS') or \
                                     os.getenv('CONSUMER_SERVERS') or \
                                     os.getenv('PRODUCER_SERVERS') or \
                                     kafka_servers
        self.admin = admin.AdminClient(
            {'bootstrap.servers': self.manager_kafka_servers})
        self.kafka_config = {
            'loop': self.loop,
            'client_id': socket.gethostname(),
            'bootstrap_servers': self.manager_kafka_servers,
        }
        self.logger.std_log('Initiating Manager with config: %s',
                            self.kafka_config)
        self.topic = topic
        self.producer = AIOKafkaProducer(**self.kafka_config)
        self.consumer = AIOKafkaConsumer(self.topic, **self.kafka_config)
        self.to_deploy = to_deploy
        self.run_event = asyncio.Event()
        if not self.to_deploy:
            self.run_event.set()
        self.manager_topic_config = manager_topic_config
        self.manager_topic_partitions = manager_topic_partitions
        self.topic_str_format = topic_str_format

    async def check_topics(self):
        prog_path = Path()
        yaml = (prog_path / 'topic.yaml')
        topic_data = {}
        if yaml.is_file():
            topic_data = read_topic_yaml(yaml,str_format=self.topic_str_format)
        topic_data.setdefault(self.manager_kafka_servers, {'admin': self.admin,
                                                           'topics':[]})
        topic_data[self.manager_kafka_servers]['topics'].append(
            (self.topic,self.manager_topic_partitions,self.manager_topic_config)
        )
        for _, data in topic_data.items():
            create_topics(data['admin'], data['topics'])

    async def start(self):
        await self.consumer.start()
        await self.producer.start()

    async def stop(self):
        await self.consumer.stop()
        await self.producer.stop()

    async def send_message(self, command, message, **kwargs) -> asyncio.Future:
        msg = {
            'command': command,
            'from': self.manager_id,
            'message': message
        }
        msg = json.dumps(msg).encode()
        return await self.producer.send(self.topic, msg, **kwargs)

    async def send_running_message(self):
        msg = dict(app_id=self.app_id)
        return await self.send_message(FOGV_STATUS_RUNNING, msg)

    async def send_request_component(self):
        while True:
            if self.run_event.is_set(): return
            self.logger.std_log('Sending request component')
            self.logger.std_log('to_deploy: %s', self.to_deploy)
            pending_comp = False
            for comp_type, comp_data in self.to_deploy.items():
                if not comp_data.get('wait_to_start', False): continue
                if comp_data.get('status') == FOGV_STATUS_RUNNING: continue
                if comp_data.get('status') is None:
                    pending_comp = True
                msg = {
                    'image': comp_data['image'],
                    'app_id': comp_data['app_id'],
                    'env': comp_data['env'],
                }
                task = self.send_message(FOGV_CMD_REQUEST_DEPLOY, msg)
                asyncio.ensure_future(task)
            if pending_comp == False: break
            await asyncio.sleep(10)
        self.logger.std_log('Leaving component request sending procedure')
        self.run_event.set()

    async def send_shutdown(self):
        self.logger.std_log('Sending shutdown status')
        msg = dict(app_id=self.app_id)
        future = await self.send_message(FOGV_STATUS_SHUTDOWN, msg)
        return await future

    async def handle_shutdown(self, message):
        self.logger.std_log('Handle shutdown')
        if message['app_id'] != self.app_id: return
        sys.exit(0)

    async def handle_running(self, message):
        self.logger.std_log('Handle running')
        for _, comp_data in self.to_deploy.items():
            if comp_data['app_id'] == message['app_id']:
                comp_data['status'] = FOGV_STATUS_RUNNING

    async def handle_message(self, command, message):
        self.logger.std_log('Handle general message')
        pass

    async def receive_message(self):
        try:
            async for msg in self.consumer:
                msg = msg.value.decode()
                data = json.loads(msg)
                command = data['command']
                message = data['message']
                if data['from'] == self.manager_id: continue
                self.logger.std_log('got message: %s', data)
                self.logger.std_log('to_deploy: %s', self.to_deploy)
                handler = getattr(self, f'handle_{command.lower()}', None)
                if callable(handler):
                    ret = handler(message)
                    if asyncio.iscoroutine(ret):
                        await ret
                    continue
                await self.handle_message(command, message)
        except Exception as e:
            self.logger.std_log('masok exc1')
            err = traceback.format_exc()
            self.logger.std_log(err)
            raise e

    async def run_components(self, components=[]):
        if not self.run_event.is_set():
            self.logger.std_log('Waiting for dependencies to run')
            await self.run_event.wait()
            self.logger.std_log('Running all components')

        try:
            comps = components or self.components
            tasks = [asyncio.ensure_future(i.run()) for i in comps]
            await self.send_running_message()
            self.logger.std_log('Components are running')
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.std_log('masok exc2')
            err = traceback.format_exc()
            self.logger.std_log(err)
            for t in tasks:
                print(t)
                t.cancel()
            raise e

    async def run(self):
        await self.check_topics()
        await self.start()
        try:
            tasks = [self.receive_message(), self.run_components()]
            tasks = [asyncio.ensure_future(i) for i in tasks]
            self.logger.std_log('Manager %s is running', self.manager_id)
            await self.send_request_component()
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.std_log('masok exc3')
            err = traceback.format_exc()
            self.logger.std_log(err)
            for t in tasks:
                print(t)
                t.cancel()
            raise e
        finally:
            await self.stop()
            self.logger.std_log('Manager has stopped')
