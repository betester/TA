import os
import sys
import yaml

from confluent_kafka import admin, KafkaError, KafkaException
from pathlib import Path
from pprint import pprint

def check_server_env(server):
    new = server.copy()
    for server_name, host in new.items():
        if host.startswith('$'):
            host = os.getenv(host[1:])
            assert host is not None,\
                f'Host {host} couldn\'t be found in env variables.'
        new[server_name] = host
    return new

def create_topics(client: admin.AdminClient, topics, alter_if_exist=True):
    """Topics are a list of tuple(topic's name, number of partitions, and its
    config) or list of :class:`admin.NewTopic` or mixed.
    """
    if len(topics) == 0: return
    topic_instances = []
    for topic in topics:
        if isinstance(topic, admin.NewTopic):
            topic_instances.append(topic)
        else:
            topic_instance = admin.NewTopic(topic[0],
                                            num_partitions=topic[1],
                                            config=topic[2])
            topic_instances.append(topic_instance)
    res = client.create_topics(topic_instances)
    for topic_instance in topic_instances:
        topic_name = topic_instance.topic
        exc = res[topic_name].exception(5)
        if not alter_if_exist:
            print(f'Topic {topic_name} already exists, skipping...')
            continue
        if exc is not None:
            err = exc.args[0]
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f'Topic {topic_name} already exists, altering configs...')
                create_partitions(topic_name, client,
                                  topic_instance.num_partitions)
                alter_config(topic_name, client, topic_instance.config)
                status = 'altered'
        else:
            status = 'created'
        print(f'Topic {topic_name} has been {status}')

def alter_config(topic_name, adm, config):
    if len(config) == 0: return
    res = adm.alter_configs([
        admin.ConfigResource(admin.ConfigResource.Type.TOPIC,
                            topic_name,
                            set_config=config)
    ])
    for future in res.values():
        exc = future.exception(5)
        if exc is not None:
            raise exc

def create_partitions(topic_name, adm, num_partitions):
    new_partitions = admin.NewPartitions(topic_name, num_partitions)
    res = adm.create_partitions([new_partitions])
    for future in res.values():
        exc = future.exception(5)
        if exc is not None:
            err = exc.args[0]
            if err.code() == KafkaError.INVALID_PARTITIONS: continue
            raise exc

def read_topic_yaml(filepath, str_format={}, create=False):
    if isinstance(filepath, str):
        filepath = Path(filepath)
    admins = {}
    with filepath.open() as f:
        config = yaml.safe_load(f)
    topic = config['topic']
    server = check_server_env(config['server'])
    final_data = {}
    for server_name, host in server.items():
        admins[server_name] = admin.AdminClient({'bootstrap.servers':host})
    for topic, attr in topic.items():
        _partition = attr.get('partitions', 1)
        _config = attr.get('config', {})
        _server = attr['server']

        _pattern = attr.get('pattern')
        if _pattern is not None:
            _topic_name = _pattern.format(**str_format)
        else:
            _topic_name = topic

        if not isinstance(_server, list):
            _server = [_server]
        for server_name in _server:
            adm = admins[server_name]
            host = server[server_name]
            final_data.setdefault(host, {'admin': adm, 'topics': []})
            final_data[host]['topics'].append((_topic_name,
                                               _partition,
                                               _config))

    if not create: return final_data
    for topic_data in final_data.values():
        _admin = topic_data['admin']
        topics = topic_data['topics']
        create_topics(_admin, topics)
    print('Done')

if __name__ == '__main__':
    filepath = Path(sys.argv[1])
    read_topic_yaml(filepath, create=True)
