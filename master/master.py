from time import sleep
from typing import Optional
from confluent_kafka import TopicCollection, admin
from contract import Master


class ConsumerAutoScaler(Master):
    
    def __init__(self, admin_client: admin.AdminClient):
        self._admin = admin_client

    def add_new_consumer(self, topic_id, group_id):
        topic_id_total_consumer, topic_id_total_partition = self._count_topic_total_partition_and_consumer(
            topic_id,
            group_id
        )
        for topic in topic_id:
            if topic_id_total_consumer[topic] > topic_id_total_partition[topic]:
                self._add_new_partition_on(
                    topic,
                    topic_id_total_consumer[topic]
                )
    
    def _get_topic_total_partitions(self, topic_ids: list[str]) -> dict[str, int]:
        topic_total_partitions: dict[str, int] = {}
        
        topic_description_future = self._admin.describe_topics(TopicCollection(topic_ids))

        for topic_id in topic_ids:
            topic_description: admin.TopicDescription = topic_description_future[topic_id].result()
            topic_total_partitions[topic_id] = len(topic_description.partitions)

        return topic_total_partitions

    def _add_new_partition_on(self, topic: str, offset: int):
        pass

    def _get_consumer_groups_members(self, group_ids: list[str]) -> dict[str, list[admin.MemberDescription]]:
        groups_description_future = self._admin.describe_consumer_groups(group_ids)
        consumer_group_members: dict[str, int] = {}

        for group_id in group_ids:
            group_description: admin.ConsumerGroupDescription = groups_description_future[group_id].result()
            consumer_group_members[group_id] = len(group_description.members)

        return consumer_group_members

consumer_auto_scaler = ConsumerAutoScaler(
    admin.AdminClient(
        conf= {
            "bootstrap.servers": "localhost"
        }
    )
)

while True:
    total_partition = consumer_auto_scaler._get_topic_total_partitions(["client"])
    total_group_member = consumer_auto_scaler._get_consumer_groups_members(["client"])
    print(f'Partitions: {total_partition}, Members: {total_group_member}')
    sleep(1)

