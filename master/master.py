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
            topic_total_partitions[topic_id] = len(topic_description.partitions())

        return topic_total_partitions

    def _add_new_partition_on(self, topic: str, offset: int):
        pass

    def _get_consumer_groups_members(self, group_ids: list[str]) -> dict[str, list[admin.MemberDescription]]:
        groups_description_future = self._admin.describe_consumer_groups(group_ids)
        consumer_group_members: dict[str, list[admin.MemberDescription]] = {}

        for group_id in group_ids:
            group_description: admin.ConsumerGroupDescription = groups_description_future[group_id].result()
            consumer_group_members[group_id] = group_description.members

        return consumer_group_members
    
    def _count_topic_total_partition_and_consumer(self, topic_id: list[str], group_ids: Optional[list[str]]):

        if group_ids is None:
            group_ids =list(map(lambda x : x.id, self._admin.list_groups()))
        
        consumer_group_members = self._get_consumer_groups_members(group_ids)

        topic_id_total_consumer: dict[str, int] = {}
        topic_id_total_partition: dict[str, int] = {}

        for group_members in consumer_group_members.values():
            for member in group_members:
                assignment: admin.MemberAssignment = member.assignment

                if not assignment:
                    continue
                
                consumed_topic_by_member = set()

                if len(assignment.topic_partitions) > 0:
                    for partition in assignment.topic_partitions:
                        consumed_topic_by_member.add(partition.topic)

                        topic_id_total_partition[partition.topic] = max(
                            topic_id_total_partition.get(partition.topic, 0),
                            partition.partition + 1
                        )

                for topic in consumed_topic_by_member:
                    if topic in topic_id:
                        topic_id_total_consumer[topic] += 1
        
        return topic_id_total_consumer, topic_id_total_partition

consumer_auto_scaler = ConsumerAutoScaler(
    admin.AdminClient(
        conf= {
            "bootstrap.servers": "localhost"
        }
    )
)

while True:
    print(consumer_auto_scaler._count_topic_total_partition_and_consumer(["client"], ["client"]))
    sleep(1)

