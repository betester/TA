from confluent_kafka import TopicCollection, admin
from contract import Master

class ConsumerAutoScaler(Master):
    
    def __init__(self, admin_client: admin.AdminClient):
        self._admin = admin_client

    def add_new_consumer(self):
        return super().add_new_consumer()
    
    def _get_topic_total_partitions(self, topic_ids: list[str]) -> dict[str, int]:
        topic_total_partitions: dict[str, int] = {}
        
        topic_description_future = self._admin.describe_topics(TopicCollection(topic_ids))

        for topic_id in topic_ids:
            topic_description: admin.TopicDescription = topic_description_future[topic_id].result()
            topic_total_partitions[topic_id] = len(topic_description.partitions())

        return topic_total_partitions

    def _get_consumer_groups_members(self, group_ids: list[str]) -> dict[str, list[admin.MemberDescription]]:
        groups_description_future = self._admin.describe_consumer_groups(group_ids)
        consumer_group_members: dict[str, list[admin.MemberDescription]] = {}

        for group_id in group_ids:
            group_description: admin.ConsumerGroupDescription = groups_description_future[group_id].result()
            consumer_group_members[group_id] = group_description.members

        return consumer_group_members
    
    def _unused_consumer_on_topics(self, topic_id: list[str], group_ids: list[str] = None):

        if group_ids is None:
            group_ids = self._admin.list_groups()

        consumer_group_members = self._get_consumer_groups_members(group_ids)

        for group_members in consumer_group_members.values():
            for member in group_members:
                assignment: admin.MemberAssignment = member.assignment
                assignment.topic_partitions

