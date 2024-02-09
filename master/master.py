
from kafka import KafkaConsumer, KafkaAdminClient

def get_group_coordinator(bootstrap_servers):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    group_coordinator = admin_client._find_coordinator_ids(["client"])
    return group_coordinator[0].nodeId

def list_consumers_for_topic(topic, coordinator_node_id, bootstrap_servers):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    group_list = consumer.list_groups()
    for group in group_list:
        group_id = group.group_id
        consumer_group_info = consumer.describe_groups(group_ids=[group_id])
        members = consumer_group_info[group_id].members
        for member in members:
            if topic in member.assignment.topic_names:
                print(f"Consumer group: {group_id}, Member ID: {member.member_id}, Client ID: {member.client_id}")

# Kafka brokers
bootstrap_servers = "localhost:9092"
topic_name = "your_topic_name"

coordinator_node_id = get_group_coordinator(bootstrap_servers)
list_consumers_for_topic(topic_name, coordinator_node_id, bootstrap_servers)

