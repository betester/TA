from kafka import KafkaAdminClient

client = KafkaAdminClient(bootstrap_servers="localhost:9092")
for group in client.list_consumer_groups():
    print(group[0])