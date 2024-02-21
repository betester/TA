from fogverse.util import get_config
from master.event_handler import Master
from master.master import ConsumerAutoScaler, ProducerObserver
from confluent_kafka.admin import AdminClient



class MasterComponent:

    def consumer_auto_scaler(self):
        
        bootstrap_host = str(get_config("KAFKA_ADMIN_HOST", self, "localhost"))
        sleep_time = int(str(get_config("SLEEP_TIME", self, 4)))


        kafka_admin=AdminClient(
            conf={
                "bootstrap.servers": bootstrap_host
            },
        )

        return ConsumerAutoScaler(
            kafka_admin=kafka_admin,
            sleep_time=sleep_time
        )
    
    def producer_observer(self):
        observer_topic = str(get_config("OBSERVER_TOPIC", self, "observer"))
        return ProducerObserver(observer_topic)
    
    def master_event_handler(self):
        consumer_topic = str(get_config("OBSERVER_CONSUMER_TOPIC", self, "observer"))
        consumer_servers = str(get_config("OBSERVER_CONSUMER_SERVERS", self, "localhost:9092"))
        consumer_group_id = str(get_config("OBSERVER_CONSUMER_GROUP_ID", self, "observer"))
        scheduler_time = int(str(get_config("SCHEDULER_TIME", self, 60)))

        return Master(
            consumer_topic=consumer_topic,
            consumer_group_id=consumer_group_id,
            consumer_servers=consumer_servers,
            scheduler_time=scheduler_time
        )

