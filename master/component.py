
from confluent_kafka.admin import AdminClient
from fogverse.util import get_config
from master.master import ConsumerAutoScaler
from master.event_handler import ConsumerAutoScalerEventHandler

class MasterComponent:

    def __init__(self):
        self._producer_topic = str(get_config("CONSUMER_EVENT_HANDLER_PRODUCER_TOPIC", self, "client_v4"))
        self._producer_servers = str(get_config("CONSUMER_EVENT_HANDLER_PRODUCER_SERVERS", self, "localhost:9092"))
        self._consumer_topic = str(get_config("CONSUMER_EVENT_HANDLER_CONSUMER_TOPIC", self, "analyze"))
        self._consumer_servers = str(get_config("CONSUMER_EVENT_HANDLER_CONSUMER_SERVERS", self, "localhost:9092"))
        self._consumer_group_id = str(get_config("CONSUMER_EVENT_HANDLER_CONSUMER_GROUP_ID", self, "analyzer"))
        
        self._kafka_admin_bootstrap_server =str(get_config("KAFKA_ADMIN_BOOTSTRAP_SERVER", self, "localhost"))
        self._initial_partition = int(str(get_config("INITIAL_PARTITION", self, 1)))


    def consumer_partition_auto_scaler(self) -> ConsumerAutoScalerEventHandler:
        
        kafka_admin = AdminClient(
            conf= {
                "bootstrap.servers": self._kafka_admin_bootstrap_server
            }
        )

        consumer_auto_scaler = ConsumerAutoScaler(
            admin_client=kafka_admin,
            initial_partition=self._initial_partition
        )

        return ConsumerAutoScalerEventHandler(
            producer_topic=self._producer_topic,
            producer_servers=self._producer_servers, 
            consumer_topic=self._consumer_topic, 
            consumer_servers=self._consumer_servers, 
            auto_scaler=consumer_auto_scaler,
            consumer_group_id=self._consumer_group_id
        )

