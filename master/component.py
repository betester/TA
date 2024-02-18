from fogverse.util import get_config
from master.master import ConsumerAutoScaler
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
