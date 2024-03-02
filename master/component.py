from fogverse.util import get_config
from master.event_handler import Master
from master.master import AutoDeployer, ConsumerAutoScaler, ProducerObserver
from confluent_kafka.admin import AdminClient

from master.worker import InputOutputRatioWorker, StatisticWorker



class MasterComponent:

    def consumer_auto_scaler(self):
        
        bootstrap_host = str(get_config("KAFKA_ADMIN_HOST", self, "localhost"))
        sleep_time = int(str(get_config("SLEEP_TIME", self, 3)))


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

    async def mock_deploy(self, string: str) -> Boolean:
        return True

    def mock_should_be_deployed(self, string: str, integer: int) -> Boolean:
        return True
    
    def master_event_handler(self):
        consumer_topic = str(get_config("OBSERVER_CONSUMER_TOPIC", self, "observer"))
        consumer_servers = str(get_config("OBSERVER_CONSUMER_SERVERS", self, "localhost:9092"))
        consumer_group_id = str(get_config("OBSERVER_CONSUMER_GROUP_ID", self, "observer"))
        deploy_delay = int(str(get_config("DEPLOY_DELAY", self, 60)))

        auto_deployer = AutoDeployer(
            deploy_command=self.mock_deploy,
            should_be_deployed=self.mock_should_be_deployed,
            deploy_delay=deploy_delay,
            machine_ids=[],
            topic_machine_consumer={}
        )

        statistic_worker = StatisticWorker(maximum_seconds=300)
        input_output_worker = InputOutputRatioWorker(
            refresh_rate_second=60,
            input_output_ratio_threshold=0.7,
            below_threshold_callback=auto_deployer.deploy
        )

        workers = [statistic_worker, input_output_worker]

        return Master(
            consumer_topic=consumer_topic,
            consumer_group_id=consumer_group_id,
            consumer_servers=consumer_servers,
            observers=workers
        )

