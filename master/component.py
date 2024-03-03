
from collections.abc import Callable, Coroutine
from typing import Tuple
from fogverse.util import get_config
from master.contract import TopicDeploymentConfig
from master.event_handler import Master
from master.master import AutoDeployer, ConsumerAutoScaler, ProducerObserver, topic_is_outlier
from confluent_kafka.admin import AdminClient
from functools import partial

from master.worker import InputOutputRatioWorker, StatisticWorker



class MasterComponent:

    def __init__(self):
        self.local_machine_ids = ['local'] 
        self.local_topic_machine_consumer: dict[str, list[str]] = {
            'analyze': self.local_machine_ids,
            'crawler': self.local_machine_ids,
            'client' : self.local_machine_ids
        }

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

    async def mock_deploy(self, topic_configs: TopicDeploymentConfig) -> Tuple[str, Callable]:
        print(f"Deploying {topic_configs.topic_id}")
        return "mock", lambda x : print(f"Turn off {x}")
    
    def master_event_handler(self):
        consumer_topic = str(get_config("OBSERVER_CONSUMER_TOPIC", self, "observer"))
        consumer_servers = str(get_config("OBSERVER_CONSUMER_SERVERS", self, "localhost:9092"))
        consumer_group_id = str(get_config("OBSERVER_CONSUMER_GROUP_ID", self, "observer"))
        deploy_delay = int(str(get_config("DEPLOY_DELAY", self, 60)))
        z_value = int(str(get_config("Z_VALUE", self, 3)))

        statistic_worker = StatisticWorker(maximum_seconds=300)

        outlier_detector = partial(topic_is_outlier, statistic_worker, z_value)

        auto_deployer = AutoDeployer(
            deploy_machine=self.mock_deploy,
            should_be_deployed=outlier_detector,
            deploy_delay=deploy_delay
        )


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

