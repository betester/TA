
from fogverse.util import get_config
from master.event_handler import Master
from master.master import AutoDeployer, ConsumerAutoScaler, DeployScripts, ProducerObserver, TopicSpikeChecker
from confluent_kafka.admin import AdminClient
from functools import partial

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

    def master_event_handler(self):
        consumer_topic = str(get_config("OBSERVER_CONSUMER_TOPIC", self, "observer"))
        consumer_servers = str(get_config("OBSERVER_CONSUMER_SERVERS", self, "localhost:9092"))
        print(consumer_servers)
        consumer_group_id = str(get_config("OBSERVER_CONSUMER_GROUP_ID", self, "observer"))
        deploy_delay = int(str(get_config("DEPLOY_DELAY", self, 60)))
        z_value = int(str(get_config("Z_VALUE", self, 3)))
        window_max_second = int(str(get_config("WINDOW_MAX_SECOND", self, 300))) # 5 minutes default
        input_output_ratio_threshold = float(str(get_config("INPUT_OUTPUT_RATIO_THRESHOLD", self, 0.7)))
        input_output_refresh_rate = float(str(get_config("INPUT_OUTPUT_REFRESH_RATE", self, 0.7)))

        statistic_worker = StatisticWorker(maximum_seconds=window_max_second)
        topic_spike_checker = TopicSpikeChecker(statistic_worker)
        deploy_script= DeployScripts()

        auto_deployer = AutoDeployer(
            deploy_script=deploy_script,
            should_be_deployed=partial(topic_spike_checker.check_spike_by_z_value, z_value),
            deploy_delay=deploy_delay
        )

        input_output_worker = InputOutputRatioWorker(
            refresh_rate_second=input_output_refresh_rate,
            input_output_ratio_threshold=input_output_ratio_threshold,
            below_threshold_callback=auto_deployer.deploy
        )

        workers = [statistic_worker, input_output_worker, auto_deployer]

        return Master(
            consumer_topic=consumer_topic,
            consumer_group_id=consumer_group_id,
            consumer_servers=consumer_servers,
            observers=workers
        )

