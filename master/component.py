
from os import chmod
from uuid import uuid4
from logging import Logger
from fogverse.util import get_config
from master.contract import DeployResult, TopicDeploymentConfig
from master.event_handler import Master
from master.master import AutoDeployer, ConsumerAutoScaler, DeployScripts, ProducerObserver, TopicSpikeChecker
from confluent_kafka.admin import AdminClient
from functools import partial

from master.worker import InputOutputRatioWorker, ProfillingWorker, StatisticWorker
from scripts.local_deploy import deploy_instance_with_process
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

    def parse_dict_to_docker_env(self, container_env: dict):
        docker_container_env = ""

        for key, val in container_env.items():
            docker_container_env += f" -e {key}={val}"

        return docker_container_env

    def parse_dict_to_txt_env(self, container_env: dict, machine_id : str):
        
        config_file_name = f"{machine_id}.txt" 

        with open(config_file_name, "w+") as f:
            for key, val in container_env.items():
                f.write(f"{key}={val}\n")

        chmod(config_file_name, 0o777)

        return f"./{config_file_name}"

    async def google_deployment(self, topic_deployment_config: TopicDeploymentConfig, logger: Logger) -> DeployResult:
        
        random_unique_id = uuid4()
        machine_id = f"{topic_deployment_config.service_name}{random_unique_id}"
        machine_type = topic_deployment_config.machine_type
        image_env = topic_deployment_config.image_env
        
        container_env = self.parse_dict_to_txt_env(image_env, machine_id) if  machine_type == 'CPU' else self.parse_dict_to_docker_env(image_env)

        logger.info(f"Deploying google instance with id : {machine_id}")

        process = await deploy_instance_with_process(
            topic_deployment_config.project_name,
            machine_id,
            topic_deployment_config.image_name,
            topic_deployment_config.zone,
            topic_deployment_config.service_account,
            container_env,
            topic_deployment_config.machine_type
        )

        if process.stdout:
            async for line in process.stdout:
                logger.info(line.decode('utf-8'))

        async def shutdown_google_cloud_instance():
            logger.info("Not implemented yet")
            return True
        
        
        return DeployResult(
            machine_id=machine_id, 
            shut_down_machine=shutdown_google_cloud_instance
        )

    def master_event_handler(self):
        consumer_topic = str(get_config("OBSERVER_CONSUMER_TOPIC", self, "observer"))
        consumer_servers = str(get_config("OBSERVER_CONSUMER_SERVERS", self, "localhost:9092"))
        consumer_group_id = str(get_config("OBSERVER_CONSUMER_GROUP_ID", self, "observer"))
        deploy_delay = int(str(get_config("DEPLOY_DELAY", self, 900))) # 15 minutes over the next deployment
        z_value = int(str(get_config("Z_VALUE", self, 3)))
        window_max_second = int(str(get_config("WINDOW_MAX_SECOND", self, 300))) # 5 minutes default
        input_output_ratio_threshold = float(str(get_config("INPUT_OUTPUT_RATIO_THRESHOLD", self, 0.7)))
        input_output_refresh_rate = float(str(get_config("INPUT_OUTPUT_REFRESH_RATE", self, 60)))
        profilling_time_window = int(str(get_config("PROFILLING_TIME_WINDOW", self, 1)))
        hearbeart_deploy_delay = int(str(get_config("HEARBEART_DEPLOY_DELAY", self, 120))) # 2 minutes after deployment happens

        statistic_worker = StatisticWorker(maximum_seconds=window_max_second)
        topic_spike_checker = TopicSpikeChecker(statistic_worker)

        deploy_script = DeployScripts()

        deploy_script.set_deploy_functions(
            "GOOGLE_CLOUD",
            self.google_deployment
        )
        auto_deployer = AutoDeployer(
            deploy_script=deploy_script,
            should_be_deployed=partial(topic_spike_checker.check_spike_by_z_value, z_value),
            deploy_delay=deploy_delay,
            after_heartbeat_delay=hearbeart_deploy_delay
        )
        input_output_worker = InputOutputRatioWorker(
            refresh_rate_second=input_output_refresh_rate,
            input_output_ratio_threshold=input_output_ratio_threshold,
            below_threshold_callback=auto_deployer.deploy
        )
        profilling_worker = ProfillingWorker(auto_deployer.get_topic_total_machine, profilling_time_window)

        workers = [statistic_worker, profilling_worker, input_output_worker, auto_deployer]

        return Master(
            consumer_topic=consumer_topic,
            consumer_group_id=consumer_group_id,
            consumer_servers=consumer_servers,
            observers=workers
        )
