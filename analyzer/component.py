from typing import Optional

from aiokafka.conn import functools
from analyzer.processor import AnalyzerProcessor
from fogverse.consumer_producer import ConfluentConsumer, ConfluentProducer
from fogverse.general import ParallelRunnable
from fogverse.util import get_config
from master.contract import TopicDeploymentConfig
from master.master import ConsumerAutoScaler, ProducerObserver
from .analyzer import DisasterAnalyzerImpl
from .handler import AnalyzerProducer, ParallelAnalyzerJobService

class AnalyzerComponent:

    def __init__(self):
        self._producer_topic = str(get_config("ANALYZER_PRODUCER_TOPIC", self, "client_v6"))
        self._producer_servers = str(get_config("ANALYZER_PRODUCER_SERVERS", self, "localhost:9092"))
        self._consumer_topic = str(get_config("ANALYZER_CONSUMER_TOPIC", self, "xi"))
        self._consumer_servers = str(get_config("ANALYZER_CONSUMER_SERVERS", self, "localhost:9092"))
        self._consumer_group_id = str(get_config("ANALYZER_CONSUMER_GROUP_ID", self, "analyzer_v2"))
        # assigns based on the attribute and model source
        self._disaster_classifier_model_source = ("is_disaster", str(get_config("DISASTER_CLASSIFIER_MODEL_SOURCE", self, "./mocking_bird")))
        self._keyword_classifier_model_source = ("keyword", str(get_config("KEYWORD_CLASSIFIER_MODEL_SOURCE", self, "./jay_bird")))

        # cloud configs 
        self._cloud_provider = str(get_config("CLOUD_PROVIDER", self, "LOCAL"))
        self._max_instance = int(str(get_config("MAX_INSTANCE", self, "4")))
        self._machine_type = str(get_config("MACHINE_TYPE", self, "GPU"))

        self._container_env = {
            "MACHINE_TYPE": self._machine_type,
            "MAX_INSTANCE": self._max_instance,
            "CLOUD_PROVIDER": self._cloud_provider,
            "keyword": self._keyword_classifier_model_source,
            "is_disaster": self._disaster_classifier_model_source,
            "ANALYZER_CONSUMER_GROUP_ID": self._consumer_group_id,
            "ANALYZER_CONSUMER_SERVERS": self._consumer_servers,
            "ANALYZER_CONSUMER_TOPIC": self._consumer_topic,
            "ANALYZER_PRODUCER_SERVERS": self._producer_servers,
            "ANALYZER_PRODUCER_TOPIC": self._producer_topic,

        }



    def disaster_analyzer(self, consumer_auto_scaler: Optional[ConsumerAutoScaler], producer_observer: ProducerObserver):
        
        disaster_analyzers = DisasterAnalyzerImpl(
            self._disaster_classifier_model_source,
            self._keyword_classifier_model_source
        )
        
        topic_deployment_config = TopicDeploymentConfig(
            max_instance=self._max_instance,
            topic_id=self._producer_topic,
            project_name=str(get_config("PROJECT_NAME", self, "")),
            service_name=str(get_config("SERVICE_NAME", self, "")),
            image_name=str(get_config("IMAGE_NAME", self, "")),
            zone=str(get_config("ZONE", self, "")),
            service_account=str(get_config("SERVICE_ACCOUNT", self, "")),
            image_env=self._container_env,
            machine_type=self._machine_type,
            provider=self._cloud_provider
            )

        analyzer_producer = AnalyzerProducer(
            producer_topic=self._producer_topic,
            producer_servers=self._producer_servers, 
            consumer_topic=self._consumer_topic, 
            consumer_servers=self._consumer_servers, 
            classifier_model=disaster_analyzers,
            consumer_group_id=self._consumer_group_id,
            consumer_auto_scaler=consumer_auto_scaler,
            producer_observer=producer_observer,
            topic_deployment_config=topic_deployment_config
        )

        return analyzer_producer
    
    def parallel_disaster_analyzer(self, consumer_auto_scaler: ConsumerAutoScaler, producer_observer: ProducerObserver):

        disaster_analyzers = DisasterAnalyzerImpl(
            self._disaster_classifier_model_source
        )

        analyzer_processor = AnalyzerProcessor(disaster_analyzers)

        consumer = ConfluentConsumer(
            topics=self._consumer_topic,
            kafka_server=self._consumer_servers,
            group_id=self._consumer_group_id,
            consumer_auto_scaler=consumer_auto_scaler,
            consumer_extra_config={
                "auto.offset.reset": "latest"
            }
        )

        topic_deployment_config = TopicDeploymentConfig(
            max_instance=self._max_instance,
            topic_id=self._producer_topic,
            project_name=str(get_config("PROJECT_NAME", self, "")),
            service_name=str(get_config("SERVICE_NAME", self, "")),
            image_name=str(get_config("IMAGE_NAME", self, "")),
            zone=str(get_config("ZONE", self, "")),
            service_account=str(get_config("SERVICE_ACCOUNT", self, "")),
            image_env=self._container_env,
            machine_type=self._machine_type,
            provider=self._cloud_provider
        )

        start_producer_callback = functools.partial(
            producer_observer.send_input_output_ratio_pair,
            self._consumer_topic,
            self._producer_topic,
            topic_deployment_config
        )


        producer = ConfluentProducer(
            topic=self._producer_topic,
            kafka_server=self._producer_servers,
            processor=analyzer_processor,
            start_producer_callback=start_producer_callback,
            batch_size=20
        )

        runnable = ParallelRunnable(
            consumer,
            producer,
            None,
            total_producer=5
        )

        return ParallelAnalyzerJobService(runnable, producer_observer)

