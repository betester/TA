from confluent_kafka import TopicCollection, admin
from fogverse.fogverse_logging import get_logger
from master.contract import ConsumerMaster, AddConsumerRequest


class ConsumerAutoScaler(ConsumerMaster):
    
    def __init__(self, admin_client: admin.AdminClient, initial_partition: int):
        #TODO: initialize group id on initialization
        """
        Initializes a new instance of the class.

        Args:
            admin_client (admin.AdminClient): An instance of confluent kafka admin.AdminClient for administrative operations.
            initial_partition (int): initial partition value for topic that has not been created yet.
        """
        self._admin = admin_client
        self._logger = get_logger(name=self.__class__.__name__)
        self._initial_partition = initial_partition
        # safe the total consumer based on the group id -> topic_id
        self._topics_total_consumer : dict[str, dict[str, int]] = {}
    

    def add_consumer(self, request: AddConsumerRequest) -> bool:
        group_topic_total_consumer = self._topics_total_consumer.get(request.group_id , {})
        topic_total_consumer = group_topic_total_consumer.get(request.topic_id, 0)
        
        topic_does_not_exist = topic_total_consumer == 0

        if topic_does_not_exist:
            self._create_topic(
                request.topic_id,
                number_of_partition=self._initial_partition
            )

        topic_total_partiton = self._get_topic_total_partitions(request.topic_id)

        partition_is_enough = topic_total_partiton > topic_total_consumer 

        if not partition_is_enough:
            partition_is_enough = self._add_partition_on(request.topic_id, topic_total_consumer)

        if partition_is_enough:
            group_topic_total_consumer[request.topic_id] = topic_total_consumer + 1 
            self._topics_total_consumer[request.group_id] = group_topic_total_consumer

        return partition_is_enough

    
    def _create_topic(self, topic_id: str, number_of_partition: int):
        new_topic = admin.NewTopic(
            topic_id,
            num_partitions=number_of_partition
        )

        create_topic_future = self._admin.create_topics([new_topic])[new_topic]
        # blocks the process until the topic is created
        create_topic_future.result()

    def _add_partition_on(self, topic_id: str, total_partition: int) -> bool:
        new_partition = admin.NewPartitions(
            topic_id, 
            total_partition
        )
        
        create_partition_future = self._admin.create_partitions(new_partition)[topic_id]
        # blocks the process until the partition is created
        create_partition_future.result()
        
        return True

    def _get_topic_total_partitions(self, topic_id: str) -> int:
        
        topic_description_future = self._admin.describe_topics(TopicCollection([topic_id]))

        topic_description: admin.TopicDescription = topic_description_future[topic_id].result()

        if not topic_description or not topic_description.partitions:
            return 0
        
        return len(topic_description.partitions)

