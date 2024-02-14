
from fogverse import Producer, Consumer
from fogverse.fogverse_logging import get_logger
from master.contract import AddConsumerResponse, ConsumerMaster, AddConsumerRequest


class ConsumerAutoScalerEventHandler(Producer, Consumer):

    def __init__(self, 
                 producer_topic: str, 
                 producer_servers: list[str] | str, 
                 consumer_topic: str, 
                 consumer_servers: str,
                 consumer_group_id: str,
                 auto_scaler: ConsumerMaster
                 ):

        self.consumer_topic =  consumer_topic
        self.consumer_servers = consumer_servers
        self.producer_topic = producer_topic 
        self.producer_servers = producer_servers
        self._auto_scaler = auto_scaler 
        self.group_id = consumer_group_id
        self.__log = get_logger(name=self.__class__.__name__)
        self.auto_decode = False

        Producer.__init__(self)
        Consumer.__init__(self)

        self._closed = False
    
    def decode(self, data: bytes) -> AddConsumerRequest:
        return AddConsumerRequest.model_validate_json(data)
    
    async def process(self, data: AddConsumerRequest) -> AddConsumerResponse:
        try:
            consumer_can_consume = self._auto_scaler.add_consumer(data)
            return AddConsumerResponse(
                consumer_can_consume=consumer_can_consume
            )
        except Exception as e:
            self.__log.error(e)
            return AddConsumerResponse(
                consumer_can_consume=False
            )
    
    def encode(self, data: AddConsumerResponse) -> bytes:
        return data.model_dump_json().encode()
