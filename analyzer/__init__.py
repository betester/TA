from fogverse import Consumer, Producer


class TestAnalyzer(Consumer, Producer):
        
    def __init__(self):
        self.consumer_topic =  "testing_topic"
        self.consumer_servers = "localhost:9092"
        self.producer_topic = "client"
        self.producer_servers = "localhost:9092"
        Producer.__init__(self)
        Consumer.__init__(self)
    
    
    async def process(self, data):
        return data[::-1]
