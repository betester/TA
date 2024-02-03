import csv
from random import random

from aiokafka.client import asyncio

from fogverse import Producer


class TestCrawler(Producer):
        
    def __init__(self):

        self.file = open('./crawler/data/train.csv')
        self.csv_reader = csv.reader(self.file)
        self.header = next(self.csv_reader)
        self.producer_topic = "testing_topic"
        self.producer_servers = "localhost:9092"
        Producer.__init__(self)

    async def receive(self):
        try:
            random_wait =  random() * 2
            await asyncio.sleep(random_wait)
            data = next(self.csv_reader)
            print(data)
            return data
        except StopAsyncIteration:
            self.csv_reader = csv.reader(self.file)
            next(self.csv_reader)
            return next(self.csv_reader)

    async def process(self, data):
        return data[3]
