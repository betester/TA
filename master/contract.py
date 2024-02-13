
from abc import ABC, abstractmethod


class Master(ABC):

    @abstractmethod
    def add_new_consumer(self, topic_id, group_id):
        pass
