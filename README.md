## Final Project: Real-time Social Media Data Streaming Platform for Incident Detection with FogVerse 

This thesis uses FogVerse library as it's basis for communication using Kafka. FogVerse can be used for multiple purposes
such as adding consumer, producer, profilling, and so on. This thesis aims to include several improvements on the library 
as well as the way of using the new feature(s).

## Improvements
1. Dynamic Partition
2. Auto Deployment
3. Message Multithreading Consumption 

### Dynamic Partition
Partition in kafka are usually set up statically before using it, we can set it an arbitrary number. Partition on topic limits the amount of consumer that can consume a certain topic. With dynamic partition, you don't need to set up the partition number, you can set it any number and if there is any new consumer consuming a partition, then it will be incremented automatically by the library.

#### Usage

You can use `ConsumerAutoScaler` class from `master` module. This class is a helper class that will automatically increment partition of a topic.


```py
# instantiate consumer auto scaler class

from master.component import MasterComponent

master_component = MasterComponent()
consumer_auto_scaler = master_component.consumer_auto_scaler()

# inject to fog verse Runnable that you wanted to auto scale

executor = Executor(consumer_auto_scaler, *args, **kwargs)
```
on `start` method of `Runnable` class, you need to override it as follows,

```py
    async def _start(self):
        await self._consumer_auto_scaler.async_start(
            consumer=self.consumer,
            consumer_group=self.group_id,
            consumer_topic=self.consumer_topic,
            producer=self.producer,
        )

```

#### Limitation
There is a limitation on the auto scaler, it needs to have `consumer group` with only **1 topic only**. The reason is because Kafka will only tell the amount of consumer on a certain consumer group but does not tell you which topic they are consuming. This can be problematic if we wanted to know which topic that don't have sufficient partition.

Consider the case below.

```
Consumer Group Topics = {A, B}
A partitions = 1 
B partitions = 2

current consumer group member = 2 

```

new consumer wants consume partition A. we don't know if

* All the members are consuming B or not.
* Or 1 in A and 1 in B. 

Therefore the only workaround is to limit for each group to only have 1 topic.

#### Why Not Just use Inheritance?
There is too much of inheritance in FogVerse, it could lead to unexpected behavior.

### Auto Deployment

Auto Deployment allows a node to auto scale based on the IO throughput. If it is less than a certain threshold, then deployment will be invoked. 

### Usage
There are a few components that needed to be done.

1. A Master Node 

You need to have a separate master node which act as the center of deployment. You can either create it your own or use master component.

2. Sending Out Data from Consumer to Master 

You need to send data to master so that master can know the throughput of your topic.

### Using Master Component

If your are using master component, the following arguments are required on your virtual environment.

**OBSERVER_CONSUMER_TOPIC : string**

---
it defines the topic that the master will listen to, by default it will use observer.

**OBSERVER_CONSUMER_SERVERS : string**

---
The kafka server that the master will use.

**OBSERVER_CONSUMER_GROUP_ID (optional) : string**

---

**DEPLOY_DELAY (optional): int** 

---
Once a machine has deployed, it will set up a delay in seconds. By default it will delay for 1 minute.

**Z_VALUE (optional): float**

---
Z value will be used to detect whether the low ratio is the effect of spike, by default the value will be 3.

**WINDOW_MAX_SECOND (optional): int**

---
On the master, there will be statistic count which will collect the data in N seconds. by default it will collect data from the last 5 minutes.

**INPUT_OUTPUT_RATIO_THRESHOLD (optional): float (between 0 and 1)**

---

The ratio between IO throughput by default it will set to 0.8


### Using Master

TODO


### Sending Out Information to Master
TODO