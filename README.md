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

Before using it, you need to set up a few environment variables.

- **KAFKA_ADMIN_HOST**
  - *Description*:  The host of kafka endpoint. 
  - *Default Value*: localhost

- **SLEEP_TIME (optional)**
  - *Description*: Defines the delay for retrying partition creation.

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

```py
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

You need to inject the following variables into your environment variables.

- **OBSERVER_CONSUMER_TOPIC**
  - *Description*: Defines the topic that the master will listen to.
  - *Default Value*: observer

- **OBSERVER_CONSUMER_SERVERS**
  - *Description*: The Kafka server that the master will use.

- **OBSERVER_CONSUMER_GROUP_ID (optional)**
  - *Description*: The consumer group ID for the Kafka consumer.

- **DEPLOY_DELAY (optional)**
  - *Description*: Sets up a delay in seconds after a machine has deployed.
  - *Default Value*: 60 seconds

- **Z_VALUE (optional)**
  - *Description*: Used to detect whether the low ratio is the effect of a spike.
  - *Default Value*: 3

- **WINDOW_MAX_SECOND (optional)**
  - *Description*: Sets the duration in seconds for statistic count on the master.
  - *Default Value*: 300 seconds (5 minutes)

- **INPUT_OUTPUT_RATIO_THRESHOLD (optional)**
  - *Description*: Sets the ratio threshold between input and output throughput.
  - *Default Value*: 0.8 (between 0 and 1)

After injecting the environment variables, you can use it as follows

```py
master_component = MasterComponent()
event_handler = master_component.master_event_handler()
await event_handler.run()
```

### Using Master

If you want to customize your own master implementation, for example exclude or include your own implementation of worker on the master, you can create your own master class.

```py
master = Master(
            consumer_topic=consumer_topic,
            consumer_group_id=consumer_group_id,
            consumer_servers=consumer_servers,
            observers=workers
        )

await master.run()
```

You don't have to inject all of the environment above from the master component, instead you define your own environment variables. The important part is the implementation of workers, the workers need to implement the interface `MasterObserver`.

### Sending Out Information to Master

> 🚧 Prerequisite
> 
> You have to make sure that you already have master running on the background 
> otherwise the data will not be received.

Each node, especially producer node need to send out the data to  master for profilling purposes. To do that, you can use the `ProducerObserver` class. You can use it as follows,

```py
master_component = MasterComponent()
producer_observer = master_component.producer_observer()

## your producer class
producer = Producer(producer_observer)
```

You also need to inject environment variables as well with the following arguments.

- **OBSERVER_TOPIC**
  - *Description*: Defines the topic that the master will listen to.
  - *Default Value*: observer

Then, inside the producer class, you can use it on either `_start` or `send` method. 

On `_start` method, you need to call `send_input_output_ratio_pair` function. This will send data to master. As of current research, it is mainly used for deployment but you can instead override it to something else.

One usage example:

```py
await self._observer.send_input_output_ratio_pair(
    source_topic=self.consumer_topic,
    target_topic=self.producer_topic,
    topic_configs=self._topic_deployment_config,
    send = lambda topic, converted_value: self.producer.send(topic=x, value=converted_value)
)
```

On `send` method you need to tell how much message has been consumed by calling `send_total_successful_messages`. Here is how you would use it,

```py
result = await super().send(data, topic, key, headers, callback)
await self._observer.send_total_successful_messages(
    target_topic=self.producer_topic,
    send = lambda x, y: self.producer.send(topic=x, value=y),
    total_messages = 1
)
return result
```

## Components of The System
### Discord Bot
### Line Bot
### Analyzer
### Client
### Crawler