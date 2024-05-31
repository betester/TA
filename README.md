## Final Project: Auto Scaling, Dynamic Partition, and Multiprocessing Implementation on FogVerse: Study Case of Real-Time Social Media Data Processing for Emergency Event Detection


This thesis uses FogVerse library as it's basis for communication with Kafka. FogVerse can be used for multiple purposes
such as adding consumer, producer, profilling, and so on. This thesis aims to include several improvements on the library 
as well as the way of using the new feature(s) for a certain use case.

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

## Use Case Implementation using FogVerse
The system built using FogVerse is structured into several components, each of which serves a specific purpose. Each component is built using Python, except `Receiver`. Below, you'll find detailed information about each component and instructions on how to run them.

### Discord Bot
This module contains the implementation of a Discord bot that interacts with users on a Discord server. The bot component is responsible for receiving messages from users and sending them to the `Analyzer` component. It is mainly built using the `discord.py` library.

Before starting the Discord bot, you need to set up the following environment variables:

- **TOKEN**
  - *Description*: The token of the Discord bot.
  - *Default Value*: None

- **DISCORD_ANALYZER_SERVERS**
  - *Description*: The Kafka server that the Discord bot will use.
  - *Default Value*: localhost:9092

- **DISCORD_PRODUCER_TOPIC**
  - *Description*: The topic that the Discord bot will produce messages to.
  - *Default Value*: analyze

- **DISCORD_CONSUMER_GROUP_ID**
  - *Description*: The consumer group ID for the Kafka consumer.
  - *Default Value*: crawler

You can get the **Discord token** by creating a new bot on the Discord Developer Portal.

To start the Discord bot, 
use the following command on the terminal:

```bash
python -m discord_bot
```

### LINE Bot

This module contains the implementation of a LINE bot that interacts with users on a LINE chat. The bot component is responsible for receiving messages from users and sending them to the `Analyzer` component. It is mainly built using the `line-bot-sdk` and `Flask` library. LINE Developer Portal only accepts HTTPS endpoints so you will need a working server with SSL certificate to run the LINE bot.

Before starting the LINE bot, you need to set up the following environment variables:

- **CHANNEL_SECRET**
  - *Description*: The channel secret of the LINE bot.
  - *Default Value*: None
- **CHANNEL_ACCESS_TOKEN**
  - *Description*: The channel access token of the LINE bot.
  - *Default Value*: None
- **BOOTSTRAP_SERVERS**
  - *Description*: The Kafka server that the LINE bot will use.
  - *Default Value*: localhost:9092
- **MOKE_PRODUCER_TOPIC**
  - *Description*: The topic that the LINE bot will produce messages to.
  - *Default Value*: analyze
- **CLIENT_ID**
  - *Description*: The client ID for the LINE bot.
  - *Default Value*: None
- **PORT**
  - *Description*: The port number for the LINE bot.
  - *Default Value*: 8080

You can get the **channel secret** and **channel access token** by creating a new bot on the LINE Developer Portal.

To start the LINE bot, use the following command on the terminal:

```bash
python -m moke
```

### Crawler

This module contains the implementation of a mock crawler. The crawler component is responsible for supplying mock data and sending it to the `Analyzer` component for flooding the component. It is mainly used for testing purposes.

Before starting the crawler, you need to set up the following environment variables:

- **CRAWLER_ANALYZER_SERVERS**
  - *Description*: The Kafka server that the crawler will use.
  - *Default Value*: localhost:9092
- **CRAWLER_PRODUCER_TOPIC**
  - *Description*: The topic that the crawler will produce messages to.
  - *Default Value*: analyze
- **CRAWLER_CONSUMER_GROUP_ID**
  - *Description*: The consumer group ID for the Kafka consumer.
  - *Default Value*: crawler

There are two ways to use the crawler. You can use some random data from csv file or you can just send same message over and over again. Just comment out the code that you don't want to use.

To start the crawler, use the following command on the terminal:

```bash
python -m crawler
```

### Analyzer

This module contains the implementation of the analyzer component. The analyzer component is responsible for processing the messages received from the `Discord Bot`, `LINE Bot`, and `Crawler` components. It is mainly used for analyzing the messages and detecting emergency events using machine learning models. Currently there are two models that are being used for this research.

Before starting the analyzer, you need to set up the following environment variables:

- **ANALYZER_MODE**
  - *Description*: The mode of the analyzer component.
  - *Default Value*: serial
- **OBSERVER_TOPIC**
  - *Description*: The topic that the analyzer will send throughput data to.
  - *Default Value*: observer
- **MACHINE_TYPE**
  - *Description*: The compute engine machine type for the analyzer.
  - *Default Value*: CPU
- **MAX_INSTANCE**
  - *Description*: The maximum number of instances for the analyzer that can be deployed for auto deploy purposes.
  - *Default Value*: 3
- **CLOUD_PROVIDER**
  - *Description*: The cloud provider for the analyzer.
  - *Default Value*: GOOGLE_CLOUD
- **KEYWORD_CLASSIFIER_MODEL_SOURCE**
  - *Description*: The source of the keyword classifier model.
  - *Default Value*: ./mocking_bird
- **DISASTER_CLASSIFIER_MODEL_SOURCE**
  - *Description*: The source of the disaster classifier model.
  - *Default Value*: ./jay_bird
- **ANALYZER_CONSUMER_GROUP_ID**
  - *Description*: The consumer group ID for the Kafka consumer.
  - *Default Value*: analyzer
- **ANALYZER_CONSUMER_SERVERS**
  - *Description*: The Kafka server that the analyzer will use.
  - *Default Value*: localhost:9092
- **ANALYZER_CONSUMER_TOPIC**
  - *Description*: The topic that the analyzer will consume messages from.
  - *Default Value*: analyze
- **ANALYZER_PRODUCER_SERVERS**
  - *Description*: The Kafka server that the analyzer will produce messages to.
  - *Default Value*: localhost:9092
- **ANALYZER_PRODUCER_TOPIC**
  - *Description*: The topic that the analyzer will produce messages to.
  - *Default Value*: client
- **CLOUD_ZONE** 
  - *Description*: The zone of the cloud provider.
  - *Default Value*: None
- **PROJECT_NAME** 
  - *Description*: The project name of the cloud provider.
  - *Default Value*: None
- **SERVICE_NAME**
  - *Description*: The service name of the analyzer.
  - *Default Value*: None
- **IMAGE_NAME**
  - *Description*: The image name of the analyzer.
  - *Default Value*: None
- **SERVICE_ACCOUNT**
  - *Description*: The service account of the analyzer.
  - *Default Value*: None
- **KAFKA_ADMIN_HOST**
  - *Description*: The host of the Kafka endpoint.
  - *Default Value*: localhost

Before starting the analyzer, you need to run the `Master` component first. The `Master` component is responsible for auto scaling the analyzer component based on the throughput ratio.

Before starting the master, you need to set up the following environment variables:

- **KAFKA_ADMIN_HOST**
  - *Description*: The host of the Kafka endpoint.
  - *Default Value*: localhost
- **SLEEP_TIME**
  - *Description*: The delay for retrying partition creation.
  - *Default Value*: 3
- **MASTER_HOST**
  - *Description*: The host of the master.
  - *Default Value*: localhost
- **MASTER_PORT**
  - *Description*: The port of the master.
  - *Default Value*: 4242
- **OBSERVER_CONSUMER_TOPIC**
  - *Description*: The topic that the master will listen to.
  - *Default Value*: observer
- **OBSERVER_CONSUMER_SERVERS**
  - *Description*: The Kafka server that the master will use.
  - *Default Value*: localhost:9092
- **OBSERVER_CONSUMER_GROUP_ID**
  - *Description*: The consumer group ID for the Kafka consumer.
  - *Default Value*: observer
- **DEPLOY_DELAY**
  - *Description*: Sets up a delay in seconds after a machine has deployed.
  - *Default Value*: 900
- **PROFILLING_TIME_WINDOW**
  - *Description*: Sets the time window in seconds for the throughput ratio.
  - *Default Value*: 1
- **HEARBEART_DEPLOY_DELAY**
  - *Description*: Sets up a delay in seconds after a machine has deployed.
  - *Default Value*: 120
- **Z_VALUE**
  - *Description*: Used to detect whether the low ratio is the effect of a spike.
  - *Default Value*: 2
- **WINDOW_MAX_SECOND**
  - *Description*: Sets the duration in seconds for statistic count on the master.
  - *Default Value*: 300
- **INPUT_OUTPUT_RATIO_THRESHOLD**
  - *Description*: Sets the ratio threshold between input and output throughput.
  - *Default Value*: 0.7
- **INPUT_OUTPUT_REFRESH_RATE**
  - *Description*: Sets the refresh rate in seconds for the throughput ratio.
  - *Default Value*: 60
- **SERVICE_ACCOUNT_KEY**
  - *Description*: The service account key for the cloud provider.
  - *Default Value*: None
- **WORKERS**
  - *Description*: The workers that the master will use.
  - *Default Value*: []

To start the master, use the following command on the terminal:

```bash
python -m master
```

To start the analyzer, there are several use the following command on the terminal:

```bash
python -m analyzer
```

### Client

This module contains the implementation of the client component. The client component is responsible for receiving messages from the `Analyzer` component and sending them to the `Receiver` component. It is mainly used for sending the analyzed messages to the client. `Firebase Cloud Messaging` is used to send the messages as notification to the `Receiver`.

- **CLIENT_CONSUMER_GROUP_ID**
  - *Description*: The consumer group ID for the Kafka consumer.
  - *Default Value*: client
- **CLIENT_CONSUMER_SERVERS**
  - *Description*: The Kafka server that the client will use.
  - *Default Value*: localhost:9092
- **CLIENT_CONSUMER_TOPIC**
  - *Description*: The topic that the client will consume messages from.
  - *Default Value*: client
- **FIREBASE_KEY**
  - *Description*: The Firebase key for the client.
  - *Default Value*: None
- **DEVICE_TOKENS**
  - *Description*: The device tokens for the client.
  - *Default Value*: []

Since the client component is mainly used for sending messages to the client, you need to set up the `Firebase Cloud Messaging` key and device tokens. You can get the `Firebase Cloud Messaging` key by creating a new project on the `Firebase Console`.

**Device tokens** are unique identifiers for each device that you want to send messages to. You can get the **device tokens** by registering the devices on the client side and sending them to the server.

To start the client, use the following command on the terminal:

```bash
python -m client
```

### Receiver

The receiver component is responsible for receiving messages from the `Client` component and displaying them to the user as **push notifications**. It is in the form of mobile application that is built using `Flutter` framework. You can run the receiver component on an Android emulator or a physical device by installing the `APK` file. The notification will be displayed on the device when a message is sent from the `Firebase Cloud Messaging`.

There are several things that you need to set up before running the receiver component:

- **Firebase**
  - *Description*: The Firebase configuration for the receiver. You can use the `google-services.json` file to configure the Firebase project or by using Flutterfire plugin.
  - *Default Value*: None
- **Device Tokens**
  - *Description*: The device tokens for the `Client`. You can get it by using logger from the `Receiver` component.
  - *Default Value*: None

To run the receiver component, you can install the `APK` file on an Android device or emulator. You can also build the receiver component from the source code by running the following command:
  
  ```bash
  flutter build apk
  ```

Or you can just run the receiver component on an Android emulator or physical device by running the following command:

  ```bash
  flutter run
  ```

### Running the System

To run the system, you need to start the components in the following order:

1. Start the `Kafka` server.
2. Start the `Master` component.
3. Start the `Analyzer` component.
4. Start the `Discord Bot`, `LINE Bot`, or `Crawler` components.
5. Start the `Client` component.
6. Start the `Receiver` component.

After starting all the components, you can interact with the system by sending messages to the `Discord Bot`, `LINE Bot`, or by using `Crawler` components. The messages will be analyzed by the `Analyzer` component and sent to the `Client` component. The analyzed messages will be displayed as push notifications on the `Receiver` component.

In this case you can deploy the system on a cloud provider such as `Google Cloud Platform` or `Amazon Web Services`. To deploy the system on a cloud provider, you can use the script on the `scripts` folder. The script will deploy the system on the cloud provider and set up the necessary configurations. You might need the necessary `Dockerfile` files to deploy the system on the cloud provider using the script.

