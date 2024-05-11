import asyncio
import firebase_admin
from firebase_admin import credentials
from client.notif_client import NotificationClient
from client.consumer import NotificationConsumer
import os
import json
from master.component import MasterComponent

DEVICE_TOKENS = os.environ.get("DEVICE_TOKENS", "").split(",")

if __name__ == "__main__":
    # get key from env variable
    # key = json.loads(os.environ.get("FIREBASE_KEY"))

    # cred = credentials.Certificate(key)
    # firebase_admin.initialize_app(cred)

    master_component = MasterComponent()
    producer_observer = master_component.producer_observer()

    client = NotificationClient(device_tokens=DEVICE_TOKENS)
    consumer = NotificationConsumer(notification_client=client, producer_observer=producer_observer)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(consumer.run())
    