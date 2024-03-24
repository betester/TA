import asyncio
import firebase_admin
from firebase_admin import credentials
from client.notif_client import NotificationClient
from client.consumer import NotificationConsumer

DEVICE_TOKENS = [""]

if __name__ == "__main__":
    cred = credentials.Certificate("./firebase-key.json")
    firebase_admin.initialize_app(cred)

    client = NotificationClient(device_tokens=DEVICE_TOKENS)
    consumer = NotificationConsumer(notification_client=client)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(consumer.run())