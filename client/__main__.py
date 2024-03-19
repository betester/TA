import asyncio
import firebase_admin
from firebase_admin import credentials
from client.notif_client import NotificationClient
from client.consumer import NotificationConsumer

DEVICE_TOKENS = ["fHXdiqWuSme_XUvD_HbeSS:APA91bFeMSrPfJBjLJmEU4BVXRylhv21AFp3Swb7oOxZ2nlyH1vOEuz3A5npMgtAab2d9XtlknbldGL8IP-cEdtP9TWxlCzXf9JHERnQ52LgzmT-PFWitExvEECDluB3IHFbd7RAAXYm"]

if __name__ == "__main__":
    cred = credentials.Certificate("./firebase-key.json")
    firebase_admin.initialize_app(cred)

    client = NotificationClient(device_tokens=DEVICE_TOKENS)
    consumer = NotificationConsumer(notification_client=client)
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(consumer.run())