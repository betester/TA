from firebase_admin import messaging
from fogverse.fogverse_logging import get_logger

class NotificationClient:
    def __init__(self, device_tokens: str):
        self.__device_tokens = device_tokens
        self.__log = get_logger(name=self.__class__.__name__)
        self.__log.info(f"NotificationClient initialized with device tokens: {self.__device_tokens}")

    def send_notification(self, message: str):
        try:
            for token in self.__device_tokens:
                messaging.send(messaging.Message(notification=messaging.Notification(body=message), token=token))
                self.__log.info("Notification sent successfully")
        except Exception as e:
            self.__log.error("Error sending notification:", e)
