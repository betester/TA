from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import os

from master.component import MasterComponent
from confluent_kafka import Producer
from model.crawler_contract import CrawlerResponse

app = Flask(__name__)

component = MasterComponent()
producer_observer = component.producer_observer()

_producer_conf = {
    'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVERS", ""),
    'client.id': os.environ.get("CLIENT_ID", ""),
}

producer = Producer(**_producer_conf)


CHANNEL_SECRET = os.environ.get("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("CHANNEL_ACCESS_TOKEN", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)


@app.route("/", methods=["GET", "POST"])
def hello():
    return "Hello World!"

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    text = event.message.text

    producer.send(topic="analyzer", data=CrawlerResponse(message = text, source="MOKE").model_dump_json().encode())
    producer_observer.send_total_successful_messages(
        "analyzer",
        1,
        lambda x, y: producer.produce(
            topic=x,
            value=y
        )
    )
    reply = f"You said: {text}"
    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=True, port=port, host='0.0.0.0')