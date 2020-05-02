import json
import time

from kafka import KafkaConsumer

TIMEOUT_MS = 1000

class ConsumerServer():

    def __init__(self, topic, **kwargs):
        self.consumer = KafkaConsumer(topic, **kwargs)
        self.topic = topic

    def consume_data(self):
        while True:
            topic_messages = self.consumer.poll(TIMEOUT_MS)
            for message in topic_messages.get(self.topic, []):
                print(f"received message {json.loads(message)}")
            time.sleep(1)