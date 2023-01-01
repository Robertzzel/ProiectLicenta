import threading

import confluent_kafka as kafka
from confluent_kafka.admin import AdminClient
from math import ceil
from typing import List
import time


class KafkaProducerWrapper(kafka.Producer):
    def __init__(self, config):
        super().__init__(config)
        self.maxSingleMessageSize = 500_000

    def sendBigMessage(self, topic: str, value=None, headers=None, key=None, partition=0, timestamp_ms=None):
        numberOfMessages = ceil(len(value) / self.maxSingleMessageSize)
        numberOfMessagesHeader = ("number-of-messages", str(numberOfMessages).zfill(5).encode())
        if headers is None:
            headers = [numberOfMessagesHeader]
        else:
            headers.append(numberOfMessagesHeader)

        for i in range(numberOfMessages):
            start = i * self.maxSingleMessageSize
            end = min((i + 1) * self.maxSingleMessageSize, len(value))
            self.produce(topic=topic, value=value[start: end],
                         headers=headers + [("message-number", str(i).zfill(5).encode())], key=key, partition=partition)


class KafkaConsumerWrapper(kafka.Consumer):
    def __init__(self, config, topics: List[str]):
        super().__init__(config)
        self.subscribe(topics)

    def __next__(self):
        while True:
            msg = self.poll(1)

            if msg is None:
                continue
            if msg.error():
                raise Exception(f"Consumer error: {msg.error()}")

            return msg

    def receiveBigMessage(self, interMessageTimeoutSeconds: int = None):
        message = self.consumeMessage(interMessageTimeoutSeconds)
        headersToBeReturned: List[str, bytes] = list(
            filter(lambda h: h[0] not in ("number-of-messages", "message-number"), message.headers()))
        numberOfMessages: int = 0
        currentMessageNumber: int = 0

        for header in message.headers():
            if header[0] == "number-of-messages":
                numberOfMessages = int(header[1].decode())
            elif header[0] == "message-number":
                currentMessageNumber = int(header[1].decode())

        messages: List[bytes] = [b""] * numberOfMessages
        messages[currentMessageNumber] = message.value()
        for i in range(numberOfMessages - 1):
            message = self.consumeMessage(interMessageTimeoutSeconds)
            for header in message.headers():
                if header[0] == "message-number":
                    currentMessageNumber = int(header[1].decode())
            messages[currentMessageNumber] = message.value()

        finalMessage = b""
        for message in messages:
            finalMessage += message

        return finalMessage, headersToBeReturned

    def consumeMessage(self, timeoutSeconds: int = None):
        if timeoutSeconds is None:
            while True:
                msg = self.poll(0.5)

                if msg is None:
                    continue
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")

                return msg
        else:
            s = time.time() + timeoutSeconds
            while time.time() < s:
                msg = self.poll(0.5)

                if msg is None:
                    continue
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")

                return msg
            raise StopIteration()


def createTopic(brokerAddress: str, topic: str, partitions: int = 1):
    adminClient = AdminClient({"bootstrap.servers": brokerAddress})
    s = adminClient.create_topics(new_topics=[kafka.admin.NewTopic(topic, partitions, 1)])
    while not s[topic].done():
        time.sleep(0.01)


def deleteTopic(brokerAddress: str, topic: str):
    adminClient = AdminClient({"bootstrap.servers": brokerAddress})
    s = adminClient.delete_topics([topic])
    while not s[topic].done():
        time.sleep(0.01)


def checkKafkaActive(brokerAddress: str):
    import kafka
    kafka.KafkaConsumer(bootstrap_servers=[brokerAddress])

def receive(consumer: KafkaConsumerWrapper):
    msg = consumer.receiveBigMessage()
    print(msg)


if __name__ == "__main__":
    consumer = KafkaConsumerWrapper({
        'bootstrap.servers': "localhost:9092",
        'group.id': "test12301",
        'auto.offset.reset': 'latest',
        'allow.auto.create.topics': "true",
    }, ["test"])
    producer = KafkaProducerWrapper({'bootstrap.servers': "localhost:9092"})
    t = threading.Thread(target=receive, args=(consumer,))
    t.start()
    time.sleep(1)
    producer.sendBigMessage(topic="test", value=b"10" * 2_000_000)

    t.join()
