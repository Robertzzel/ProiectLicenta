from __future__ import annotations

from typing import *
import confluent_kafka as kafka
from confluent_kafka.admin import AdminClient
from math import ceil
from typing import List
import time


class CustomKafkaMessage:
    def __init__(self, value: bytes, headers: List, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.savedValue = value
        self.savedHeaders = headers

    def value(self):
        return self.savedValue

    def headers(self):
        return self.savedHeaders


class KafkaProducerWrapper(kafka.Producer):
    def __init__(self, config):
        super().__init__(config)
        self.maxSingleMessageSize = 500_000

    def sendBigMessage(self, topic: str, value=None, headers=None, key=None, partition=0):
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

    def __next__(self) -> kafka.Message:
        while True:
            msg = self.poll(0.25)

            if msg is None:
                continue
            if msg.error():
                raise Exception(f"Consumer error: {msg.error()}")

            return msg

    def receiveBigMessage(self, timeoutSeconds: float = None) -> Optional[kafka.Message | CustomKafkaMessage]:
        endTime = None if timeoutSeconds is None else time.time() + timeoutSeconds

        message = self.consumeMessage(None if endTime is None else endTime - time.time())
        if message is None:
            return None

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
            message = self.consumeMessage(None if endTime is None else endTime - time.time())
            if message is None:
                return None

            for header in message.headers():
                if header[0] == "message-number":
                    currentMessageNumber = int(header[1].decode())
            messages[currentMessageNumber] = message.value()

        finalMessage = b""
        for message in messages:
            finalMessage += message

        return CustomKafkaMessage(value=finalMessage, headers=headersToBeReturned)

    def consumeMessage(self, timeoutSeconds: int = None) -> Optional[kafka.Message]:
        if timeoutSeconds is None:
            while True:
                msg = self.poll(0.25)

                if msg is None:
                    continue
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")

                return msg

        s = time.time() + timeoutSeconds
        while time.time() < s:
            msg = self.poll(0.5)

            if msg is None:
                continue
            if msg.error():
                raise Exception(f"Consumer error: {msg.error()}")

            return msg
        return None


def createTopic(brokerAddress: str, topic: str, partitions: int = 1, timeoutSeconds: float = 5):
    adminClient = AdminClient({"bootstrap.servers": brokerAddress})
    s = adminClient.create_topics(new_topics=[kafka.admin.NewTopic(topic, partitions, 1)])

    endTime = time.time() + timeoutSeconds
    while not s[topic].done() and endTime > time.time():
        time.sleep(0.02)

    if not s[topic].done():
        raise Exception(f"Cannot create topic {topic}")


def deleteTopic(brokerAddress: str, topic: str):
    adminClient = AdminClient({"bootstrap.servers": brokerAddress})
    s = adminClient.delete_topics([topic])
    while not s[topic].done():
        time.sleep(0.01)


def checkKafkaActive(brokerAddress: str) -> bool:
    import kafka
    try:
        kafka.KafkaConsumer(bootstrap_servers=[brokerAddress])
    except BaseException:
        return False
    return True
