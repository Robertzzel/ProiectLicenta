from __future__ import annotations

import enum
from typing import *
import confluent_kafka as kafka
from confluent_kafka.admin import AdminClient
from math import ceil
from typing import List
import time


class Partitions(enum.Enum):
    VideoMicroservice: int = 0
    AggregatorMicroservice: int = 1
    AggregatorMicroserviceStart: int = 5
    AudioMicroservice: int = 2
    Client: int = 3
    MergerMicroservice: int = 4
    Input: int = 6


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
    def __init__(self, config: Dict, topics: List[Tuple[str, int]]):
        super().__init__(config)
        self.assign([kafka.TopicPartition(topic=pair[0], partition=pair[1]) for pair in topics])

    def __next__(self) -> kafka.Message:
        while True:
            msg = self.poll(0.25)

            if msg is None:
                continue
            if msg.error():
                raise Exception(f"Consumer error: {msg.error()}")

            return msg

    def seekToEnd(self, topic: str, partition: int):
        tp = kafka.TopicPartition(topic, partition)
        _, high = self.get_watermark_offsets(tp)
        self.seek(kafka.TopicPartition(topic, partition, high))

    def receiveBigMessage(self, timeoutSeconds: float = None, partition = 0) -> Optional[kafka.Message | CustomKafkaMessage| str]:
        endTime = None if timeoutSeconds is None else time.time() + timeoutSeconds

        message = self.consumeMessage(None if endTime is None else endTime - time.time(), partition=partition)
        if message is None:
            return None

        for header in message.headers():
            if header == ("status", b"FAILED"):
                return message

        headersToBeReturned: List[str, bytes] = list(filter(lambda h: h[0] not in ("number-of-messages", "message-number"), message.headers()))
        numberOfMessages: int = 0
        currentMessageNumber: int = 0

        msgNrIndex = None
        for i,header in enumerate(message.headers()):
            if header[0] == "number-of-messages":
                numberOfMessages = int(header[1].decode())
            elif header[0] == "message-number":
                currentMessageNumber = int(header[1].decode())
                msgNrIndex = i

        messages: List[bytes] = [0] * numberOfMessages
        messages[currentMessageNumber] = message.value()
        for i in range(numberOfMessages - 1):
            message = self.consumeMessage(None if endTime is None else endTime - time.time(), partition=partition)
            if message is None:
                return None

            currentMessageNumber = int(message.headers()[msgNrIndex][1].decode())
            messages[currentMessageNumber] = message.value()

        return CustomKafkaMessage(value=b"".join(messages), headers=headersToBeReturned)

    def consumeMessage(self, timeoutSeconds: int = None, partition=0) -> Optional[kafka.Message]:
        if timeoutSeconds is None:
            while True:
                msg = self.poll(0.2)

                if msg is None or msg.partition() != partition:
                    continue
                if msg.error():
                    raise Exception(f"Consumer error: {msg.error()}")
                if msg.partition() is None:
                    raise Exception(f"partition does not exist")
                if msg.partition() != partition:
                    continue
                return msg

        s = time.time() + timeoutSeconds
        while time.time() < s:
            msg = self.poll(0.2)

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
    import kafka as kafka_python
    try:
        kafka_python.KafkaConsumer(bootstrap_servers=[brokerAddress])
    except BaseException:
        return False
    return True
