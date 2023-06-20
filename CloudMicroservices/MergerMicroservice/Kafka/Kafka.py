from __future__ import annotations
import enum
import os.path
from typing import *
import confluent_kafka as kafka
from math import ceil
from typing import List
import time

from confluent_kafka.admin import AdminClient


class Partitions(enum.Enum):
    VideoMicroservice: int = 0
    AggregatorMicroservice: int = 1
    AudioMicroservice: int = 2
    Client: int = 3
    Input: int = 4
    FileTransferReceiveFile: int = 5
    FileTransferReceiveConfirmation: int = 6
    ClientDatabase: int = 7
    MergerMicroservice: int = 8


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
    def __init__(self, config, certificatePath: str):
        config.update({
            'security.protocol': 'SSL',
            'ssl.ca.location': certificatePath,
            'ssl.endpoint.identification.algorithm': "none",
        })
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
            try:
                self.produce(topic=topic, value=value[start: end],
                             headers=headers + [("message-number", str(i).zfill(5).encode())], key=key,
                             partition=partition)
            except BufferError:
                self.flush()
                self.produce(topic=topic, value=value[start: end],
                             headers=headers + [("message-number", str(i).zfill(5).encode())], key=key,
                             partition=partition)

    def sendFile(self, topic: str, fileName: str, headers=None, key=None, partition=0):
        fileSize = os.path.getsize(fileName)
        numberOfMessages = ceil(fileSize / self.maxSingleMessageSize)
        numberOfMessagesHeader = ("number-of-messages", str(numberOfMessages).zfill(5).encode())
        if headers is None:
            headers = [numberOfMessagesHeader]
        else:
            headers.append(numberOfMessagesHeader)

        with open(fileName, "rb") as f:
            for i in range(numberOfMessages):
                buff = f.read(self.maxSingleMessageSize)
                try:
                    self.produce(topic=topic, value=buff,
                                 headers=headers + [("message-number", str(i).zfill(5).encode())], key=key,
                                 partition=partition)
                except BufferError:
                    self.flush()
                    self.produce(topic=topic, value=buff,
                                 headers=headers + [("message-number", str(i).zfill(5).encode())], key=key,
                                 partition=partition)


class KafkaConsumerWrapper(kafka.Consumer):
    def __init__(self, config: Dict, topics: List[Tuple[str, int]], certificatePath: str):
        config.update({
            'security.protocol': 'SSL',
            'ssl.ca.location': certificatePath,
            'ssl.endpoint.identification.algorithm': "none",
        })
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

    def receiveBigMessage(self, timeoutSeconds: float = None, partition=0) -> Optional[
        kafka.Message | CustomKafkaMessage]:
        endTime = None if timeoutSeconds is None else time.time() + timeoutSeconds

        message = self.consumeMessage(None if endTime is None else endTime - time.time(), partition=partition)
        if message is None:
            return None

        for header in message.headers():
            if header == ("status", b"FAILED"):
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
            message = self.consumeMessage(None if endTime is None else endTime - time.time(), partition=partition)
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


def deleteTopic(brokerAddress: str, topic: str, certificatePath: str):
    configs = {
        "bootstrap.servers": brokerAddress,
        'security.protocol': 'SSL',
        'ssl.ca.location': certificatePath,
        'ssl.endpoint.identification.algorithm': "none",
    }
    adminClient = AdminClient(configs)
    s = adminClient.delete_topics([topic])
    while not s[topic].done():
        time.sleep(0.01)
