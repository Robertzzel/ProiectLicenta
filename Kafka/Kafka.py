import confluent_kafka as kafka
from confluent_kafka.admin import AdminClient
from math import ceil
from typing import List
import time


class KafkaProducerWrapper(kafka.Producer):
    def __init__(self, config):
        super().__init__(config)
        self.maxSingleMessageSize = 1_000_000

    def sendBigMessage(self, topic: str, value=None, headers=None, key=None, partition=0, timestamp_ms=None):
        numberOfMessages = ceil(len(value) / self.maxSingleMessageSize)
        numberOfMessagesHeader = ("number-of-messages", str(numberOfMessages).zfill(5).encode())
        if headers is None:
            headers = [numberOfMessagesHeader]
        else:
            headers.append(numberOfMessagesHeader)

        for i in range(numberOfMessages):
            start = i * self.maxSingleMessageSize
            end = min((i+1) * self.maxSingleMessageSize, len(value))
            self.produce(topic=topic, value=value[start: end], headers=headers + [("message-number", str(i).zfill(5).encode())], key=key, partition=partition, timestamp_ms=timestamp_ms)


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

    def receiveBigMessage(self, interMessageTimeoutSeconds: int = -1):
        message = self.consumeMessage(interMessageTimeoutSeconds)
        headersToBeReturned: List[str, bytes] = list(filter(lambda h: h[0] not in ("number-of-messages", "message-number"),message.headers))
        numberOfMessages: int = 0
        currentMessageNumber: int = 0

        for header in message.headers:
            if header[0] == "number-of-messages":
                numberOfMessages = int(header[1].decode())
            elif header[0] == "message-number":
                currentMessageNumber = int(header[1].decode())

        messages: List[bytes] = [b""] * numberOfMessages
        messages[currentMessageNumber] = message.value
        for i in range(numberOfMessages - 1):
            message = self.consumeMessage(interMessageTimeoutSeconds)
            for header in message.headers:
                if header[0] == "message-number":
                    currentMessageNumber = int(header[1].decode())
            messages[currentMessageNumber] = message.value

        finalMessage = b""
        for message in messages:
            finalMessage += message

        return finalMessage, headersToBeReturned

    def consumeMessage(self, timeoutSeconds: int = -1):
        s = time.time() + timeoutSeconds
        while time.time() < s:
            msg = self.poll(1)

            if msg is None:
                continue
            if msg.error():
                raise Exception(f"Consumer error: {msg.error()}")

            return msg


def createTopic(brokerAddress: str, topic: str, partitions: int = 1):
    adminClient = AdminClient({"bootstrap.servers": brokerAddress})
    s = adminClient.create_topics(new_topics=[kafka.admin.NewTopic(topic, partitions, 1)])
    while not s[topic].done():
        time.sleep(0.1)


if __name__ == "__main__":
    createTopic("localhost:9092", "sal")
    # p = KafkaProducerWrapper({'bootstrap.servers': 'localhost:9092'})
    # c = KafkaConsumerWrapper({
    #     'bootstrap.servers': 'localhost:9092',
    #     'group.id': 'd',
    #     'auto.offset.reset': 'smallest'
    # }, ["TEST"])
    #
    # p.produce(topic="TEST", value=b"1", callback=lambda x,y: print(x,y))
    # p.flush()
    # msg = c.consumeMessage(1)
    #
    # print(msg.value())
