import kafka
from math import ceil
from typing import List


class KafkaProducerWrapper(kafka.KafkaProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.maxSingleMessageSize = 1_000_000

    def sendBigMessage(self, topic: str, value=None, headers=None, key=None, partition=None, timestamp_ms=None):
        numberOfMessages = ceil(len(value) / self.maxSingleMessageSize)
        numberOfMessagesHeader = ("number-of-messages", str(numberOfMessages).zfill(5).encode())
        if headers is None:
            headers = [numberOfMessagesHeader]
        else:
            headers.append(numberOfMessagesHeader)

        for i in range(numberOfMessages):
            start = i * self.maxSingleMessageSize
            end = min((i+1) * self.maxSingleMessageSize, len(value))
            self.send(topic=topic, value=value[start: end], headers=headers + [("message-number", str(i).zfill(5).encode())], key=key, partition=partition, timestamp_ms=timestamp_ms)


class KafkaConsumerWrapper(kafka.KafkaConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def receiveBigMessage(self):
        message = next(self)
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
            message = next(self)
            for header in message.headers:
                if header[0] == "message-number":
                    currentMessageNumber = int(header[1].decode())
            messages[currentMessageNumber] = message.value

        finalMessage = b""
        for message in messages:
            finalMessage += message

        return finalMessage, headersToBeReturned


def createTopic(brokerAddress:str, topic: str, partitions: int = 1):
    admin_client = kafka.KafkaAdminClient(
        bootstrap_servers=brokerAddress,
        client_id='test'
    )

    topic_list = [kafka.admin.NewTopic(name=topic, num_partitions=partitions, replication_factor=1, topic_configs={ "retention.ms": "100"})]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
