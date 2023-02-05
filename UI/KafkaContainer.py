from dataclasses import dataclass

import Kafka.partitions
from Kafka.Kafka import *
import uuid

DATABASE_TOPIC = "DATABASE"


@dataclass
class KafkaContainer:
    address: str
    producer: KafkaProducerWrapper
    consumer: KafkaConsumerWrapper

    def __init__(self, address: str, consumerConfigs: Dict, consumerTopic: str):
        if not KafkaContainer.checkBrokerExists(address):
            raise Exception("Broker does not exists")

        createTopic(address, consumerTopic, partitions=7)

        self.address = address
        self.topic = consumerTopic
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.address})

        consumerConfigs['bootstrap.servers'] = self.address
        consumerConfigs['group.id'] = "-"
        self.consumer = KafkaConsumerWrapper(consumerConfigs, [(consumerTopic, Kafka.partitions.ClientPartition)])

    def databaseCall(self, topic: str, operation: str, message: bytes, bigFile=False, timeoutSeconds: float = None) -> kafka.Message:
        self.producer.produce(topic=DATABASE_TOPIC, headers=[
            ("topic", topic.encode()), ("partition", str(Kafka.partitions.ClientPartition).encode()), ("operation", operation.encode()),
        ], value=message)

        return self.consumer.receiveBigMessage(timeoutSeconds, partition=Kafka.partitions.ClientPartition) if bigFile else self.consumer.consumeMessage(timeoutSeconds, partition=Kafka.partitions.ClientPartition)

    @staticmethod
    def checkBrokerExists(address) -> bool:
        return checkKafkaActive(brokerAddress=address)

    @staticmethod
    def getStatusFromMessage(message: kafka.Message) -> Optional[str]:
        status = None

        for header in message.headers():
            if header[0] == "status":
                status = header[1].decode()

        return status

    def __del__(self):
        deleteTopic(self.address, self.topic)