from dataclasses import dataclass
import Kafka.partitions
from Kafka.Kafka import *
DATABASE_TOPIC = "DATABASE"
import uuid


@dataclass
class KafkaContainer:
    address: str
    producer: KafkaProducerWrapper
    consumer: KafkaConsumerWrapper

    def __init__(self, address: str, consumerConfigs: Dict):
        if not KafkaContainer.checkBrokerExists(address):
            raise Exception("Broker does not exists")

        self.topic = str(uuid.uuid1())
        createTopic(address, self.topic, partitions=7)

        self.consumerConfigs = consumerConfigs
        self.address = address
        self.partition = Kafka.partitions.ClientPartition
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.address})

        consumerConfigs['bootstrap.servers'] = self.address
        consumerConfigs['group.id'] = "-"
        self.consumer = KafkaConsumerWrapper(consumerConfigs, [(self.topic, self.partition)])

    def databaseCall(self, operation: str, message: bytes, timeoutSeconds: float = None) -> kafka.Message:
        self.seekToEnd()
        self.producer.produce(topic=DATABASE_TOPIC, headers=[
            ("topic", self.topic.encode()), ("partition", str(self.partition).encode()), ("operation", operation.encode()),
        ], value=message)
        self.producer.flush(timeout=1)

        return self.consumer.receiveBigMessage(timeoutSeconds, partition=self.partition)

    def resetTopic(self):
        self.topic = str(uuid.uuid1())
        createTopic(self.address, self.topic, partitions=7)
        self.consumer = KafkaConsumerWrapper(self.consumerConfigs, [(self.topic, self.partition)])

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

    def seekToEnd(self):
        self.consumer.seekToEnd(self.topic, self.partition)

    def __del__(self):
        try:
            deleteTopic(self.address, self.topic)
        except:
            pass
