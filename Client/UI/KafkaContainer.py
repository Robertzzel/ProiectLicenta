from dataclasses import dataclass
from Kafka.Kafka import *
import uuid
from Client.UI.Kafka.partitions import ClientPartition

DATABASE_TOPIC = "DATABASE"

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
        self.partition = ClientPartition
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.address})

        consumerConfigs['bootstrap.servers'] = self.address
        consumerConfigs['group.id'] = "-"
        self.consumer = KafkaConsumerWrapper(consumerConfigs, [(self.topic, self.partition)])

    def databaseCall(self, operation: str, message: bytes, timeoutSeconds: float = None, username: str = None, password: str = None) -> kafka.Message:
        self.seekToEnd()
        headers = [
            ("topic", self.topic.encode()),
            ("partition", str(self.partition).encode()),
            ("operation", operation.encode()),
        ]

        if operation not in ("LOGIN", "REGISTER") and username is not None and password is not None:
            headers.append(("Name", username))
            headers.append(("Password", password))

        self.producer.produce(topic=DATABASE_TOPIC, headers=headers, value=message)
        self.producer.flush(timeout=1)

        return self.consumer.receiveBigMessage(timeoutSeconds, partition=self.partition)

    @staticmethod
    def getStatusFromMessage(responseMessage):
        for header in responseMessage.headers():
            if header[0] == "status":
                return header[1].decode()
        return None

    def resetTopic(self):
        self.topic = str(uuid.uuid1())
        createTopic(self.address, self.topic, partitions=7)
        self.consumer = KafkaConsumerWrapper(self.consumerConfigs, [(self.topic, self.partition)])

    @staticmethod
    def checkBrokerExists(address) -> bool:
        return checkKafkaActive(brokerAddress=address)

    def seekToEnd(self):
        self.consumer.seekToEnd(self.topic, self.partition)

    def __del__(self):
        try:
            deleteTopic(self.address, self.topic)
        except:
            pass
