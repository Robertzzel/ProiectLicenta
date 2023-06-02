from dataclasses import dataclass
from Client.Kafka.Kafka import *
import uuid

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
        createTopic(address, self.topic, partitions=9)

        self.address = address
        self.partition = Partitions.Client.value
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.address})

        consumerConfigs['bootstrap.servers'] = self.address
        consumerConfigs['group.id'] = "-"
        self.consumer = KafkaConsumerWrapper(consumerConfigs, [(self.topic, self.partition)])
        self.consumerConfigs = consumerConfigs

    def databaseCall(self, operation: str, message: bytes, timeoutSeconds, username: str = None,
                     password: str = None, sessionId: str = None) -> kafka.Message:
        self.seekToEnd()
        headers = [
            ("topic", self.topic.encode()),
            ("partition", str(self.partition).encode()),
            ("operation", operation.encode()),
        ]
        if sessionId is not None:
            headers.append(("sessionId", str(sessionId).encode()))

        if operation not in ("LOGIN", "REGISTER") and username is not None and password is not None:
            headers.append(("Name", username))
            headers.append(("Password", password))

        self.producer.produce(topic=DATABASE_TOPIC, headers=headers, value=message)
        self.producer.flush(timeout=1)

        return self.consumer.receiveBigMessage(timeoutSeconds)

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
