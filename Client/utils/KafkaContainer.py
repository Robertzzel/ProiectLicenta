import pathlib
from dataclasses import dataclass
from Client.Kafka.Kafka import *
import uuid

DATABASE_TOPIC = "DATABASE"


@dataclass
class KafkaContainer:
    address: str
    producer: KafkaProducerWrapper
    consumerAggregator: KafkaConsumerWrapper
    consumerDatabase: KafkaConsumerWrapper

    def __init__(self, address: str, consumerConfigs: Dict):
        consumerConfigs.update({
            'bootstrap.servers': address,
            'group.id': "-"
        })
        if not KafkaContainer.checkBrokerExists(address):
            raise Exception("Broker does not exists")

        self.topic = str(uuid.uuid1())
        self.address = address
        self.truststorePath = str(pathlib.Path(__file__).parent.parent / "truststore.pem")

        createTopic(address, self.topic, partitions=9)

        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.address}, certificatePath=self.truststorePath)

        self.clientPartition = Partitions.Client.value
        self.clientDatabasePartition = Partitions.ClientDatabase.value
        self.consumerAggregator = KafkaConsumerWrapper(consumerConfigs, [(self.topic, self.clientPartition)], certificatePath=self.truststorePath)
        self.consumerDatabase = KafkaConsumerWrapper(consumerConfigs, [(self.topic, self.clientDatabasePartition)], certificatePath=self.truststorePath)
        self.consumerConfigs = consumerConfigs

    def databaseCall(self, operation: str, message: bytes, timeoutSeconds, username: str = None,
                     password: str = None, sessionId: str = None) -> kafka.Message:
        self.seekToEnd()
        headers = [
            ("topic", self.topic.encode()),
            ("partition", str(self.clientDatabasePartition).encode()),
            ("operation", operation.encode()),
        ]
        if sessionId is not None:
            headers.append(("sessionId", str(sessionId).encode()))
        if operation not in ("LOGIN", "REGISTER") and username is not None and password is not None:
            headers.append(("Name", username))
            headers.append(("Password", password))

        self.producer.produce(topic=DATABASE_TOPIC, headers=headers, value=message)
        self.producer.flush(timeout=1)

        return self.consumerDatabase.receiveBigMessage(timeoutSeconds)

    @staticmethod
    def getStatusFromMessage(responseMessage):
        for header in responseMessage.headers():
            if header[0] == "status":
                return header[1].decode()
        return None

    def resetTopic(self):
        self.topic = str(uuid.uuid1())
        createTopic(self.address, self.topic, partitions=9)
        self.consumerAggregator = KafkaConsumerWrapper(self.consumerConfigs, [(self.topic, self.clientPartition)], certificatePath=self.truststorePath)
        self.consumerDatabase = KafkaConsumerWrapper(self.consumerConfigs, [(self.topic, self.clientDatabasePartition)], certificatePath=self.truststorePath)

    @staticmethod
    def checkBrokerExists(address) -> bool:
        return checkKafkaActive(brokerAddress=address)

    def seekToEnd(self):
        self.consumerDatabase.seekToEnd(self.topic, self.clientDatabasePartition)

    def __del__(self):
        try:
            deleteTopic(self.address, self.topic)
        except:
            pass
