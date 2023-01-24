from dataclasses import dataclass
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

        createTopic(address, consumerTopic)

        self.address = address
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.address})

        consumerConfigs['bootstrap.servers'] = self.address
        consumerConfigs['group.id'] = str(uuid.uuid1())
        self.consumer = KafkaConsumerWrapper(consumerConfigs, [consumerTopic])

    def databaseCall(self, topic: str, operation: str, message: bytes, bigFile=False, timeoutSeconds: float = None) -> kafka.Message:
        self.producer.produce(topic=DATABASE_TOPIC, headers=[
            ("topic", topic.encode()), ("operation", operation.encode()),
        ], value=message)

        return self.consumer.receiveBigMessage(timeoutSeconds) if bigFile else self.consumer.consumeMessage(timeoutSeconds)

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
