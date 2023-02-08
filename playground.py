import threading
import time

from Kafka.Kafka import createTopic, deleteTopic
import kafka

import Kafka.partitions

TOPIC = "TEST"


def delayedSend():
    prod = kafka.KafkaProducer(bootstrap_servers="localhost:9092")
    prod.send(topic="test", partition=Kafka.partitions.AggregatorMicroserviceStartPartition, value=b"sal")
    prod.flush()
    print("send")


if __name__ == "__main__":
    delayedSend()
    #createTopic("localhost:9092", "test", partitions=7)
    #deleteTopic("localhost:9092", "test")