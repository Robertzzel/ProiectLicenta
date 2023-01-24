import threading
import time

from Kafka.Kafka import *
from kafka import *
TOPIC = "TEST"


def delayedSend(producer):
    time.sleep(2)
    producer.sendBigMessage(topic=TOPIC, value=str(list(range(1_000_000))).encode())
    print("send")


if __name__ == "__main__":
    pass