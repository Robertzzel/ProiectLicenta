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
    consumer: KafkaConsumerWrapper = KafkaConsumerWrapper(TOPIC, bootstrap_servers="localhost:9092")
    producer: KafkaProducerWrapper = KafkaProducerWrapper(bootstrap_servers="localhost:9092", acks=1)

    threading.Thread(target=delayedSend, args=(producer, )).start()
    message, header = consumer.receiveBigMessage()

    with open("file.txt", "w") as f:
        f.write(message.decode())