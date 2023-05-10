import os
import sys
import signal
import Kafka.partitions
from Kafka.Kafka import *
from TempFile import TempFile
from VideoAggregator import VideoAggregator


DATABASE_TOPIC = "DATABASE"


class Merger:
    def __init__(self, broker: str, topic: str, sessionId: int):
        self.broker = broker
        self.topic = topic
        self.sessionId = sessionId
        self.consumer = KafkaConsumerWrapper({
            'bootstrap.servers': self.broker,
            'group.id': "-",
            'auto.offset.reset': 'latest',
        }, [(self.topic, Kafka.partitions.ClientPartition)])
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.broker})
        self.running = True

    def start(self):
        makefile = TempFile(delete=False)

        while self.running:
            message = self.consumer.receiveBigMessage(timeoutSeconds=1, partition=Kafka.partitions.ClientPartition)
            if message is None:
                continue

            if message.value() == b"quit":
                self.stop()
                break

            file = TempFile(delete=False)
            file.writeBytes(message.value())
            makefile.write(f"file {file.name}\n")
            print(f"Added {file.name}")

        if makefile.readString().strip() == "":
            print("No file received, closing...")
            return

        videoFile = TempFile(False)
        VideoAggregator.aggregateVideos(makefile.name, videoFile.name)
        print(f"generated file {videoFile.name}")

        text = videoFile.readBytes()
        try:
            self.consumer.seekToEnd(self.topic, Kafka.partitions.MergerMicroservicePartition)
        except:
            pass

        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=text, headers=[
            ("topic", self.topic.encode()),
            ("operation", b"ADD_VIDEO"),
            ("partition", str(Kafka.partitions.MergerMicroservicePartition).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])

        self.consumer.receiveBigMessage(partition=Kafka.partitions.MergerMicroservicePartition, timeoutSeconds=10)
        print("Video saved")

        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=b"msg", headers=[
            ("topic", self.topic.encode()),
            ("operation", b"DELETE_SESSION"),
            ("partition", str(Kafka.partitions.MergerMicroservicePartition).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])

        self.consumer.receiveBigMessage(partition=Kafka.partitions.MergerMicroservicePartition, timeoutSeconds=10)
        print("Session deleted")

        print("cleaning...")
        for line in makefile.readStringLines():
            filename = line.split(" ")[1].strip()
            print(filename)
            os.remove(filename)

        self.consumer.close()
        self.producer.flush()
        deleteTopic(self.broker, self.topic)

    def stop(self):
        self.running = False


if __name__ == "__main__":
    try:
        brokerAddress = os.environ["BROKER_ADDRESS"]
        receiveTopic = os.environ["TOPIC"]
        sessionId = os.environ["SESSION_ID"]
    except KeyError:
        print("No broker address, topic and sessionId given")
        sys.exit(1)

    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        Merger(brokerAddress, receiveTopic, int(sessionId)).start()
    except BaseException as ex:
        print("Error:", ex)
