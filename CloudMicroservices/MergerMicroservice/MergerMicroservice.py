import os
import sys
import signal
import time

from Kafka.Kafka import KafkaConsumerWrapper, KafkaProducerWrapper, deleteTopic
from TempFile import TempFile
from VideoAggregator import VideoAggregator
from Kafka.Kafka import Partitions


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
        }, [(self.topic, Partitions.Client.value)], "truststore.pem")
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.broker}, certificatePath="truststore.pem")
        self.running = True

    def start(self):
        makefile = TempFile(delete=False)

        while self.running:
            message = self.consumer.receiveBigMessage(timeoutSeconds=1, partition=Partitions.Client.value)
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

        try:
            self.consumer.seekToEnd(self.topic, Partitions.MergerMicroservice.value)
        except:
            pass

        print("Saving session...")
        self.producer.sendFile(topic=DATABASE_TOPIC, fileName=videoFile.name, headers=[
            ("topic", self.topic.encode()),
            ("operation", b"ADD_VIDEO"),
            ("partition", str(
                Partitions.MergerMicroservice.value).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])

        time.sleep(2)
        print("Video saved")

        print("Deleting session...")
        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=b"msg", headers=[
            ("topic", self.topic.encode()),
            ("operation", b"DELETE_SESSION"),
            ("partition", str(Partitions.MergerMicroservice.value).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])

        time.sleep(2)
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
