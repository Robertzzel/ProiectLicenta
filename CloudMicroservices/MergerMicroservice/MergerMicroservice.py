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
        sys.stderr.write("Initalizing merger...")
        self.broker = broker
        self.topic = topic
        self.sessionId = sessionId
        self.consumer = KafkaConsumerWrapper({
            'bootstrap.servers': self.broker,
            'group.id': "-",
            'auto.offset.reset': 'latest',
        }, [(self.topic, Partitions.Client.value)], "truststore.pem")
        self.databaseConsumer = KafkaConsumerWrapper({
            'bootstrap.servers': self.broker,
            'group.id': "-",
            'auto.offset.reset': 'latest',
        }, [(self.topic, Partitions.MergerMicroservice.value)], "truststore.pem")
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.broker}, certificatePath="truststore.pem")
        self.running = True

    def start(self):
        sys.stderr.write("Starting merger...")
        makefile = TempFile(delete=False)

        sys.stderr.write("Listening for audio-video files")
        while self.running:
            message = self.consumer.receiveBigMessage(timeoutSeconds=1, partition=Partitions.Client.value)
            if message is None:
                continue

            if message.value() == b"quit":
                self.stop()
                break

            sys.stderr.write(f"Received file...")
            file = TempFile(delete=False)
            file.writeBytes(message.value())
            makefile.write(f"file {file.name}\n")

        if makefile.readString().strip() == "":
            print("No file received, closing...")
            return

        sys.stderr.write(f"Generating final file")
        videoFile = TempFile(False)
        VideoAggregator.aggregateVideos(makefile.name, videoFile.name)
        sys.stderr.write(f"generated file {videoFile.name}")

        sys.stderr.write("Saving session...")
        self.producer.sendFile(topic=DATABASE_TOPIC, fileName=videoFile.name, headers=[
            ("topic", self.topic.encode()),
            ("operation", b"ADD_VIDEO"),
            ("partition", str(Partitions.MergerMicroservice.value).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])

        self.producer.flush(2)
        self.databaseConsumer.consumeMessage(partition=Partitions.MergerMicroservice.value)
        sys.stderr.write("Video saved")

        sys.stderr.write("Deleting session...")
        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=b"msg", headers=[
            ("topic", self.topic.encode()),
            ("partition", str(Partitions.MergerMicroservice.value).encode()),
            ("operation", b"DELETE_SESSION"),
            ("sessionId", str(self.sessionId).encode()),
        ])

        self.producer.flush(2)
        self.databaseConsumer.consumeMessage(partition=Partitions.MergerMicroservice.value)
        sys.stderr.write("Session deleted")

        sys.stderr.write("cleaning...")
        for line in makefile.readStringLines():
            filename = line.split(" ")[1].strip()
            sys.stderr.write(filename)
            os.remove(filename)

        self.consumer.close()
        self.producer.flush()
        #deleteTopic(self.broker, self.topic, certificatePath="truststore.pem")

    def stop(self):
        self.running = False


if __name__ == "__main__":
    try:
        brokerAddress = os.environ["BROKER_ADDRESS"]
        receiveTopic = os.environ["TOPIC"]
        sessionId = os.environ["SESSION_ID"]
    except KeyError:
        sys.stderr.write("No broker address, topic and sessionId given\n")
        sys.exit(1)

    try:
        signal.signal(signal.SIGINT, signal.default_int_handler)
        Merger(brokerAddress, receiveTopic, int(sessionId)).start()
    except BaseException as ex:
        sys.stderr.write("Error:", ex)
