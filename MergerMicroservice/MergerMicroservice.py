import os
import sys
import tempfile
import subprocess
import signal

import Kafka.partitions
from Kafka.Kafka import *
from typing import Optional

KAFKA_ADDRESS = "localhost:9092"
DATABASE_TOPIC = "DATABASE"


class VideoAggregator:
    @staticmethod
    def aggregateVideos(files: str, resultFile: str):
        process = subprocess.Popen(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", files, "-c", "copy", resultFile], stdout=subprocess.PIPE, stderr=sys.stderr)
        out, err = process.communicate()
        if err is not None and err != b"":
            raise Exception("CONCAT ERROR:\n" + err.decode())


class TempFile:

    def __init__(self, delete=True):
        self.file = tempfile.NamedTemporaryFile(mode="w+", suffix=".mp4", delete=delete)
        self.name = self.file.name

    def readStringLines(self):
        with open(self.name, 'r') as f:
            return f.readlines()

    def readString(self):
        with open(self.name, 'r') as f:
            return f.read()

    def readBytes(self):
        with open(self.name, "rb") as f:
            return f.read()

    def write(self, text):
        with open(self.name, "a+") as f:
            f.write(text)

    def writeBytes(self, text):
        with open(self.name, "ab") as f:
            f.write(text)

    def close(self):
        self.file.close()


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

        if makefile.readString().strip() == "":
            print("No file received, closing...")
            return

        videoFile = TempFile(False)
        VideoAggregator.aggregateVideos(makefile.name, videoFile.name)
        print(f"generated file {videoFile.name}")

        text = videoFile.readBytes()
        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=text, headers=[
            ("topic", self.topic.encode()),
            ("operation", b"ADD_VIDEO"),
            ("partition", str(Kafka.partitions.MergerMicroservicePartition).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])

        print("cleaning...")
        for line in makefile.readStringLines():
            filename = line.split(" ")[1].strip()
            print(filename)
            os.remove(filename)

        self.producer.flush(timeout=5)

    def stop(self):
        self.running = False
        self.consumer.close()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("No broker address, topic and sessionId given")
        sys.exit(1)

    try:
        brokerAddress = sys.argv[1]
        receiveTopic = sys.argv[2]
        sessionId = sys.argv[3]

        signal.signal(signal.SIGINT, signal.default_int_handler)
        m = Merger(brokerAddress, receiveTopic, int(sessionId))
        m.start()

    except BaseException as ex:
        print("Error:", ex)
