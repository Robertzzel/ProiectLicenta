import sys
import queue
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
    def aggregateVideos(files: List[str], resultFile: str):
        infoFile = VideoAggregator.createFileWithVideoNames(files)
        process = subprocess.Popen(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", infoFile.name, "-c", "copy", resultFile], stdout=subprocess.PIPE, stderr=sys.stderr)
        out, err = process.communicate()
        if err is not None and err != b"":
            raise Exception("CONCAT ERROR:\n" + err.decode())

    @staticmethod
    def createFileWithVideoNames(files: List[str]):
        contents = ""
        for file in files:
            contents += f"file {file}\n"

        tempFile = tempfile.NamedTemporaryFile(mode="w+")
        tempFile.write(contents)
        tempFile.flush()
        return tempFile


class TempFile:
    def __init__(self, fileContents: bytes):
        self.file = tempfile.NamedTemporaryFile(suffix=".mp4")
        self.file.write(fileContents)
        self.name = self.file.name

    def read(self):
        return self.file.read()

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
        self.videosQueue = queue.Queue()
        self.finalVideo: Optional[TempFile] = None

    def start(self):
        signal.signal(signal.SIGINT, signal.default_int_handler)
        try:
            self.consumer.seekToEnd(topic=self.topic, partition=Kafka.partitions.ClientPartition)
        except Exception as ex:
            pass

        try:
            while self.running:
                message = self.consumer.receiveBigMessage(timeoutSeconds=1, partition=Kafka.partitions.ClientPartition)
                if message is None:
                    continue

                if message.value() == b"quit":
                    self.stop()
                    break

                self.videosQueue.put(TempFile(message.value()))

                if self.videosQueue.qsize() > 50:
                    self.aggregateVideosFromQueue()
        except BaseException as ex:
            print(ex)

        self.aggregateVideosFromQueue()
        self.compressFinalFile()

        if self.finalVideo is None:
            print("not a file")
            return

        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=self.finalVideo.read(), headers=[
            ("topic", self.topic.encode()),
            ("operation", b"ADD_VIDEO"),
            ("partition", str(Kafka.partitions.MergerMicroservicePartition).encode()),
            ("sessionId", str(self.sessionId).encode()),
        ])
        self.producer.flush(timeout=5)
        self.finalVideo.close()

    def aggregateVideosFromQueue(self):
        videos = []
        try:
            while True:
                videos.append(self.videosQueue.get_nowait())
        except queue.Empty:
            pass

        if len(videos) == 0:
            return

        fileNames: List[str] = [file.name for file in videos]
        if self.finalVideo is not None:
            fileNames.insert(0, self.finalVideo.name)

        newFile = TempFile(b"")
        VideoAggregator.aggregateVideos(fileNames, newFile.name)
        self.finalVideo = newFile

        [file.close() for file in videos]

    def compressFinalFile(self):
        if self.finalVideo is None:  # no file created
            return

        nextFile = TempFile(b"")
        process = subprocess.Popen(
            ["ffmpeg", "-y", "-i", self.finalVideo.name, "-c:v", "libx264", "-b:v", "500k", nextFile.name],
            stdout=subprocess.PIPE,
            stderr=sys.stderr
        )
        process.wait()
        out, err = process.communicate()
        if err is not None and err != b"":
            raise Exception("CONCAT ERROR:\n" + err.decode())
        self.finalVideo.close()
        self.finalVideo = nextFile

    def stop(self):
        self.running = False
        self.consumer.close()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("No broker address, topic and sessionId given")
        sys.exit(1)

    brokerAddress = sys.argv[1]
    receiveTopic = sys.argv[2]
    sessionId = sys.argv[3]

    m = Merger(brokerAddress, receiveTopic, int(sessionId))
    m.start()