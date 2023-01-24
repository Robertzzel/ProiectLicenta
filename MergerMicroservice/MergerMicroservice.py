import os
import sys
import pathlib
import queue
import tempfile
import subprocess
import uuid
import signal
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
    def __init__(self, fileContents: str):
        self.file = tempfile.NamedTemporaryFile()
        self.file.write(fileContents)
        self.name = self.file.name

    def close(self):
        self.file.close()


class Merger:
    def __init__(self, broker: str, topic: str, sessionId: int):
        self.broker = broker
        self.topic = topic
        self.sessionId = sessionId
        self.consumer = KafkaConsumerWrapper({
            'bootstrap.servers': self.broker,
            'group.id': str(uuid.uuid1()),
            'auto.offset.reset': 'latest',
            'allow.auto.create.topics': "true",
        }, [self.topic])
        self.producer = KafkaProducerWrapper({'bootstrap.servers': self.broker})
        self.running = True
        self.videosQueue = queue.Queue()
        self.finalVideo: Optional[str] = None
        self.i = 0

    def start(self):
        signal.signal(signal.SIGINT, signal.default_int_handler)
        try:
            while self.running:
                message = self.consumer.consumeMessage(timeoutSeconds=1)
                if message is None:
                    continue
                print(f"goo msg {len(message.value())}", self.running)

                if message.value() == b"quit":
                    print("Quitting")
                    self.stop()
                    break

                self.videosQueue.put(message.value())

                if self.videosQueue.qsize() > 20:
                    self.aggregateVideosFromQueue()
        except BaseException as ex:
            print(ex)

        self.aggregateVideosFromQueue()
        self.compressFinalFile()

        with open(self.finalVideo, "rb") as f:
            videoContent = f.read()

        self.producer.sendBigMessage(topic=DATABASE_TOPIC, value=videoContent, headers=[
            ("topic", self.topic.encode()),
            ("operation", b"ADD_VIDEO"),
            ("sessionId", str(self.sessionId).encode())
        ])

        print(f"MERGER: message sent at {DATABASE_TOPIC}, {self.broker} ---")
        self.producer.flush(timeout=5)

    def aggregateVideosFromQueue(self):
        videos = []

        while self.videosQueue.qsize() > 0:
            videos.append(TempFile(self.videosQueue.get(block=True)))

        fileNames = list(map(lambda f: f.name, videos))
        if self.finalVideo:
            fileNames.insert(0, self.finalVideo)

        VideoAggregator.aggregateVideos(fileNames, f"{self.i}.mp4")

        if self.finalVideo is not None:
            os.remove(self.finalVideo)

        self.finalVideo = f"{pathlib.Path(os.getcwd())}/{self.i}.mp4"
        [file.close() for file in videos]

        self.i += 1
        videos.clear()

    def compressFinalFile(self):
        self.i += 1
        nextFinalFile = f"final{self.i}.mp4"
        process = subprocess.Popen(["ffmpeg", "-y", "-i", self.finalVideo, "-c:v", "libx264", "-b:v", "500k", nextFinalFile], stdout=subprocess.PIPE, stderr=sys.stderr)
        process.wait()
        out, err = process.communicate()
        if err is not None and err != b"":
            raise Exception("CONCAT ERROR:\n" + err.decode())

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