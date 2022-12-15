import os
import sys
import pathlib
from typing import List
import kafka
import queue
import tempfile
import subprocess

KAFKA_ADDRESS = "localhost:9092"
TOPIC = "aggregator"


class VideoAggregator:
    @staticmethod
    def aggregateVideos(files: List[str], resultFile: str):
        infoFile = VideoAggregator.createFileWithVideoNames(files)
        process = subprocess.Popen(["./VideoConcatter", infoFile.name, resultFile], stdout=subprocess.PIPE, stderr=sys.stderr, cwd=str(pathlib.Path(os.getcwd()).parent))
        process.wait()
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
    def __init__(self):
        self.consumer: kafka.KafkaConsumer = kafka.KafkaConsumer(TOPIC, bootstrap_servers=KAFKA_ADDRESS, consumer_timeout_ms=2000)
        self.running = True
        self.videosQueue = queue.Queue()
        self.finalVideo = None
        self.i = 0

    def start(self):
        while self.running:
            try:
                message = next(self.consumer).value
            except StopIteration:
                continue

            if message == b"quit":
                self.stop()
                break

            self.videosQueue.put(message)

            if self.videosQueue.qsize() > 10:
                self.aggregateVideosFromQueue()

        self.aggregateVideosFromQueue()

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

        self.finalVideo = f"{pathlib.Path(os.getcwd()).parent}/{self.i}.mp4"
        map(lambda file: file.close(), videos)

        self.i += 1
        videos.clear()

    def stop(self):
        self.running = False
        self.consumer.close()


if __name__ == "__main__":
    Merger().start()