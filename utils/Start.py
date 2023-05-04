import os.path
import signal
import subprocess
import sys
import pathlib
import time
import platform
from concurrent.futures import ThreadPoolExecutor


class VideoMerger:
    def __init__(self, kafkaBroker: str, topic: str, sessionId: int):
        import docker
        client = docker.from_env()
        client.containers.run("merger-microservice:v6", detach=True,
                              environment={"BROKER_ADDRESS": kafkaBroker, "TOPIC": topic, "SESSION_ID": str(sessionId)},
                              network_mode="host")


class Recorder:
    def __init__(self, brokerAddress: str):
        self.threadPool = ThreadPoolExecutor(max_workers=4)
        self.brokerAddress = brokerAddress
        self.videoProcess = None
        self.audioProcess = None
        self.aggregatorProcess = None
        self.inputExecutorProcess = None
        self.buildProcess = None
        self.path = str(pathlib.Path(os.getcwd()))
        self.running = False

    def build(self):
        self.buildProcess = subprocess.Popen(["/bin/sh", "./build"], cwd=self.path, stdout=sys.stdout, stderr=subprocess.PIPE)
        stdout, stderr = self.buildProcess.communicate()

        try:
            self.buildProcess.wait(timeout=20)
        except:
            print("Build cannot be finished")
            self.buildProcess.kill()
            return False

        if stderr != b"":
            print("Cannot build,", stderr.decode())
            return False

        return True

    def start(self, topic: str):
        if platform.system().lower() == "windows":
            return

        try:
            self.videoProcess = subprocess.Popen(["./VideoMicroservice.exe", self.brokerAddress, topic],
                                                 stdout=sys.stdout, stderr=sys.stderr)
            self.audioProcess = subprocess.Popen(["venv/bin/python3", "AudioMicroservice/AudioMicroservice.py", self.brokerAddress, topic],
                                                 cwd=self.path, stdout=sys.stdout, stderr=sys.stderr)
            self.aggregatorProcess = subprocess.Popen(["./AggregatorMicroservice.exe", self.brokerAddress, topic],
                                                      stdout=sys.stdout, stderr=sys.stderr)
            self.inputExecutorProcess = subprocess.Popen(["venv/bin/python3", "InputExecutorMicroservice/InputExecutorMicroservice.py", self.brokerAddress, topic],
                                                         cwd=self.path, stdout=subprocess.PIPE, stderr=sys.stderr)

            self.running = True
        except Exception as ex:
            self.stop()
            raise ex

    def stop(self):
        threads = [self.threadPool.submit(self.stopVideoProcess), self.threadPool.submit(self.stopAudioProcess),
                   self.threadPool.submit(self.stopAggregatorProcess), self.threadPool.submit(self.stopInputProcess)]

        for thread in threads:
            if thread is not None:
                thread.result()

        self.running = False
        print("Sharer Closed")

    def stopAudioProcess(self):
        if self.audioProcess is None:
            return

        s = time.time()

        self.audioProcess.send_signal(signal.SIGINT)

        try:
            self.audioProcess.wait(timeout=5)
        except BaseException as ex:
            self.aggregatorProcess.kill()
        finally:
            print(f"audio process closed in", time.time() - s)

    def stopVideoProcess(self):
        if self.videoProcess is None:
            return

        s = time.time()

        self.videoProcess.send_signal(signal.SIGINT)

        try:
            self.videoProcess.wait(timeout=5)
        except BaseException as ex:
            self.videoProcess.kill()
        finally:
            print("video process closed", time.time() - s)

    def stopAggregatorProcess(self):
        if self.aggregatorProcess is None:
            return

        s = time.time()
        self.aggregatorProcess.send_signal(signal.SIGINT)

        try:
            self.aggregatorProcess.wait(timeout=5)
        except BaseException as ex:
            self.aggregatorProcess.kill()
        finally:
            print("aggregator process closed", time.time() - s)

    def stopInputProcess(self):
        if self.inputExecutorProcess is None:
            return

        s = time.time()

        self.inputExecutorProcess.send_signal(signal.SIGINT)

        try:
            self.inputExecutorProcess.wait(timeout=5)
        except BaseException as ex:
            self.inputExecutorProcess.kill()
        finally:
            print("input process closed", time.time() - s)