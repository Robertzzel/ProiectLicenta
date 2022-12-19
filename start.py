import os.path
import signal
import subprocess
import sys
import time
import pathlib


class Sender:
    def __init__(self):
        self.videoProcess = None
        self.audioProcess = None
        self.aggregatorProcess = None
        self.inputExecutorProcess = None
        self.buildProcess = None
        self.path = str(pathlib.Path(os.getcwd()).parent)

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

    def start(self):
        if not self.build():
            return

        timestamp = str(int(time.time()) + 2)
        self.videoProcess = subprocess.Popen(["./VideoMicroservice/VideoMicroservice", timestamp],cwd=self.path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.audioProcess = subprocess.Popen(["venv/bin/python3", "AudioMicroservice/AudioMicroservice.py", timestamp],cwd=self.path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.aggregatorProcess = subprocess.Popen(["./AggregatorMicroservice/AggregatorMicroservice"],cwd=self.path, stdout=sys.stdout, stderr=sys.stderr)
        self.inputExecutorProcess = subprocess.Popen(["venv/bin/python3", "InputExecutorMicroservice/InputExecutorMicroservice.py"],cwd=self.path, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop(self):
        self.aggregatorProcess.send_signal(signal.SIGINT)
        self.videoProcess.send_signal(signal.SIGINT)
        self.audioProcess.send_signal(signal.SIGINT)
        self.inputExecutorProcess.send_signal(signal.SIGINT)

        try:
            self.videoProcess.wait(timeout=5)
        except subprocess.TimeoutExpired as ex:
            print("video process not closing")
            self.videoProcess.kill()

        try:
            self.audioProcess.wait(timeout=5)
        except subprocess.TimeoutExpired as ex:
            print("audio process not closing")
            self.aggregatorProcess.kill()

        try:
            self.inputExecutorProcess.wait(timeout=5)
        except subprocess.TimeoutExpired as ex:
            print("input process not closing")
            self.inputExecutorProcess.kill()

        try:
            self.aggregatorProcess.wait(timeout=5)
        except subprocess.TimeoutExpired as ex:
            print("aggregator process not closing")
            self.aggregatorProcess.kill()


if __name__ == "__main__":
    m = Sender()
    m.start()
    try:
        input("Enter to stop")
    except KeyboardInterrupt:
        pass
    m.stop()