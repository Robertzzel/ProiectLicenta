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
        self.buildProcess = None

    def build(self):
        print(str(pathlib.Path(os.getcwd())))
        self.buildProcess = subprocess.Popen(["/bin/sh", "./build"], cwd=str(pathlib.Path(os.getcwd()).parent), stdout=sys.stdout, stderr=subprocess.PIPE)
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
        self.videoProcess = subprocess.Popen(["./VideoMicroservice/VideoMicroservice", timestamp],cwd=str(pathlib.Path(os.getcwd()).parent), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.audioProcess = subprocess.Popen(["venv/bin/python3", "AudioMicroservice/AudioMicroservice.py", timestamp],cwd=str(pathlib.Path(os.getcwd()).parent), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.aggregatorProcess = subprocess.Popen(["./AggregatorMicroservice/AggregatorMicroservice"],cwd=str(pathlib.Path(os.getcwd()).parent), stdout=sys.stdout, stderr=subprocess.PIPE)

    def stop(self):
        self.aggregatorProcess.send_signal(signal.SIGINT)
        self.videoProcess.send_signal(signal.SIGINT)
        self.audioProcess.send_signal(signal.SIGINT)
        try:
            self.aggregatorProcess.wait(timeout=5)
            self.videoProcess.wait(timeout=5)
            self.audioProcess.wait(timeout=5)
        except:
            self.aggregatorProcess.kill()
            self.videoProcess.kill()
            self.audioProcess.kill()


if __name__ == "__main__":
    m = Sender()
    m.start()
    input("Enter to stop")
    m.stop()