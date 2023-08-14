import os
import signal
import sys
import subprocess
import time
import psutil


class Stream:
    def __init__(self, host: str, port: int):
        self.cmd = f'bash ./stream {host} {port}'
        self.process = subprocess.Popen(self.cmd, shell=True)

    def stop(self):
        try:
            for child in psutil.Process(self.process.pid).children(recursive=True):
                child.kill()
            self.process.kill()
            self.process.wait(2)
        except Exception as ex:
            pass




if __name__ == "__main__":
    s = Stream("localhost", 12345)
    time.sleep(10)
    s.stop()