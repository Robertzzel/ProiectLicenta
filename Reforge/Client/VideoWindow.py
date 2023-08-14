import socket
import struct
import time
import PIL
import av
from PIL import Image
from PIL.ImageQt import ImageQt
from PyQt5.QtGui import QPixmap
from PyQt5.QtWidgets import QLabel, QWidget
from PyQt5.QtCore import QThread, pyqtSignal

from Reforge.Client.InpusRecorder import InputsRecorder

MOVE = 1
CLICK = 2
SCROLL = 3
PRESS = 4
RELEASE = 5
framerate = 30
rate = 1 /framerate

class SendInputsThread(QThread):
    def __init__(self, master):
        super().__init__()
        self.master: VideoWindow = master

    def run(self):
        while True:
            time.sleep(0.1)
            inputs = self.master.inputRecorder.getBuffer()
            self.master.inputsSocket.sendto(inputs, ("localhost", 12347))


class DisplayContentThread(QThread):
    imageEvent = pyqtSignal(PIL.ImageQt.ImageQt)

    def __init__(self, master):
        super().__init__()
        self.master = master
        input_url = f"udp://{self.master.host}:{self.master.port}"
        container = av.open(input_url)
        video_stream = next(s for s in container.streams if s.type == 'video')
        self.frameGenerator = container.decode(video_stream)

    def getNextFrame(self, timeoutSecs: float=1):
        end = time.time() + timeoutSecs
        while time.time() < end:
            frame = next(self.frameGenerator, None)
            if frame is not None:
                return ImageQt(frame.to_image().resize((self.master.windowSize[0], self.master.windowSize[1]), PIL.Image.NEAREST))
        return None

    def run(self):
        img = None
        while img is None:
            img = self.getNextFrame()

        while True:
            start = time.time()
            if img is not None:
                self.imageEvent.emit(img)
            img = self.getNextFrame(1)
            if img is None:
                continue
            end = max(rate - (time.time() - start) % rate, 0)
            while time.time() < end:
                self.getNextFrame(0.001)


class VideoWindow(QWidget):
    def __init__(self, master, host, port, inputsSocket):
        super().__init__()
        self.master = master
        self.label = QLabel(self)
        self.resize(300, 300)
        self.inputsSocket: socket.socket = inputsSocket

        self.host = host
        self.port = port
        self.label.setMouseTracking(True)
        self.windowSize = ()

        self.inputRecorder = InputsRecorder()
        self.inputsThread = SendInputsThread(self)
        self.inputsThread.start()
        self.inputRecorder.start()
        self.dct = DisplayContentThread(self)
        self.dct.imageEvent.connect(self.displayFrame)
        self.label.mouseMoveEvent = self.mouseMoveEvent

        self.play()

    def closeEvent(self, event):
        self.stop()
        event.accept()

    def resizeEvent(self, event):
        self.windowSize = (self.width(), self.height())
        self.label.resize(self.windowSize[0], self.windowSize[1])

    def play(self):
        self.dct.start()

    def displayFrame(self, img):
        self.label.setPixmap(QPixmap.fromImage(img).copy())

    def mouseMoveEvent(self, event):
        if not self.inputRecorder.enable:
            return
        self.inputRecorder.buffer.extend(struct.pack("B", 4))
        self.inputRecorder.buffer.extend(struct.pack("ff", round(event.x() / self.windowSize[0], 3), round(event.y() / self.windowSize[1], 3)))

    def stop(self):
        self.dct.quit()
        self.inputsThread.quit()
        self.inputRecorder.stop()
        self.close()

    def enterEvent(self, event):
        print("Cursor entered the window.")
        self.inputRecorder.resumeRecording()
        print(len(self.inputRecorder.buffer))

    def leaveEvent(self, event):
        print("Cursor left the window.")
        self.inputRecorder.pauseRecording()
        print(len(self.inputRecorder.buffer))
        self.inputRecorder.buffer = bytearray()
