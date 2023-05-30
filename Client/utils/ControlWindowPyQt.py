import queue
import time
from io import BytesIO
from queue import Queue
from typing import Optional
import gc
import PIL
import av
import numpy as np
import sounddevice as sd
from PIL import Image
from PIL.ImageQt import ImageQt
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QLabel, QWidget
from PySide6.QtCore import QThread, Qt, Signal
from Client.utils.InputsBuffer import InputsBuffer
from Client.Kafka.Kafka import Partitions, KafkaProducerWrapper, KafkaConsumerWrapper

MOVE = 1
CLICK = 2
SCROLL = 3
PRESS = 4
RELEASE = 5


class DisplayContentThread(QThread):
    imageEvent = Signal(PIL.Image.Image)

    def __init__(self, master):
        super().__init__()
        self.master = master

    def run(self):
        try:
            while not self.master.stopEvent:
                try:
                    self.master.videoFramesQueue.get(block=True, timeout=1)  # wait for the stream to init
                    if self.master.audioStream:
                        self.master.audioStream.start()
                except queue.Empty:
                    continue
                else:
                    break

            rate = 1 / self.master.videoFramerate

            start = time.time()
            img = self.getNextImage()
            if not self.master.stopEvent:
                self.imageEvent.emit(img)
                img = self.getNextImage()
                time.sleep(max(rate - (time.time() - start) % rate, 0))

            while not self.master.stopEvent:
                try:
                    start = time.time()
                    if not self.master.stopEvent:
                        self.imageEvent.emit(img)
                        img = self.getNextImage()
                        time.sleep(max(rate - (time.time() - start) % rate, 0))
                except queue.Empty:
                    continue

            self.master.videoFramesQueue.queue.clear()
            self.master.audioBlocksQueue.queue.clear()
        except BaseException as ex:
            print("DisplayContentThread", ex)

    def getNextImage(self):
        return ImageQt(
            self.master.videoFramesQueue.get(timeout=1).to_image().resize((self.master.windowSize[0], self.master.windowSize[1]), self.master._resampling_method)
        )


class StreamReceiverThread(QThread):

    def __init__(self, master):
        super().__init__()
        self.streamConsumer = None
        self.master = master

    def run(self):
        try:
            self.streamConsumer = KafkaConsumerWrapper({
                'bootstrap.servers': self.master.kafkaAddress,
                'group.id': '-',
            }, [(self.master.topic, Partitions.Client.value)])
            i = 0
            while not self.master.stopEvent:
                message = self.streamConsumer.receiveBigMessage(timeoutSeconds=1)
                if message is None:
                    continue

                if len(message.value()) == 4 and message.value().decode() == "quit":
                    self.master.stopEvent = True
                    print("Sharer stopped")
                    return

                print( i, "RECEIVED", time.time())
                with av.open(BytesIO(message.value())) as container:
                    container.fast_seek, container.discard_corrupt = True, True

                    if self.master.videoFramerate == 0:
                        self.master.initVideoStream(container)
                    self.clearAudioAndVideoQueues()

                    for packet in container.demux():
                        for frame in packet.decode():
                            if isinstance(frame, av.VideoFrame) and not self.master.stopEvent:
                                self.master.videoFramesQueue.put(item=frame, block=True)
                            elif isinstance(frame, av.AudioFrame) and not self.master.stopEvent:
                                self.master.audioBlocksQueue.put(item=frame.to_ndarray(), block=True)
                    gc.collect()
                print("PROCESSED FINISHED AT", time.time())
                i += 1

            self.master.stopEvent = True
        except BaseException as ex:
            print("StreamReceiverThread",ex)

    def clearAudioAndVideoQueues(self):
        with self.master.videoFramesQueue.mutex:
            self.master.videoFramesQueue.queue.clear()
        with self.master.audioBlocksQueue.mutex:
            self.master.audioBlocksQueue.queue.clear()


class SendInputsThread(QThread):
    def __init__(self, master):
        super().__init__()
        self.master: VideoWindow = master

    def run(self):
        try:
            while not self.master.stopEvent:
                time.sleep(0.1)
                inputs = self.master.inputsBuffer.get()
                if inputs != "":
                    self.master.kafkaProducer.produce(topic=self.master.topic, value=inputs.encode(),
                                                      partition=Partitions.Input.value)
        except BaseException as ex:
            print("SendInputsThread", ex)


class VideoWindow(QWidget):
    def __init__(self, topic: str, kafkaAddress: str = "localhost:9092"):
        super().__init__()
        self.label = QLabel(self)

        self.kafkaAddress = kafkaAddress
        self.topic = topic
        self.kafkaProducer = KafkaProducerWrapper({'bootstrap.servers': self.kafkaAddress})

        self.videoFramesQueue: Queue = Queue(60)
        self.audioBlocksQueue: Queue = Queue(60)
        self.audioStream: Optional[sd.OutputStream] = None
        self.videoFramerate = 0
        self.audioSamplerate = 0
        self.audioBlockSize = 0
        self.stopEvent: bool = False
        self.inputsBuffer: InputsBuffer = InputsBuffer()
        self._resampling_method: int = Image.BOX

        self.dct = DisplayContentThread(self)
        self.srt = StreamReceiverThread(self)
        self.sit = SendInputsThread(self)

        self.dct.imageEvent.connect(self.displayFrame)
        self.play()

        self.label.setMouseTracking(True)
        self.label.mouseMoveEvent = self.mouseMoveEvent
        self.label.wheelEvent = self.wheelEvent
        self.label.mousePressEvent = self.mousePressEvent
        self.windowSize = ()

    def keyPressEvent(self, event):
        self.inputsBuffer.add(f"{PRESS},{event.key() if not event.text().isalpha() else ord(event.text())}")

    def keyReleaseEvent(self, event):
        self.inputsBuffer.add(f"{RELEASE},{event.key() if not event.text().isalpha() else ord(event.text())}")

    def wheelEvent(self, event):
        self.inputsBuffer.add(f"{SCROLL},{int(event.angleDelta().y() > 0)}")

    def mousePressEvent(self, event):
        button = event.button()
        if button == Qt.LeftButton:
            self.inputsBuffer.add(f"{CLICK},1,1")
        elif button == Qt.RightButton:
            self.inputsBuffer.add(f"{CLICK},3,1")
        elif button == Qt.MiddleButton:
            self.inputsBuffer.add(f"{CLICK},2,1")
        else:
            self.inputsBuffer.add(f"{CLICK},1,1")

    def closeEvent(self, event):
        self.stop()
        event.accept()

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.inputsBuffer.add(f"{CLICK},1,0")
        elif event.button() == Qt.RightButton:
            self.inputsBuffer.add(f"{CLICK},3,0")
        elif event.button() == Qt.MiddleButton:
            self.inputsBuffer.add(f"{CLICK},2,0")
        else:
            self.inputsBuffer.add(f"{CLICK},1,0")

    def mouseMoveEvent(self, event):
        self.inputsBuffer.add(
            f"{MOVE},{round(event.x() / self.windowSize[0], 3)},{round(event.y() / self.windowSize[1], 3)}")

    def resizeEvent(self, event):
        self.windowSize = (self.width(), self.height())
        self.label.resize(self.windowSize[0], self.windowSize[1])

    def play(self):
        self.srt.start()
        self.dct.start()
        self.sit.start()

    def initVideoStream(self, container):
        videoStream = container.streams.video[0]
        audioStream = container.streams.audio[0]

        self.videoFramerate = videoStream.guessed_rate
        self.audioSamplerate = audioStream.sample_rate
        self.audioBlockSize = self.audioSamplerate // self.videoFramerate

        self.audioStream = sd.OutputStream(channels=audioStream.codec_context.channels,
                                           samplerate=self.audioSamplerate, dtype="float32",
                                           callback=self.audioCallback,
                                           blocksize=audioStream.codec_context.frame_size)

    def audioCallback(self, outdata: np.ndarray, frames: int, timet, status):
        try:
            data: np.ndarray = self.audioBlocksQueue.get_nowait()
            data = np.append(data, np.array([0] * (1024 - data.size), dtype=data.dtype))
            data.shape = (1024, 1)
        except queue.Empty:
            data = np.zeros(shape=(1024, 1), dtype=self.audioStream.dtype)
        except Exception as ex:
            print("Error:", ex)
            return

        outdata[:] = data

    def displayFrame(self, img):
        self.label.setPixmap(QPixmap.fromImage(img).copy())

    def stop(self):
        self.stopEvent = True
        self.kafkaProducer.flush(timeout=5)

        print("Stopping inputs thread")
        self.srt.quit()
        print("Stopped inputs thread")
        print("Stopping audio stream")
        self.audioStream.stop()
        print("Stopped audio stream")
        print("Stopping diplay thread")
        self.dct.quit()
        print("Stopped diplay thread")
        print("Stopping receiver thread")
        self.sit.quit()
        print("Stopped receiver thread")
