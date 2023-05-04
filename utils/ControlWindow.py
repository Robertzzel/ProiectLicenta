import queue
import av
import logging
from queue import Queue
import tkinter as tk
from PIL import ImageTk, Image
import sounddevice as sd
from io import BytesIO
import numpy as np
from typing import Optional
from .Kafka import KafkaConsumerWrapper, KafkaProducerWrapper, Partitions
from .InputsBuffer import InputsBuffer
import threading, time
from asyncio import Event

logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used
MOVE = 1
CLICK = 2
PRESS = 4
RELEASE = 5
DATABASE_TOPIC = "DATABASE"


# class VideoWindow(tk.Toplevel):
#     def __init__(self, topic: str, kafkaAddress: str):
#         super().__init__()
#         self.player = TkinterVideo(master=self, topic=topic, kafkaAddress=kafkaAddress)
#         self.player.pack(expand=True, fill="both")
#
#     def play(self):
#         self.player.play()
#
#     def stop(self):
#         self.player.stop()
#         self.destroy()
#
#     def __del__(self):
#         self.player.stop()
#         self.destroy()

class VideoWindow:
    def __init__(self, topic: str, kafkaAddress: str):
        self.window = tk.Tk()
        self.window.geometry('130x60')
        self.player = TkinterVideo(master=self.window, topic=topic, kafkaAddress=kafkaAddress)
        self.player.pack(expand=True, fill="both")

    def play(self):
        self.player.play()

    def stop(self):
        self.player.stop()
        try:
            self.window.destroy()
        except:
            pass
        del self.window


class TkinterVideo(tk.Label):
    def __init__(self, master, topic: str, kafkaAddress: str = "localhost:9092", *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self.kafkaAddress = kafkaAddress
        self.topic = topic
        self.kafkaProducer = KafkaProducerWrapper({'bootstrap.servers': self.kafkaAddress})

        self.streamConsumer: Optional[KafkaConsumerWrapper] = None
        self.displayContentThread: threading.Thread = threading.Thread(target=self.displayContent, daemon=True)
        self.streamReceiverThread: threading.Thread = threading.Thread(target=self.receiveStream, daemon=True)
        self.sendInputsThread: threading.Thread = threading.Thread(target=self.sendInputs, daemon=True)
        self.videoFramesQueue: Queue = Queue(60)
        self.audioBlocksQueue: Queue = Queue(60)
        self.currentImage = None
        self.audioStream: Optional[sd.OutputStream] = None
        self.currentDisplaySize = (0, 0)
        self.videoFramerate = 0
        self.audioSamplerate = 0
        self.audioBlockSize = 0
        self.stopEvent = Event()
        self.inputsBuffer: InputsBuffer = InputsBuffer()
        self.currentTkImage: Optional[ImageTk.PhotoImage] = None

        self.bind("<Configure>", self.resizeEvent)
        self._resampling_method: int = Image.NEAREST

        self.bind('<Motion>', self.motionHandler)
        self.bind("<Button>", self.mouseButtonPressHandler)
        self.bind("<ButtonRelease>", self.mouseButtonReleaseHandler)

        master.bind('<KeyPress>', self.keyPressHandler)
        master.bind('<KeyRelease>', self.keyReleaseHandler)

        self.bind("<<FrameGenerated>>", self.displayFrame)

    def motionHandler(self, event: tk.Event):
        self.inputsBuffer.add(
            f"{MOVE},{round(event.x / self.currentDisplaySize[0], 3)},{round(event.y / self.currentDisplaySize[1], 3)}")

    def mouseButtonPressHandler(self, event: tk.Event):
        self.inputsBuffer.add(f"{CLICK},{event.num},1")

    def mouseButtonReleaseHandler(self, event: tk.Event):
        self.inputsBuffer.add(f"{CLICK},{event.num},0")

    def keyPressHandler(self, event: tk.Event):
        self.inputsBuffer.add(f"{PRESS},{event.keysym_num}")

    def keyReleaseHandler(self, event: tk.Event):
        self.inputsBuffer.add(f"{RELEASE},{event.keysym_num}")

    def sendInputs(self):
        try:
            while not self.stopEvent.is_set():
                time.sleep(0.05)
                inputs = self.inputsBuffer.get()
                if inputs != "":
                    self.kafkaProducer.produce(topic=self.topic, value=inputs.encode(), partition=Partitions.Input.value)
        except BaseException as ex:
            print(ex)

    def play(self):
        if self.streamReceiverThread is not None:
            self.streamReceiverThread.start()

        if self.displayContentThread is not None:
            self.displayContentThread.start()

        if self.sendInputsThread is not None:
            self.sendInputsThread.start()

    def resizeEvent(self, event):
        self.currentDisplaySize = event.width, event.height

    def initVideoStream(self, container):
        videoStream = container.streams.video[0]
        audioStream = container.streams.audio[0]

        self.videoFramerate = videoStream.guessed_rate
        self.audioSamplerate = audioStream.sample_rate
        self.audioBlockSize = self.audioSamplerate // self.videoFramerate

        self.currentTkImage = ImageTk.PhotoImage(Image.new("RGB", (videoStream.width, videoStream.height), (0, 0, 0)))
        self.config(width=300, height=300, image=self.currentTkImage)

        self.audioStream = sd.OutputStream(channels=audioStream.codec_context.channels,
                                           samplerate=self.audioSamplerate, dtype="float32",
                                           callback=self.audioCallback,
                                           blocksize=audioStream.codec_context.frame_size)

    def displayContent(self):
        try:
            while not self.stopEvent.is_set():
                try:
                    self.currentImage = self.videoFramesQueue.get(block=True, timeout=1)  # wait for the stream to init
                    if self.audioStream:
                        self.audioStream.start()
                except queue.Empty:
                    continue
                else:
                    break

            rate = 1 / self.videoFramerate
            while not self.stopEvent.is_set():
                try:
                    start = time.time()
                    self.currentImage = self.videoFramesQueue.get(timeout=1).to_image()
                    if not self.stopEvent.is_set():
                        self.event_generate("<<FrameGenerated>>")
                        time.sleep(max(rate - (time.time() - start) % rate, 0))
                except queue.Empty:
                    continue

            self.videoFramesQueue.queue.clear()
            self.audioBlocksQueue.queue.clear()
        except BaseException as ex:
            print(ex)

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

    def receiveStream(self):
        try:
            self.streamConsumer = KafkaConsumerWrapper({
                'bootstrap.servers': self.kafkaAddress,
                'group.id': '-',
            }, [(self.topic, Partitions.Client.value)])

            print(1)
            while not self.stopEvent.is_set():
                message = self.streamConsumer.receiveBigMessage(timeoutSeconds=1, partition=Partitions.Client.value)
                print(2)
                if message is None:
                    continue
                print(3)

                with av.open(BytesIO(message.value())) as container:
                    print(4)
                    if self.videoFramerate == 0:
                        self.initVideoStream(container)
                        print(5)
                    container.fast_seek, container.discard_corrupt = True, True
                    print(6)
                    self.clearAudioAndVideoQueues()
                    print(7)
                    for packet in container.demux():
                        print(8)
                        decoded = packet.decode()
                        print(8.1)
                        if self.stopEvent.is_set():
                            print(8.2)
                            return
                        print(8.3)
                        for frame in decoded:
                            print(9)
                            if isinstance(frame, av.VideoFrame) and not self.stopEvent.is_set():
                                print(10)
                                self.videoFramesQueue.put(item=frame, block=True)
                                print(11)
                            elif isinstance(frame, av.AudioFrame) and not self.stopEvent.is_set():
                                print(12)
                                self.audioBlocksQueue.put(item=frame.to_ndarray(), block=True)
                                print(13)
        except BaseException as ex:
            print(ex)

        print(14)
        self.stopEvent.set()

    def clearAudioAndVideoQueues(self):
        with self.videoFramesQueue.mutex:
            self.videoFramesQueue.queue.clear()
        with self.audioBlocksQueue.mutex:
            self.audioBlocksQueue.queue.clear()

    def displayFrame(self, event):
        self.currentTkImage.paste(self.currentImage.resize(self.currentDisplaySize, self._resampling_method))
        self.config(image=self.currentTkImage)

    def stop(self):
        self.stopEvent.set()
        self.kafkaProducer.flush(timeout=5)
        self.streamConsumer.close()

        print("Stopping inputs thread")
        if self.sendInputsThread:
            self.sendInputsThread.join()
        print("Stopped inputs thread")

        print("Stopping audio stream")
        if self.audioStream:
            self.audioStream.stop()
        print("Stopped audio stream")

        print("Stopping diplay thread")
        if self.displayContentThread:
            self.displayContentThread.join()
        print("Stopped diplay thread")

        print("Stopping receiver thread")
        if self.streamReceiverThread:
            self.streamReceiverThread.join()
        print("Stopped receiver thread")
