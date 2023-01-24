import queue
import av
import logging
from queue import Queue
import tkinter as tk
from PIL import ImageTk, Image, ImageOps
import sounddevice as sd
from kafka import KafkaAdminClient
from Kafka.Kafka import *
from io import BytesIO
import numpy as np
from typing import Optional
from Kafka.Kafka import KafkaConsumerWrapper
from UI.InputsBuffer import InputsBuffer
import uuid
import threading

logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used
MOVE = 1
CLICK = 2
PRESS = 4
RELEASE = 5
DATABASE_TOPIC = "DATABASE"


class TkinterVideo(tk.Label):
    def __init__(self, master, aggregatorTopic: str, inputsTopic: str, kafkaAddress: str = "localhost:9092", keepAspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self.kafkaAddress = kafkaAddress
        self.aggregatorTopic = aggregatorTopic
        self.aggregatorStartTopic = f"s{self.aggregatorTopic}"
        self.inputsTopic = inputsTopic
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
        self.keepAspectRatio = keepAspect
        self.streamRunning = True
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
        while self.streamRunning:
            time.sleep(0.1)
            inputs = self.inputsBuffer.get()
            if inputs != "":
                self.kafkaProducer.produce(topic=self.inputsTopic, value=inputs.encode())

        try:
            KafkaAdminClient(bootstrap_servers=self.kafkaAddress).delete_topics([self.inputsTopic])
        except Exception as ex:
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
        while self.streamRunning:
            try:
                self.currentImage = self.videoFramesQueue.get(block=True, timeout=2)  # wait for the stream to init
                break
            except queue.Empty:
                continue

        if self.audioStream:
            self.audioStream.start()

        rate = 1 / self.videoFramerate

        while self.streamRunning:
            try:
                start = time.time()

                self.currentImage = self.videoFramesQueue.get(timeout=2).to_image()
                self.event_generate("<<FrameGenerated>>")

                time.sleep(max(rate - (time.time() - start) % rate, 0))
            except queue.Empty:
                continue

        self.videoFramesQueue.queue.clear()
        self.audioBlocksQueue.queue.clear()
        print("DISPLAYED THREAD: I AM STOPPED")

    def audioCallback(self, outdata: np.ndarray, frames: int, timet, status):
        try:
            data: np.ndarray = self.audioBlocksQueue.get_nowait()

            diff = 1024 - data.size
            if diff > 0:
                data = np.append(data, np.array([0] * diff, dtype=data.dtype))

            data.shape = (1024, 1)
        except queue.Empty:
            data = np.zeros(shape=(1024, 1), dtype=self.audioStream.dtype)
        except Exception as ex:
            print("Error:", ex)
            return

        outdata[:] = data

    def receiveStream(self):
        self.streamConsumer = KafkaConsumerWrapper({
            'bootstrap.servers': self.kafkaAddress,
            'group.id': str(uuid.uuid1()),
            'auto.offset.reset': 'latest',
            'allow.auto.create.topics': "true",
        }, [self.aggregatorTopic])

        self.kafkaProducer.produce(topic=self.aggregatorStartTopic, value=b"")
        print("Signal sent")

        while self.streamRunning:
            message = self.streamConsumer.consumeMessage(timeoutSeconds=1)
            if message is None:
                continue

            with av.open(BytesIO(message.value())) as container:
                self.initVideoStream(container)

            break

        try:
            while self.streamRunning:
                message = self.streamConsumer.consumeMessage(timeoutSeconds=1)
                if message is None:
                    continue

                with av.open(BytesIO(message.value())) as container:
                    container.fast_seek, container.discard_corrupt = True, True

                    self.clearAudioAndVideoQueues()

                    for packet in container.demux():
                        for frame in packet.decode():
                            if isinstance(frame, av.VideoFrame):
                                self.videoFramesQueue.put(item=frame, block=True)
                            elif isinstance(frame, av.AudioFrame):
                                self.audioBlocksQueue.put(item=frame.to_ndarray(), block=True)
        except BaseException as ex:
            print(ex)

    def clearAudioAndVideoQueues(self):
        with self.videoFramesQueue.mutex:
            self.videoFramesQueue.queue.clear()
        with self.audioBlocksQueue.mutex:
            self.audioBlocksQueue.queue.clear()

    def displayFrame(self, event):
        if self.keepAspectRatio:
            self.currentImage = ImageOps.contain(self.currentImage, self.currentDisplaySize, self._resampling_method)
        else:
            self.currentImage = self.currentImage.resize(self.currentDisplaySize, self._resampling_method)

        self.currentTkImage.paste(self.currentImage)
        self.config(image=self.currentTkImage)

    def stop(self):
        self.streamRunning = False
        self.streamConsumer.close()

        print("Stopping inputs thread")
        if self.sendInputsThread:
            self.sendInputsThread.join()

        print("Stopping audio stream")
        if self.audioStream:
            self.audioStream.stop()

        print("Stopping diplay thread")
        if self.displayContentThread:
            self.displayContentThread.join()

        print("Stopping receiver thread")
        if self.streamReceiverThread:
            self.streamReceiverThread.join()
