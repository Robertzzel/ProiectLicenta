import pickle
import queue
from start import Sender
import av
import time
import threading
import logging
from queue import Queue
import tkinter as tk
import tkinter.ttk as ttk
from PIL import ImageTk, Image, ImageOps
import sounddevice as sd
import kafka
from io import BytesIO
import numpy as np
from typing import Optional
from UI.InputsBuffer import InputsBuffer

TOPIC = "aggregator"
logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used
MOVE = 1
CLICK = 2
PRESS = 4
RELEASE = 5
INPUTS_TOPIC = "inputs"


class MainWindow:
    def __init__(self):
        self.root = tk.Tk()

        self.tabControl = ttk.Notebook(self.root)
        self.receiverTab = tk.Frame(self.tabControl)
        self.senderTab = tk.Frame(self.tabControl)
        self.tabControl.add(child=self.receiverTab, text="Receive")
        self.tabControl.add(child=self.senderTab, text="Send")
        self.tabControl.pack()

        self.receiverLabel = ttk.Label(master=self.receiverTab, text="IP to receive from:")
        self.receiverLabel.pack()
        self.receiverInput = ttk.Entry(master=self.receiverTab)
        self.receiverInput.pack()
        self.receiverButton: tk.Button = tk.Button(master=self.receiverTab, text="START", command=self.openWindow)
        self.receiverButton.pack()

        self.senderButton: tk.Button = tk.Button(master=self.senderTab, text="START", command=self.startStreaming)
        self.senderButton.pack()

        self.topLevelWindow: Optional[tk.Toplevel] = None
        self.videoPlayer: Optional[TkinterVideo] = None
        self.sendingProcess: Sender = Sender()

        self.root.mainloop()

    def openWindow(self):
        self.receiverButton.config(text="STOP", command=self.closeWindow)

        self.topLevelWindow = tk.Toplevel()
        if self.receiverInput.get() != "":
            self.videoPlayer = TkinterVideo(master=self.topLevelWindow, kafkaAddress=self.receiverInput.get())
        else:
            self.videoPlayer = TkinterVideo(master=self.topLevelWindow)
        self.videoPlayer.pack(expand=True, fill="both")
        self.videoPlayer.play()

    def closeWindow(self):
        self.receiverButton.config(text="START", command=self.openWindow)
        self.videoPlayer.stop()
        self.topLevelWindow.destroy()

    def startStreaming(self):
        self.senderButton.config(text="STOP", command=self.stopStreaming)
        self.sendingProcess.start()

    def stopStreaming(self):
        self.senderButton.config(text="START", command=self.startStreaming)
        self.sendingProcess.stop()


class TkinterVideo(tk.Label):
    def __init__(self, master, kafkaAddress: str = "localhost:9092", keepAspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self.kafkaAddress = kafkaAddress
        self.streamConsumer: Optional[kafka.KafkaConsumer] = None
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
        producer = kafka.KafkaProducer(bootstrap_servers=self.kafkaAddress, acks=1)
        while self.streamRunning:
            time.sleep(0.1)
            inputs = self.inputsBuffer.get()
            if inputs != "":
                producer.send(INPUTS_TOPIC, inputs.encode())

        kafka.KafkaAdminClient(bootstrap_servers=self.kafkaAddress).delete_topics([INPUTS_TOPIC])

    def play(self):
        if self.streamReceiverThread is not None:
            self.streamReceiverThread.start()

        if self.displayContentThread is not None:
            self.displayContentThread.start()

        if self.sendInputsThread is not None:
            self.sendInputsThread.start()

    def resizeEvent(self, event):
        self.currentDisplaySize = event.width, event.height

    def initVideoStream(self, stream, audio_sample_rate):
        self.videoFramerate = stream.guessed_rate
        self.audioSamplerate = audio_sample_rate
        self.audioBlockSize = self.audioSamplerate // self.videoFramerate

        self.currentTkImage = ImageTk.PhotoImage(Image.new("RGB", (stream.width, stream.height), (255, 255, 255)))
        self.config(width=300, height=300, image=self.currentTkImage)

        self.audioStream = sd.OutputStream(channels=1, samplerate=self.audioSamplerate, dtype="int16",
                                           callback=self.audioCallback,
                                           blocksize=self.audioBlockSize)

    def displayContent(self):
        self.currentImage = self.videoFramesQueue.get(block=True)  # wait for the stream to init
        self.audioStream.start()
        rate = 1 / self.videoFramerate

        try:
            while self.streamRunning:
                start = time.time()

                self.currentImage = self.videoFramesQueue.get(timeout=2).to_image()
                self.event_generate("<<FrameGenerated>>")

                time.sleep(max(rate - (time.time() - start) % rate, 0))
        except Exception as ex:
            print(ex)

    def audioCallback(self, outdata: np.ndarray, frames: int, timet, status):
        try:
            data: np.ndarray = self.audioBlocksQueue.get_nowait()

            if len(data) < self.audioBlockSize:
                np.append(data, [0] * (self.audioBlockSize - len(data)))

        except queue.Empty:
            data = np.zeros(shape=(self.audioBlockSize, 1), dtype=np.int16)
        except Exception as ex:
            print("Error:", ex)
            return

        outdata[:] = data

    def receiveStream(self):
        self.streamConsumer = kafka.KafkaConsumer(TOPIC, bootstrap_servers=self.kafkaAddress, consumer_timeout_ms=2000)

        # init stream
        while self.streamRunning:
            try:
                message = next(self.streamConsumer)
            except StopIteration as ex:
                continue

            video, audio = self.getAudioVideoFromMessage(message)
            with av.open(BytesIO(video)) as container:
                self.initVideoStream(container.streams.video[0], self.getAudioSamplerateFromMessage(message))
            break

        # start stream
        while self.streamRunning:
            try:
                video, audio = self.getAudioVideoFromMessage(next(self.streamConsumer))
            except StopIteration as ex:
                continue

            audio.shape = (len(audio), 1)
            with av.open(BytesIO(video)) as container:
                container.fast_seek, container.discard_corrupt = True, True

                self.clearAudioAndVideoQueues()

                for frame in container.decode(video=0):
                    self.videoFramesQueue.put(item=frame, block=True)
                    self.audioBlocksQueue.put(
                        item=audio[frame.index * self.audioBlockSize: (frame.index + 1) * self.audioBlockSize],
                        block=True)

    def getAudioVideoFromMessage(self, message):
        audioStartIndex = None
        videoStartIndex = None

        for header in message.headers:
            if header[0] == "audio-start-index":
                audioStartIndex = int(header[1].decode())
            elif header[0] == "video-start-index":
                videoStartIndex = int(header[1].decode())

        if audioStartIndex > videoStartIndex:
            return message.value[videoStartIndex:audioStartIndex], \
                   pickle.loads(message.value[audioStartIndex:])

        return message.value[videoStartIndex:], \
               pickle.loads(message.value[audioStartIndex:videoStartIndex])

    def getAudioSamplerateFromMessage(self, message) -> Optional[int]:
        for header in message.headers:
            if header[0] == "audio-sample-rate":
                return int(header[1].decode())

        return None

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

        if self.sendInputsThread:
            self.sendInputsThread.join()

        if self.audioStream:
            self.audioStream.stop()

        if self.displayContentThread:
            self.displayContentThread.join()

        if self.streamReceiverThread:
            self.streamReceiverThread.join()


if __name__ == "__main__":
    MainWindow()
