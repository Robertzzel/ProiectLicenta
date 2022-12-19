import queue
from start import Sender
import av
import time
import threading
from typing import Dict, List
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
import json
from Kafka.Kafka import KafkaConsumerWrapper

TOPIC = "aggregator"
START_TOPIC = "saggregator"
logging.getLogger('libav').setLevel(logging.ERROR)  # removes warning: deprecated pixel format used
MOVE = 1
CLICK = 2
PRESS = 4
RELEASE = 5
INPUTS_TOPIC = "inputs"
DATABASE_TOPIC = "DATABASE"


class KafkaWindow:
    def __init__(self):
        self.root = tk.Tk()

        self.label = ttk.Label(master=self.root, text="Kafka Address:")
        self.label.pack()
        self.input = ttk.Entry(master=self.root)
        self.input.pack()
        self.button: ttk.Button = tk.Button(master=self.root, text="SUBMIT", command=self.openMainWindow)
        self.button.pack()

        self.root.mainloop()

    def checkKafkaAddress(self, address) -> bool:
        try:
            kafka.KafkaAdminClient(bootstrap_servers=address)
        except Exception as ex:
            return False
        return True

    def openMainWindow(self):
        address = self.input.get() if self.input.get() != "" else "localhost:9092"
        if self.checkKafkaAddress(address):
            self.root.destroy()
            MainWindow(address)
        else:
            self.notValidLabel = ttk.Label(master=self.root, text="Broker not available on this address")
            self.notValidLabel.pack()


class MainWindow:
    def __init__(self, kafkaAddress: str):
        self.root = tk.Tk()

        self.kafkaAddress = kafkaAddress
        self.databaseProducer = kafka.KafkaProducer(bootstrap_servers=self.kafkaAddress, acks=1)
        self.databaseConsumer = KafkaConsumerWrapper("UI", bootstrap_servers=self.kafkaAddress, auto_offset_reset="earliest")
        partition = kafka.TopicPartition("UI", 0)
        end_offset = self.databaseConsumer.end_offsets([partition])
        self.databaseConsumer.seek(partition, list(end_offset.values())[0])

        self.loggedInUserID = None
        self.statusLabel = ttk.Label(master=self.root, text="")
        self.statusLabel.pack()
        self.tabControl = ttk.Notebook(self.root)
        self.loginTab = tk.Frame(self.tabControl)
        self.receiverTab = tk.Frame(self.tabControl)
        self.senderTab = tk.Frame(self.tabControl)
        self.videosTab = tk.Frame(self.tabControl)
        ttk.Label(master=self.videosTab, text="Log In to see your videos").pack(expand=True, fill="both")
        self.tabControl.add(child=self.loginTab, text="Login")
        self.tabControl.add(child=self.receiverTab, text="Receive")
        self.tabControl.add(child=self.senderTab, text="Send")
        self.tabControl.add(child=self.videosTab, text="My Videos")
        self.tabControl.pack()

        self.receiverButton: tk.Button = tk.Button(master=self.receiverTab, text="START", command=self.openWindow)
        self.receiverButton.pack()

        self.senderButton: tk.Button = tk.Button(master=self.senderTab, text="START", command=self.startStreaming)
        self.senderButton.pack()

        self.configureLoginTab()

        self.topLevelWindow: Optional[tk.Toplevel] = None
        self.videoPlayer: Optional[TkinterVideo] = None
        self.sendingProcess: Sender = Sender(brokerAddress=kafkaAddress)
        self.tabControl.bind('<<NotebookTabChanged>>', self.on_tab_change)

        self.root.mainloop()

    def on_tab_change(self, event):
        if event.widget.tab('current')['text'] != "My Videos" or self.loggedInUserID is None:
            return

        msg = self.databaseCall("UI", "READ_VIDEOS", "users", json.dumps({"ID": self.loggedInUserID}), b"")
        videos: List[Dict] = json.loads(msg.value)

        for widget in self.videosTab.winfo_children():
            widget.destroy()

        for i, video in enumerate(videos):
            print(video.get("ID"))
            ttk.Label(master=self.videosTab, text=video.get("CreatedAt")).grid(row=i, column=0)
            ttk.Button(master=self.videosTab, text="Download", command=lambda id=video.get("ID"): self.downloadVideo(id)).grid(row=i, column=1)

    def downloadVideo(self, videoId: int):
        print(videoId)
        msg, headers = self.databaseCall("UI", "READ", "videos", json.dumps({"ID": videoId}), b"", bigFile=True)
        with open("downloadedVideo.mp4", "wb") as f:
            f.write(msg)

    def databaseCall(self, topic: str, operation: str, table: str, inp: str, message: bytes, bigFile=False):
        self.databaseProducer.send(topic=DATABASE_TOPIC, headers=[
            ("topic", topic.encode()),
            ("operation", operation.encode()),
            ("table", table.encode()),
            ("input", inp.encode())
        ], value=message)

        if bigFile:
            return self.databaseConsumer.receiveBigMessage()
        return next(self.databaseConsumer)

    def configureLoginTab(self):
        self.loginUsernameLabel: ttk.Label = ttk.Label(master=self.loginTab, text="Username:")
        self.loginUsernameLabel.pack()
        self.loginUsernameEntry: ttk.Entry = ttk.Entry(master=self.loginTab)
        self.loginUsernameEntry.pack()
        self.loginPasswordLabel: ttk.Label = ttk.Label(master=self.loginTab, text="Password:")
        self.loginPasswordLabel.pack()
        self.loginPasswordEntry: ttk.Entry = ttk.Entry(master=self.loginTab)
        self.loginPasswordEntry.pack()
        self.loginButton: ttk.Button = tk.Button(master=self.loginTab, text="LOGIN", command=self.login)
        self.loginButton.pack()

    def login(self):
        if self.loginUsernameEntry.get() == "" or self.loginPasswordEntry.get() == "":
            self.statusLabel.config(text="please provide both name and password")
            return

        loggInDetails = {
            "Name": self.loginUsernameEntry.get(),
            "Password": self.loginPasswordEntry.get(),
        }

        message = self.databaseCall("UI", "READ", "users", json.dumps(loggInDetails), b"")

        for header in message.headers:
            if header[0] == "status":
                status = header[1].decode()

        if status.lower() == "ok":
            self.loggedInUserID = int(message.value.decode())

        if self.loggedInUserID is None:
            self.statusLabel.config(text="Username or Password wrong")
        else:
            self.statusLabel.config(text=f"Logged In As {self.loginUsernameEntry.get()}")

    def openWindow(self):
        if self.loggedInUserID is None:
            self.statusLabel.config(text="Not logged in")
            return
        else:
            self.statusLabel.config(text="")

        self.receiverButton.config(text="STOP", command=self.closeWindow)

        self.topLevelWindow = tk.Toplevel()
        self.videoPlayer = TkinterVideo(master=self.topLevelWindow, kafkaAddress=self.kafkaAddress, userId=self.loggedInUserID)

        self.videoPlayer.pack(expand=True, fill="both")
        self.videoPlayer.play()

    def closeWindow(self):
        self.receiverButton.config(text="START", command=self.openWindow)
        self.videoPlayer.stop()
        self.topLevelWindow.destroy()

    def startStreaming(self):
        if self.loggedInUserID is None:
            self.statusLabel.config(text="Not logged in")
            return
        else:
            self.statusLabel.config(text="")

        self.senderButton.config(text="STOP", command=self.stopStreaming)
        self.sendingProcess.start()

    def stopStreaming(self):
        self.senderButton.config(text="START", command=self.startStreaming)
        self.sendingProcess.stop()


class TkinterVideo(tk.Label):
    def __init__(self, master, userId: int, kafkaAddress: str = "localhost:9092", keepAspect: bool = False, *args, **kwargs):
        super(TkinterVideo, self).__init__(master, *args, **kwargs)

        self.userId = userId
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
        self.streamConsumer = kafka.KafkaConsumer(bootstrap_servers=self.kafkaAddress, consumer_timeout_ms=2000)
        self.streamConsumer.assign([kafka.TopicPartition(TOPIC, 0)])

        producer = kafka.KafkaProducer(bootstrap_servers=self.kafkaAddress, acks=1)
        producer.send(topic=START_TOPIC, value=str(self.userId).encode())
        print("Signal sent")

        while self.streamRunning:
            try:
                message = next(self.streamConsumer).value
            except StopIteration:
                continue
            with av.open(BytesIO(message)) as container:
                self.initVideoStream(container)
            break

        while self.streamRunning:
            try:
                with av.open(BytesIO(next(self.streamConsumer).value)) as container:
                    container.fast_seek, container.discard_corrupt = True, True

                    self.clearAudioAndVideoQueues()

                    for packet in container.demux():
                        for frame in packet.decode():
                            if isinstance(frame, av.VideoFrame):
                                self.videoFramesQueue.put(item=frame, block=True)
                            elif isinstance(frame, av.AudioFrame):
                                self.audioBlocksQueue.put(item=frame.to_ndarray(), block=True)
            except StopIteration:
                continue
            except Exception as ex:
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


if __name__ == "__main__":
    KafkaWindow()
