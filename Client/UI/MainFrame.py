import json
import tkinter as tk
from tkinter import filedialog
from start import *
import customtkinter
from ControlWindow import VideoWindow
from User import User
from KafkaContainer import KafkaContainer
from typing import *
from concurrent.futures import ThreadPoolExecutor
from Client.UI.FrameBuilder import FrameBuilder


class MainFrame(customtkinter.CTkFrame):
    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.mainWindow = parent
        self.user: Optional[User] = None
        self.kafkaContainer: Optional[KafkaContainer] = None
        self.threadPool = ThreadPoolExecutor(4)
        self.sender: Optional[Sender] = None
        self.videoWindow: Optional[VideoWindow] = None
        self.frameBuilder = FrameBuilder(self)

    def buttonStartCall(self, callKey: str, callPassword: str):
        self.startCall(callKey, callPassword)
        self.frameBuilder.buildRemoteControlFrame()

    def buttonStartSharing(self):
        self.startSharing()
        self.frameBuilder.buildRemoteControlFrame()

    def buttonStopSharing(self):
        self.stopSharing()
        self.frameBuilder.buildRemoteControlFrame()

    def startSharing(self):
        msg = self.kafkaContainer.databaseCall(operation="CREATE_SESSION", message=json.dumps({
            "Topic": self.kafkaContainer.topic,
            "UserID": str(self.user.id),
        }).encode(), timeoutSeconds=5, username=self.user.name, password=self.user.password)

        if msg is None:
            self.setStatusMessage("Cannot start sharing")
            return

        status = KafkaContainer.getStatusFromMessage(msg)
        if status is None or status.lower() != "ok":
            self.setStatusMessage("Cannot start sharing")
            return

        sId = msg.value().decode()
        self.user.sessionId = int(sId)

        if self.sender is not None:
            self.sender.start(self.kafkaContainer.topic)

        Merger(self.kafkaContainer.address, self.kafkaContainer.topic, self.user.sessionId)

    def stopSharing(self):
        if self.sender is not None:
            self.sender.stop()

        self.user.sessionId = None
        self.kafkaContainer.resetTopic()
        self.setStatusMessage("Call stopped")

    def startCall(self, callKey: str, callPassword: str):
        if callKey == "" or callPassword == "":
            self.setStatusMessage("provide both key and password")
            return

        responseMessage = self.kafkaContainer.databaseCall("GET_CALL_BY_KEY", json.dumps({"Key": callKey, "Password": callPassword, "CallerId": str(self.user.id)}).encode(),
                                                           username=self.user.name, password=self.user.password)
        if responseMessage is None:
            self.setStatusMessage("Cannot start call, database not responding")
            return

        status = self.kafkaContainer.getStatusFromMessage(responseMessage)
        if status.lower() != "ok":
            self.setStatusMessage(f"Cannot start call, {status}")
            return

        responseValue = json.loads(responseMessage.value())
        topic = responseValue["Topic"]

        self.videoWindow = VideoWindow(topic=topic, kafkaAddress=self.kafkaContainer.address)
        self.videoWindow.protocol("WM_DELETE_WINDOW", self.stopCall)

    def stopCall(self):
        if self.videoWindow is not None:
            self.videoWindow.stop()
            self.videoWindow = None

    def cleanFrame(self):
        for widget in self.winfo_children():
            widget.destroy()

    def getKafkaConnection(self, address: str):
        try:
            self.kafkaContainer = KafkaContainer(address, {
                'auto.offset.reset': 'latest',
                'allow.auto.create.topics': "true",
            })
            self.setStatusMessage("Connected to Kafka")
        except Exception as ex:
            self.setStatusMessage(f"Cannot connect to kafka, {ex}")

        self.frameBuilder.buildKafkaFrame()

    def resetKafkaConnection(self):
        self.kafkaContainer = None

    def buttonDisconnectFromKafka(self):
        self.resetKafkaConnection()
        self.frameBuilder.buildKafkaFrame()

    def setStatusMessage(self, message: str):
        self.mainWindow.setStatusMessage(message)

    def downloadVideoButtonPressed(self, videoId: int):
        if self.threadPool._work_queue.qsize() > 0:
            self.setStatusMessage("Another video is downloading")
            return

        self.threadPool.submit(lambda: self.downloadVideo(videoId))

    def downloadVideo(self, videoId: int):
        file = filedialog.asksaveasfile(mode="wb", defaultextension=".mp4")
        if file is None:
            return

        msg = self.kafkaContainer.databaseCall("DOWNLOAD_VIDEO_BY_ID", str(videoId).encode(), username=self.user.name, password=self.user.password)
        if msg is None:
            self.setStatusMessage("Cannot download video, database not responding")
            return

        status = self.kafkaContainer.getStatusFromMessage(msg)
        if status.lower() != "ok":
            self.setStatusMessage(f"Download video, {status}")
            return

        file.write(msg.value())
        file.close()

        self.setStatusMessage("Video downloaded")

    def login(self, username: str, password: str):
        if username == "" or password == "":
            self.setStatusMessage("Need both name and pass")
            return

        loggInDetails = {"Name": username, "Password": password}
        message = self.kafkaContainer.databaseCall("LOGIN", json.dumps(loggInDetails).encode(), timeoutSeconds=5)
        if message is None:
            self.setStatusMessage("Cannot talk to the database")
            return

        if KafkaContainer.getStatusFromMessage(message).lower() != "ok":
            self.setStatusMessage("username or password wrong")
            return

        message = json.loads(message.value())
        self.user = User(message.get("Id", None), username, password, message.get("CallKey", None), message.get("CallPassword", None), message.get("SessionId", None))

        self.sender = Sender(self.kafkaContainer.address)

        self.frameBuilder.buildLoginFrame()
        self.mainWindow.title(f"RMI {self.user.name}")
        self.setStatusMessage("Logged in")

    def disconnect(self):
        self.user = None

        if self.sender is not None and self.sender.running:
            self.sender.stop()

        self.frameBuilder.buildLoginFrame()

    def registerNewAccount(self, username: str, password: str, confirmPassword: str):
        if username == "" or password == "" or confirmPassword == "":
            self.setStatusMessage("All fields must be filled")
            return

        if password != confirmPassword:
            self.setStatusMessage("password and confirmation are not the same")
            return

        message = self.kafkaContainer.databaseCall("REGISTER", json.dumps({"Name": username, "Password": password}).encode(), timeoutSeconds=1)
        if message is None:
            self.setStatusMessage("Cannot talk with the database")

        if KafkaContainer.getStatusFromMessage(message).lower() == "ok":
            self.setStatusMessage("Account registered")
            self.frameBuilder.buildRegisterFrame()
        else:
            self.setStatusMessage("Cannot create account")
