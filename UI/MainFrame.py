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
from tkinter import ttk


class MainFrame(customtkinter.CTkFrame):
    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.mainWindow = parent
        self.user: Optional[User] = None
        self.kafkaContainer: Optional[KafkaContainer] = None
        self.threadPool = ThreadPoolExecutor(4)
        self.sender: Optional[Sender] = None
        self.videoWindow: Optional[VideoWindow] = None
        self.merger: Optional[Merger] = None

        self.LABEL_FONT = customtkinter.CTkFont(size=10, family="Arial")

    def buildRemoteControlFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            self.buildNotLoggedInFrame()
            return

        leftFrame = customtkinter.CTkFrame(self)
        leftFrame.pack(expand=True, fill=tk.BOTH, side=tk.LEFT)
        leftFrame.pack_propagate(False)

        customtkinter.CTkLabel(master=leftFrame, text="Allow Remote Control", font=self.LABEL_FONT, ).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        div = customtkinter.CTkFrame(leftFrame)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        customtkinter.CTkLabel(master=div, text="YOUR ID:", font=self.LABEL_FONT).grid(row=0, column=0, padx=(50, 20), pady=(50, 0))
        customtkinter.CTkEntry(master=div, height=1, state="readonly", width=250, textvariable=tk.StringVar(value=str(self.user.callKey))).grid(row=1, column=0, padx=(50, 50))

        customtkinter.CTkLabel(master=div, text="PASSWORD:", font=self.LABEL_FONT, ).grid(row=2, column=0, pady=(30, 20), padx=(0, 20))
        customtkinter.CTkEntry(master=div, height=1, width=250, state="readonly", textvariable=tk.StringVar(value=str(self.user.callPassword))).grid(row=3, column=0)

        if self.user.sessionId is None and self.videoWindow is None:
            customtkinter.CTkButton(master=div, text="START SHARING", command=self.buttonStartSharing).grid(row=4, column=0, pady=(30, 50))
        elif self.videoWindow is not None:
            customtkinter.CTkButton(master=div, text="START SHARING", state=tk.DISABLED).grid(row=4, column=0, pady=(30, 50))
        else:
            customtkinter.CTkButton(master=div, text="STOP SHARING", command=self.buttonStopSharing).grid(row=4, column=0, pady=(30, 50))

        rightFrame = customtkinter.CTkFrame(self)
        rightFrame.pack(expand=True, fill=tk.BOTH, side=tk.RIGHT)
        rightFrame.pack_propagate(False)

        customtkinter.CTkLabel(master=rightFrame, text="Control Remote Computer", font=self.LABEL_FONT).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        divRight = customtkinter.CTkFrame(rightFrame)
        divRight.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        customtkinter.CTkLabel(master=divRight, text="USER ID:", font=self.LABEL_FONT).grid(row=0, column=0, padx=(0, 5), pady=(20, 0))
        idEntry = customtkinter.CTkEntry(master=divRight, font=self.LABEL_FONT, width=250)
        idEntry.grid(row=0, column=2, pady=(20, 0), padx=(0, 20))

        customtkinter.CTkLabel(master=divRight, text="PASSWORD:", font=self.LABEL_FONT).grid(row=1, column=0, pady=(30, 0), padx=(20, 5))
        passwordEntry = customtkinter.CTkEntry(master=divRight, font=self.LABEL_FONT, width=250)
        passwordEntry.grid(row=1, column=2, pady=(30, 0), padx=(0, 20))

        if self.videoWindow is not None:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.LABEL_FONT, state=tk.DISABLED).grid(row=2, column=1, pady=(30, 20))
        else:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.LABEL_FONT, command=lambda: self.buttonStartCall(idEntry.get(), passwordEntry.get())).grid(row=2, column=1, pady=(30, 20))

    def buttonStartCall(self, callKey: str, callPassword: str):
        self.startCall(callKey, callPassword)
        self.buildRemoteControlFrame()

    def buttonStartSharing(self):
        self.startSharing()
        self.buildRemoteControlFrame()

    def buttonStopSharing(self):
        self.stopSharing()
        self.buildRemoteControlFrame()

    def buildMyVideosFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            self.buildNotLoggedInFrame()
            return

        msg = self.kafkaContainer.databaseCall(self.kafkaContainer.topic, "GET_VIDEOS_BY_USER", json.dumps({"ID": self.user.id}).encode(), timeoutSeconds=1)
        data = json.loads(msg.value())

        if len(data) == 0:
            return

        div = customtkinter.CTkFrame(self)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        for i, video in enumerate(data):
            customtkinter.CTkLabel(master=div, text=f"DURATION: {video.get('DurationInSeconds')} secs").grid(row=i, column=0, padx=(0, 30), pady=(10, 10))
            customtkinter.CTkLabel(master=div, text=f"SIZE: {video.get('Size')} bytes").grid(row=i, column=1, padx=(0, 30), pady=(10, 10))
            date = video.get("CreatedAt")
            customtkinter.CTkLabel(master=div, text=f"CREATED: {date[:10]} {date[11:-1]}").grid(row=i, column=2, padx=(0, 20), pady=(10, 10))
            customtkinter.CTkButton(master=div, text="Download",
                                    command=lambda videoId=video.get("ID"): self.downloadVideo(videoId)).grid(row=i, column=3, pady=(10, 10))

    def buildLoginFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        if self.user is None:
            customtkinter.CTkLabel(master=div, text="Username:", font=self.LABEL_FONT, ).pack(pady=(50, 0))
            usernameEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
            usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

            customtkinter.CTkLabel(master=div, text="Password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
            passwordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, show="*")
            passwordEntry.pack(pady=(10, 0), padx=(50, 50))

            customtkinter.CTkButton(master=div, text="SUBMIT", font=self.LABEL_FONT, command=lambda: self.login(usernameEntry.get(), passwordEntry.get())).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Logged in as {self.user.name}", font=self.LABEL_FONT, ).pack(pady=(50, 0), padx=50)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.LABEL_FONT, command=lambda: self.disconnect()).pack(pady=(30, 50))

    def buildRegisterFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        customtkinter.CTkLabel(master=div, text="Username:", font=self.LABEL_FONT).pack(pady=(50, 0))
        usernameEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
        usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

        customtkinter.CTkLabel(master=div, text="Password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
        passwordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, show="*")
        passwordEntry.pack(pady=(10, 0))

        customtkinter.CTkLabel(master=div, text="Confirm password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
        confirmPasswordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, show="*")
        confirmPasswordEntry.pack(pady=(10, 0))

        customtkinter.CTkButton(master=div, text="SUBMIT", font=self.LABEL_FONT,
                                command=lambda: self.registerNewAccount(usernameEntry.get(), passwordEntry.get(), confirmPasswordEntry.get())).pack(pady=(30, 50))

    def buildKafkaFrame(self):
        self.cleanFrame()

        div = customtkinter.CTkFrame(self)
        if self.kafkaContainer is None:
            customtkinter.CTkLabel(master=div, text="Address:", font=self.LABEL_FONT).pack(padx=(100, 100), pady=(50, 0), anchor=tk.CENTER)
            entry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, width=200)
            entry.pack(pady=(10, 0), anchor=tk.CENTER)

            customtkinter.CTkButton(master=div, text="CONNECT", font=self.LABEL_FONT,
                                    command=lambda: self.getKafkaConnection(entry.get() if entry.get() != "" else "localhost:9092")).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Connected to {self.kafkaContainer.address}").pack(pady=(50, 10), anchor=tk.CENTER)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.LABEL_FONT, command=lambda: self.buttonDisconnectFromKafka()).pack(pady=(30, 50), padx=50, anchor=tk.CENTER)

        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

    def buildNotConnectedToKafkaFrame(self):
        customtkinter.CTkLabel(master=self, text="Connect to kafka", font=self.LABEL_FONT).pack(anchor=tk.CENTER)

    def buildNotLoggedInFrame(self):
        customtkinter.CTkLabel(master=self, text="Log In", font=self.LABEL_FONT).pack(anchor=tk.CENTER)

    def startSharing(self):
        msg = self.kafkaContainer.databaseCall(topic=self.kafkaContainer.topic, operation="CREATE_SESSION", message=json.dumps({
            "Topic": self.kafkaContainer.topic,
            "UserID": str(self.user.id),
        }).encode())

        status = KafkaContainer.getStatusFromMessage(msg)
        if status is None or status.lower() != "ok":
            self.setStatusMessage("Cannot start sharing")
            return

        self.user.sessionId = int(msg.value().decode())

        if self.sender is not None:
            self.sender.start(self.kafkaContainer.topic)

        if self.merger is not None:
            self.merger.start(str(self.user.sessionId), self.kafkaContainer.topic)

    def stopSharing(self):
        if self.sender is not None:
            self.sender.stop()

        if self.merger is not None:
            self.merger.stop()

        self.kafkaContainer.databaseCall(topic=self.kafkaContainer.topic, operation="DELETE_SESSION", message=json.dumps({
            "SessionId": str(self.user.sessionId),
            "UserId": str(self.user.id),
        }).encode())

        self.user.sessionId = None
        self.kafkaContainer.resetTopic()
        self.setStatusMessage("Call stopped")

    def startCall(self, callKey: str, callPassword: str):
        if callKey == "" or callPassword == "":
            self.setStatusMessage("provide both key and password")
            return

        responseMessage = self.kafkaContainer.databaseCall(self.kafkaContainer.topic, "GET_CALL_BY_KEY", json.dumps({"Key": callKey, "Password": callPassword, "CallerId": str(self.user.id)}).encode())

        status: Optional[str] = None
        for header in responseMessage.headers():
            if header[0] == "status":
                status = header[1].decode()
        if status is None or status.lower() != "ok":
            self.setStatusMessage("Cannot start call")
            return

        if responseMessage.value() == b"NOT ACTIVE":
            self.setStatusMessage("User not active")
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

        self.buildKafkaFrame()

    def resetKafkaConnection(self):
        self.kafkaContainer = None

    def buttonDisconnectFromKafka(self):
        self.resetKafkaConnection()
        self.buildKafkaFrame()

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

        msg = self.kafkaContainer.databaseCall(self.kafkaContainer.topic, "DOWNLOAD_VIDEO_BY_ID", str(videoId).encode())
        if msg is None:
            self.setStatusMessage("Failed to download the video")
            return

        file.write(msg.value())
        file.close()

        self.setStatusMessage("Video downloaded")

    def login(self, username: str, password: str):
        if username == "" or password == "":
            self.setStatusMessage("Need both name and pass")
            return

        loggInDetails = {"Name": username, "Password": password}
        message = self.kafkaContainer.databaseCall(self.kafkaContainer.topic, "LOGIN", json.dumps(loggInDetails).encode(), timeoutSeconds=1.5)
        if message is None:
            self.setStatusMessage("Cannot talk to the database")
            return

        if KafkaContainer.getStatusFromMessage(message).lower() != "ok":
            self.setStatusMessage("username or password wrong")
            return

        message = json.loads(message.value())
        self.user = User(message.get("ID", None), username, message.get("CallKey", None), message.get("CallPassword", None), message.get("SessionId", None))

        self.sender = Sender(self.kafkaContainer.address)
        self.merger = Merger(self.kafkaContainer.address)

        self.buildLoginFrame()
        self.mainWindow.title(f"RMI {self.user.name}")
        self.setStatusMessage("Logged in")

    def disconnect(self):
        self.user = None

        if self.sender is not None and self.sender.running:
            self.sender.stop()

        if self.merger is not None and self.merger.running:
            self.merger.stop()

        self.buildLoginFrame()

    def registerNewAccount(self, username: str, password: str, confirmPassword: str):
        if username == "" or password == "" or confirmPassword == "":
            self.setStatusMessage("All fields must be filled")
            return

        if password != confirmPassword:
            self.setStatusMessage("password and confirmation are not the same")
            return

        message = self.kafkaContainer.databaseCall(self.kafkaContainer.topic, "REGISTER", json.dumps({"Name": username, "Password": password}).encode(), timeoutSeconds=1)
        if message is None:
            self.setStatusMessage("Cannot talk with the database")

        if KafkaContainer.getStatusFromMessage(message).lower() == "ok":
            self.setStatusMessage("Account registered")
            self.buildRegisterFrame()
        else:
            self.setStatusMessage("Cannot create account")
