import json
import tkinter as tk
from tkinter import filedialog
from Kafka.Kafka import *
from start import *
import customtkinter
from typing import Optional
from ControlWindow import TkinterVideo
from User import User

BACKGROUND = "#161616"
FRAME_BACKGROUND = "#1c1c1c"
TEXT_COLOR = "#FFFFFF"
BUTTON_BG = "#d5f372"
BUTTON_FG = "#000000"
DATABASE_TOPIC = "DATABASE"

MY_TOPIC = f"{uuid.uuid1()}"


class MainFrame(customtkinter.CTkFrame):
    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.user: Optional[User] = None
        self.kafkaAddress = None
        self.databaseProducer = None
        self.databaseConsumer = None
        self.sender: Optional[Sender] = None
        self.videoPlayer: Optional[TkinterVideo] = None
        self.videoPlayerWindow: Optional[tk.Toplevel] = None
        self.merger: Optional[Merger] = None
        self.mainWindow = parent
        self.kafkaConnection = None

        self.LABEL_FONT = customtkinter.CTkFont(size=20, family="Arial")

    def buildRemoteControlFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
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

        customtkinter.CTkLabel(master=div, text="YOUR ID:", font=self.LABEL_FONT, ).grid(row=0, column=0, padx=(50, 20), pady=(50, 0))
        customtkinter.CTkLabel(master=div, text=str(self.user.callKey), font=self.LABEL_FONT, ).grid(row=1, column=0, padx=(0, 50))

        customtkinter.CTkLabel(master=div, text="PASSWORD:", font=self.LABEL_FONT, ).grid(row=2, column=0, pady=(30, 20), padx=(0, 20))
        customtkinter.CTkLabel(master=div, text=str(self.user.callPassword), font=self.LABEL_FONT, ).grid(row=3, column=0)

        if self.user.sessionId is None and self.videoPlayerWindow is None:
            customtkinter.CTkButton(master=div, text="START SHARING", command=self.buttonStartSharing).grid(row=4, column=0, pady=(30, 50))
        elif self.videoPlayerWindow is not None:
            customtkinter.CTkButton(master=div, text="START SHARING", state=tk.DISABLED).grid(row=4, column=0, pady=(30, 50))
        else:
            customtkinter.CTkButton(master=div, text="STOP SHARING", command=self.buttonStopSharing).grid(row=4, column=0, pady=(30, 50))

        rightFrame = customtkinter.CTkFrame(self)
        rightFrame.pack(expand=True, fill=tk.BOTH, side=tk.RIGHT)
        rightFrame.pack_propagate(False)

        customtkinter.CTkLabel(master=rightFrame, text="Control Remote Computer", font=self.LABEL_FONT, ).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        divRight = customtkinter.CTkFrame(rightFrame)
        divRight.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        customtkinter.CTkLabel(master=divRight, text="USER ID:", font=self.LABEL_FONT, ).grid(row=0, column=0, padx=(0, 20))
        idEntry = customtkinter.CTkEntry(master=divRight, font=self.LABEL_FONT)
        idEntry.grid(row=0, column=2)

        customtkinter.CTkLabel(master=divRight, text="PASSWORD:", font=self.LABEL_FONT, ).grid(row=1, column=0, pady=(30, 0), padx=(0, 20))
        passwordEntry = customtkinter.CTkEntry(master=divRight, font=self.LABEL_FONT)
        passwordEntry.grid(row=1, column=2, pady=(30, 0))

        if self.videoPlayerWindow is not None:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.LABEL_FONT, state=tk.DISABLED).grid(row=2, column=1, pady=(30, 0))
        else:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.LABEL_FONT, command=lambda: self.buttonStartCall(idEntry.get(), passwordEntry.get())).grid(row=2, column=1, pady=(30, 0))

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

        if self.databaseProducer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            self.buildNotLoggedInFrame()
            return

        msg = self.databaseCall(MY_TOPIC, "GET_VIDEOS_BY_USER", json.dumps({"ID": self.user.id}).encode())
        data = json.loads(msg.value())

        if len(data) == 0:
            return

        div = customtkinter.CTkFrame(self)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        for i, video in enumerate(data):
            customtkinter.CTkLabel(master=div, text=video.get("CreatedAt"), ).grid(row=i, column=0)
            customtkinter.CTkButton(master=div, text="Download",
                      command=lambda videoId=video.get("ID"): self.downloadVideo(videoId)).grid(row=i, column=1)

    def buildLoginFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        if self.user is None:
            customtkinter.CTkLabel(master=div, text="Username:", font=self.LABEL_FONT, ).pack(pady=(50, 0))
            usernameEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
            usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

            customtkinter.CTkLabel(master=div, text="Password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
            passwordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
            passwordEntry.pack(pady=(10, 0), padx=(50, 50))

            customtkinter.CTkButton(master=div, text="SUBMIT", font=self.LABEL_FONT, command=lambda: self.login(usernameEntry.get(), passwordEntry.get())).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Logged in as {self.user.name}", font=self.LABEL_FONT, ).pack(pady=(50, 0), padx=50)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.LABEL_FONT, command=lambda: self.disconnect()).pack(pady=(30, 50))

    def buildRegisterFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        customtkinter.CTkLabel(master=div, text="Username:", font=self.LABEL_FONT).pack(pady=(50, 0))
        usernameEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
        usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

        customtkinter.CTkLabel(master=div, text="Password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
        passwordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
        passwordEntry.pack(pady=(10, 0))

        customtkinter.CTkLabel(master=div, text="Confirm password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
        confirmPasswordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
        confirmPasswordEntry.pack(pady=(10, 0))

        customtkinter.CTkButton(master=div, text="SUBMIT", font=self.LABEL_FONT,
                  command=lambda: self.registerNewAccount(usernameEntry.get(), passwordEntry.get(), confirmPasswordEntry.get())).pack(pady=(30, 50))

    def buildKafkaFrame(self):
        self.cleanFrame()

        div = customtkinter.CTkFrame(self)
        if self.databaseProducer is None:
            customtkinter.CTkLabel(master=div, text="Address:", font=self.LABEL_FONT).pack(padx=(100, 100), pady=(50, 0), anchor=tk.CENTER)
            entry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, width=200)
            entry.pack(pady=(10, 0), anchor=tk.CENTER)

            customtkinter.CTkButton(master=div, text="CONNECT", font=self.LABEL_FONT,
                      command=lambda: self.getKafkaConnection(entry.get() if entry.get() != "" else "localhost:9092")).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Connected to {self.kafkaAddress}").pack(pady=(50, 10), anchor=tk.CENTER)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.LABEL_FONT, command=lambda: self.buttonDisconnectFromKafka()).pack(pady=(30, 50), padx=50, anchor=tk.CENTER)

        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

    def buildNotConnectedToKafkaFrame(self):
        customtkinter.CTkLabel(master=self, text="Connect to kafka", font=self.LABEL_FONT).pack(anchor=tk.CENTER)

    def buildNotLoggedInFrame(self):
        customtkinter.CTkLabel(master=self, text="Log In", font=self.LABEL_FONT).pack(anchor=tk.CENTER)

















    def startSharing(self):
        msg = self.databaseCall(topic=MY_TOPIC, operation="CREATE_SESSION", message=json.dumps({
            "AggregatorTopic": self.sender.aggregatorTopic,
            "InputsTopic": self.sender.inputsTopic,
            "MergerTopic": self.merger.topic,
            "UserID": str(self.user.id),
        }).encode())

        status: Optional[str] = None
        for header in msg.headers():
            if header[0] == "status":
                status = header[1].decode()
        if status is None or status.lower() != "ok":
            self.setStatusMessage("Cannot start sharing")
            return

        sessionId = int(msg.value().decode())
        self.user.sessionId = sessionId

        if self.sender is not None:
            self.sender.start(self.merger.topic)

        if self.merger is not None:
            self.merger.start(str(self.user.sessionId))

    def stopSharing(self):
        if self.sender is not None:
            self.sender.stop()

        if self.merger is not None:
            self.merger.stop()

        msg = self.databaseCall(topic=MY_TOPIC, operation="DELETE_SESSION", message=json.dumps({
            "SessionId": str(self.user.sessionId),
            "UserId": str(self.user.id),
        }).encode())

        self.user.sessionId = None
        self.setStatusMessage("Call stopped")

    def startCall(self, callKey: str, callPassword: str):
        if callKey == "" or callPassword == "":
            self.setStatusMessage("provide both key and password")
            return

        responseMessage = self.databaseCall(MY_TOPIC, "GET_CALL_BY_KEY", json.dumps({"Key": callKey, "Password": callPassword, "CallerId": str(self.user.id)}).encode())

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
        aggregatorTopic = responseValue["AggregatorTopic"]
        inputsTopic = responseValue["InputsTopic"]

        self.videoPlayerWindow = tk.Toplevel()
        self.videoPlayer = TkinterVideo(master=self.videoPlayerWindow, aggregatorTopic=aggregatorTopic, inputsTopic=inputsTopic, kafkaAddress=self.kafkaAddress)
        self.videoPlayer.pack(expand=True, fill="both")
        self.videoPlayer.play()
        self.videoPlayerWindow.protocol("WM_DELETE_WINDOW", self.exitCallWindow)

    def stopCall(self):
        if self.videoPlayer is not None:
            self.videoPlayer.stop()
        self.videoPlayer = None

        if self.videoPlayerWindow is not None:
            self.videoPlayerWindow.destroy()
        self.videoPlayerWindow = None

    def cleanFrame(self):
        for widget in self.winfo_children():
            widget.destroy()

    def databaseCall(self, topic: str, operation: str, message: bytes, bigFile=False) -> kafka.Message:
        self.databaseProducer.produce(topic=DATABASE_TOPIC, headers=[
            ("topic", topic.encode()), ("operation", operation.encode()),
        ], value=message)

        return self.databaseConsumer.receiveBigMessage() if bigFile else next(self.databaseConsumer)

    def getKafkaConnection(self, address: str):
        if not checkKafkaActive(address):
            self.setStatusMessage("Kafka broker not active")
            return

        createTopic(address, MY_TOPIC)

        self.kafkaAddress = address
        self.databaseProducer = KafkaProducerWrapper({'bootstrap.servers': self.kafkaAddress})
        self.databaseConsumer = KafkaConsumerWrapper({
                'bootstrap.servers': self.kafkaAddress,
                'group.id': str(uuid.uuid1()),
                'auto.offset.reset': 'latest',
                'allow.auto.create.topics': "true",
            }, [MY_TOPIC])

        self.setStatusMessage("Connected to Kafka")
        self.buildKafkaFrame()

    def resetKafkaConnection(self):
        self.databaseProducer = None

        if self.databaseConsumer is not None:
            self.databaseConsumer.close()
        self.databaseConsumer = None

    def buttonDisconnectFromKafka(self):
        self.resetKafkaConnection()
        self.buildKafkaFrame()

    def setStatusMessage(self, message: str):
        self.mainWindow.setStatusMessage(message)

    def exitCallWindow(self):
        self.stopCall()

    def downloadVideo(self, videoId: int):
        file = filedialog.asksaveasfile(mode="wb", defaultextension=".mp4")
        if file is None:
            return

        msg = self.databaseCall(MY_TOPIC, "DOWNLOAD_VIDEO_BY_ID", str(videoId).encode(), bigFile=True)
        file.write(msg.value())
        file.close()

    def login(self, username: str, password: str):
        if username == "" or password == "":
            self.setStatusMessage("Need both name and pass")
            return

        loggInDetails = {"Name": username, "Password": password}
        message = self.databaseCall(MY_TOPIC, "LOGIN", json.dumps(loggInDetails).encode())

        status = None
        for header in message.headers():
            if header[0] == "status":
                status = header[1].decode()

        if status.lower() != "ok":
            self.setStatusMessage("username or password wrong")
            return

        message = json.loads(message.value())
        self.user = User(message.get("ID", None), username, message.get("CallKey", None), message.get("CallPassword", None), message.get("SessionId", None))

        self.sender = Sender(self.kafkaAddress)
        self.merger = Merger(self.kafkaAddress)

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
            self.setStatusMessage("password and confimation are not the same")
            return

        message = self.databaseCall(MY_TOPIC, "REGISTER", json.dumps({"Name": username, "Password": password}).encode())

        status: Optional[str] = None
        for header in message.headers():
            if header[0] == "status":
                status = header[1].decode()

        if status.lower() == "ok":
            self.setStatusMessage("Account registered")
            self.buildRegisterFrame()
        else:
            self.setStatusMessage("Cannot create account")
