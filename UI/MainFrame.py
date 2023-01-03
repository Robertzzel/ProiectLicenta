import json
import tkinter as tk
from tkinter import filedialog
from Kafka.Kafka import *
from start import *
from typing import Optional
from ControlWindow import TkinterVideo
from User import User
import threading

WHITE = "white"
DATABASE_TOPIC = "DATABASE"
MY_TOPIC = f"{uuid.uuid1()}"


class MainFrame(tk.Frame):
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

    def buildRemoteControlFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            self.buildNotLoggedInFrame()
            return

        leftFrame = tk.Frame(self, background=WHITE, borderwidth=0.5, relief="solid")
        leftFrame.pack(expand=True, fill=tk.BOTH, side=tk.LEFT)
        leftFrame.pack_propagate(False)

        tk.Label(master=leftFrame, text="Allow Remote Control", background=WHITE, font=("Arial", 17)).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        div = tk.Frame(leftFrame, background=WHITE)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        tk.Label(master=div, text="YOUR ID:", font=("Arial", 17), background=WHITE).grid(row=0, column=0, padx=(0, 20))
        tk.Label(master=div, text=str(self.user.callKey), font=("Arial", 17),background=WHITE).grid(row=1, column=0)

        tk.Label(master=div, text="PASSWORD:", font=("Arial", 17), background=WHITE).grid(row=2, column=0, pady=(30, 0), padx=(0, 20))
        tk.Label(master=div, text=str(self.user.callPassword), font=("Arial", 17), background=WHITE).grid(row=3, column=0)

        if self.user.sessionId is None:
            tk.Button(master=div, text="START SHARING", command=self.buttonStartSharing).grid(row=4, column=0, pady=(30, 0))
        elif self.videoPlayerWindow is not None:
            tk.Button(master=div, text="START SHARING", state=tk.DISABLED).grid(row=4, column=0, pady=(30, 0))
        else:
            tk.Button(master=div, text="STOP SHARING", command=self.buttonStopSharing).grid(row=4, column=0, pady=(30, 0))

        rightFrame = tk.Frame(self, background=WHITE, borderwidth=0.5, relief="solid")
        rightFrame.pack(expand=True, fill=tk.BOTH, side=tk.RIGHT)
        rightFrame.pack_propagate(False)

        tk.Label(master=rightFrame, text="Control Remote Computer", background=WHITE, font=("Arial", 17)).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        divRight = tk.Frame(rightFrame, background=WHITE)
        divRight.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        tk.Label(master=divRight, text="USER ID:", font=("Arial", 17), background=WHITE).grid(row=0, column=0, padx=(0, 20))
        idEntry = tk.Entry(master=divRight, font=("Arial", 17))
        idEntry.grid(row=0, column=2)

        tk.Label(master=divRight, text="PASSWORD:", font=("Arial", 17), background=WHITE).grid(row=1, column=0, pady=(30, 0), padx=(0, 20))
        passwordEntry = tk.Entry(master=divRight, font=("Arial", 17))
        passwordEntry.grid(row=1, column=2, pady=(30, 0))

        tk.Button(master=divRight, text="SUBMIT", font=("Arial", 17), command=lambda: self.buttonStartCall(idEntry.get(), passwordEntry.get())).grid(row=2, column=1, pady=(30, 0))

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

        div = tk.Frame(self, background=WHITE)
        div.pack()

        msg = self.databaseCall(MY_TOPIC, "GET_VIDEOS_BY_USER", json.dumps({"ID": self.user.id}).encode())
        data = json.loads(msg.value())

        for i, video in enumerate(data):
            tk.Label(master=div, text=video.get("CreatedAt")).grid(row=i, column=0)
            tk.Button(master=div, text="Download",
                      command=lambda videoId=video.get("ID"): self.downloadVideo(videoId)).grid(row=i, column=1)

    def buildLoginFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            div = tk.Frame(self, background=WHITE)
            div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

            tk.Label(master=div, text="Username:", font=("Arial", 17), background=WHITE).pack()
            usernameEntry = tk.Entry(master=div, font=("Arial", 17))
            usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0))

            tk.Label(master=div, text="Password:", font=("Arial", 17), background=WHITE).pack(pady=(30, 0))
            passwordEntry = tk.Entry(master=div, font=("Arial", 17))
            passwordEntry.pack(pady=(10, 0))

            tk.Button(master=div, text="SUBMIT", font=("Arial", 17), command=lambda: self.login(usernameEntry.get(), passwordEntry.get())).pack(pady=(30, 0))
        else:
            tk.Label(master=self, text=f"Logged in as {self.user.name}", font=("Arial", 17), background=WHITE).pack()
            tk.Button(master=self, text="DISCONNECT", font=("Arial", 17), command=lambda: self.disconnect()).pack()

    def buildRegisterFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = tk.Frame(self, background=WHITE)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        tk.Label(master=div, text="Username:", font=("Arial", 17), background=WHITE).pack()
        usernameEntry = tk.Entry(master=div, font=("Arial", 17))
        usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0))

        tk.Label(master=div, text="Password:", font=("Arial", 17), background=WHITE).pack(pady=(30, 0))
        passwordEntry = tk.Entry(master=div, font=("Arial", 17))
        passwordEntry.pack(pady=(10, 0))

        tk.Label(master=div, text="Confirm password:", font=("Arial", 17), background=WHITE).pack(pady=(30, 0))
        confirmPasswordEntry = tk.Entry(master=div, font=("Arial", 17))
        confirmPasswordEntry.pack(pady=(10, 0))

        tk.Button(master=div, text="SUBMIT", font=("Arial", 17),
                  command=lambda: self.registerNewAccount(usernameEntry.get(), passwordEntry.get(), confirmPasswordEntry.get())).pack(pady=(30, 0))

    def buildKafkaFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            tk.Label(master=self, text="Address:", font=("Arial", 17), background=WHITE).pack(pady=(30, 0))
            entry = tk.Entry(master=self, font=("Arial", 17))
            entry.pack(pady=(10, 0))

            tk.Button(master=self, text="CONNECT", font=("Arial", 17),
                      command=lambda: self.getKafkaConnection(entry.get() if entry.get() != "" else "localhost:9092")).pack(pady=(30, 0))
        else:
            tk.Label(master=self, text=f"Connected to {self.kafkaAddress}").pack(pady=(30, 0))
            tk.Button(master=self, text="DISCONNECT", font=("Arial", 17), command=lambda: self.buttonDisconnectFromKafka()).pack(pady=(30, 0))

    def buildNotConnectedToKafkaFrame(self):
        tk.Label(master=self, text="Connect to kafka", font=("Arial", 17), background=WHITE).pack(anchor=tk.CENTER)

    def buildNotLoggedInFrame(self):
        tk.Label(master=self, text="Log In", font=("Arial", 17), background=WHITE).pack(anchor=tk.CENTER)

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
