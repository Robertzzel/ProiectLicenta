import json
import tkinter as tk
import tkinter.font as tkFont
import uuid
from concurrent.futures import ThreadPoolExecutor
from tkinter import filedialog
from typing import Optional

import customtkinter

from UI.ControlWindow import VideoWindow
from UI.KafkaContainer import KafkaContainer
from UI.User import User
from start import Merger, Sender


class App:
    def __init__(self, root):
        self.root = root
        root.title("Remote Control")
        width = 1280
        height = 720
        screenwidth = root.winfo_screenwidth()
        screenheight = root.winfo_screenheight()
        root.geometry('%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2))
        root.resizable(width=False, height=False)

        btnRemoteControl = tk.Button(root)
        btnRemoteControl["bg"] = "#e9e9ed"
        ft = tkFont.Font(family='Times', size=15)
        btnRemoteControl["font"] = ft
        btnRemoteControl["fg"] = "#000000"
        btnRemoteControl["justify"] = "center"
        btnRemoteControl["text"] = "Remote Control"
        btnRemoteControl.place(x=0, y=0, width=144, height=144)
        btnRemoteControl["command"] = self.buildRemoteControlFrame

        btnMyVideos = tk.Button(root)
        btnMyVideos["bg"] = "#e9e9ed"
        btnMyVideos["font"] = ft
        btnMyVideos["fg"] = "#000000"
        btnMyVideos["justify"] = "center"
        btnMyVideos["text"] = "My Videos"
        btnMyVideos.place(x=0, y=140, width=144, height=144)
        btnMyVideos["command"] = self.buildMyVideosFrame

        btnLogin = tk.Button(root)
        btnLogin["bg"] = "#e9e9ed"
        btnLogin["font"] = ft
        btnLogin["fg"] = "#000000"
        btnLogin["justify"] = "center"
        btnLogin["text"] = "Login"
        btnLogin.place(x=0, y=280, width=144, height=144)
        btnLogin["command"] = self.buildLoginFrame

        btnRegister = tk.Button(root)
        btnRegister["bg"] = "#e9e9ed"
        btnRegister["font"] = ft
        btnRegister["fg"] = "#000000"
        btnRegister["justify"] = "center"
        btnRegister["text"] = "Register"
        btnRegister.place(x=0, y=420, width=144, height=144)
        btnRegister["command"] = self.buildRegisterFrame

        btnKafka = tk.Button(root)
        btnKafka["bg"] = "#e9e9ed"
        btnKafka["font"] = ft
        btnKafka["fg"] = "#000000"
        btnKafka["justify"] = "center"
        btnKafka["text"] = "Kafka"
        btnKafka.place(x=0, y=560, width=144, height=144)
        btnKafka["command"] = self.buildKafkaFrame

        self.div = tk.Frame(master=root, width=1137, height=720)
        self.div.place(x=143, y=0)

        self.myTopic = str(uuid.uuid1())
        self.user: Optional[User] = None
        self.kafkaContainer: Optional[KafkaContainer] = None
        self.threadPool = ThreadPoolExecutor(4)
        self.sender: Optional[Sender] = None
        self.videoWindow: Optional[VideoWindow] = None
        self.merger: Optional[Merger] = None

        self.labelFont = customtkinter.CTkFont(size=25, family="Arial")

    def buildRemoteControlFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            self.buildNotLoggedInFrame()
            return

        leftFrame = customtkinter.CTkFrame(self.div)
        leftFrame.pack(expand=True, fill=tk.BOTH, side=tk.LEFT)
        leftFrame.pack_propagate(False)

        customtkinter.CTkLabel(master=leftFrame, text="Allow Remote Control", font=self.labelFont).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        div = customtkinter.CTkFrame(leftFrame)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        customtkinter.CTkLabel(master=div, text="YOUR ID:", font=self.labelFont).grid(row=0, column=0, padx=(50, 20), pady=(50, 0))
        customtkinter.CTkEntry(master=div, height=1, state="readonly", width=250, textvariable=tk.StringVar(value=str(self.user.callKey))).grid(row=1, column=0, padx=(50, 50))

        customtkinter.CTkLabel(master=div, text="PASSWORD:", font=self.labelFont, ).grid(row=2, column=0, pady=(30, 20), padx=(0, 20))
        customtkinter.CTkEntry(master=div, height=1, width=250, state="readonly", textvariable=tk.StringVar(value=str(self.user.callPassword))).grid(row=3, column=0)

        if self.user.sessionId is None and self.videoWindow is None:
            customtkinter.CTkButton(master=div, text="START SHARING", command=self.buttonStartSharing).grid(row=4, column=0, pady=(30, 50))
        elif self.videoWindow is not None:
            customtkinter.CTkButton(master=div, text="START SHARING", state=tk.DISABLED).grid(row=4, column=0, pady=(30, 50))
        else:
            customtkinter.CTkButton(master=div, text="STOP SHARING", command=self.buttonStopSharing).grid(row=4, column=0, pady=(30, 50))

        rightFrame = customtkinter.CTkFrame(self.div)
        rightFrame.pack(expand=True, fill=tk.BOTH, side=tk.RIGHT)
        rightFrame.pack_propagate(False)

        customtkinter.CTkLabel(master=rightFrame, text="Control Remote Computer", font=self.labelFont).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        divRight = customtkinter.CTkFrame(rightFrame)
        divRight.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        customtkinter.CTkLabel(master=divRight, text="USER ID:", font=self.labelFont).grid(row=0, column=0, padx=(0, 5), pady=(20, 0))
        idEntry = customtkinter.CTkEntry(master=divRight, font=self.labelFont, width=250)
        idEntry.grid(row=0, column=2, pady=(20, 0), padx=(0, 20))

        customtkinter.CTkLabel(master=divRight, text="PASSWORD:", font=self.labelFont).grid(row=1, column=0, pady=(30, 0), padx=(20, 5))
        passwordEntry = customtkinter.CTkEntry(master=divRight, font=self.labelFont, width=250)
        passwordEntry.grid(row=1, column=2, pady=(30, 0), padx=(0, 20))

        if self.videoWindow is not None:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.labelFont, state=tk.DISABLED).grid(row=2, column=1, pady=(30, 20))
        else:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.labelFont, command=lambda: self.buttonStartCall(idEntry.get(), passwordEntry.get())).grid(row=2, column=1, pady=(30, 20))

    def buildMyVideosFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.user is None:
            self.buildNotLoggedInFrame()
            return

        msg = self.kafkaContainer.databaseCall(self.myTopic, "GET_VIDEOS_BY_USER", json.dumps({"ID": self.user.id}).encode(), timeoutSeconds=1)
        data = json.loads(msg.value())

        if len(data) == 0:
            return

        div = customtkinter.CTkFrame(self.div)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        for i, video in enumerate(data):
            customtkinter.CTkLabel(master=div, text=f"DURATION: {video.get('DurationInSeconds')} secs").grid(row=i, column=0, padx=(0, 30))
            customtkinter.CTkLabel(master=div, text=f"SIZE: {video.get('Size')} bytes").grid(row=i, column=1, padx=(0, 30))
            date = video.get("CreatedAt")
            customtkinter.CTkLabel(master=div, text=f"CREATED: {date[:10]} {date[11:-1]}").grid(row=i, column=2, padx=(0, 20))
            customtkinter.CTkButton(master=div, text="Download",
                                    command=lambda videoId=video.get("ID"): self.downloadVideo(videoId)).grid(row=i, column=3)

    def buildLoginFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self.div)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        if self.user is None:
            customtkinter.CTkLabel(master=div, text="Username:", font=self.labelFont ).pack(pady=(50, 0))
            usernameEntry = customtkinter.CTkEntry(master=div, font=self.labelFont)
            usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

            customtkinter.CTkLabel(master=div, text="Password:", font=self.labelFont).pack(pady=(30, 0))
            passwordEntry = customtkinter.CTkEntry(master=div, font=self.labelFont, show="*")
            passwordEntry.pack(pady=(10, 0), padx=(50, 50))

            customtkinter.CTkButton(master=div, text="SUBMIT", font=self.labelFont, command=lambda: self.login(usernameEntry.get(), passwordEntry.get())).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Logged in as {self.user.name}", font=self.labelFont).pack(pady=(50, 0), padx=50)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.labelFont, command=lambda: self.disconnect()).pack(pady=(30, 50))

    def buildRegisterFrame(self):
        self.cleanFrame()

        if self.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self.div)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        customtkinter.CTkLabel(master=div, text="Username:", font=self.labelFont).pack(pady=(50, 0))
        usernameEntry = customtkinter.CTkEntry(master=div, font=self.labelFont)
        usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

        customtkinter.CTkLabel(master=div, text="Password:", font=self.labelFont).pack(pady=(30, 0))
        passwordEntry = customtkinter.CTkEntry(master=div, font=self.labelFont, show="*")
        passwordEntry.pack(pady=(10, 0))

        customtkinter.CTkLabel(master=div, text="Confirm password:", font=self.labelFont).pack(pady=(30, 0))
        confirmPasswordEntry = customtkinter.CTkEntry(master=div, font=self.labelFont, show="*")
        confirmPasswordEntry.pack(pady=(10, 0))

        customtkinter.CTkButton(master=div, text="SUBMIT", font=self.labelFont,
                                command=lambda: self.registerNewAccount(usernameEntry.get(), passwordEntry.get(), confirmPasswordEntry.get())).pack(pady=(30, 50))

    def buildKafkaFrame(self):
        self.cleanFrame()

        div = customtkinter.CTkFrame(self.div)
        if self.kafkaContainer is None:
            customtkinter.CTkLabel(master=div, text="Address:", font=self.labelFont).pack(padx=(100, 100), pady=(50, 0), anchor=tk.CENTER)
            entry = customtkinter.CTkEntry(master=div, font=self.labelFont, width=400)
            entry.pack(pady=(10, 0), anchor=tk.CENTER)

            customtkinter.CTkButton(master=div, text="CONNECT", font=self.labelFont,
                                    command=lambda: self.getKafkaConnection(entry.get() if entry.get() != "" else "localhost:9092")).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Connected to {self.kafkaContainer.address}").pack(pady=(50, 10), anchor=tk.CENTER)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.labelFont, command=lambda: self.buttonDisconnectFromKafka()).pack(pady=(30, 50), padx=50, anchor=tk.CENTER)

        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

    def buildNotConnectedToKafkaFrame(self):
        customtkinter.CTkLabel(master=self.div, text="Connect to kafka", font=self.labelFont).pack(anchor=tk.CENTER)

    def buildNotLoggedInFrame(self):
        customtkinter.CTkLabel(master=self.div, text="Log In", font=self.labelFont).pack(anchor=tk.CENTER)

    def cleanFrame(self):
        for widget in self.div.winfo_children():
            widget.destroy()

    def getKafkaConnection(self, address: str):
        try:
            self.kafkaContainer = KafkaContainer(address, {
                'auto.offset.reset': 'latest',
                'allow.auto.create.topics': "true",
            }, self.myTopic)
            self.setStatusMessage("Connected to Kafka")
        except Exception as ex:
            self.setStatusMessage(f"Cannot connect to kafka, {ex}")

        self.buildKafkaFrame()

    def buttonDisconnectFromKafka(self):
        self.kafkaContainer = None
        self.buildKafkaFrame()

    def registerNewAccount(self, username: str, password: str, confirmPassword: str):
        if username == "" or password == "" or confirmPassword == "":
            self.setStatusMessage("All fields must be filled")
            return

        if password != confirmPassword:
            self.setStatusMessage("password and confirmation are not the same")
            return

        message = self.kafkaContainer.databaseCall(self.myTopic, "REGISTER", json.dumps({"Name": username, "Password": password}).encode(), timeoutSeconds=1)
        if message is None:
            self.setStatusMessage("Cannot talk with the database")

        if KafkaContainer.getStatusFromMessage(message).lower() == "ok":
            self.setStatusMessage("Account registered")
            self.buildRegisterFrame()
        else:
            self.setStatusMessage("Cannot create account")

    def login(self, username: str, password: str):
        if username == "" or password == "":
            self.setStatusMessage("Need both name and pass")
            return

        loggInDetails = {"Name": username, "Password": password}
        message = self.kafkaContainer.databaseCall(self.myTopic, "LOGIN", json.dumps(loggInDetails).encode(), timeoutSeconds=1.5)
        if message is None:
            self.setStatusMessage("Cannot talk to the database")
            return

        if KafkaContainer.getStatusFromMessage(message).lower() != "ok":
            self.setStatusMessage("username or password wrong")
            return

        message = json.loads(message.value())
        self.user = User(message.get("ID", None), username, message.get("CallKey", None),
                         message.get("CallPassword", None), message.get("SessionId", None))

        self.sender = Sender(self.kafkaContainer.address)
        self.merger = Merger(self.kafkaContainer.address, self.myTopic)

        self.buildLoginFrame()
        self.root.title(f"RMI {self.user.name}")
        self.setStatusMessage("Logged in")

    def setStatusMessage(self, msg: str):
        print(msg)

    def disconnect(self):
        self.user = None

        if self.sender is not None and self.sender.running:
            self.sender.stop()

        if self.merger is not None and self.merger.running:
            self.merger.stop()

        self.buildLoginFrame()

    def downloadVideo(self, videoId: int):
        file = filedialog.asksaveasfile(mode="wb", defaultextension=".mp4")
        if file is None:
            return

        msg = self.kafkaContainer.databaseCall(self.myTopic, "DOWNLOAD_VIDEO_BY_ID", str(videoId).encode(), bigFile=True)
        file.write(msg.value())
        file.close()

        self.setStatusMessage("Video downloaded")

    def buttonStartCall(self, callKey: str, callPassword: str):
        self.startCall(callKey, callPassword)
        self.buildRemoteControlFrame()

    def buttonStartSharing(self):
        self.startSharing()
        self.buildRemoteControlFrame()

    def buttonStopSharing(self):
        self.stopSharing()
        self.buildRemoteControlFrame()

    def stopSharing(self):
        if self.sender is not None:
            self.sender.stop()

        if self.merger is not None:
            self.merger.stop()

        self.kafkaContainer.seekToEnd()
        self.kafkaContainer.databaseCall(topic=self.myTopic, operation="DELETE_SESSION", message=json.dumps({
            "SessionId": str(self.user.sessionId),
            "UserId": str(self.user.id),
        }).encode())

        self.user.sessionId = None
        self.setStatusMessage("Call stopped")

    def startSharing(self):
        msg = self.kafkaContainer.databaseCall(topic=self.myTopic, operation="CREATE_SESSION", message=json.dumps({
            "Topic": self.myTopic,
            "UserID": str(self.user.id),
        }).encode(), bigFile=True)

        status = KafkaContainer.getStatusFromMessage(msg)
        if status is None or status.lower() != "ok":
            self.setStatusMessage("Cannot start sharing")
            return

        self.user.sessionId = int(msg.value().decode())

        if self.sender is not None:
            self.sender.start(self.myTopic)

        if self.merger is not None:
            self.merger.start(str(self.user.sessionId))

    def startCall(self, callKey: str, callPassword: str):
        if callKey == "" or callPassword == "":
            self.setStatusMessage("provide both key and password")
            return

        responseMessage = self.kafkaContainer.databaseCall(self.myTopic, "GET_CALL_BY_KEY", json.dumps({"Key": callKey, "Password": callPassword, "CallerId": str(self.user.id)}).encode())

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


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
