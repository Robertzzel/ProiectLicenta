import json

import customtkinter
import tkinter as tk


class FrameBuilder:
    def __init__(self, master):
        self.master = master
        self.LABEL_FONT = customtkinter.CTkFont(size=10, family="Arial")

    def buildRemoteControlFrame(self):
        self.master.cleanFrame()

        if self.master.kafkaContainer is None:
            self.master.buildNotConnectedToKafkaFrame()
            return

        if self.master.user is None:
            self.master.buildNotLoggedInFrame()
            return

        leftFrame = customtkinter.CTkFrame(self.master)
        leftFrame.pack(expand=True, fill=tk.BOTH, side=tk.LEFT)
        leftFrame.pack_propagate(False)

        customtkinter.CTkLabel(master=leftFrame, text="Allow Remote Control", font=self.LABEL_FONT, ).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        div = customtkinter.CTkFrame(leftFrame)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        customtkinter.CTkLabel(master=div, text="YOUR ID:", font=self.LABEL_FONT).grid(row=0, column=0, padx=(50, 20), pady=(50, 0))
        customtkinter.CTkEntry(master=div, height=1, state="readonly", width=250, textvariable=tk.StringVar(value=str(self.master.user.callKey))).grid(row=1, column=0, padx=(50, 50))

        customtkinter.CTkLabel(master=div, text="PASSWORD:", font=self.LABEL_FONT, ).grid(row=2, column=0, pady=(30, 20), padx=(0, 20))
        customtkinter.CTkEntry(master=div, height=1, width=250, state="readonly", textvariable=tk.StringVar(value=str(self.master.user.callPassword))).grid(row=3, column=0)

        if self.master.user.sessionId is None and self.master.videoWindow is None:
            customtkinter.CTkButton(master=div, text="START SHARING", command=self.master.buttonStartSharing).grid(row=4, column=0, pady=(30, 50))
        elif self.master.videoWindow is not None:
            customtkinter.CTkButton(master=div, text="START SHARING", state=tk.DISABLED).grid(row=4, column=0, pady=(30, 50))
        else:
            customtkinter.CTkButton(master=div, text="STOP SHARING", command=self.master.buttonStopSharing).grid(row=4, column=0, pady=(30, 50))

        rightFrame = customtkinter.CTkFrame(self.master)
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

        if self.master.videoWindow is not None:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.LABEL_FONT, state=tk.DISABLED).grid(row=2, column=1, pady=(30, 20))
        else:
            customtkinter.CTkButton(master=divRight, text="SUBMIT", font=self.LABEL_FONT, command=lambda: self.master.buttonStartCall(idEntry.get(), passwordEntry.get())).grid(row=2, column=1, pady=(30, 20))

    def buildMyVideosFrame(self):
        self.master.cleanFrame()

        if self.master.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        if self.master.user is None:
            self.buildNotLoggedInFrame()
            return

        msg = self.master.kafkaContainer.databaseCall("GET_VIDEOS_BY_USER", str(self.master.user.id).encode(), timeoutSeconds=1, username=self.master.user.name, password=self.master.user.password)
        data = json.loads(msg.value())

        if len(data) == 0:
            return

        div = customtkinter.CTkFrame(self.master)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

        for i, video in enumerate(data):
            customtkinter.CTkLabel(master=div, text=f"DURATION: {video.get('Duration')} secs").grid(row=i, column=0, padx=(0, 30), pady=(10, 10))
            customtkinter.CTkLabel(master=div, text=f"SIZE: {video.get('Size')} bytes").grid(row=i, column=1, padx=(0, 30), pady=(10, 10))
            date = video.get("CreatedAt")
            customtkinter.CTkLabel(master=div, text=f"CREATED: {date}").grid(row=i, column=2, padx=(0, 20), pady=(10, 10))
            customtkinter.CTkButton(master=div, text="Download",
                                    command=lambda videoId=video.get("ID"): self.master.downloadVideo(videoId)).grid(row=i, column=3, pady=(10, 10))

    def buildLoginFrame(self):
        self.master.cleanFrame()

        if self.master.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self.master)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        if self.master.user is None:
            customtkinter.CTkLabel(master=div, text="Username:", font=self.LABEL_FONT, ).pack(pady=(50, 0))
            usernameEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT)
            usernameEntry.pack(anchor=tk.CENTER, pady=(10, 0), padx=(50, 50))

            customtkinter.CTkLabel(master=div, text="Password:", font=self.LABEL_FONT, ).pack(pady=(30, 0))
            passwordEntry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, show="*")
            passwordEntry.pack(pady=(10, 0), padx=(50, 50))

            customtkinter.CTkButton(master=div, text="SUBMIT", font=self.LABEL_FONT, command=lambda: self.master.login(usernameEntry.get(), passwordEntry.get())).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Logged in as {self.master.user.name}", font=self.LABEL_FONT, ).pack(pady=(50, 0), padx=50)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.LABEL_FONT, command=lambda: self.master.disconnect()).pack(pady=(30, 50))

    def buildRegisterFrame(self):
        self.master.cleanFrame()

        if self.master.kafkaContainer is None:
            self.buildNotConnectedToKafkaFrame()
            return

        div = customtkinter.CTkFrame(self.master)
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
                                command=lambda: self.master.registerNewAccount(usernameEntry.get(), passwordEntry.get(), confirmPasswordEntry.get())).pack(pady=(30, 50))

    def buildKafkaFrame(self):
        self.master.cleanFrame()

        div = customtkinter.CTkFrame(self.master)
        if self.master.kafkaContainer is None:
            customtkinter.CTkLabel(master=div, text="Address:", font=self.LABEL_FONT).pack(padx=(100, 100), pady=(50, 0), anchor=tk.CENTER)
            entry = customtkinter.CTkEntry(master=div, font=self.LABEL_FONT, width=200)
            entry.pack(pady=(10, 0), anchor=tk.CENTER)

            customtkinter.CTkButton(master=div, text="CONNECT", font=self.LABEL_FONT,
                                    command=lambda: self.master.getKafkaConnection(entry.get() if entry.get() != "" else "localhost:9092")).pack(pady=(30, 50))
        else:
            customtkinter.CTkLabel(master=div, text=f"Connected to {self.master.kafkaContainer.address}").pack(pady=(50, 10), anchor=tk.CENTER)
            customtkinter.CTkButton(master=div, text="DISCONNECT", font=self.LABEL_FONT, command=lambda: self.master.buttonDisconnectFromKafka()).pack(pady=(30, 50), padx=50, anchor=tk.CENTER)

        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)

    def buildNotConnectedToKafkaFrame(self):
        customtkinter.CTkLabel(master=self.master, text="Connect to kafka", font=self.LABEL_FONT).pack(anchor=tk.CENTER)

    def buildNotLoggedInFrame(self):
        customtkinter.CTkLabel(master=self.master, text="Log In", font=self.LABEL_FONT).pack(anchor=tk.CENTER)
