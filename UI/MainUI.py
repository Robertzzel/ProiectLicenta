import json
import tkinter as tk
from Kafka.Kafka import *
import uuid

WHITE = "white"
DATABASE_TOPIC = "DATABASE"
MY_TOPIC = "UI"#f{uuid.uuid1()}"


class MainWindow2(tk.Tk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("RMI")
        self.minsize(800, 600)

        windowWidth = int(self.winfo_screenwidth() * 2 / 3)
        windowHeight = int(self.winfo_screenheight() * 2 / 3)
        self.geometry(f"{windowWidth}x{windowHeight}")
        self.config(background=WHITE)

        self.menuFrame = MenuFrame(self, background="lightgray", width=windowWidth // 8, height=windowHeight)
        self.menuFrame.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        self.menuFrame.pack_propagate(False)
        self.mainFrame = MainFrame(self, background=WHITE, width=windowWidth // 8 * 7, height=windowHeight)
        self.mainFrame.pack(fill=tk.BOTH, expand=True, side=tk.RIGHT)
        self.mainFrame.pack_propagate(False)

        self.menuFrame.bind("<<RemoteControlClick>>", lambda x: self.mainFrame.buildRemoteControlFrame())
        self.menuFrame.bind("<<MyVideosClick>>", lambda x: self.mainFrame.buildMyVideosFrame())
        self.menuFrame.bind("<<LoginClick>>", lambda x: self.mainFrame.buildLoginFrame())
        self.menuFrame.bind("<<RegisterClick>>", lambda x: self.mainFrame.buildRegisterFrame())
        self.menuFrame.bind("<<KafkaClick>>", lambda x: self.mainFrame.buildKafkaFrame())


class MenuFrame(tk.Frame):
    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)

        self.remoteControlLabel = tk.Label(master=self, text="Remote Control", height=5, font=("Arial", 15))
        self.remoteControlLabel.pack(fill=tk.X)
        self.remoteControlLabel.bind("<Enter>", func=lambda x: self.onMenuButtonEnter(x))
        self.remoteControlLabel.bind("<Leave>", func=lambda x: self.onMenuButtonLeave(x))
        self.remoteControlLabel.bind("<Button-1>", func=lambda x: self.event_generate("<<RemoteControlClick>>"))

        self.myVideosLabel = tk.Label(master=self, text="My Videos", height=5, font=("Arial", 15))
        self.myVideosLabel.pack(fill=tk.X)
        self.myVideosLabel.bind("<Enter>", func=lambda x: self.onMenuButtonEnter(x))
        self.myVideosLabel.bind("<Leave>", func=lambda x: self.onMenuButtonLeave(x))
        self.myVideosLabel.bind("<Button-1>", func=lambda x: self.event_generate("<<MyVideosClick>>"))

        self.loginLabel = tk.Label(master=self, text="Login", height=5, font=("Arial", 15))
        self.loginLabel.pack(fill=tk.X)
        self.loginLabel.bind("<Enter>", func=lambda x: self.onMenuButtonEnter(x))
        self.loginLabel.bind("<Leave>", func=lambda x: self.onMenuButtonLeave(x))
        self.loginLabel.bind("<Button-1>", func=lambda x: self.event_generate("<<LoginClick>>"))

        self.registerLabel = tk.Label(master=self, text="Register", height=5, font=("Arial", 15))
        self.registerLabel.pack(fill=tk.X)
        self.registerLabel.bind("<Enter>", func=lambda x: self.onMenuButtonEnter(x))
        self.registerLabel.bind("<Leave>", func=lambda x: self.onMenuButtonLeave(x))
        self.registerLabel.bind("<Button-1>", func=lambda x: self.event_generate("<<RegisterClick>>"))

        self.registerLabel = tk.Label(master=self, text="Kafka", height=5, font=("Arial", 15))
        self.registerLabel.pack(fill=tk.X)
        self.registerLabel.bind("<Enter>", func=lambda x: self.onMenuButtonEnter(x))
        self.registerLabel.bind("<Leave>", func=lambda x: self.onMenuButtonLeave(x))
        self.registerLabel.bind("<Button-1>", func=lambda x: self.event_generate("<<KafkaClick>>"))

    def onMenuButtonEnter(self, ev: tk.Event):
        ev.widget.config(background="gray")

    def onMenuButtonLeave(self, ev: tk.Event):
        ev.widget.config(background="lightgray")


class MainFrame(tk.Frame):
    def __init__(self, parent=None, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)
        self.userId = None
        self.userName = None
        self.callKey = None
        self.callPassword = None
        self.kafkaAddress = None
        self.databaseProducer = None  # KafkaProducerWrapper(bootstrap_servers=self.kafkaAddress, acks=1)
        self.databaseConsumer = None

    def buildRemoteControlFrame(self):
        self.cleanFrame()

        leftFrame = tk.Frame(self, background=WHITE, borderwidth=0.5, relief="solid")
        leftFrame.pack(expand=True, fill=tk.BOTH, side=tk.LEFT)
        leftFrame.pack_propagate(False)

        tk.Label(master=leftFrame, text="Allow Remote Control", background=WHITE, font=("Arial", 17)).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        div = tk.Frame(leftFrame, background=WHITE)
        div.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        tk.Label(master=div, text="YOUR ID:", font=("Arial", 17), background=WHITE).grid(row=0, column=0, padx=(0, 20))
        tk.Label(master=div, text=str(self.callKey), font=("Arial", 17),background=WHITE).grid(row=0, column=1)

        tk.Label(master=div, text="PASSWORD:", font=("Arial", 17),background=WHITE).grid(row=1, column=0, pady=(30, 0), padx=(0, 20))
        tk.Label(master=div, text=str(self.callPassword), font=("Arial", 17),background=WHITE).grid(row=1, column=1, pady=(30, 0))

        rightFrame = tk.Frame(self, background=WHITE, borderwidth=0.5, relief="solid")
        rightFrame.pack(expand=True, fill=tk.BOTH, side=tk.RIGHT)
        rightFrame.pack_propagate(False)

        tk.Label(master=rightFrame, text="Control Remote Computer", background=WHITE, font=("Arial", 17)).place(relx=0.5, rely=0.25, anchor=tk.CENTER)

        divRight = tk.Frame(rightFrame, background=WHITE)
        divRight.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        tk.Label(master=divRight, text="USER ID:", font=("Arial", 17), background=WHITE).grid(row=0, column=0, padx=(0, 20))
        tk.Entry(master=divRight, font=("Arial", 17)).grid(row=0, column=2)

        tk.Label(master=divRight, text="PASSWORD:", font=("Arial", 17), background=WHITE).grid(row=1, column=0, pady=(30, 0), padx=(0, 20))
        tk.Entry(master=divRight, font=("Arial", 17)).grid(row=1, column=2, pady=(30, 0))

        tk.Button(master=divRight, text="SUBMIT", font=("Arial", 17)).grid(row=2, column=1, pady=(30, 0))

    def buildMyVideosFrame(self):
        self.cleanFrame()
        tk.Label(master=self, text="My Videos").pack()

        div = tk.Frame(self, background=WHITE)
        div.pack()

        msg = self.databaseCall(MY_TOPIC, "GET_VIDEOS_BY_USER", json.dumps({"ID": self.userId}).encode())
        data = json.loads(msg.value)
        print(data)

    def buildLoginFrame(self):
        self.cleanFrame()

        tk.Label(master=self, text="Login", background=WHITE, font=("Arial", 17)).pack()

        if self.databaseProducer is None:
            tk.Label(master=self, text="Connect to kafka", font=("Arial", 17), background=WHITE).pack(anchor=tk.CENTER)
            return

        if self.userId is None:
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
            tk.Label(master=self, text=f"Logged in as {self.userName}", font=("Arial", 17), background=WHITE).pack()
            tk.Button(master=self, text="DISCONNECT", font=("Arial", 17), command=lambda: self.disconnect()).pack()

    def login(self, username: str, password: str):
        if username == "" or password == "":
            return

        loggInDetails = {"Name": username, "Password": password}
        message = self.databaseCall(MY_TOPIC, "LOGIN", json.dumps(loggInDetails).encode())

        status = None
        for header in message.headers():
            if header[0] == "status":
                status = header[1].decode()

        if status.lower() != "ok":
            return

        message = json.loads(message.value())
        self.userId = message.get("ID", None)
        self.userName = username
        self.callKey = message.get("CallKey", None)
        self.callPassword = message.get("CallPassword", None)
        self.buildLoginFrame()

    def disconnect(self):
        self.userId = None
        self.userName = None
        self.buildLoginFrame()

    def buildRegisterFrame(self):
        self.cleanFrame()
        tk.Label(master=self, text="Register", background=WHITE, font=("Arial", 17)).pack()

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

    def registerNewAccount(self, username: str, password: str, confirmPassword: str):
        if username == "" or password == "" or confirmPassword == "" or password != confirmPassword:
            return

        loggInDetails = {"Name": username, "Password": password}
        message = self.databaseCall("UI", "REGISTER", json.dumps(loggInDetails).encode())

        status: str = None
        for header in message.headers():
            if header[0] == "status":
                status = header[1].decode()

        if status.lower() == "ok":
            self.buildRegisterFrame()

    def buildKafkaFrame(self):
        self.cleanFrame()

        if self.databaseProducer is None:
            tk.Label(master=self, text="Address:", font=("Arial", 17), background=WHITE).pack(pady=(30, 0))
            entry = tk.Entry(master=self, font=("Arial", 17))
            entry.pack(pady=(10, 0))

            tk.Button(master=self, text="CONNECT", font=("Arial", 17),
                      command=lambda: self.getKafkaConnection(entry.get() if entry.get() != "" else "localhost:9092")).pack(pady=(30, 0))
        else:
            tk.Button(master=self, text="DISCONNECT", font=("Arial", 17), command=lambda: self.resetKafkaConnection()).pack(pady=(30, 0))

    def cleanFrame(self):
        for widget in self.winfo_children():
            widget.destroy()

    def databaseCall(self, topic: str, operation: str, message: bytes, bigFile=False):
        self.databaseProducer.produce(topic=DATABASE_TOPIC, headers=[
            ("topic", topic.encode()),
            ("operation", operation.encode()),
        ], value=message)

        return self.databaseConsumer.receiveBigMessage() if bigFile else next(self.databaseConsumer)

    def getKafkaConnection(self, address: str):
        try:
            self.kafkaAddress = address
            self.databaseProducer = KafkaProducerWrapper({'bootstrap.servers': self.kafkaAddress})

            createTopic(self.kafkaAddress, MY_TOPIC)
            self.databaseConsumer = KafkaConsumerWrapper({
                    'bootstrap.servers': self.kafkaAddress,
                    'group.id': str(uuid.uuid1()),
                    'auto.offset.reset': 'latest',
                    'allow.auto.create.topics': "true",
                }, [MY_TOPIC])
        except Exception as ex:
            print(ex)
            self.databaseProducer = None
            self.databaseConsumer = None
            return False

        self.buildKafkaFrame()
        return True

    def resetKafkaConnection(self):
        if self.databaseProducer:
            self.databaseProducer.close()
            self.databaseProducer = None

        if self.databaseConsumer:
            self.databaseConsumer.close()
            self.databaseConsumer = None

        self.buildKafkaFrame()


if __name__ == "__main__":
    app = MainWindow2()
    app.mainloop()