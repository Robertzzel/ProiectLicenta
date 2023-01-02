import tkinter as tk


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

        self.statusLabel = tk.Label(master=self, text="No notification", height=5, font=("Arial", 10), justify=tk.CENTER, wraplength=100)
        self.statusLabel.pack(fill=tk.BOTH, expand=True)

    def setStatusMessage(self, message: str):
        self.statusLabel.config(text=message)

    def onMenuButtonEnter(self, ev: tk.Event):
        ev.widget.config(background="gray")

    def onMenuButtonLeave(self, ev: tk.Event):
        ev.widget.config(background="lightgray")