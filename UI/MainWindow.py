import tkinter as tk
import customtkinter
from MenuFrame import MenuFrame
from MainFrame import MainFrame

BACKGROUND = "#161616"


class MainWindow(customtkinter.CTk):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title("RMI")

        windowWidth = 1000
        windowHeight = 800
        self.minsize(windowWidth, windowHeight)

        self.geometry(f"{windowWidth}x{windowHeight}")
        self.config(background=BACKGROUND)

        self.menuFrame = MenuFrame(self, width=windowWidth // 8, height=windowHeight)
        self.menuFrame.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        self.menuFrame.pack_propagate(False)
        self.mainFrame = MainFrame(self, width=windowWidth // 8 * 7, height=windowHeight)
        self.mainFrame.pack(fill=tk.BOTH, expand=True, side=tk.RIGHT)
        self.mainFrame.pack_propagate(False)

        self.menuFrame.bind("<<RemoteControlClick>>", lambda x: self.mainFrame.buildRemoteControlFrame())
        self.menuFrame.bind("<<MyVideosClick>>", lambda x: self.mainFrame.buildMyVideosFrame())
        self.menuFrame.bind("<<LoginClick>>", lambda x: self.mainFrame.buildLoginFrame())
        self.menuFrame.bind("<<RegisterClick>>", lambda x: self.mainFrame.buildRegisterFrame())
        self.menuFrame.bind("<<KafkaClick>>", lambda x: self.mainFrame.buildKafkaFrame())
        self.protocol("WM_DELETE_WINDOW", self.on_exit)

    def on_exit(self):
        if self.mainFrame.videoWindow is not None:
            self.mainFrame.videoWindow = None

        if self.mainFrame.sender is not None:
            self.mainFrame.sender.stop()

        self.mainFrame.disconnect()
        self.destroy()

    def setStatusMessage(self, message: str):
        self.menuFrame.setStatusMessage(message)


if __name__ == "__main__":
    app = MainWindow()
    customtkinter.set_appearance_mode("dark")
    customtkinter.set_default_color_theme("dark-blue")
    app.mainloop()