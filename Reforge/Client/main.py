import sys
import socket

from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QLabel
from PyQt5 import uic

from Reforge.Client.InputsExecutor import InputsExecutorThread
from Reforge.Stream.Stream import Stream
from Reforge.Client.VideoWindow import *
from Reforge.Client.app import App
from pyautogui import size




class MyApp(QMainWindow):
    def __init__(self):
        super(MyApp, self).__init__()
        self.form = App()
        self.setCentralWidget(self.form)

        self.form.shareButton.clicked.connect(self.share)
        self.form.receiveButton.clicked.connect(self.receive)
        self.sharingWindow = None
        self.stream = None
        self.inputsSocket = None
        self.inputsExecutor = None
        self.screenSizes = size()

    def share(self):
        ip = self.form.ipEdit.text()
        tag = self.form.tagEdit.text()
        try:
            self.stream = Stream("localhost", 12345)
            self.form.shareButton.clicked.disconnect()
            self.form.shareButton.clicked.connect(self.stopShare)
            self.form.shareButton.setText("STOP")

            self.inputsSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            server_address = ('localhost', 12347)
            self.inputsSocket.bind(server_address)

            self.inputsExecutor = InputsExecutorThread(self.inputsSocket, self.screenSizes)
            self.inputsExecutor.start()
        except Exception as ex:
            print(ex)

    def stopShare(self):
        self.stream.stop()
        self.form.shareButton.clicked.disconnect()
        self.form.shareButton.clicked.connect(self.share)
        self.form.shareButton.setText("SHARE")
        self.inputsSocket.close()
        self.inputsSocket = None
        self.inputsExecutor.quit()

    def receive(self):
        self.inputsSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_address = ('localhost', 12346)
        self.inputsSocket.bind(server_address)

        self.sharingWindow = VideoWindow(self, "localhost", 12345, self.inputsSocket)
        self.sharingWindow.show()
        self.form.receiveButton.clicked.disconnect()
        self.form.receiveButton.clicked.connect(self.stopReceive)
        self.form.receiveButton.setText("STOP")


    def stopReceive(self):
        self.sharingWindow.stop()
        self.form.receiveButton.clicked.disconnect()
        self.form.receiveButton.clicked.connect(self.receive)
        self.form.receiveButton.setText("RECEIVE")
        self.inputsSocket.close()
        self.inputsSocket = None



if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyApp()
    window.show()
    sys.exit(app.exec())