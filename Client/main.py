import os
import sys
import threading
import time
from queue import Queue
from typing import List
from Kafka.Kafka import Partitions, KafkaConsumerWrapper
from PySide6.QtCore import QSize, Signal
from modules import *
from PySide6.QtWidgets import *
from modules import UiMainWindow
from modules.backend import Backend
from utils.ControlWindowPyQt import VideoWindow

os.environ["QT_FONT_DPI"] = "96" # FIX Problem for High DPI and Scale above 100%


class MainWindow(QMainWindow):
    receiveFileSignal = Signal(str)

    def __init__(self):
        QMainWindow.__init__(self)
        self.resize(940, 560)
        self.setMinimumSize(QSize(940, 560))
        self.setObjectName(u"MainWindow")

        self.videoWindow = None
        self.ui = UiMainWindow(self)
        self.ui.uiDefinitions()
        self.setWindowTitle("RMI")
        self.backend = Backend()

        self.widgets = self.ui
        self.widgets.titleRightInfo.setText("Remote Desktop Application")

        self.setCallbacks()

        self.widgets.pagesStack.setCurrentWidget(self.widgets.kafkaWindow)

        self.isDarkThemeOn = False
        self.incomingFileQueue = Queue()
        self.toggleTheme()

        self.receiveFileSignal.connect(self.handleQuestionSignal)
        self.ui.topLogo.setStyleSheet(Settings.TOP_LOGO_URL)
        self.show()

    def setCallbacks(self):
        self.widgets.toggleButton.clicked.connect(lambda: self.ui.toggleMenu())
        self.widgets.btnKafka.clicked.connect(self.btnKafkaPressed)
        self.widgets.btnLogin.clicked.connect(self.btnLoginPressed)
        self.widgets.btnRegister.clicked.connect(self.btnRegisterPressed)
        self.widgets.btnMyVideos.clicked.connect(self.btnMyVideosPresses)
        self.widgets.kafkaWindow.pushButton.clicked.connect(self.connectToKafka)
        self.widgets.loginWindow.pushButton.clicked.connect(self.loginAccount)
        self.widgets.registerWindow.btnRegister.clicked.connect(self.registerAccount)
        self.widgets.btnCall.clicked.connect(self.btnCallWindowPressed)
        self.widgets.callWindow.startSessionBtn.clicked.connect(self.startCall)
        self.widgets.callWindow.joinSessionBtn.clicked.connect(self.joinCall)
        self.widgets.btnChangeTheme.clicked.connect(self.toggleTheme)
        self.widgets.btnShareFile.clicked.connect(self.btnShareFilePressed)
        self.widgets.shareFileWindow.selectFileButton.clicked.connect(self.selectFile)
        self.widgets.shareFileWindow.uploadButtonn.clicked.connect(self.uploadSelectedFilehandler)

    def toggleTheme(self):
        if self.isDarkThemeOn:
            self.ui.theme("./themes/py_dracula_light.qss")
            Settings.MENU_SELECTED_STYLESHEET = Settings.MENU_SELECTED_LIGHT_STYLESHEET
        else:
            self.ui.theme("./themes/py_dracula_dark.qss")
            Settings.MENU_SELECTED_STYLESHEET = Settings.MENU_SELECTED_DARK_STYLESHEET
        self.ui.resetStyle("")
        self.isDarkThemeOn = not self.isDarkThemeOn

    def btnKafkaPressed(self):
        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.kafkaWindow)
        self.ui.resetStyle("btnKafka")
        btn.setStyleSheet(self.ui.selectMenu(btn.styleSheet()))

    def btnShareFilePressed(self):
        if self.backend.user.sessionId is None:
            self.ui.setStatusMessage("You are not in a session", True)
            return

        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.shareFileWindow)
        self.ui.resetStyle("btnShareFile")
        btn.setStyleSheet(self.ui.selectMenu(btn.styleSheet()))

    def btnLoginPressed(self):
        if self.backend.kafkaContainer is None:
            self.ui.setStatusMessage("Not connected to Kafka", True)
            return

        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.loginWindow)
        self.ui.resetStyle("btnLogin")
        btn.setStyleSheet(self.ui.selectMenu(btn.styleSheet()))

    def btnRegisterPressed(self):
        if self.backend.kafkaContainer is None:
            self.ui.setStatusMessage("Not connected to Kafka", True)
            return

        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.registerWindow)
        self.ui.resetStyle("btnRegister")
        btn.setStyleSheet(self.ui.selectMenu(btn.styleSheet()))

    def btnMyVideosPresses(self):
        if self.backend.user is None:
            self.ui.setStatusMessage("No user connected", True)
            return

        self.widgets.myVideosWindow.clear()
        videos = self.backend.getUserVideos()
        if type(videos) is Exception:
            self.ui.setStatusMessage(str(videos), True)

        for video in videos:
            self.widgets.myVideosWindow.addVideoRow(f"DURATION: {video[0]} secs", f"SIZE: {video[1]} bytes",
                                                    f"CREATED: {video[2]}",
                                                    lambda checked=True, id=video[3]: self.downloadVideo(id))

        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.myVideosWindow)
        self.ui.resetStyle("btnMyVideos")
        btn.setStyleSheet(self.ui.selectMenu(btn.styleSheet()))

    def btnCallWindowPressed(self):
        if self.backend.user is None:
            self.ui.setStatusMessage("No user connected", True)
            return

        self.ui.callWindow.yourCallKeyEdit.setText(self.backend.user.callKey)
        self.ui.callWindow.yourCallPasswordEdit.setText(self.backend.user.callPassword)
        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.callWindow)
        self.ui.resetStyle("btnCall")
        btn.setStyleSheet(self.ui.selectMenu(btn.styleSheet()))

    def mousePressEvent(self, event):
        self.dragPos = event.globalPos()

    def connectToKafka(self):
        address = self.widgets.kafkaWindow.lineEdit.text()
        address = "localhost:9092" if address is None or address == "" else address
        connectionSet = self.backend.setKafkaConnection(address)
        if connectionSet is not True:
            self.ui.setStatusMessage(str(connectionSet), True)
            return

        self.ui.setConnectedToKafkaState(address)
        self.ui.setStatusMessage("Connected to kafka")

    def disconnectFromKafka(self):
        self.backend.disconnectFromKafka()
        self.ui.setIsNotConnectedToKafkaState()
        self.ui.setStatusMessage("Disconnected from kafka")

    def loginAccount(self):
        username = self.ui.loginWindow.usernameLineEdit.text()
        password = self.ui.loginWindow.passwordLineEit.text()
        if username == "" or password == "":
            self.ui.setStatusMessage("Must give username and password", True)
            return

        loginResult = self.backend.login(username, password)
        if loginResult is not None:
            self.ui.setStatusMessage(str(loginResult), True)
            return

        self.ui.setUserLoggedIn(username)
        self.ui.setStatusMessage(f"Successfully connected as {username}")

    def disconnectAccount(self):
        self.backend.disconnect()
        self.ui.setUserNotLoggedIn()
        self.ui.setStatusMessage(f"Successfully disconnected")

    def registerAccount(self):
        username = self.ui.registerWindow.usernameEdit.text()
        password = self.ui.registerWindow.passwordEdit.text()
        confirmPassword = self.ui.registerWindow.confirmPasswordEdit.text()
        if username == "" or password == "" or confirmPassword == "":
            self.ui.setStatusMessage("Must give username, password and confirm password", True)
            return
        if password != confirmPassword:
            self.ui.setStatusMessage("The Password and the confirmation are not the same", True)
            return

        registerResult = self.backend.registerNewAccount(username, password)
        if type(registerResult) is Exception:
            self.ui.setStatusMessage(str(registerResult), True)
            return

        self.ui.registerWindow.usernameEdit.setText("")
        self.ui.registerWindow.passwordEdit.setText("")
        self.ui.registerWindow.confirmPasswordEdit.setText("")
        self.ui.setStatusMessage("Successfully registered")

    def startCall(self):
        if self.backend.user is None:
            self.ui.setStatusMessage("No user connected", True)
            return

        if Settings.PLATFORM != "linux":
            self.ui.setStatusMessage("This feature is ony for linux OS", True)
            return

        self.backend.createSession()
        self.backend.startRecorder()
        self.backend.startMerger()
        self.ui.callWindow.startSessionBtn.clicked.disconnect()
        self.ui.callWindow.startSessionBtn.clicked.connect(self.stopCall)
        self.ui.callWindow.startSessionBtn.setText("STOP SHARING")
        self.ui.setStatusMessage("Call started")

        self.receiveFileThread = threading.Thread(target=self.startReceiveFiles)
        self.receiveFileThread.start()

    def stopCall(self):
        self.backend.stopRecorder()
        self.backend.user.sessionId = None
        self.backend.kafkaContainer.resetTopic()
        self.ui.callWindow.startSessionBtn.clicked.disconnect()
        self.ui.callWindow.startSessionBtn.clicked.connect(self.startCall)
        self.ui.callWindow.startSessionBtn.setText("START SHARING")
        self.ui.setStatusMessage("Call stopped")

    def joinCall(self):
        if self.backend.user is None:
            self.ui.setStatusMessage("No user connected", True)
            return

        callKey = self.ui.callWindow.partnerCallKeyEdit.text()
        callPassword = self.ui.callWindow.partnerCallPasswordEdit.text()
        if callKey == "" or callPassword == "":
            self.ui.setStatusMessage("provide both key and password", True)
            return

        response = self.backend.getPartnerTopic(callKey, callPassword)
        if type(response) is Exception:
            self.ui.setStatusMessage(str(response), True)
            return

        topic = response["Topic"]
        self.backend.user.sessionId = int(response["SessionId"])

        threading.Thread(target=self.startReceiveFiles).start()

        self.w = VideoWindow(self, topic, self.backend.kafkaContainer.address)
        self.w.show()

    def downloadVideo(self, videoId: int):
        if self.backend.user is None:
            self.ui.setStatusMessage("No user connected", True)
            return

        file = QFileDialog()
        file.setDefaultSuffix("mp4")
        f = file.getSaveFileName(self, 'Save File')
        if file is None or f[0] == "":
            self.ui.setStatusMessage("No file selected", True)
            return

        video = self.backend.downloadVideo(videoId)
        if type(video) is Exception:
            self.ui.setStatusMessage(str(video), True)
            return

        with open(f[0], "wb") as file:
            file.write(video)
        self.ui.setStatusMessage("Video downloaded")

    def selectFile(self):
        file = QFileDialog()
        f = file.getOpenFileName(self)[0]
        if file is None or f == "":
            self.ui.setStatusMessage("No file selected", True)
            return

        self.widgets.shareFileWindow.label.setText(f"Current select file: {f}")
        self.widgets.shareFileWindow.selectedFileForUpload = f

    def uploadSelectedFilehandler(self):
        file: str = self.widgets.shareFileWindow.selectedFileForUpload
        if file is None:
            self.ui.setStatusMessage("No file selected", True)
            return
        if not os.path.isfile(file):
            self.ui.setStatusMessage("No file selected", True)
            return

        threading.Thread(target=self.uploadSelectedFile).start()

    def uploadSelectedFile(self):
        topicsOnCurrentSession: List = self.backend.getTopicsFromCurrentSession()
        if type(topicsOnCurrentSession) is Exception:
            self.widgets.setStatusMessage(str(topicsOnCurrentSession))

        topicsOnCurrentSession.remove(self.backend.getMyTopic())
        if len(topicsOnCurrentSession) == 0:
            return

        c = self.backend.getNewKafkaConsumer(self.backend.getMyTopic(), Partitions.FileTransferReceiveConfirmation.value)
        for topic in topicsOnCurrentSession:
            self.backend.kafkaContainer.producer.sendBigMessage(topic=topic, value=f"file,{self.backend.getMyTopic()}", partition=Partitions.FileTransferReceiveFile.value)
        self.backend.kafkaContainer.producer.flush(1)

        topicsToSendTheFile = []
        wait_end = time.time() + 10
        while wait_end > time.time() and len(topicsToSendTheFile) < len(topicsOnCurrentSession):
            msg = c.receiveBigMessage(timeoutSeconds=1)
            if msg is None:
                continue

            if msg.value()[:4] == b"true":
                topicsToSendTheFile.append(msg.value()[5:].decode())

        for topic in topicsToSendTheFile:
            self.backend.kafkaContainer.producer.sendFile(topic=topic, partition=Partitions.FileTransferReceiveFile.value, fileName=self.widgets.shareFileWindow.selectedFileForUpload)
        self.backend.kafkaContainer.producer.flush(1)
        c.close()

    def startReceiveFiles(self):
        consumer = self.backend.getNewKafkaConsumer(self.backend.getMyTopic(), Partitions.FileTransferReceiveFile.value)

        while self.backend.user.sessionId is not None:
            msg = consumer.receiveBigMessage(timeoutSeconds=1)
            if msg is None:
                continue

            if msg.value()[:4] != b"file":
                continue

            topic = msg.value()[5:].decode()
            self.receiveFileSignal.emit(topic)

            if not self.incomingFileQueue.get():
                continue

            file = consumer.receiveBigMessage(timeoutSeconds=10)
            if file is None:
                print("File not received")
                continue

            with open("out", "wb") as f:
                f.write(file.value())

        consumer.close()

    def handleQuestionSignal(self, topic):
        reply = QMessageBox.question(self,'Receive file', "You are receiving a file", QMessageBox.Yes, QMessageBox.No)
        msg = b""
        if reply == QMessageBox.Yes:
            msg = b"true"
        elif reply == QMessageBox.No:
            msg = b"false"

        self.incomingFileQueue.put(msg)
        self.backend.kafkaContainer.producer.sendBigMessage(topic=topic, value=msg + f",{self.backend.getMyTopic()}".encode(), partition=Partitions.FileTransferReceiveConfirmation.value)



if __name__ == "__main__":
    app = QApplication()
    window = MainWindow()
    sys.exit(app.exec_())
