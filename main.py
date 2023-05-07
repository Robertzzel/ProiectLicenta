import os
import sys
from pathlib import Path

from PySide6.QtCore import QSize
from playsound import playsound
from modules import *
from PySide6.QtWidgets import *

from modules import UiMainWindow
from modules.backend import Backend
from utils.Audio import AudioTool
from utils.CameraInputsTool import CameraInputTool
from utils.ControlWindowPyQt import VideoWindow

os.environ["QT_FONT_DPI"] = "96"  # FIX Problem for High DPI and Scale above 100%


class MainWindow(QMainWindow):
    def __init__(self):
        QMainWindow.__init__(self)
        self.buttonClickSoundFilePath = Path(__file__).parent / "sounds" / "click.wav"
        self.errorSoundFilePath = Path(__file__).parent / "sounds" / "error.wav"
        self.themeSoundFilePath = Path(__file__).parent / "sounds" / "theme.wav"
        self.resize(940, 560)
        self.setMinimumSize(QSize(940, 560))
        self.setObjectName(u"MainWindow")

        self.videoWindow = None
        self.ui = UiMainWindow(self)
        self.setWindowTitle("RMI")
        self.backend = Backend()
        self.audio = AudioTool()
        self.audio.audioClickSignal.connect(self.audioSignaleCallback)
        self.audio.start()

        self.widgets = self.ui
        self.widgets.titleRightInfo.setText("Remote Desktop Application")
        self.uiFunctions = UIFunctions(self)
        self.uiFunctions.uiDefinitions()

        self.setCallbacks()

        self.widgets.pagesStack.setCurrentWidget(self.widgets.kafkaWindow)

        self.isDarkThemeOn = False
        self.__toggleTheme()
        playsound(Path(__file__).parent / "sounds" / "start.mp3", False)

        self.ui.topLogo.setStyleSheet(
            f"background-image: url({Path(__file__).parent / 'images' / 'images' / 'PyDracula.png'});")
        self.show()

    def setCallbacks(self):
        self.widgets.toggleButton.clicked.connect(lambda x=True: UIFunctions.toggleMenu(self))
        self.widgets.toggleButton.clicked.connect(lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.btnKafka.clicked.connect(self.btnKafkaPressed)
        self.widgets.btnKafka.clicked.connect(lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.btnLogin.clicked.connect(self.btnLoginPressed)
        self.widgets.btnLogin.clicked.connect(lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.btnRegister.clicked.connect(self.btnRegisterPressed)
        self.widgets.btnRegister.clicked.connect(lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.btnMyVideos.clicked.connect(self.btnMyVideosPresses)
        self.widgets.btnMyVideos.clicked.connect(lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.kafkaWindow.pushButton.clicked.connect(self.connectToKafka)
        self.widgets.kafkaWindow.pushButton.clicked.connect(
            lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.loginWindow.pushButton.clicked.connect(self.loginAccount)
        self.widgets.loginWindow.pushButton.clicked.connect(
            lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.registerWindow.btnRegister.clicked.connect(self.registerAccount)
        self.widgets.registerWindow.btnRegister.clicked.connect(
            lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.btnCall.clicked.connect(self.btnCallWindowPressed)
        self.widgets.btnCall.clicked.connect(lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.callWindow.startSessionBtn.clicked.connect(self.startCall)
        self.widgets.callWindow.startSessionBtn.clicked.connect(
            lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.callWindow.joinSessionBtn.clicked.connect(self.joinCall)
        self.widgets.callWindow.joinSessionBtn.clicked.connect(
            lambda x=True: playsound(self.buttonClickSoundFilePath, False))

        self.widgets.btnChangeTheme.clicked.connect(self.__toggleTheme)
        self.widgets.btnChangeTheme.clicked.connect(lambda x=True: playsound(self.themeSoundFilePath, False))

        self.widgets.btnVideoInputToggle.clicked.connect(lambda x=True: playsound(self.themeSoundFilePath, False))
        self.widgets.btnVideoInputToggle.clicked.connect(self.startVideoControl)

    def startVideoControl(self):
        self.widgets.btnVideoInputToggle.clicked.disconnect(self.startVideoControl)
        self.widgets.btnVideoInputToggle.clicked.connect(self.stopVideoControl)
        self.cameraInptTool = CameraInputTool()
        self.cameraInptTool.start()

    def stopVideoControl(self):
        self.widgets.btnVideoInputToggle.clicked.disconnect(self.stopVideoControl)
        self.widgets.btnVideoInputToggle.clicked.connect(self.startVideoControl)
        self.cameraInptTool.stop()

    def audioSignaleCallback(self, text):
        text = text.lower()
        if text == "kafka":
            self.ui.btnKafka.click()
        elif text == "register":
            self.ui.btnRegister.click()
        elif text == "login":
            self.ui.btnRegister.click()
        elif text == "videos" or text == "video":
            self.ui.btnMyVideos.click()
        elif text == "call" or text == "calls":
            self.ui.btnCall.click()
        elif "theme" in text:
            self.ui.btnChangeTheme.click()

    def __toggleTheme(self):
        if self.isDarkThemeOn:
            UIFunctions.theme(self, "./themes/py_dracula_light.qss")
            Settings.MENU_SELECTED_STYLESHEET = Settings.MENU_SELECTED_LIGHT_STYLESHEET
        else:
            UIFunctions.theme(self, "./themes/py_dracula_dark.qss")
            Settings.MENU_SELECTED_STYLESHEET = Settings.MENU_SELECTED_DARK_STYLESHEET
        UIFunctions.resetStyle(self, "")
        self.isDarkThemeOn = not self.isDarkThemeOn

    def btnKafkaPressed(self):
        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.kafkaWindow)
        UIFunctions.resetStyle(self, "btnKafka")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnLoginPressed(self):
        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.loginWindow)
        UIFunctions.resetStyle(self, "btnLogin")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnRegisterPressed(self):
        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.registerWindow)
        UIFunctions.resetStyle(self, "btnRegister")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnMyVideosPresses(self):
        if self.backend.user is None:
            self.uiFunctions.setStatusMessage("No user connected", True)
            return

        self.widgets.myVideosWindow.clear()
        videos = self.backend.getUserVideos()
        if type(videos) is Exception:
            self.uiFunctions.setStatusMessage(str(videos), True)

        for video in videos:
            self.widgets.myVideosWindow.addVideoRow(f"DURATION: {video[0]} secs", f"SIZE: {video[1]} bytes",
                                                    f"CREATED: {video[2]}",
                                                    lambda checked=True, id=video[3]: self.downloadVideo(id))

        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.myVideosWindow)
        UIFunctions.resetStyle(self, "btnMyVideos")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnCallWindowPressed(self):
        if self.backend.user is None:
            self.uiFunctions.setStatusMessage("No user connected", True)
            return

        self.ui.callWindow.yourCallKeyEdit.setText(self.backend.user.callKey)
        self.ui.callWindow.yourCallPasswordEdit.setText(self.backend.user.callPassword)
        btn = self.sender()
        self.widgets.pagesStack.setCurrentWidget(self.widgets.callWindow)
        UIFunctions.resetStyle(self, "btnCall")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def mousePressEvent(self, event):
        self.dragPos = event.globalPos()

    def connectToKafka(self):
        address = self.widgets.kafkaWindow.lineEdit.text()
        address = "localhost:9092" if address is None or address == "" else address
        connectionSet = self.backend.setKafkaConnection(address)
        if connectionSet is not True:
            self.uiFunctions.setStatusMessage(str(connectionSet), True)
            return

        self.uiFunctions.setConnectedToKafkaState(address)
        self.uiFunctions.setStatusMessage("Connected to kafka")

    def disconnectFromKafka(self):
        self.backend.disconnectFromKafka()
        self.uiFunctions.setIsNotConnectedToKafkaState()
        self.uiFunctions.setStatusMessage("Disconnected from kafka")

    def loginAccount(self):
        username = self.ui.loginWindow.usernameLineEdit.text()
        password = self.ui.loginWindow.passwordLineEit.text()
        if username == "" or password == "":
            self.uiFunctions.setStatusMessage("Must give username and password", True)
            return

        loginResult = self.backend.login(username, password)
        if loginResult is not None:
            self.uiFunctions.setStatusMessage(str(loginResult), True)
            return

        self.uiFunctions.setUserLoggedIn(username)
        self.uiFunctions.setStatusMessage(f"Successfully connected as {username}")

    def disconnectAccount(self):
        self.backend.disconnect()
        self.uiFunctions.setUserNotLoggedIn()
        self.uiFunctions.setStatusMessage(f"Successfully disconnected")

    def registerAccount(self):
        username = self.ui.registerWindow.usernameEdit.text()
        password = self.ui.registerWindow.passwordEdit.text()
        confirmPassword = self.ui.registerWindow.confirmPasswordEdit.text()
        if username == "" or password == "" or confirmPassword == "":
            self.uiFunctions.setStatusMessage("Must give username, password and confirm password", True)
            return
        if password != confirmPassword:
            self.uiFunctions.setStatusMessage("The Password and the confirmation are not the same", True)
            return

        registerResult = self.backend.registerNewAccount(username, password)
        if type(registerResult) is Exception:
            self.uiFunctions.setStatusMessage(str(registerResult), True)
            return

        self.ui.registerWindow.usernameEdit.setText("")
        self.ui.registerWindow.passwordEdit.setText("")
        self.ui.registerWindow.confirmPasswordEdit.setText("")
        self.uiFunctions.setStatusMessage("Successfully registered")

    def startCall(self):
        self.backend.createSession()
        self.backend.startRecorder()
        self.backend.startMerger()
        self.ui.callWindow.startSessionBtn.clicked.disconnect(self.startCall)
        self.ui.callWindow.startSessionBtn.clicked.connect(self.stopCall)
        self.ui.callWindow.startSessionBtn.setText("STOP SHARING")
        self.uiFunctions.setStatusMessage("Call started")

    def stopCall(self):
        self.backend.stopRecorder()
        self.backend.user.sessionId = None
        self.backend.kafkaContainer.resetTopic()
        self.ui.callWindow.startSessionBtn.clicked.disconnect(self.stopCall)
        self.ui.callWindow.startSessionBtn.clicked.connect(self.startCall)
        self.ui.callWindow.startSessionBtn.setText("START SHARING")
        self.uiFunctions.setStatusMessage("Call stopped")

    def joinCall(self):
        callKey = self.ui.callWindow.partnerCallKeyEdit.text()
        callPassword = self.ui.callWindow.partnerCallPasswordEdit.text()
        if callKey == "" or callPassword == "":
            self.uiFunctions.setStatusMessage("provide both key and password", True)
            return

        topic = self.backend.getPartnerTopic(callKey, callPassword)
        if type(topic) is Exception:
            self.uiFunctions.setStatusMessage(str(topic), True)
            return

        self.w = VideoWindow(topic, self.backend.kafkaContainer.address)
        self.w.show()

    def downloadVideo(self, videoId: int):
        file = QFileDialog()
        file.setDefaultSuffix("mp4")
        f = file.getSaveFileName(self, 'Save File')
        if file is None or f[0] == "":
            self.uiFunctions.setStatusMessage("No file selected", True)
            return

        video = self.backend.downloadVideo(videoId)
        if type(video) is Exception:
            self.uiFunctions.setStatusMessage(str(video),True)
            return

        with open(f[0], "wb") as file:
            file.write(video)
        self.uiFunctions.setStatusMessage("Video downloaded")


if __name__ == "__main__":
    app = QApplication()
    window = MainWindow()
    sys.exit(app.exec_())
