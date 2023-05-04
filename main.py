import os, sys
from modules import *
from PySide6.QtCore import *
from PySide6.QtGui import *
from PySide6.QtWidgets import *
from tkinter import Tk

from modules.backend import Backend
from utils.ControlWindow import VideoWindow
from utils.ControlWindowPyQt import AnotherWindow

os.environ["QT_FONT_DPI"] = "96" # FIX Problem for High DPI and Scale above 100%


class MainWindow(QMainWindow):
    def __init__(self):
        QMainWindow.__init__(self)

        self.videoWindow = None
        self.ui = UiMainWindow(self)
        self.setWindowTitle("RMI")
        self.backend = Backend()

        self.widgets = self.ui
        self.widgets.titleRightInfo.setText("Remote Desktop Application")
        self.uiFunctions = UIFunctions(self)
        self.uiFunctions.uiDefinitions()

        self.setCallbacks()

        self.widgets.stackedWidget.setCurrentWidget(self.widgets.kafkaWindow)

        self.themeFile = "./themes/py_dracula_dark.qss"
        self.__toggleTheme()
        self.show()

    def setCallbacks(self):
        self.widgets.toggleButton.clicked.connect(lambda: UIFunctions.toggleMenu(self))
        self.widgets.btnKafka.clicked.connect(self.btnKafkaPressed)
        self.widgets.btnLogin.clicked.connect(self.btnLoginPressed)
        self.widgets.btnRegister.clicked.connect(self.btnRegisterPressed)
        self.widgets.kafkaWindowButton.clicked.connect(self.connectToKafka)
        self.widgets.loginButton.clicked.connect(self.loginAccount)
        self.widgets.btnCall.clicked.connect(self.btnCallWindowPressed)
        self.widgets.callStartSharingButton.clicked.connect(self.startCall)
        self.widgets.callJoinSessionButton.clicked.connect(self.joinCall)
        self.widgets.btnChangeTheme.clicked.connect(self.__toggleTheme)

    def __toggleTheme(self):
        UIFunctions.theme(self, self.themeFile)
        self.themeFile = "./themes/py_dracula_dark.qss" if self.themeFile == "./themes/py_dracula_light.qss" else "./themes/py_dracula_light.qss"

    def btnKafkaPressed(self):
        btn = self.sender()
        self.widgets.stackedWidget.setCurrentWidget(self.widgets.kafkaWindow)
        UIFunctions.resetStyle(self, "btnKafka")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnLoginPressed(self):
        btn = self.sender()
        self.widgets.stackedWidget.setCurrentWidget(self.widgets.loginWindow)
        UIFunctions.resetStyle(self, "btnLogin")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnRegisterPressed(self):
        btn = self.sender()
        self.widgets.stackedWidget.setCurrentWidget(self.widgets.registerWindow)
        UIFunctions.resetStyle(self, "btnRegister")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def btnCallWindowPressed(self):
        if self.backend.user is None:
            print("No user connected")
            return

        self.ui.callUserCallKeyEdit.setText(self.backend.user.callKey)
        self.ui.callUserCallPasswordEdit.setText(self.backend.user.callPassword)
        btn = self.sender()
        self.widgets.stackedWidget.setCurrentWidget(self.widgets.callWindow)
        UIFunctions.resetStyle(self, "btnCall")
        btn.setStyleSheet(UIFunctions.selectMenu(btn.styleSheet()))

    def mousePressEvent(self, event):
        self.dragPos = event.globalPos()

    def connectToKafka(self):
        address = self.widgets.kafkaWindowTextEdit.text()
        address = "localhost:9092" if address is None or address == "" else address
        connectionSet = self.backend.setKafkaConnection(address)
        if connectionSet is not True:
            print(connectionSet)
            return

        self.uiFunctions.setConnectedToKafkaState(address)

    def disconnectFromKafka(self):
        self.backend.disconnectFromKafka()
        self.uiFunctions.setIsNotConnectedToKafkaState()

    def loginAccount(self):
        username = self.ui.loginUsernameEdit.text()
        password = self.ui.loginPasswordEdit.text()
        if username == "" or password == "":
            print("Must give username and password")
            return

        loginResult = self.backend.login(username, password)
        if loginResult is not None:
            print(loginResult)
            return
        self.uiFunctions.setUserLoggedIn(username)

    def disconnectAccount(self):
        self.backend.disconnect()
        self.uiFunctions.setUserNotLoggedIn()

    def registerAccount(self):
        username = self.ui.registerUsernameEdit.text()
        password = self.ui.registerPasswordEdit.text()
        confirmPassword = self.ui.registerConfirmPasswordEdit.text()
        if username == "" or password == "" or confirmPassword == "":
            print("Must give username, password and confirm password")
            return
        if password != confirmPassword:
            print("The Password and the confirmation are not the same")
            return

        registerResult = self.backend.registerNewAccount(username, password)
        if registerResult is not None:
            print(registerResult)

    def startCall(self):
        self.backend.createSession()
        self.backend.startRecorder()
        self.backend.startMerger()
        self.ui.callStartSharingButton.clicked.disconnect()
        self.ui.callStartSharingButton.clicked.connect(self.stopCall)
        self.ui.callStartSharingButton.setText("Stop Sharing")

    def stopCall(self):
        self.backend.stopRecorder()
        self.backend.user.sessionId = None
        self.backend.kafkaContainer.resetTopic()
        self.ui.callStartSharingButton.clicked.disconnect()
        self.ui.callStartSharingButton.clicked.connect(self.startCall)
        self.ui.callStartSharingButton.setText("Start Sharing")

    def joinCall(self):
        callKey = self.ui.callPartnerCallKeyEdit.text()
        callPassword = self.ui.callPartnerCallPasswordEdit.text()
        if callKey == "" or callPassword == "":
            print("provide both key and password")
            return

        topic = self.backend.getPartnerTopic(callKey, callPassword)
        if type(topic) is Exception:
            print(topic)
            return

        self.w = AnotherWindow(topic, self.backend.kafkaContainer.address)
        self.w.show()

    def exitCall(self):
        # TODO: sterge toate ferestrele deschise
        pass


if __name__ == "__main__":
    app = QApplication()
    window = MainWindow()
    sys.exit(app.exec_())
