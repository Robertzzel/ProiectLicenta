from PySide6 import QtCore

from main import *


class UIFunctions:
    state = False
    titleBar = True

    def __init__(self, mainWindow):
        self.master: MainWindow = mainWindow
        self.ui: UiMainWindow = self.master.ui
        self.state = False
        self.titleBar = True

    def maximize_restore(self):
        if not UIFunctions.state:
            self.master.showMaximized()
        else:
            self.master.showNormal()
            self.master.resize(1280, 720)
        UIFunctions.state = not UIFunctions.state

    def returStatus(self):
        return self.state

    def setStatus(self, status):
        self.state = status

    def toggleMenu(self):
        width = Settings.MENU_WIDTH if self.ui.leftMenuBg.width() == 60 else 60
        # ANIMATION
        self.animation = QPropertyAnimation(self.ui.leftMenuBg, b"minimumWidth")
        self.animation.setDuration(Settings.TIME_ANIMATION)
        self.animation.setStartValue(60 if self.ui.leftMenuBg.width() == 60 else Settings.MENU_WIDTH)
        self.animation.setEndValue(width)
        self.animation.setEasingCurve(QEasingCurve.InOutQuart)
        self.animation.start()

    @staticmethod
    def selectMenu(getStyle):
        select = getStyle + Settings.MENU_SELECTED_STYLESHEET
        return select

    @staticmethod
    def deselectMenu(getStyle):
        deselect = getStyle.replace(Settings.MENU_SELECTED_STYLESHEET, "")
        return deselect

    def selectStandardMenu(self, widget):
        for w in self.ui.topMenu.findChildren(QPushButton):
            if w.objectName() == widget:
                w.setStyleSheet(UIFunctions.selectMenu(w.mainWindowWidget()))

    def resetStyle(self, widget):
        for w in self.ui.topMenu.findChildren(QPushButton):
            if w.objectName() != widget:
                try:
                    w.setStyleSheet(UIFunctions.deselectMenu(w.mainWindowWidget()))
                except Exception:
                    pass

    def theme(self, file):
        str = open(file, 'r').read()
        self.ui.mainWindowWidget.setStyleSheet(str)

    def uiDefinitions(self):
        def dobleClickMaximizeRestore(event):
            if event.type() == QEvent.MouseButtonDblClick:
                QTimer.singleShot(250, lambda: UIFunctions.maximize_restore(self))
        self.ui.titleRightInfo.mouseDoubleClickEvent = dobleClickMaximizeRestore

        self.master.setWindowFlags(Qt.FramelessWindowHint)
        self.master.setAttribute(Qt.WA_TranslucentBackground)

        def moveWindow(event):
            # IF MAXIMIZED CHANGE TO NORMAL
            if UIFunctions.returStatus(self):
                UIFunctions.maximize_restore(self)
            # MOVE WINDOW
            if event.buttons() == Qt.LeftButton:
                self.master.move(self.master.pos() + event.globalPos() - self.master.dragPos)
                self.master.dragPos = event.globalPos()
                event.accept()
        self.ui.titleRightInfo.mouseMoveEvent = moveWindow

        # DROP SHADOW
        self.shadow = QGraphicsDropShadowEffect(self.master)
        self.shadow.setBlurRadius(17)
        self.shadow.setXOffset(0)
        self.shadow.setYOffset(0)
        self.shadow.setColor(QColor(0, 0, 0, 150))
        self.ui.bgApp.setGraphicsEffect(self.shadow)

        # RESIZE WINDOW
        self.sizegrip = QSizeGrip(self.ui.frame_size_grip)
        self.sizegrip.setStyleSheet("width: 20px; height: 20px; margin 0px; padding: 0px;")

        # MINIMIZE
        self.ui.minimizeAppBtn.clicked.connect(lambda: self.master.showMinimized())

        # MAXIMIZE/RESTORE
        self.ui.maximizeRestoreAppBtn.clicked.connect(lambda: UIFunctions.maximize_restore(self))

        # CLOSE APPLICATION
        self.ui.closeAppBtn.clicked.connect(lambda: self.master.close())

    def setConnectedToKafkaState(self, address):
        self.ui.kafkaWindowButton.setText("Disconnect")
        self.ui.kafkaWindowTextEdit.setText(address)
        self.ui.kafkaWindowTextEdit.setDisabled(True)

        self.ui.kafkaWindowButton.clicked.disconnect()
        self.ui.kafkaWindowButton.clicked.connect(self.master.disconnectFromKafka)

    def setIsNotConnectedToKafkaState(self):
        self.ui.kafkaWindowButton.setText("Connect")
        self.ui.kafkaWindowTextEdit.setText("")
        self.ui.kafkaWindowTextEdit.setDisabled(False)

        self.ui.kafkaWindowButton.clicked.disconnect()
        self.ui.kafkaWindowButton.clicked.connect(self.master.connectToKafka)

    def setUserLoggedIn(self, username: str):
        self.ui.loginUsernameEdit.setText(username)
        self.ui.loginPasswordEdit.setText("")
        self.ui.loginPasswordEdit.setDisabled(True)
        self.ui.loginUsernameEdit.setDisabled(True)
        self.ui.loginButton.setText("Disconnect")
        self.ui.loginButton.clicked.disconnect()
        self.ui.loginButton.clicked.connect(self.master.disconnectAccount)

    def setUserNotLoggedIn(self):
        self.ui.loginUsernameEdit.setText("")
        self.ui.loginPasswordEdit.setText("")
        self.ui.loginPasswordEdit.setDisabled(False)
        self.ui.loginUsernameEdit.setDisabled(False)
        self.ui.loginButton.setText("Connect")
        self.ui.loginButton.clicked.disconnect()
        self.ui.loginButton.clicked.connect(self.master.loginAccount)
