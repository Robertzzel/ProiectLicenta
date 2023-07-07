from pathlib import Path
from PySide6.QtCore import *
from PySide6.QtGui import *
from PySide6.QtWidgets import *

from .Settings import Settings
from .CallWindow import CallWindow
from .KafkaWindow import KafkaWindow
from .Login import Login
from .MyVideosWindow import MyVideosWindow
from .shareFileWindow import ShareFileWindow
from .Register import Register
from .ChangePassoword import ChangePassword

font = QFont()
font.setFamily(u"Segoe UI")
font.setPointSize(10)
font.setBold(False)
font.setItalic(False)

font1 = QFont()
font1.setFamily(u"Segoe UI Semibold")
font1.setPointSize(12)
font1.setBold(False)
font1.setItalic(False)

font2 = QFont()
font2.setFamily(u"Segoe UI")
font2.setPointSize(8)
font2.setBold(False)
font2.setItalic(False)

font3 = QFont()
font3.setFamily(u"Segoe UI")
font3.setPointSize(10)
font3.setBold(False)
font3.setItalic(False)
font3.setStyleStrategy(QFont.PreferDefault)

font5 = QFont()
font5.setFamily(u"Segoe UI")
font5.setBold(False)
font5.setItalic(False)


class UiMainWindow(object):
    def __init__(self, MainWindow):
        self.state = False
        self.titleBar = True
        self.master = MainWindow

        self.mainWindowWidget = QWidget(MainWindow)
        self.mainWindowWidget.setFont(font)
        self.mainWindowWidget.setStyleSheet((Path(__file__).parent.parent / "themes" / "py_dracula_dark.qss").read_text())
        MainWindow.setCentralWidget(self.mainWindowWidget)

        self.windowMargins = QVBoxLayout(self.mainWindowWidget)

        self.applicationFrame = QFrame(self.mainWindowWidget)
        self.applicationFrame.setObjectName(u"applicationFrame")
        self.windowMargins.addWidget(self.applicationFrame)

        self.appLayout = QHBoxLayout(self.applicationFrame)
        self.appLayout.setSpacing(0)
        self.appLayout.setContentsMargins(0, 0, 0, 0)

        self.leftMenuWithLogoFrame = QFrame(self.applicationFrame)
        self.leftMenuWithLogoFrame.setObjectName(u"leftMenuBg")
        self.leftMenuWithLogoFrame.setMinimumSize(QSize(60, 0))
        self.leftMenuWithLogoFrame.setMaximumSize(QSize(60, 16777215))
        self.leftMenuWithLogoFrame.setFrameShape(QFrame.NoFrame)
        self.leftMenuWithLogoFrame.setFrameShadow(QFrame.Raised)
        self.appLayout.addWidget(self.leftMenuWithLogoFrame)

        self.leftMenuWithLogoLayout = QVBoxLayout(self.leftMenuWithLogoFrame)
        self.leftMenuWithLogoLayout.setSpacing(0)
        self.leftMenuWithLogoLayout.setContentsMargins(0, 0, 0, 0)

        self.topLogoInfo = QFrame(self.leftMenuWithLogoFrame)
        self.topLogoInfo.setMinimumSize(QSize(0, 50))
        self.topLogoInfo.setMaximumSize(QSize(16777215, 50))
        self.topLogoInfo.setFrameShape(QFrame.NoFrame)
        self.topLogoInfo.setFrameShadow(QFrame.Raised)
        self.leftMenuWithLogoLayout.addWidget(self.topLogoInfo)

        self.topLogo = QFrame(self.topLogoInfo)
        self.topLogo.setObjectName(u"topLogo")
        self.topLogo.setGeometry(QRect(10, 5, 42, 42))
        self.topLogo.setMinimumSize(QSize(42, 42))
        self.topLogo.setMaximumSize(QSize(42, 42))
        self.topLogo.setFrameShape(QFrame.NoFrame)
        self.topLogo.setFrameShadow(QFrame.Raised)

        self.titleLeftApp = QLabel(self.topLogoInfo)
        self.titleLeftApp.setObjectName(u"titleLeftApp")
        self.titleLeftApp.setGeometry(QRect(70, 8, 160, 20))
        self.titleLeftApp.setFont(font1)
        self.titleLeftApp.setAlignment(Qt.AlignLeading | Qt.AlignLeft | Qt.AlignTop)
        self.titleLeftApp.setText("RMI")

        self.titleLeftDescription = QLabel(self.topLogoInfo)
        self.titleLeftDescription.setObjectName(u"titleLeftDescription")
        self.titleLeftDescription.setGeometry(QRect(70, 27, 160, 16))
        self.titleLeftDescription.setMaximumSize(QSize(16777215, 16))
        self.titleLeftDescription.setFont(font2)
        self.titleLeftDescription.setAlignment(Qt.AlignLeading | Qt.AlignLeft | Qt.AlignTop)
        self.titleLeftDescription.setText("Remote Desktop Application")

        self.leftMenuFrame = QFrame(self.leftMenuWithLogoFrame)
        self.leftMenuFrame.setObjectName(u"leftMenuFrame")
        self.leftMenuFrame.setFrameShape(QFrame.NoFrame)
        self.leftMenuFrame.setFrameShadow(QFrame.Raised)
        self.leftMenuWithLogoLayout.addWidget(self.leftMenuFrame)

        self.leftMenuLayout = QVBoxLayout(self.leftMenuFrame)
        self.leftMenuLayout.setSpacing(0)
        self.leftMenuLayout.setObjectName(u"verticalMenuLayout")
        self.leftMenuLayout.setContentsMargins(0, 0, 0, 0)

        self.toggleMenuSizeFrame = QFrame(self.leftMenuFrame)
        self.toggleMenuSizeFrame.setObjectName(u"toggleBox")
        self.toggleMenuSizeFrame.setMaximumSize(QSize(16777215, 45))
        self.toggleMenuSizeFrame.setFrameShape(QFrame.NoFrame)
        self.toggleMenuSizeFrame.setFrameShadow(QFrame.Raised)
        self.leftMenuLayout.addWidget(self.toggleMenuSizeFrame)

        self.toggleMenuSizeLayout = QVBoxLayout(self.toggleMenuSizeFrame)
        self.toggleMenuSizeLayout.setSpacing(0)
        self.toggleMenuSizeLayout.setObjectName(u"verticalLayout_4")
        self.toggleMenuSizeLayout.setContentsMargins(0, 0, 0, 0)

        self.toggleButton = QPushButton(self.toggleMenuSizeFrame)
        self.toggleButton.setObjectName(u"toggleButton")
        sizePolicy = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.toggleButton.sizePolicy().hasHeightForWidth())
        self.toggleButton.setSizePolicy(sizePolicy)
        self.toggleButton.setMinimumSize(QSize(0, 45))
        self.toggleButton.setFont(font)
        self.toggleButton.setCursor(QCursor(Qt.PointingHandCursor))
        self.toggleButton.setLayoutDirection(Qt.LeftToRight)
        self.toggleButton.setStyleSheet(Settings.TOGGLE_BUTTON_UL)
        self.toggleMenuSizeLayout.addWidget(self.toggleButton)

        self.buttonMenu = QFrame(self.leftMenuFrame)
        self.buttonMenu.setObjectName(u"buttonMenu")
        self.buttonMenu.setFrameShape(QFrame.NoFrame)
        self.buttonMenu.setFrameShadow(QFrame.Raised)
        self.leftMenuLayout.addWidget(self.buttonMenu, 0, Qt.AlignTop)

        self.buttonMenuLayout = QVBoxLayout(self.buttonMenu)
        self.buttonMenuLayout.setSpacing(0)
        self.buttonMenuLayout.setObjectName(u"verticalLayout_8")
        self.buttonMenuLayout.setContentsMargins(0, 0, 0, 0)

        # Kafka Button
        self.btnKafka = QPushButton(self.buttonMenu)
        self.btnKafka.setObjectName(u"btnKafka")
        sizePolicy.setHeightForWidth(self.btnKafka.sizePolicy().hasHeightForWidth())
        self.btnKafka.setSizePolicy(sizePolicy)
        self.btnKafka.setMinimumSize(QSize(0, 45))
        self.btnKafka.setFont(font)
        self.btnKafka.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnKafka.setLayoutDirection(Qt.LeftToRight)
        self.btnKafka.setStyleSheet(Settings.BUTTON_KAFKA_URL)
        self.buttonMenuLayout.addWidget(self.btnKafka)
        # end kafka Button

        # Login Window Button
        self.btnLogin = QPushButton(self.buttonMenu)
        self.btnLogin.setObjectName(u"btnLogin")
        sizePolicy.setHeightForWidth(self.btnLogin.sizePolicy().hasHeightForWidth())
        self.btnLogin.setSizePolicy(sizePolicy)
        self.btnLogin.setMinimumSize(QSize(0, 45))
        self.btnLogin.setFont(font)
        self.btnLogin.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnLogin.setLayoutDirection(Qt.LeftToRight)
        self.btnLogin.setStyleSheet(Settings.BUTTON_LOGIN_URL)
        self.buttonMenuLayout.addWidget(self.btnLogin)
        # End Login Window Button

        # Administrate Account Button
        self.btnAdministrateAccount = QPushButton(self.buttonMenu)
        self.btnAdministrateAccount.setObjectName(u"btnAdministrateAccount")
        sizePolicy.setHeightForWidth(self.btnAdministrateAccount.sizePolicy().hasHeightForWidth())
        self.btnAdministrateAccount.setSizePolicy(sizePolicy)
        self.btnAdministrateAccount.setMinimumSize(QSize(0, 45))
        self.btnAdministrateAccount.setFont(font)
        self.btnAdministrateAccount.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnAdministrateAccount.setLayoutDirection(Qt.LeftToRight)
        self.btnAdministrateAccount.setStyleSheet(Settings.BUTTON_ACCOUNT_SETTINGS)
        self.buttonMenuLayout.addWidget(self.btnAdministrateAccount)
        # End Administrate Account Window Button

        # Register Window Button
        self.btnRegister = QPushButton(self.buttonMenu)
        self.btnRegister.setObjectName(u"btnRegister")
        sizePolicy.setHeightForWidth(self.btnRegister.sizePolicy().hasHeightForWidth())
        self.btnRegister.setSizePolicy(sizePolicy)
        self.btnRegister.setMinimumSize(QSize(0, 45))
        self.btnRegister.setFont(font)
        self.btnRegister.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnRegister.setLayoutDirection(Qt.LeftToRight)
        self.btnRegister.setStyleSheet(Settings.BUTTON_REGISTER_URL)
        self.buttonMenuLayout.addWidget(self.btnRegister)
        # End register Window Button

        # Call Window Button
        self.btnCall = QPushButton(self.buttonMenu)
        self.btnCall.setObjectName(u"btnCall")
        sizePolicy.setHeightForWidth(self.btnCall.sizePolicy().hasHeightForWidth())
        self.btnCall.setSizePolicy(sizePolicy)
        self.btnCall.setMinimumSize(QSize(0, 45))
        self.btnCall.setFont(font)
        self.btnCall.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnCall.setLayoutDirection(Qt.LeftToRight)
        self.btnCall.setStyleSheet(Settings.BUTTON_CALL_URL)
        self.buttonMenuLayout.addWidget(self.btnCall)
        # End Call Window Button

        # My Videos Button
        self.btnMyVideos = QPushButton(self.buttonMenu)
        self.btnMyVideos.setObjectName(u"btnMyVideos")
        sizePolicy.setHeightForWidth(self.btnMyVideos.sizePolicy().hasHeightForWidth())
        self.btnMyVideos.setSizePolicy(sizePolicy)
        self.btnMyVideos.setMinimumSize(QSize(0, 45))
        self.btnMyVideos.setFont(font)
        self.btnMyVideos.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnMyVideos.setLayoutDirection(Qt.LeftToRight)
        self.btnMyVideos.setStyleSheet(Settings.BUTTON_MY_VIDEOS_URL)
        self.buttonMenuLayout.addWidget(self.btnMyVideos)
        # End My Videos Button

        # Share File Button
        self.btnShareFile = QPushButton(self.buttonMenu)
        self.btnShareFile.setObjectName(u"btnShareFile")
        sizePolicy.setHeightForWidth(self.btnShareFile.sizePolicy().hasHeightForWidth())
        self.btnShareFile.setSizePolicy(sizePolicy)
        self.btnShareFile.setMinimumSize(QSize(0, 45))
        self.btnShareFile.setFont(font)
        self.btnShareFile.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnShareFile.setLayoutDirection(Qt.LeftToRight)
        self.btnShareFile.setStyleSheet(Settings.BUTTON_FILE_SHARING)
        self.buttonMenuLayout.addWidget(self.btnShareFile)
        # End Share File Button

        # change theme button
        self.btnChangeTheme = QPushButton(self.buttonMenu)
        self.btnChangeTheme.setObjectName(u"btnChangeTheme")
        sizePolicy.setHeightForWidth(self.btnChangeTheme.sizePolicy().hasHeightForWidth())
        self.btnChangeTheme.setSizePolicy(sizePolicy)
        self.btnChangeTheme.setMinimumSize(QSize(0, 45))
        self.btnChangeTheme.setFont(font)
        self.btnChangeTheme.setCursor(QCursor(Qt.PointingHandCursor))
        self.btnChangeTheme.setLayoutDirection(Qt.LeftToRight)
        self.btnChangeTheme.setStyleSheet(Settings.BUTTON_CHANGE_THEME_URL)
        self.buttonMenuLayout.addWidget(self.btnChangeTheme)
        # change theme button end

        self.contentBox = QFrame(self.applicationFrame)
        self.contentBox.setObjectName(u"contentBox")
        self.contentBox.setFrameShape(QFrame.NoFrame)
        self.contentBox.setFrameShadow(QFrame.Raised)
        self.appLayout.addWidget(self.contentBox)

        self.contentBoxLayout = QVBoxLayout(self.contentBox)
        self.contentBoxLayout.setSpacing(0)
        self.contentBoxLayout.setObjectName(u"verticalLayout_2")
        self.contentBoxLayout.setContentsMargins(0, 0, 0, 0)

        self.topBarFrame = QFrame(self.contentBox)
        self.topBarFrame.setObjectName(u"topBarFrame")
        self.topBarFrame.setMinimumSize(QSize(0, 50))
        self.topBarFrame.setMaximumSize(QSize(16777215, 50))
        self.topBarFrame.setFrameShape(QFrame.NoFrame)
        self.topBarFrame.setFrameShadow(QFrame.Raised)
        self.contentBoxLayout.addWidget(self.topBarFrame)

        self.contentTopLayout = QHBoxLayout(self.topBarFrame)
        self.contentTopLayout.setSpacing(0)
        self.contentTopLayout.setObjectName(u"horizontalLayout")
        self.contentTopLayout.setContentsMargins(0, 0, 10, 0)

        self.topLeftBox = QFrame(self.topBarFrame)
        self.topLeftBox.setObjectName(u"leftBox")
        sizePolicy1 = QSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        sizePolicy1.setHorizontalStretch(0)
        sizePolicy1.setVerticalStretch(0)
        sizePolicy1.setHeightForWidth(self.topLeftBox.sizePolicy().hasHeightForWidth())
        self.topLeftBox.setSizePolicy(sizePolicy1)
        self.topLeftBox.setFrameShape(QFrame.NoFrame)
        self.topLeftBox.setFrameShadow(QFrame.Raised)
        self.contentTopLayout.addWidget(self.topLeftBox)

        self.topLeftBoxLayout = QHBoxLayout(self.topLeftBox)
        self.topLeftBoxLayout.setSpacing(0)
        self.topLeftBoxLayout.setObjectName(u"horizontalLayout_3")
        self.topLeftBoxLayout.setContentsMargins(0, 0, 0, 0)

        self.titleRightInfo = QLabel(self.topLeftBox)
        self.titleRightInfo.setObjectName(u"titleRightInfo")
        sizePolicy2 = QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)
        sizePolicy2.setHorizontalStretch(0)
        sizePolicy2.setVerticalStretch(0)
        sizePolicy2.setHeightForWidth(self.titleRightInfo.sizePolicy().hasHeightForWidth())
        self.titleRightInfo.setSizePolicy(sizePolicy2)
        self.titleRightInfo.setMaximumSize(QSize(16777215, 45))
        self.titleRightInfo.setFont(font)
        self.titleRightInfo.setAlignment(Qt.AlignLeading | Qt.AlignLeft | Qt.AlignVCenter)
        self.topLeftBoxLayout.addWidget(self.titleRightInfo)

        self.rightButtonsFrame = QFrame(self.topBarFrame)
        self.rightButtonsFrame.setObjectName(u"rightButtons")
        self.rightButtonsFrame.setMinimumSize(QSize(0, 28))
        self.rightButtonsFrame.setFrameShape(QFrame.NoFrame)
        self.rightButtonsFrame.setFrameShadow(QFrame.Raised)
        self.contentTopLayout.addWidget(self.rightButtonsFrame, 0, Qt.AlignRight)

        self.rightButtonsLayout = QHBoxLayout(self.rightButtonsFrame)
        self.rightButtonsLayout.setSpacing(5)
        self.rightButtonsLayout.setObjectName(u"horizontalLayout_2")
        self.rightButtonsLayout.setContentsMargins(0, 0, 0, 0)

        # User Button
        self.authenticatedUserBtn = QPushButton(self.rightButtonsFrame)
        self.authenticatedUserBtn.setObjectName(u"authenticatedUserBtn")
        self.authenticatedUserBtn.setMinimumSize(QSize(28, 28))
        self.authenticatedUserBtn.setMaximumSize(QSize(200, 28))
        self.authenticatedUserBtn.setCursor(QCursor(Qt.PointingHandCursor))
        icon2 = QIcon()
        icon2.addFile(str(Path(__file__).parent.parent / 'images' / 'icons' / 'cil-user.png'), QSize(),
                      QIcon.Normal, QIcon.Off)
        self.authenticatedUserBtn.setIcon(icon2)
        self.authenticatedUserBtn.setIconSize(QSize(20, 20))
        self.authenticatedUserBtn.setText("Guest")
        self.rightButtonsLayout.addWidget(self.authenticatedUserBtn)
        # End User Button

        # Minimize App Button
        self.minimizeAppBtn = QPushButton(self.rightButtonsFrame)
        self.minimizeAppBtn.setObjectName(u"minimizeAppBtn")
        self.minimizeAppBtn.setMinimumSize(QSize(28, 28))
        self.minimizeAppBtn.setMaximumSize(QSize(28, 28))
        self.minimizeAppBtn.setCursor(QCursor(Qt.PointingHandCursor))
        icon2 = QIcon()
        icon2.addFile(str(Path(__file__).parent.parent / 'images' / 'icons' / 'icon_minimize.png'), QSize(), QIcon.Normal, QIcon.Off)
        self.minimizeAppBtn.setIcon(icon2)
        self.minimizeAppBtn.setIconSize(QSize(20, 20))
        self.rightButtonsLayout.addWidget(self.minimizeAppBtn)
        # End Minimize App Button

        # Maximize App Button
        self.maximizeRestoreAppBtn = QPushButton(self.rightButtonsFrame)
        self.maximizeRestoreAppBtn.setObjectName(u"maximizeRestoreAppBtn")
        self.maximizeRestoreAppBtn.setMinimumSize(QSize(28, 28))
        self.maximizeRestoreAppBtn.setMaximumSize(QSize(28, 28))
        self.maximizeRestoreAppBtn.setFont(font3)
        self.maximizeRestoreAppBtn.setCursor(QCursor(Qt.PointingHandCursor))
        icon3 = QIcon()
        icon3.addFile(str(Path(__file__).parent.parent / 'images' / 'icons' / 'icon_maximize.png'), QSize(), QIcon.Normal, QIcon.Off)
        self.maximizeRestoreAppBtn.setIcon(icon3)
        self.maximizeRestoreAppBtn.setIconSize(QSize(20, 20))
        self.rightButtonsLayout.addWidget(self.maximizeRestoreAppBtn)
        # End Maximize App Button

        # Close App Button
        self.closeAppBtn = QPushButton(self.rightButtonsFrame)
        self.closeAppBtn.setObjectName(u"closeAppBtn")
        self.closeAppBtn.setMinimumSize(QSize(28, 28))
        self.closeAppBtn.setMaximumSize(QSize(28, 28))
        self.closeAppBtn.setCursor(QCursor(Qt.PointingHandCursor))
        iconClose = QIcon()
        iconClose.addFile(str(Path(__file__).parent.parent / 'images' / 'icons' / 'cil-x.png'), QSize(), QIcon.Normal, QIcon.Off)
        self.closeAppBtn.setIcon(iconClose)
        self.closeAppBtn.setIconSize(QSize(20, 20))
        self.rightButtonsLayout.addWidget(self.closeAppBtn)
        # Close App Button

        self.contentBottomFrame = QFrame(self.contentBox)
        self.contentBottomFrame.setObjectName(u"contentBottom")
        self.contentBottomFrame.setFrameShape(QFrame.NoFrame)
        self.contentBottomFrame.setFrameShadow(QFrame.Raised)
        self.contentBoxLayout.addWidget(self.contentBottomFrame)

        self.contentBottomLayout = QVBoxLayout(self.contentBottomFrame)
        self.contentBottomLayout.setSpacing(0)
        self.contentBottomLayout.setObjectName(u"verticalLayout_6")
        self.contentBottomLayout.setContentsMargins(0, 0, 0, 0)

        self.contentFrame = QFrame(self.contentBottomFrame)
        self.contentFrame.setObjectName(u"content")
        self.contentFrame.setFrameShape(QFrame.NoFrame)
        self.contentFrame.setFrameShadow(QFrame.Raised)
        self.contentBottomLayout.addWidget(self.contentFrame)

        self.contentLayout = QHBoxLayout(self.contentFrame)
        self.contentLayout.setSpacing(0)
        self.contentLayout.setObjectName(u"horizontalLayout_4")
        self.contentLayout.setContentsMargins(0, 0, 0, 0)

        self.pagesContainer = QFrame(self.contentFrame)
        self.pagesContainer.setObjectName(u"pagesContainer")
        self.pagesContainer.setFrameShape(QFrame.NoFrame)
        self.pagesContainer.setFrameShadow(QFrame.Raised)
        self.contentLayout.addWidget(self.pagesContainer)

        self.pagesContainerLayout = QVBoxLayout(self.pagesContainer)
        self.pagesContainerLayout.setSpacing(0)
        self.pagesContainerLayout.setObjectName(u"verticalLayout_15")
        self.pagesContainerLayout.setContentsMargins(10, 10, 10, 10)

        self.pagesStack = QStackedWidget(self.pagesContainer)
        self.pagesStack.setObjectName(u"stackedWidget")
        self.pagesStack.setStyleSheet(u"background: transparent;")
        self.pagesContainerLayout.addWidget(self.pagesStack)

        self.bottomBar = QFrame(self.contentBottomFrame)
        self.bottomBar.setObjectName(u"bottomBar")
        self.bottomBar.setMinimumSize(QSize(0, 22))
        self.bottomBar.setMaximumSize(QSize(16777215, 22))
        self.bottomBar.setFrameShape(QFrame.NoFrame)
        self.bottomBar.setFrameShadow(QFrame.Raised)
        self.contentBottomLayout.addWidget(self.bottomBar)

        self.bottomBarLayout = QHBoxLayout(self.bottomBar)
        self.bottomBarLayout.setSpacing(0)
        self.bottomBarLayout.setObjectName(u"horizontalLayout_5")
        self.bottomBarLayout.setContentsMargins(0, 0, 0, 0)

        self.nameLabel = QLabel(self.bottomBar)
        self.nameLabel.setObjectName(u"creditsLabel")
        self.nameLabel.setMaximumSize(QSize(3840, 16))
        self.nameLabel.setFont(font5)
        self.nameLabel.setAlignment(Qt.AlignLeading | Qt.AlignLeft | Qt.AlignVCenter)
        self.bottomBarLayout.addWidget(self.nameLabel)
        self.nameLabel.setText("By: Tutuianu Robert-Constantin")

        self.resizeGrip = QFrame(self.bottomBar)
        self.resizeGrip.setObjectName(u"frame_size_grip")
        self.resizeGrip.setMinimumSize(QSize(20, 0))
        self.resizeGrip.setMaximumSize(QSize(20, 3840))
        self.resizeGrip.setFrameShape(QFrame.NoFrame)
        self.resizeGrip.setFrameShadow(QFrame.Raised)
        self.bottomBarLayout.addWidget(self.resizeGrip)

        # Kafka Window
        self.kafkaWindow = KafkaWindow(MainWindow)
        self.pagesStack.addWidget(self.kafkaWindow)
        # End Kafka Window

        # Login Window
        self.loginWindow = Login(MainWindow)
        self.pagesStack.addWidget(self.loginWindow)
        # End Login Window

        # Administrate account Window
        self.administrateAccountWindow = ChangePassword(MainWindow)
        self.pagesStack.addWidget(self.administrateAccountWindow)
        # End Administrate account Window

        # Register Window
        self.registerWindow = Register(MainWindow)
        self.pagesStack.addWidget(self.registerWindow)
        # End register Window

        # Call Window
        self.callWindow = CallWindow(MainWindow)
        self.pagesStack.addWidget(self.callWindow)
        # End Call Window

        # My Videos Window
        self.myVideosWindow = MyVideosWindow()
        self.pagesStack.addWidget(self.myVideosWindow)
        # End My Videos Window

        # My Videos Window
        self.shareFileWindow = ShareFileWindow(MainWindow)
        self.pagesStack.addWidget(self.shareFileWindow)
        # End My Videos Window

        MainWindow.setWindowTitle("MainWindow")
        self.toggleButton.setText("Hide")
        self.btnKafka.setText("Kafka")
        self.btnLogin.setText("Login")
        self.btnRegister.setText("Register")
        self.btnCall.setText("Call")
        self.btnMyVideos.setText("MyVideos")
        self.btnShareFile.setText("Share File")
        self.btnAdministrateAccount.setText("Administrate Account")
        self.btnChangeTheme.setText("Change Theme")
        self.minimizeAppBtn.setToolTip("Minimize")
        self.minimizeAppBtn.setText("")
        self.maximizeRestoreAppBtn.setToolTip("Maximize")
        self.maximizeRestoreAppBtn.setText("")
        self.closeAppBtn.setToolTip("Close")
        self.closeAppBtn.setText("")

    def maximize_restore(self):
        if not self.state:
            self.master.showMaximized()
        else:
            self.master.showNormal()
            self.master.resize(1280, 720)
        self.state = not self.state

    def returStatus(self):
        return self.state

    def setStatus(self, status):
        self.state = status

    def toggleMenu(self):
        width = Settings.MENU_WIDTH if self.leftMenuWithLogoFrame.width() == 60 else 60
        # ANIMATION
        self.animation = QPropertyAnimation(self.leftMenuWithLogoFrame, b"minimumWidth")
        self.animation.setDuration(Settings.TIME_ANIMATION)
        self.animation.setStartValue(60 if self.leftMenuWithLogoFrame.width() == 60 else Settings.MENU_WIDTH)
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

    def resetStyle(self, widget):
        for w in self.buttonMenu.findChildren(QPushButton):
            if w.objectName() != widget:
                try:
                    mainWindowWidget = w.styleSheet()
                    style = self.deselectMenu(mainWindowWidget)
                    w.setStyleSheet(style)
                except Exception as ex:
                    print(ex)

    def theme(self, file):
        str = open(file, 'r').read()
        self.mainWindowWidget.setStyleSheet(str)

    def uiDefinitions(self):
        def dobleClickMaximizeRestore(event):
            if event.type() == QEvent.MouseButtonDblClick:
                QTimer.singleShot(250, lambda: self.maximize_restore())
        self.titleRightInfo.mouseDoubleClickEvent = dobleClickMaximizeRestore

        self.master.setWindowFlags(Qt.FramelessWindowHint)
        self.master.setAttribute(Qt.WA_TranslucentBackground)

        def moveWindow(event):
            # IF MAXIMIZED CHANGE TO NORMAL
            if self.returStatus():
                self.maximize_restore()
            # MOVE WINDOW
            if event.buttons() == Qt.LeftButton:
                self.master.move(self.master.pos() + event.globalPos() - self.master.dragPos)
                self.master.dragPos = event.globalPos()
                event.accept()
        self.titleRightInfo.mouseMoveEvent = moveWindow

        # DROP SHADOW
        self.shadow = QGraphicsDropShadowEffect(self.master)
        self.shadow.setBlurRadius(17)
        self.shadow.setXOffset(0)
        self.shadow.setYOffset(0)
        self.shadow.setColor(QColor(0, 0, 0, 150))
        self.applicationFrame.setGraphicsEffect(self.shadow)

        # RESIZE WINDOW
        self.sizegrip = QSizeGrip(self.resizeGrip)
        self.sizegrip.setStyleSheet("width: 20px; height: 20px; margin 0px; padding: 0px;")

        # MINIMIZE
        self.minimizeAppBtn.clicked.connect(lambda: self.master.showMinimized())

        # MAXIMIZE/RESTORE
        self.maximizeRestoreAppBtn.clicked.connect(lambda: self.maximize_restore())

        # CLOSE APPLICATION
        self.closeAppBtn.clicked.connect(self.master.closeApplication)

    def setConnectedToKafkaState(self, address):
        self.kafkaWindow.pushButton.setText("Disconnect")
        self.kafkaWindow.lineEdit.setText(address)
        self.kafkaWindow.lineEdit.setDisabled(True)

        self.kafkaWindow.pushButton.clicked.disconnect()
        self.kafkaWindow.pushButton.clicked.connect(self.master.disconnectFromKafka)

    def setIsNotConnectedToKafkaState(self):
        self.kafkaWindow.pushButton.setText("Connect")
        self.kafkaWindow.lineEdit.setText("")
        self.kafkaWindow.lineEdit.setDisabled(False)

        self.kafkaWindow.pushButton.clicked.disconnect()
        self.kafkaWindow.pushButton.clicked.connect(self.master.connectToKafka)

    def setUserLoggedIn(self, username: str):
        self.loginWindow.usernameLineEdit.setText(username)
        self.loginWindow.passwordLineEit.setDisabled(True)
        self.loginWindow.usernameLineEdit.setDisabled(True)
        self.loginWindow.pushButton.setText("Disconnect")
        self.loginWindow.pushButton.clicked.disconnect()
        self.loginWindow.pushButton.clicked.connect(self.master.disconnectAccount)
        self.authenticatedUserBtn.setText(username)

    def setUserNotLoggedIn(self):
        self.loginWindow.usernameLineEdit.setText("")
        self.loginWindow.passwordLineEit.setText("")
        self.loginWindow.passwordLineEit.setDisabled(False)
        self.loginWindow.usernameLineEdit.setDisabled(False)
        self.loginWindow.pushButton.setText("Connect")
        self.loginWindow.pushButton.clicked.disconnect()
        self.loginWindow.pushButton.clicked.connect(self.master.loginAccount)
        self.authenticatedUserBtn.setText("Guest")

    def setStatusMessage(self, message, isError=False):
        if isError:
            self.titleRightInfo.setStyleSheet("color: #ff0000;")
        else:
            self.titleRightInfo.setStyleSheet("color: #ffffff;")
        self.titleRightInfo.setText(message)
