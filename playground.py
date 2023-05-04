# from PySide6.QtCore import *
# from PySide6.QtWidgets import *
# from PySide6.QtGui import *
#
#
# from tkinter import *
# from tkinter.ttk import Progressbar
# from tkinter.ttk import Combobox
# from tkinter.ttk import Notebook
# from tkinter.ttk import Treeview
# from PIL import Image, ImageTk
# import tkinter.font
#
#
# class Widget2():
#     def __init__(self, parent):
#         self.gui(parent)
#
#     def gui(self, parent):
#         if parent == 0:
#             self.w2 = Tk()
#             self.w2.geometry('130x60')
#         else:
#             self.w2 = Frame(parent)
#             self.w2.place(x = 0, y = 0, width = 130, height = 60)
#         self.button1 = Button(self.w2, text = "Button", font = tkinter.font.Font(family = "Calibri", size = 9), cursor = "arrow", state = "normal")
#         self.button1.place(x = 20, y = 20, width = 90, height = 22)
#
#
# class Widget1(QWidget):
#     def __init__(self, parent=None):
#         super(Widget1, self).__init__(parent)
#         self.gui()
#
#     def gui(self):
#         self.w1 = self
#         self.w1.setAutoFillBackground(True)
#         self.w1.setWindowTitle("")
#         self.w1.resize(500, 450)
#         self.w1.setCursor(Qt.ArrowCursor)
#         self.w1.setToolTip("")
#         self.button1 = QToolButton(self.w1)
#         self.button1.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
#         self.button1.setText("Button")
#         self.button1.move(9, 214)
#         self.button1.resize(482, 22)
#         self.button1.setCursor(Qt.ArrowCursor)
#         self.button1.setToolTip("")
#         self.button1.clicked.connect(event)
#         #return self.w1
#
#
# def event():
#     b = Widget2(0)
#     b.w2.mainloop()
#
# if __name__ == '__main__':
#     import sys
#     app = QApplication(sys.argv)
#     a = Widget1()
#     a.show()
#     sys.exit(app.exec_())
import PySide6
import pynput
from PySide6 import QtCore
from PySide6.QtCore import *
from PySide6.QtWidgets import QApplication, QMainWindow, QPushButton, QLabel, QVBoxLayout, QWidget

import sys

from InputExecutorMicroservice.TkPynputKeyCodes import KeyTranslator


class AnotherWindow(QWidget):
    sig = Signal(int)

    def __init__(self, master):
        super().__init__()
        layout = QVBoxLayout()
        self.label = QLabel("Another Window")
        self.label2 = QLabel("Another Window2")
        layout.addWidget(self.label)
        layout.addWidget(self.label2)
        self.setLayout(layout)
        self.label.mouseMoveEvent = self.mouseMoveEvent
        self.label.keyPressEvent = self.keyPressEvent
        self.label.keyReleaseEvent = self.keyReleaseEvent
        self.label.mousePressEvent = self.mousePressEvent
        self.label.mouseReleaseEvent = self.mouseReleaseEvent
        self.label.setMouseTracking(True)
        self.keyboard_controller: pynput.keyboard.Controller = pynput.keyboard.Controller()

    # def em(self):
    #     self.sig.emit(1)
    #     self.sig.emit(1)

    def keyPressEvent(self, event):

        trans = KeyTranslator.translate(event.key())
        self.keyboard_controller.press(trans)
        print(f"{1},#{trans}#")

    def keyReleaseEvent(self, event):
        trans = KeyTranslator.translate(event.key())
        self.keyboard_controller.press(trans)
        print(f"{1},#{trans}#")

    def mousePressEvent(self, event):
        print("Mouse p:", event.button())

    def wheelEvent(self, event) -> None:
        print("Wheel:", event.angleDelta().y())

    def mouseReleaseEvent(self, event):
        print("Mouse r:", event.button())

    def mouseMoveEvent(self, event):
        print("Move:", event.globalPos().x(), event.globalPos().y())


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.button = QPushButton("Push for Window")
        self.button.clicked.connect(self.show_new_window)
        self.setCentralWidget(self.button)

    def show_new_window(self, checked):
        self.w = AnotherWindow(self)
        self.w.show()
        self.w.sig.connect(lambda x: print(x))


app = QApplication(sys.argv)
w = MainWindow()
w.show()
sys.exit(app.exec_())