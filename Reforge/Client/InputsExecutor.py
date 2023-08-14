import socket
import struct
import time

import pynput
from PyQt5.QtCore import QThread


class InputsExecutorThread(QThread):
    def __init__(self, receiverSocket, screenSize):
        super().__init__()
        self.receiverSocket: socket.socket = receiverSocket
        self.screenSizes = screenSize
        self.keyboard_controller: pynput.keyboard.Controller = pynput.keyboard.Controller()
        self.mouse_controller: pynput.mouse.Controller = pynput.mouse.Controller()

    def run(self):
        while True:
            data, _ = self.receiverSocket.recvfrom(65_507)
            dataSize = len(data)
            read = 0
            while read < dataSize:
                commandType = struct.unpack('B', data[read: read+1])[0]
                read += 1

                if commandType == 4:
                    x, y = struct.unpack("ff", data[read: read+8])
                    read += 8
                    self.mouse_controller.position = (int(x * self.screenSizes.width), int(y * self.screenSizes.height))
                elif commandType == 0: # keyboard press
                    buttonSize = struct.unpack("B", data[read: read + 1])[0]
                    read += 1
                    btn = data[read: read + buttonSize]
                    read += buttonSize
                    print("Key Pressed", btn)
                    btnString = btn.decode()
                    if len(btnString) == 1:
                        self.keyboard_controller.press(btnString)
                    else:
                        self.keyboard_controller.press(pynput.keyboard.Key[btnString])
                elif commandType == 1: # keyboard release
                    buttonSize = struct.unpack("B", data[read: read + 1])[0]
                    read += 1
                    btn = data[read: read + buttonSize]
                    read += buttonSize
                    print("Key Released", btn)
                    btnString = btn.decode()
                    if len(btnString) == 1:
                        self.keyboard_controller.release(btnString)
                    else:
                        self.keyboard_controller.release(pynput.keyboard.Key[btnString])
                elif commandType == 2: # mouse press
                    btn = struct.unpack("B", data[read: read + 1])[0]
                    read += 1
                    print("Mouse Pressed", btn)
                    self.mouse_controller.press(pynput.mouse.Button(btn))
                elif commandType == 3: # mouse release
                    btn = struct.unpack("B", data[read: read + 1])[0]
                    read += 1
                    print("Mouse Released", btn)
                    self.mouse_controller.release(pynput.mouse.Button(btn))
