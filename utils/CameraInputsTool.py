import time

import cv2
import pyautogui
from PySide6.QtCore import QThread


class CameraInputTool(QThread):
    def __init__(self):
        super().__init__()
        self.videoStream = cv2.VideoCapture(0)
        self.videoStream.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
        self.videoStream.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
        self._run_flag = True
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
        self.prevX = 0
        self.prevY = 0
        self.stayInPlaceTime = time.time()

    def run(self):
        while self._run_flag:
            ret, img = self.videoStream.read()
            img = cv2.flip(img, 1)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faceRects = self.face_cascade.detectMultiScale(gray, 1.3, 5)

            if len(faceRects) != 0:
                (x, y, w, h) = faceRects[0]

                if self.prevX != 0 and self.prevY != 0:
                    diffX = x - self.prevX
                    diffY = y - self.prevY
                    if diffX < -10:
                        pyautogui.moveRel(-40, 0)
                        self.stayInPlaceTime = time.time()
                    elif diffX > 10:
                        pyautogui.moveRel(40, 0)
                        self.stayInPlaceTime = time.time()
                    elif diffY < -10:
                        pyautogui.moveRel(0, -40)
                        self.stayInPlaceTime = time.time()
                    elif diffY > 10:
                        pyautogui.moveRel(0, 40)
                        self.stayInPlaceTime = time.time()
                    else:
                        if time.time() - self.stayInPlaceTime > 2:
                            pyautogui.leftClick()
                            self.stayInPlaceTime = time.time()
                self.prevX = x
                self.prevY = y

    def stop(self):
        self._run_flag = False
        QThread.sleep(1)
        self.videoStream.release()
        self.quit()