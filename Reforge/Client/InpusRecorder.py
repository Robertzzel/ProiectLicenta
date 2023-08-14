import io
import struct
import time
from pynput import keyboard, mouse

class InputsRecorder:
    def __init__(self):
        self.enable = False
        self.buffer = bytearray()
        self.keyboard_listener = keyboard.Listener(on_press=self.on_keyboard_press, on_release=self.on_keyboard_release)
        self.mouse_listener = mouse.Listener(on_click=self.on_mouse_click)

    def on_keyboard_press(self, key):
        if not self.enable:
            return
        try:
            key_str = key.char
        except AttributeError:
            key_str = str(key)[4:]
        encoding = key_str.encode("utf-8")
        cmd: bytes = struct.pack("BB", 0, len(encoding)) + encoding
        self.buffer.extend(cmd)

    def on_keyboard_release(self, key):
        if not self.enable:
            return
        try:
            key_str = key.char
        except AttributeError:
            key_str = str(key)[4:]

        encoding = key_str.encode("utf-8")
        cmd = struct.pack("BB", 1, len(encoding)) + encoding
        self.buffer.extend(cmd)

    def on_mouse_click(self, x, y, button, pressed):
        if not self.enable:
            return
        cmd = struct.pack("BB", 2 if pressed else 3, button.value)
        self.buffer.extend(cmd)

    def pauseRecording(self):
        self.enable = False

    def resumeRecording(self):
        self.enable = True

    def start(self):
        self.keyboard_listener.start()
        self.mouse_listener.start()

    def stop(self):
        self.keyboard_listener.stop()
        self.mouse_listener.stop()

    def getBuffer(self):
        x = self.buffer[:]
        self.buffer = bytearray()
        return x

if __name__ == "__main__":
    i = InputsRecorder()
    i.start()
    time.sleep(10)
    i.stop()