import threading
import time


class InputsBuffer:
    def __init__(self, txt: str = ""):
        self.text = txt
        self.lock = threading.Lock()
        self.last_update: time.time = None

    def add(self, text: str):
        with self.lock:
            current_time = time.time()
            self.text += text + "," + str(
                round(current_time - self.last_update, 4) if self.last_update is not None else 0) + ";"
            self.last_update = current_time

    def get(self) -> str:
        with self.lock:
            returned = self.text[:-1]
            self.text = ""
            self.last_update = None

        return returned
