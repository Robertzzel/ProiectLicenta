from typing import *
from time import time
from threading import Lock

import numpy as np

SAMPLERATE = 44100


class AudioBuffer:
    def __init__(self):
        self.buffer = np.array([], dtype='float32')
        self.start_time: Optional[time] = None
        self.lock = Lock()

    def __len__(self):
        with self.lock:
            return len(self.buffer)

    def append(self, iterable):
        with self.lock:
            if self.start_time is None:
                self.start_time = time()

            self.buffer = np.append(self.buffer, iterable)

    def end_time(self):
        with self.lock:
            return self.start_time + len(self.buffer) / SAMPLERATE

    def delete_from(self, index):
        with self.lock:
            self.buffer = self.buffer[index:]
            self.start_time += index / SAMPLERATE

    def delete(self, start_index: int, end_index: int):
        with self.lock:
            self.buffer = self.buffer[start_index: start_index+end_index]
            self.start_time += start_index / SAMPLERATE

    def get(self, offset: int, size: int):
        with self.lock:
            return self.buffer.tolist()[offset: offset+size]
