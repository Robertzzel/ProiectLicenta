import os
import threading
import soundfile as sf
import sounddevice as sd
import numpy as np
import queue
from typing import *
import time

SAMPLERATE = 44100
CHANNELS = 1


def create_audio_file(path, audio_buffer: np.ndarray, samplerate):
    open(path, "w").close()  # create file
    sf.write(path, audio_buffer, samplerate=samplerate)


class Recorder:
    def __init__(self, audio_queue: queue.Queue):
        self.buffer: np.ndarray = np.array([])
        self.lock = threading.Lock()
        self.audio_queue = audio_queue
        self.input_stream = sd.InputStream(
            samplerate=SAMPLERATE,
            channels=CHANNELS,
            device=self.get_device('pulse'),
            callback=self.stream_callback,
            latency='low', dtype='float32',
        )
        self.process_thread: Optional[threading.Thread] = None
        self.running = True

    def start(self, start_time: time.time, chunk_size_seconds: int):
        while start_time - 1 - time.time() > 0:
            time.sleep(start_time - time.time())

        self.input_stream.start()
        self.process_thread = threading.Thread(target=self.process_buffer, args=(start_time, chunk_size_seconds))
        self.process_thread.start()

    def close(self):
        self.input_stream.close()
        self.running = False
        self.process_thread.join()

    def stream_callback(self, indata, _, time_info, s_):
        self.lock.acquire()
        self.buffer = np.append(self.buffer, indata)
        self.lock.release()

    def process_buffer(self, start_time: time.time, chunk_size_seconds: int):
        next_chunk_end = start_time + chunk_size_seconds
        cwd = os.getcwd()

        while self.running:
            while next_chunk_end - time.time() > 0:
                time.sleep(next_chunk_end - time.time())

            self.lock.acquire()
            audio_chunk = self.buffer.tolist()[:]
            self.buffer = np.array([])
            self.lock.release()

            if len(audio_chunk) > SAMPLERATE:
                audio_chunk = audio_chunk[len(audio_chunk) - SAMPLERATE:]

            audio_file_name = cwd + "/audio/" + str(int(next_chunk_end - chunk_size_seconds)) + ".wav"
            create_audio_file(audio_file_name, audio_chunk, SAMPLERATE)
            self.audio_queue.put_nowait(audio_file_name)

            next_chunk_end = next_chunk_end + chunk_size_seconds

    @staticmethod
    def get_device(device_name: str):
        for index, dev in enumerate(sd.query_devices()):
            if device_name in dev['name']:
                return index
        return None
