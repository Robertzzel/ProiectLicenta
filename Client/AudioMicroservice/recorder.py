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
    open(path, "w").close()
    sf.write(path, audio_buffer, samplerate=samplerate)


class Recorder:
    def __init__(self, audio_queue: queue.Queue):
        self.buffer: np.ndarray = np.array([], dtype=np.int32)
        self.lock = threading.Lock()
        self.audio_queue = audio_queue
        self.input_stream = sd.InputStream(
            samplerate=SAMPLERATE,
            channels=CHANNELS,
            device=self.get_device('pulse'),
            callback=self.stream_callback,
            latency='low', dtype='int32',
        )
        self.process_thread: Optional[threading.Thread] = None
        self.running = True

    def start(self, start_time: time.time, chunk_size_seconds: float):
        while start_time - 1 - time.time() > 0:
            time.sleep(start_time - time.time())

        self.input_stream.start()
        self.process_thread = threading.Thread(target=self.process_buffer, args=(start_time, chunk_size_seconds))
        self.process_thread.start()

    def close(self):
        self.input_stream.close()
        self.running = False
        if self.process_thread is not None:
            self.process_thread.join()

    def stream_callback(self, indata, _, time_info, s_):
        self.lock.acquire()
        self.buffer = np.append(self.buffer, indata)
        self.lock.release()

    def process_buffer(self, start_time: time.time, chunk_size_seconds: float):
        next_chunk_end = start_time + chunk_size_seconds
        number_of_bytes_needed = int(SAMPLERATE * chunk_size_seconds)

        while self.running:
            while next_chunk_end - time.time() > 0:
                time.sleep(next_chunk_end - time.time())

            self.lock.acquire()
            audio_chunk = np.empty_like(self.buffer)
            audio_chunk[:] = self.buffer
            self.buffer = np.array([], dtype=np.int32)
            self.lock.release()

            bytes_difference = len(audio_chunk) - number_of_bytes_needed
            if bytes_difference > 0:
                audio_chunk = audio_chunk[bytes_difference:]
            elif bytes_difference < 0:
                audio_chunk = np.append(audio_chunk, np.array([0] * -bytes_difference, dtype=np.int16))

            audio_file_name = "/tmp/" + str(int((next_chunk_end - chunk_size_seconds) * 1000)) + ".wav"
            create_audio_file(audio_file_name, audio_chunk, SAMPLERATE)
            self.audio_queue.put_nowait(audio_file_name)

            next_chunk_end = next_chunk_end + chunk_size_seconds

    @staticmethod
    def get_device(device_name: str):
        for index, dev in enumerate(sd.query_devices()):
            if device_name in dev['name']:
                return index
        return None
