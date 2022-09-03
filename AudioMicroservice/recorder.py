import threading
import sounddevice as sd
from threading import Thread, Event
from audio_buffer import AudioBuffer
import time

SAMPLERATE = 44100
CHANNELS = 1


class Recorder:
    def __init__(self):
        self.buffer = AudioBuffer()
        self.input_stream = sd.InputStream(
            samplerate=SAMPLERATE,
            channels=CHANNELS,
            device=self.get_device('pulse'),
            callback=self.stream_callback,
            latency='low', dtype='float32',
        )
        self.cleanup_thread: Thread = threading.Thread(target=self.auto_cleanup)
        self.close_event = Event()

    def start(self):
        self.input_stream.start()
        while self.buffer.start_time is None:
            time.sleep(0.01)

        self.cleanup_thread.start()

    def stop(self):
        self.input_stream.stop()

    def close(self):
        self.close_event.set()
        self.input_stream.close()
        self.cleanup_thread.join()

    def auto_cleanup(self):
        while not self.close_event.is_set():
            if len(self.buffer) >= SAMPLERATE * 10:
                self.buffer.delete_from(SAMPLERATE * 3)
            else:
                time.sleep(1)

    def stream_callback(self, indata, _, t_, s_):
        self.buffer.append(indata)

    @staticmethod
    def get_device(device_name: str):
        for index, dev in enumerate(sd.query_devices()):
            if device_name in dev['name']:
                return index
        return None

    def get(self, start_time, seconds):
        end_time_difference = self.buffer.end_time() - (start_time + seconds)
        while end_time_difference < 0:
            time.sleep(-end_time_difference)
            end_time_difference = self.buffer.end_time() - (start_time + seconds)

        start_difference_time = start_time - self.buffer.start_time
        part_offset = int(start_difference_time * SAMPLERATE)
        part_size = seconds * SAMPLERATE

        return self.buffer.get(part_offset, part_size)
