import threading
import kafka
import numpy as np
import sounddevice as sd
import soundfile as sf
import time
import asyncio

TOPIC_AUDIO = "audio"
SYNC_TOPIC_RECEIVER = "sync"
SYNC_TOPIC_SENDER = "audioSync"
VIDE_SIZE = 1
SYNC_INTERVAL = 60
SAMPLERATE = 44100
CHANNELS = 1


class AudioRecorder:
    def __init__(self):
        self.buffer = np.array([], dtype=np.uint8)
        self.start_time = None
        self.cleanup_thread = None
        self.input_stream = sd.InputStream(
            samplerate=SAMPLERATE,
            channels=CHANNELS,
            device=self.get_device('pulse'),
            callback=self.stream_callback,
            latency='low', dtype='float32',
        )

    def get_end_time(self):
        return self.start_time + len(self.buffer) / SAMPLERATE

    def wait_until_starts(self):
        while self.start_time is None:
            time.sleep(0.01)

    def start(self):
        self.input_stream.start()
        self.cleanup_thread = threading.Thread(target=self.auto_cleanup)
        self.cleanup_thread.start()

    def stop(self):
        self.input_stream.stop()
        self.cleanup_thread.join()

    def close(self):
        self.input_stream.close()

    def auto_cleanup(self):
        if self.start_time is None:
            time.sleep(1)

        while True:
            if len(self.buffer) >= SAMPLERATE * 10:
                self.buffer = self.buffer[SAMPLERATE * 3:]
                self.start_time += 3
            else:
                time.sleep(1)

    def stream_callback(self, indata, _, t_, s_):
        if self.start_time is None:
            self.start_time = time.time()

        self.buffer = np.append(self.buffer, indata)

    @staticmethod
    def get_device(device_name: str):
        for index, dev in enumerate(sd.query_devices()):
            if device_name in dev['name']:
                return index
        return None

    def get_buffer_part(self, start_time, seconds):
        if self.start_time > start_time:
            raise Exception("start time before recorder start time", self.start_time, start_time, self.start_time > start_time)

        end_time_difference = self.get_end_time() - (start_time + seconds)
        while end_time_difference < 0:
            time.sleep(-end_time_difference)
            end_time_difference = self.get_end_time() - (start_time + seconds)

        start_difference_time = start_time - self.start_time
        part_offset = int(start_difference_time * SAMPLERATE)
        part_size = seconds * SAMPLERATE

        return self.buffer.tolist()[part_offset: part_offset + part_size]


def synchronise(producer, consumer):
    producer.send(SYNC_TOPIC_SENDER, b".")
    received = next(consumer)
    return int(received.value.decode())


def create_audio_file(file_path, audio_buffer, samplerate):
    open(file_path, "w").close()  # create file
    sf.write(file_path, audio_buffer, samplerate=samplerate)


def main():
    audio_recorder = AudioRecorder()
    audio_recorder.start()
    audio_recorder.wait_until_starts()

    producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
    consumer = kafka.KafkaConsumer(SYNC_TOPIC_RECEIVER)

    print("Waiting for sync...")
    current_time = synchronise(producer, consumer)
    print("Received: ", current_time)

    while True:
        for i in range(SYNC_INTERVAL):
            part_start_time = current_time + i * VIDE_SIZE
            buffer = audio_recorder.get_buffer_part(part_start_time, VIDE_SIZE)

            file_path = f"audio/" + str(part_start_time) + ".wav"
            create_audio_file(file_path, buffer, SAMPLERATE)
            producer.send(TOPIC_AUDIO, file_path.encode())

            print("audio ", file_path, part_start_time)

        current_time = synchronise(producer, consumer)
        print("sync")


if __name__ == "__main__":
    main()
