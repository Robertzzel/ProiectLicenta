import threading

import sounddevice as sd
import kafka, time
import soundfile as sf
import numpy as np

kafkaTopic = "audio"
syncTopic = "sync"
audioSyncTopic = "audioSync"
secondToRecord = 1

buffer_start_time = None


class AudioRecorder:
    def __init__(self):
        self.buffer = np.array([], dtype=np.uint8)
        self.start_time = None
        self.end_time = None
        self.samplerate = 44100
        self.channels = 1
        self.cleanup_thread = threading.Thread(target=self.auto_cleanup)
        self.input_stream = sd.InputStream(
            samplerate=self.samplerate,
            channels=self.channels,
            device=self.get_device('pulse'),
            callback=self.stream_callback,
            latency='low', dtype='float32',
        )

    def start(self):
        self.input_stream.start()
        #self.cleanup_thread.start()

    def stop(self):
        self.input_stream.stop()

    def close(self):
        self.input_stream.close()

    def auto_cleanup(self):
        if self.start_time is None:
            time.sleep(1)

        while True:
            time_since_start = time.time() - self.start_time
            if time_since_start > 4:
                self.start_time += 2
                self.buffer = self.buffer[44100 * 2:]
            elif time_since_start > 0:
                time.sleep(time_since_start)
            else:
                time.sleep(1)

    def stream_callback(self, indata, frames, t_, s_):
        self.buffer = np.append(self.buffer, indata)

        if self.start_time is None:
            self.start_time = time.time()
            self.end_time = self.start_time

        self.end_time += frames / self.samplerate

    def get_device(self, device_name: str):
        for index, dev in enumerate(sd.query_devices()):
            if device_name in dev['name']:
                return index
        return None

    def get_buffer_part(self, start_time, seconds):
        end_time_difference = self.end_time - (start_time + seconds)
        if end_time_difference < 0:
            time.sleep(-end_time_difference)

        start_difference_time = start_time - self.start_time
        offset_difference = int(start_difference_time * self.samplerate)
        part_size = seconds * self.samplerate

        part = self.buffer.tolist()[offset_difference: offset_difference + part_size]
        return part


def synchronise():
    producer.send(audioSyncTopic, b".")
    str(next(consumer).value)


def create_audio_file(audio_buffer, samplerate):
    file_path = f"audio/" + str(int(time.time())) + ".wav"
    open(file_path, "w").close()  # create file

    sf.write(file_path, audio_buffer, samplerate=samplerate)

    return file_path


if __name__ == "__main__":
    producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
    consumer = kafka.KafkaConsumer(syncTopic)
    audio_recorder = AudioRecorder()
    audio_recorder.start()
    synchronise()

    while True:
        current_time = time.time()
        for i in range(30):
            buffer = audio_recorder.get_buffer_part(current_time + i, 1)
            file_name = create_audio_file(buffer, audio_recorder.samplerate)
            producer.send(kafkaTopic, file_name.encode())

        synchronise()
        print("sync")







