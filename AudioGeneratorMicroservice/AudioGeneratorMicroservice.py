import asyncio
import kafka
import numpy as np
import sounddevice as sd
import soundfile as sf
import time

kafkaTopic = "audio"
syncTopic = "sync"
audioSyncTopic = "audioSync"
videoSize = 1
syncInterval = 60


class AudioRecorder:
    def __init__(self):
        self.buffer = np.array([], dtype=np.uint8)
        self.start_time = None
        self.end_time = None
        self.samplerate = 44100
        self.channels = 1
        self.cleanup_coroutine = None
        self.input_stream = sd.InputStream(
            samplerate=self.samplerate,
            channels=self.channels,
            device=self.get_device('pulse'),
            callback=self.stream_callback,
            latency='low', dtype='float32',
        )

    def start(self):
        self.input_stream.start()
        self.cleanup_coroutine = asyncio.create_task(self.auto_cleanup())

    def stop(self):
        self.input_stream.stop()

    async def close(self):
        self.input_stream.close()
        print("TODO: Nu o sa termine")
        await self.cleanup_coroutine

    async def auto_cleanup(self):
        if self.start_time is None:
            await asyncio.sleep(1)

        while True:
            if len(self.buffer) >= self.samplerate * 10:
                self.buffer = self.buffer[self.samplerate * 3:]
                self.start_time += 3
            else:
                await asyncio.sleep(1)

    def stream_callback(self, indata, frames, t_, s_):
        if self.start_time is None:
            self.start_time = time.time()
            self.end_time = self.start_time

        self.append_to_buffer(indata)

    def get_device(self, device_name: str):
        for index, dev in enumerate(sd.query_devices()):
            if device_name in dev['name']:
                return index
        return None

    async def get_buffer_part(self, start_time, seconds):
        end_time_difference = self.end_time - (start_time + seconds)
        if end_time_difference < 0:
            print("Astept ", -end_time_difference)
            await asyncio.sleep(-end_time_difference)

        start_difference_time = start_time - self.start_time
        part_offset = int(start_difference_time * self.samplerate)
        part_size = seconds * self.samplerate

        return self.buffer.tolist()[part_offset: part_offset + part_size]

    def append_to_buffer(self, indata):
        self.buffer = np.append(self.buffer, indata)
        self.end_time += len(indata) / self.samplerate


def synchronise(producer, consumer):
    producer.send(audioSyncTopic, b".")
    received = next(consumer)
    return int(received.value.decode())


def create_audio_file(audio_buffer, samplerate):
    file_path = f"audio/" + str(int(time.time())) + ".wav"
    open(file_path, "w").close()  # create file

    sf.write(file_path, audio_buffer, samplerate=samplerate)

    return file_path


async def create_and_send_file(current_time, duration, audio_recorder, producer):
    s = time.time()
    buffer = await audio_recorder.get_buffer_part(current_time, duration)
    file_name = create_audio_file(buffer, audio_recorder.samplerate)
    producer.send(kafkaTopic, file_name.encode())
    print("audio ", current_time, time.time() - s)


async def main():
    producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
    consumer = kafka.KafkaConsumer(syncTopic)
    audio_recorder = AudioRecorder()
    audio_recorder.start()
    current_time = synchronise(producer, consumer)

    while True:
        tasks = []

        for i in range(syncInterval):
            tasks.append(asyncio.create_task(create_and_send_file(current_time + videoSize * i, videoSize, audio_recorder, producer)))

        await asyncio.gather(*tasks)

        current_time = synchronise(producer, consumer)
        print("sync")

if __name__ == "__main__":
    asyncio.run(main())





