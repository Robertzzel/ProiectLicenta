import time
import kafka
import numpy as np
import soundfile as sf
from recorder import Recorder

TOPIC_AUDIO = "audio"
SYNC_TOPIC_RECEIVER = "sync"
SYNC_TOPIC_SENDER = "audioSync"
VIDE_SIZE = 1
SAMPLERATE = 44100


def synchronise(producer, consumer):
    producer.send(SYNC_TOPIC_SENDER, b".")
    received = next(consumer)
    return int(received.value.decode())


def create_audio_file(file_path, audio_buffer, samplerate):
    open(file_path, "w").close()  # create file
    sf.write(file_path, np.array(audio_buffer, dtype='float32'), samplerate=samplerate)


def main():
    audio_recorder: Recorder = Recorder()
    audio_recorder.start()

    producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
    consumer = kafka.KafkaConsumer(SYNC_TOPIC_RECEIVER)

    print("Waiting for sync...")
    current_time = synchronise(producer, consumer)
    print("Received: ", current_time)

    iteration = 0
    while True:
        part_start_time = current_time + iteration * VIDE_SIZE
        buffer = audio_recorder.get(part_start_time, VIDE_SIZE)

        file_path = f"audio/" + str(part_start_time) + ".wav"
        create_audio_file(file_path, buffer, SAMPLERATE)
        producer.send(TOPIC_AUDIO, file_path.encode())

        print("audio ", file_path, time.time())
        iteration += 1


if __name__ == "__main__":
    main()
