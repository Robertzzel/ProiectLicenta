import sounddevice as sd
from kafka import KafkaConsumer
import numpy
from threading import Lock, Thread
import asyncio

thread_lock = Lock()
kafkaTopic = "rAudio"
sampleRate = 44100


def play_audio(sounds):
    thread_lock.acquire()
    sd.play(data=sounds, samplerate=sampleRate, mapping=1, blocking=True)
    thread_lock.release()


if __name__ == "__main__":
    kafka_consumer = KafkaConsumer("rAudio")
    for audio in kafka_consumer:
        Thread(target=play_audio, args=(numpy.frombuffer(audio.value, dtype=numpy.float32),)).start()

