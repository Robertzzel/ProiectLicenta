import queue
import sys
import time
import numpy as np
import soundfile as sf
from recorder import Recorder
import kafka

AUDIO_TOPIC = "audio"
VIDE_SIZE = 1
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10
BROKER_ADDRESS = "localhost:9092"


def create_audio_file(path, audio_buffer):
    open(path, "w").close()  # create file
    sf.write(path, np.array(audio_buffer, dtype='float32'), samplerate=SAMPLERATE)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No timestamp given")
        sys.exit(1)

    recorder_queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(recorder_queue)
    producer = kafka.KafkaProducer(bootstrap_servers=BROKER_ADDRESS, acks=1)

    try:
        audio_recorder.start(int(sys.argv[1]), VIDE_SIZE)

        while True:
            audio_file: str = recorder_queue.get(block=True)
            producer.send(AUDIO_TOPIC, audio_file.encode())
            print(f"message {audio_file} sent at {time.time()}")
    except KeyboardInterrupt:
        print("Keyboard interrupt")
    except Exception as ex:
        print("ERROR!: ", ex)

    audio_recorder.close()
    producer.close()
    kafka.KafkaAdminClient(bootstrap_servers=BROKER_ADDRESS).delete_topics([AUDIO_TOPIC])
    print("Cleanup done")

