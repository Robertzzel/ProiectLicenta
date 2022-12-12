import queue
import sys
import time
from recorder import Recorder
import kafka

AUDIO_TOPIC = "audio"
VIDEO_LENGTH = 1 / 5
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10
BROKER_ADDRESS = "localhost:9092"


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("No timestamp given")
        sys.exit(1)

    recorder_queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(recorder_queue)
    producer = kafka.KafkaProducer(bootstrap_servers=BROKER_ADDRESS, acks=1)

    try:
        audio_recorder.start(int(sys.argv[1]), VIDEO_LENGTH)

        while True:
            audio_file: str = recorder_queue.get(block=True)

            producer.send(
                topic=AUDIO_TOPIC,
                value=audio_file.encode(),
                headers=[("number-of-messages", b'00001'), ("message-number", b'00000')]
            )

            print(f"message {audio_file} sent at {time.time()}")
    except KeyboardInterrupt:
        print("Keyboard interrupt")
    except Exception as ex:
        print("ERROR!: ", ex)

    audio_recorder.close()
    producer.close()
    kafka.KafkaAdminClient(bootstrap_servers=BROKER_ADDRESS).delete_topics([AUDIO_TOPIC])
    print("Cleanup done")
    sys.exit(1)

