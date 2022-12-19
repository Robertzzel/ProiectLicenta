import queue
import time
from recorder import Recorder
import kafka
import sys

AUDIO_TOPIC = "audio"
AUDIO_START_TOPIC = "saudio"
VIDEO_LENGTH = 1
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10


def main():
    if len(sys.argv) < 2:
        print("No broker address given")
        return

    brokerAddress = sys.argv[1]

    audio_blocks_recorded: queue.Queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(audio_blocks_recorded)

    producer = kafka.KafkaProducer(bootstrap_servers=brokerAddress, acks=1)
    consumer = kafka.KafkaConsumer(AUDIO_START_TOPIC, bootstrap_servers=brokerAddress, enable_auto_commit=True)

    try:
        print("Waiting for message")
        ts = int(next(consumer).value.decode())
        print("starting")

        audio_recorder.start(ts, VIDEO_LENGTH)

        while True:
            audio_file: str = audio_blocks_recorded.get(block=True)

            producer.send(
                topic=AUDIO_TOPIC,
                value=audio_file.encode(),
                headers=[("number-of-messages", b'00001'), ("message-number", b'00000')],
                partition=0
            )

            print(f"message {audio_file} sent at {time.time()}")
    except KeyboardInterrupt:
        print("Keyboard interrupt")
    except Exception as ex:
        print("ERROR!: ", ex)

    audio_recorder.close()
    producer.close()
    kafka.KafkaAdminClient(bootstrap_servers=brokerAddress).delete_topics([AUDIO_TOPIC])
    print("Cleanup done")
    sys.exit(1)

if __name__ == "__main__":
    main()

