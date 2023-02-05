import queue
from Kafka.partitions import *
from recorder import Recorder
import kafka
from Kafka.Kafka import KafkaConsumerWrapper
import sys
import time

VIDEO_LENGTH = 1
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10


def main():
    if len(sys.argv) < 2:
        print("No broker address and topic given")
        return

    brokerAddress = sys.argv[1]
    topic = sys.argv[2]

    audio_blocks_recorded: queue.Queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(audio_blocks_recorded)

    producer = kafka.KafkaProducer(bootstrap_servers=brokerAddress, acks=1)
    consumer = KafkaConsumerWrapper({'bootstrap.servers': brokerAddress, "group.id": "-"}, [(topic, AudioMicroservicePartition)])

    try:
        print("audio Waiting for message")
        ts = int(consumer.consumeMessage(None, AudioMicroservicePartition).value().decode())
        print("starting audio")

        audio_recorder.start(ts, VIDEO_LENGTH)

        while True:
            audio_file: str = audio_blocks_recorded.get(block=True)

            producer.send(
                topic=topic,
                value=audio_file.encode(),
                headers=[("number-of-messages", b'00001'), ("message-number", b'00000'), ("type", b"audio")],
                partition=AggregatorMicroservicePartition
            )

            print(f"message {audio_file} sent at {time.time()}")
    except KeyboardInterrupt:
        print("Keyboard interrupt")
    except Exception as ex:
        print("ERROR!: ", ex)

    audio_recorder.close()
    producer.close()

    print("Cleanup done")
    sys.exit(1)

if __name__ == "__main__":
    main()

