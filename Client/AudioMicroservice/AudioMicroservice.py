import pathlib
import queue
from recorder import Recorder
from Client.Kafka.Kafka import *
import sys

VIDEO_LENGTH = 1 / 5
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10


def main():
    if len(sys.argv) < 3:
        print("No broker address and topic given")
        return

    brokerAddress = sys.argv[1]
    topic = sys.argv[2]
    truststorePath = str(pathlib.Path(__file__).parent.parent / "truststore.pem")

    audio_blocks_recorded: queue.Queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(audio_blocks_recorded)

    producer = KafkaProducerWrapper({'bootstrap.servers': brokerAddress}, certificatePath=truststorePath)
    consumer = KafkaConsumerWrapper(
        {'bootstrap.servers': brokerAddress, "group.id": "-"},
        [(topic, Partitions.AudioMicroservice.value)],
        certificatePath=truststorePath
    )

    try:
        ts = None
        while True:
            if (ts := consumer.consumeMessage(time.time() + 100)) is None:
                continue
            ts = int(ts.value().decode())
            break
        del consumer

        print("AUDIO STARTING")
        audio_recorder.start(ts, VIDEO_LENGTH)
        while True:
            audio_file: str = audio_blocks_recorded.get(block=True)
            producer.produce(
                topic=topic,
                value=audio_file.encode(),
                headers=[("number-of-messages", b'00001'), ("message-number", b'00000'), ("type", b"audio")],
                partition=Partitions.AggregatorMicroservice.value
            )
    except KeyboardInterrupt:
        print("Keyboard interrupt")
    except BaseException as ex:
        print("ERROR!: ", ex)

    audio_recorder.close()

    print("Cleanup done")
    sys.exit(0)


if __name__ == "__main__":
    main()

