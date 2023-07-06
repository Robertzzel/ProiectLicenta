import pathlib
import queue
from recorder import Recorder
from Client.Kafka.Kafka import *
import sys

VIDEO_LENGTH = 1 / 2
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

    producer = KafkaProducerWrapper(brokerAddress=brokerAddress, certificatePath=truststorePath)
    consumer = KafkaConsumerWrapper(
        brokerAddress=brokerAddress,
        topics=[(topic, Partitions.AudioMicroservice.value)],
        certificatePath=truststorePath
    )

    try:
        ts = None
        while ts is None:
            ts = consumer.consumeMessage(time.time() + 100)

        ts = int(ts.value().decode())
        consumer.close()
        del consumer

        audio_recorder.start(ts, VIDEO_LENGTH)
        while True:
            producer.sendBigMessage(
                topic=topic,
                value=audio_blocks_recorded.get(block=True).encode(),
                partition=Partitions.AggregatorMicroservice.value,
                headers=[("type", b"audio")]
            )
    except KeyboardInterrupt:
        print("Keyboard interrupt")
    except BaseException as ex:
        print("ERROR!: ", ex)

    audio_recorder.close()
    print("Cleanup done")


if __name__ == "__main__":
    main()

