import queue
import sys
import time
from recorder import Recorder
import kafka

AUDIO_TOPIC = "audio"
VIDEO_LENGTH = 1
SAMPLERATE = 44100
MESSAGE_SIZE_LENGTH = 10
BROKER_ADDRESS = "localhost:9092"


def createTopic(topic: str, partitions: int = 1):
    admin_client = kafka.KafkaAdminClient(
        bootstrap_servers=BROKER_ADDRESS,
    )

    try:
        admin_client.delete_topics(topics=[topic])
        time.sleep(1)
    except:
        pass

    admin_client.create_topics(new_topics=[kafka.admin.NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])

if __name__ == "__main__":
    audio_blocks_recorded: queue.Queue = queue.Queue(10)
    audio_recorder: Recorder = Recorder(audio_blocks_recorded)

    producer = kafka.KafkaProducer(bootstrap_servers=BROKER_ADDRESS, acks=1)
    consumer = kafka.KafkaConsumer(bootstrap_servers=BROKER_ADDRESS, enable_auto_commit=True)
    createTopic(AUDIO_TOPIC, 2)
    consumer.assign([kafka.TopicPartition(AUDIO_TOPIC, 1)])

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
    kafka.KafkaAdminClient(bootstrap_servers=BROKER_ADDRESS).delete_topics([AUDIO_TOPIC])
    print("Cleanup done")
    sys.exit(1)

