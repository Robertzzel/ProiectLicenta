from Client.Kafka.Kafka import KafkaProducerWrapper, KafkaConsumerWrapper


def main():
    topic = "test"

    producer = KafkaProducerWrapper({
        'bootstrap.servers': "localhost:9092",
    })
    consumer = KafkaConsumerWrapper(
        {
            'bootstrap.servers': "localhost:9092",
            "group.id": "-"
        },
        [(topic, 0)]
    )

    producer.produce(topic, b"12345", partition=0, headers=[("sal", b"sal")])
    x = consumer.consumeMessage()
    y = x.headers().append(*[("sal1", b"sal1")])
    print(x.headers())

if __name__ == "__main__":
    main()