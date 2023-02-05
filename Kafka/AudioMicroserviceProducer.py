import confluent_kafka as kafka
from Kafka import KafkaProducerWrapper

if __name__ == "__main__":
    producer = KafkaProducerWrapper({'bootstrap.servers': "localhost:9092"})
    producer.produce(topic="testtopics", value="sal", partition=0)
    producer.flush()