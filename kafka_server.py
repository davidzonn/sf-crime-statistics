import consumer_server
import producer_server

TOPIC = "com.davidzonn.sf-crimes"
BOOTSTRAP_SERVERS="localhost:9092"

def run_kafka_server():
    input_file = "res/police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="sf-crimes"
    )

    return producer

def run_kafka_consumer():

    consumer = consumer_server.ConsumerServer(
        topic=TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="kafka-consumer-1",
        auto_offset_reset="earliest"
    )
    return consumer

def feed():
    producer = run_kafka_server()
    producer.generate_data()

    consumer = run_kafka_consumer()
    consumer.consume_data()


if __name__ == "__main__":
    feed()
