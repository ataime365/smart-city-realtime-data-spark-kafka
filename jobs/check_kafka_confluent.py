from confluent_kafka import Producer, KafkaError

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

try:
    producer = Producer({'bootstrap.servers': 'broker:29092'})
    producer.produce('test_topic', key='test_key', value='test_value', callback=delivery_report)
    producer.poll(1)
    print("Kafka broker is reachable")
except KafkaError as e:
    print(f"Kafka broker is not reachable: {e}")
