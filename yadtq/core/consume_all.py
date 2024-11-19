from kafka import KafkaConsumer

# Set the topic and Kafka server details
TOPIC_NAME = 'job_queue'
KAFKA_BROKER_URL = 'localhost:9092'

# Create a Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER_URL,
    auto_offset_reset='earliest',  # Consume from the beginning of the topic
    enable_auto_commit=True
)

# Consume and discard all messages
for message in consumer:
    print(f"Discarding message: {message.value}")  # Optionally print messages
    # Do nothing with the message to discard it
    pass
