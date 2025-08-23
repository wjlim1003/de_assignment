import csv
import time
import json
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = 'waterQuality'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CSV_FILE_PATH = '/home/student/de_assignment/water_quality.csv'
STREAMING_DELAY_SECONDS = 5

def create_producer():
    """Creates a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer created successfully.")
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

def stream_csv_data(producer, file_path):
    """Reads a CSV file and streams its data to Kafka."""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            print(f"Streaming data from '{file_path}' to Kafka topic '{KAFKA_TOPIC}'...")
            for row in reader:
                producer.send(KAFKA_TOPIC, row)
                print(f"Sent: {row}")
                time.sleep(STREAMING_DELAY_SECONDS)
            print("Finished streaming all data.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred during streaming: {e}")
    finally:
        producer.flush()
        producer.close()
        print("Kafka producer flushed and closed.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        stream_csv_data(kafka_producer, CSV_FILE_PATH)
