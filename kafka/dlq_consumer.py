import json
import logging
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
DLQ_TOPIC = f"{KAFKA_TOPIC}_dlq"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    logging.info(f"🛠 DLQ consumer started. Listening on topic: {DLQ_TOPIC}")

    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='dlq-monitor-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    try:
        for message in consumer:
            data = message.value
            logging.warning(f"🪵 Dead-letter message: {data}")
            print("🛑 From DLQ:", data)

    except KeyboardInterrupt:
        logging.info("DLQ consumer manually stopped.")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
