import json
import time
import random
import logging
import signal
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from faker import Faker

# Load environment variables
load_dotenv()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize Faker and Kafka
faker = Faker()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Graceful shutdown
is_running = True

def signal_handler(sig, frame):
    global is_running
    is_running = False
    logging.info("Shutting down gracefully...")

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Sample tea data
TEA_NAMES = [
    "Dragonwell Green Tea", "Silver Needle White Tea", "Da Hong Pao Oolong",
    "Jasmine Pearl", "Lapsang Souchong", "Chamomile Herbal Tea",
    "Pu-erh Vintage", "Matcha Ceremonial", "Tie Guan Yin", "Earl Grey Classic"
]

TEA_CATEGORIES = ["green", "black", "herbal", "oolong", "white", "pu-erh"]

def fetch_fake_tea_transaction():
    return {
        "transaction_id": f"TXN{random.randint(1000, 9999)}",
        "tea_id": random.randint(1, 100),
        "name": random.choice(TEA_NAMES),
        "category": random.choice(TEA_CATEGORIES),
        "price": round(random.uniform(4.5, 25.0), 2),
        "quantity": random.randint(1, 5),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": f"CUST{random.randint(100, 999)}",
        "country": faker.country_code()
    }

def main():
    logging.info(f"Producer started. Sending to topic: {KAFKA_TOPIC}")
    
    MAX_DURATION = 150  # Run for 2.5 minutes max
    start_time = time.time()

    while is_running and (time.time() - start_time) < MAX_DURATION:
        msg = fetch_fake_tea_transaction()
        producer.send(KAFKA_TOPIC, value=msg)
        logging.info(f"Sent: {msg}")
        print("Sent message to Kafka:", msg)
        time.sleep(5)

    producer.flush()
    producer.close()
    logging.info("Producer stopped after duration or signal.")

if __name__ == "__main__":
    main()
