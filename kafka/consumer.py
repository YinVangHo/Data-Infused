import json
import logging
import os
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables
load_dotenv()

# Kafka config
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
DLQ_TOPIC = f"{KAFKA_TOPIC}_dlq"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Snowflake config
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka DLQ producer
dlq_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def is_valid(message: dict) -> bool:
    required_fields = [
        "transaction_id", "tea_id", "name", "category",
        "price", "quantity", "timestamp", "customer_id", "country"
    ]
    for field in required_fields:
        if field not in message or message[field] in [None, "", 0]:
            return False
    return True

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

def insert_to_snowflake(conn, msg):
    insert_query = f"""
        INSERT INTO TEA_TRANSACTIONS (
            TRANSACTION_ID, TEA_ID, NAME, CATEGORY,
            PRICE, QUANTITY, TIMESTAMP, CUSTOMER_ID, COUNTRY
        )
        VALUES (
            %(transaction_id)s, %(tea_id)s, %(name)s, %(category)s,
            %(price)s, %(quantity)s, %(timestamp)s, %(customer_id)s, %(country)s
        )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, msg)

def main():
    logging.info(f"Consumer started on topic: {KAFKA_TOPIC}")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',      # keep this
        consumer_timeout_ms=10000,         # <— exit after 10s idle
        enable_auto_commit=True,
        group_id='tea-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
     )

    try:
        snowflake_conn = get_snowflake_connection()

        for message in consumer:
            data = message.value

            if is_valid(data):
                try:
                    insert_to_snowflake(snowflake_conn, data)
                    logging.info(f"✅ Inserted into Snowflake: {data}")
                    print("☁️ Inserted:", data)
                except Exception as e:
                    logging.error(f"❌ Snowflake insert failed: {e}")
                    dlq_producer.send(DLQ_TOPIC, value=data)
            else:
                logging.warning(f"❌ Invalid message sent to DLQ: {data}")
                dlq_producer.send(DLQ_TOPIC, value=data)

    except KeyboardInterrupt:
        logging.info("Consumer stopped manually.")
    finally:
        consumer.close()
        dlq_producer.flush()
        dlq_producer.close()
        if 'snowflake_conn' in locals():
            snowflake_conn.close()

if __name__ == "__main__":
    main()
