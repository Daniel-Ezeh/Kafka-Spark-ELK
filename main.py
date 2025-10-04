from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic 
import logging
import time
import random
import uuid
import json
import threading


KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "finance_transactions"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer_config = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 10000,      # Buffer up to 10,000 messages
    'queue.buffering.max.kbytes': 512000,       # Buffer up to 512MB
    'batch.num.messages': 1000,                 # Send messages in batches of 1,000
    'linger.ms': 10,                            # Wait up to 10ms before sending a batch
    'acks': 1,                                  # Wait for leader acknowledgment
    'compression.type': 'gzip'
}

producer = Producer(producer_config)


def create_topic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )

            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")

        else:
            logger.info(f"Topic '{topic_name}' already exists.")
    
    except Exception as e:
        logger.error(f"Error creating Topic: {e}")


def generate_transaction():
    return dict(
        transaction_id=str(uuid.uuid4()),
        user_id=f'user_{random.randint(1, 1000)}',
        amount=round(random.uniform(50000, 1500000), 2),
        transaction_time=int(time.time()),
        merchant_id=random.choice(["merchant_1", "merchant_2", "merchant_3", "merchant_4"]),
        transaction_type=random.choice(["topup", "withdrawal", "refund", "data"]),
        location=f'location_{random.randint(1,100)}',
        payment_method=random.choice(["credit_card", "paypal", "bank_transfer"]),
        is_international=random.choice([True, False]),
        currency=random.choice(["USD", "EUR", "GBP", "JPY"])
    )


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed for record: {msg.key()}")
    else:
        print(f"Record {msg.key()} successfully produced.")


def produce_transaction(thread_id):
    while True:
        transaction = generate_transaction()

        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['user_id'],
                value=json.dumps(transaction).encode('utf-8'),
                on_delivery=delivery_report
            )
            print(f"Thread {thread_id} - Produced transaction: {transaction}")
            time.sleep(0.2)
            producer.flush()
        except Exception as e:
            print(f'Error sending transaction: {e}')



def producer_data_in_parallel(num_threads):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join() # Joining back to the main thread

    except Exception as e:
        print(f"Error message: {e}")



if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    producer_data_in_parallel(num_threads=1)


