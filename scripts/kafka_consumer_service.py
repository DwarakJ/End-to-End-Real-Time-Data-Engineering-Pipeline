import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError

KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker_1:19092','kafka_broker_2:19093','kafka_broker_3:19094']
KAFKA_TOPIC = "names_topic"  

def consume_and_push_to_PG():
    # Configuration details for the Kafka consumer
    kafka_conf = {
        'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS),
        'group.id': 'demo_group',
        'auto.offset.reset': 'earliest'
    }

    # Configuration details for PostgreSQL
    postgres_conf = {
        'dbname': 'airflow',
        'user': 'user',
        'password': 'password',
        'host': 'postgres_db',
        'port': '5432'
    }

    # Create a Kafka consumer instance
    consumer = Consumer(kafka_conf)

    # Subscribe to the Kafka topic
    topic = 'names_topic'
    consumer.subscribe([topic])

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**postgres_conf)
    cursor = conn.cursor()

    try:
        while True:
            # Poll for a message
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Message is properly received
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8')
                print(f'Received message: key={key}, value={value}')

                # Create a PostgreSQL table to store the messages. Check if the table exists and create it if not
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS kafka_messages (
                        message_key TEXT,
                        message_value TEXT
                    )
                """
                cursor.execute(create_table_query)
                conn.commit()

                # Insert the message into the PostgreSQL table
                insert_query = "INSERT INTO kafka_messages (message_key, message_value) VALUES (%s, %s)"
                cursor.execute(insert_query, (key, value))
                conn.commit()

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer and the database connection
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume_and_push_to_PG()