import os
import json
import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

try:
    # Initialize connection using secure environment variables
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),          
        password=os.getenv("SNOWFLAKE_PASSWORD"),  
        account=os.getenv("SNOWFLAKE_ACCOUNT"),    
        warehouse='COMPUTE_WH',
        database='ECOMMERCE_DB',
        schema='PUBLIC'
    )
    cursor = conn.cursor()
    print("❄️ Connected successfully to Snowflake!")
    
    # Create the target table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clean_events (
            event_id STRING,
            customer_id STRING,
            event_type STRING,
            amount FLOAT,
            currency STRING,
            event_timestamp TIMESTAMP_NTZ
        )
    """)
    print("📊 Target table 'clean_events' is ready.")

except Exception as e:
    print(f"⚠️ Snowflake Connection Skipped/Failed: {e}")
    print("💡 The code will continue to read from Kafka and print data, but will NOT store it in Snowflake until account credentials are fixed.")
    conn = None

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'clean_events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest', 
    group_id='snowflake-storage-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📥 Snowflake Consumer is listening to 'clean_events'...")

try:
    for message in consumer:
        event = message.value
        print(f"📦 Received Clean Event: {event['event_id']} for User: {event['customer_id']}")
        
        # If Snowflake connection is active, insert the data
        if conn:
            query = """
                INSERT INTO clean_events (event_id, customer_id, event_type, amount, currency, event_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                event['event_id'],
                event['customer_id'],
                event['event_type'],
                event['amount'],
                event['currency'],
                event['event_timestamp']
            ))
            print(f"💾 Successfully written to Snowflake.")
            
except KeyboardInterrupt:
    print("\n🛑 Snowflake Consumer stopped by user.")
finally:
    # Close connections safely
    if conn:
        cursor.close()
        conn.close()
        print("🔒 Snowflake connection closed.")