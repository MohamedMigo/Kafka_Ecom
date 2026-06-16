import json
import snowflake.connector
from kafka import KafkaConsumer

# 1. إعداد الاتصال بـ Snowflake
# ملاحظة: هتحتاج تبدل البيانات دي ببيانات حسابك الحقيقي لو عندك حساب تجريبي
try:
    ctx = snowflake.connector.connect(
        user='MohamedAshraf',
        password='MohamedAshrafMigo2006#',
        account='kshuthc-lp84019', # بيكون شكله زي: xy12345.eu-west-1
        warehouse='COMPUTE_WH',
        database='ECOMMERCE_DB',
        schema='PUBLIC'
    )
    cursor = ctx.cursor()
    print("❄️ Connected successfully to Snowflake!")
    
    # إنشاء الجدول لو مش موجود قبل كده
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
    print("💡 الكود هيكمل وهيقرأ من كافكا ويطبع البيانات، بس مش هيخزن في Snowflake لحد ما تحط بيانات الحساب.")
    ctx = None

# 2. إعداد الـ Consumer لقراءة البيانات النظيفة فقط
consumer = KafkaConsumer(
    'clean_events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest', # يقرأ البيانات الجديدة اللي بتوصل من دلوقتي وطالع
    group_id='snowflake-storage-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📥 Snowflake Consumer is listening to 'clean_events'...")

try:
    for message in consumer:
        event = message.value
        print(f"📦 Received Clean Event: {event['event_id']} for User: {event['customer_id']}")
        
        # إذا كان الاتصال بـ Snowflake شغال، هنخزن البيانات هناك
        if ctx:
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
    print("\n🛑 Snowflake Consumer stopped.")
finally:
    if ctx:
        cursor.close()
        ctx.close()