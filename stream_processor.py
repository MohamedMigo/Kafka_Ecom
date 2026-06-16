import json
from kafka import KafkaConsumer, KafkaProducer

# 1. إعداد الـ Consumer لقراءة البيانات الخام
consumer = KafkaConsumer(
    'raw_events',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest', 
    group_id='ecommerce-cleaner-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) # 👈 الكلمة دي اللي اتغيرت
)

# ... باقي الكود زي ما هو ...
# 2. إعداد الـ Producer لإرسال البيانات النظيفة لقناة clean_events
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    key_serializer=lambda k: str(k).encode('utf-8') if k is not None else b'',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

clean_topic = 'clean_events'

print("🧼 Stream Processor is running and cleaning data...")

try:
    for message in consumer:
        event = message.value
        customer_id = event.get('customer_id')
        
        # شروط تنظيف وجودة البيانات (Data Quality Rules)
        is_data_clean = True
        reason = "Passed"
        
        if not customer_id:
            is_data_clean = False
            reason = "Missing Customer ID"
        elif event.get('amount', 0) <= 0:
            is_data_clean = False
            reason = "Negative or Zero Amount"
        elif event.get('currency') is None:
            is_data_clean = False
            reason = "Missing Currency"
        elif event.get('event_type') in ['click', 'view', 'pay']: # الأنواع الخاطئة
            is_data_clean = False
            reason = "Invalid Event Type"

        # إذا كانت البيانات سليمة، نرسلها للقناة النظيفة
        if is_data_clean:
            # هنشيل الحقول الزيادة اللي كنا مستخدمينها للاختبار ونبعت الصافي
            clean_event = {
                "event_id": event["event_id"],
                "customer_id": event["customer_id"],
                "event_type": event["event_type"],
                "amount": event["amount"],
                "currency": event["currency"],
                "event_timestamp": event["event_timestamp"]
            }
            producer.send(clean_topic, key=customer_id, value=clean_event)
            print(f"🧹 Cleaned & Forwarded: {customer_id} -> {event['event_type']}")
        else:
            # البيانات الفاسدة بنطبعها هنا بس (وفي الحقيقة بنرميها في قناة اسمها Dead Letter Queue)
            print(f"⚠️ Dropped Bad Data: [Reason: {reason}] -> {event}")

except KeyboardInterrupt:
    print("\n🛑 Stream Processor stopped.")