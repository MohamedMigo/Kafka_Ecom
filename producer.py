import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# 1. إعداد الـ Producer ليتصل بكافكا على البورت الخارجي 29092
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], 
    key_serializer=lambda k: str(k).encode('utf-8') if k is not None else b'',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'raw_events'

valid_events = ['page_view', 'add_to_cart', 'purchase']
invalid_events = ['click', 'view', 'pay']

def generate_event():
    # نسبة 25% إن البيانات تطلع فيها مشكلة
    is_faulty = random.random() < 0.25
    
    customer_id = f"CUST_{random.randint(1, 5)}"
    event_type = random.choice(valid_events)
    amount = round(random.uniform(10.0, 500.0), 2)
    currency = "USD"
    
    invalid_field = None
    if is_faulty:
        invalid_field = random.choice(['customer_id', 'event_type', 'amount', 'currency'])
        if invalid_field == 'customer_id':
            customer_id = None
        elif invalid_field == 'event_type':
            event_type = random.choice(invalid_events)
        elif invalid_field == 'amount':
            amount = round(random.uniform(-500.0, -10.0), 2) # سعر بالسالب بالخطأ
        elif invalid_field == 'currency':
            currency = None

    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "event_type": event_type,
        "amount": amount,
        "currency": currency,
        "event_timestamp": datetime.now(timezone.utc).isoformat(), # تم تحديث دالة الوقت
        "is_valid": not is_faulty,
        "invalid_field": invalid_field
    }
    return customer_id, event

print("🚀 Starting E-Commerce Data Producer...")
try:
    while True: 
        key, event_data = generate_event()
        producer.send(topic_name, key=key, value=event_data)
        
        status = "✅ Valid" if event_data['is_valid'] else f"❌ Invalid ({event_data['invalid_field']})"
        print(f"Sent [User: {key}]: {status}")
        
        time.sleep(1) 
except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")