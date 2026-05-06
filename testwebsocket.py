# producer.py
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def generate_event():
    return {
        "device_id": f"device_{random.randint(1, 100)}",
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(40, 80), 2),
        "timestamp": datetime.now().isoformat()
    }

def produce_events(events_per_second: int = 10):
    print("Starting producer...")
    while True:
        event = generate_event()
        
        # Usar device_id como key para ordering
        producer.send(
            'iot-events',
            key=event['device_id'],
            value=event
        )
        
        print(f"Sent: {event['device_id']}")
        time.sleep(1 / events_per_second)

if __name__ == "__main__":
    produce_events()