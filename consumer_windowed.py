# consumer_windowed.py
import json
import time
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'crypto-transaction',
    bootstrap_servers='kafka:9092',
    group_id='crypto-processor',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

WINDOW_SECONDS = 10

def process_with_windows():
    print(f"Processing with {WINDOW_SECONDS}s windows...")
    
    window_data = defaultdict(list)
    window_start = time.time()
    
    for message in consumer:
        event = message.value
        window_data[event['symbol']].append(event['price'])
        
        # Emitir cada WINDOW_SECONDS
        if time.time() - window_start >= WINDOW_SECONDS:
            print(f"\n=== Window {time.strftime('%H:%M:%S')} ===")
            
            for symbol, prices in window_data.items():
                avg = sum(prices) / len(prices)
                print(f"{symbol}: avg={avg:.2f} ({len(prices)} events)")
            
            # Commit offsets después de procesar
            consumer.commit()
            
            # Reset window
            window_data.clear()
            window_start = time.time()

if __name__ == "__main__":
    process_with_windows()