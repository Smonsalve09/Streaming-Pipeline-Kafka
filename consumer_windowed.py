import json
import time
import signal
import os
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer

running = True

def shutdown_handler(signum, frame):
    global running
    print("\nShutdown signal received...")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

consumer = KafkaConsumer(
    'crypto-transaction',
    bootstrap_servers='kafka:9092',
    group_id='crypto-processor',
    auto_offset_reset='earliest',
    enable_auto_commit=False,  
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

WINDOW_SECONDS = 10

def save_window_results(window_data: dict, output_dir: str = 'output'):
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/window_{timestamp}.json"
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'window_seconds': WINDOW_SECONDS,
        'aggregations': {
            symbol: {
                'avg_price': sum(prices) / len(prices),
                'min_price': min(prices),
                'max_price': max(prices),
                'event_count': len(prices)
            }
            for symbol, prices in window_data.items()
        }
    }
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Saved to {filename}")

def process_with_windows():
    print(f"Processing with {WINDOW_SECONDS}s windows...")
    
    window_data = defaultdict(list)
    window_start = time.time()
    
    while running:
        msg_pack = consumer.poll(timeout_ms=1000)
        
        for tp, messages in msg_pack.items():
            for message in messages:
                event = message.value
                window_data[event['symbol']].append(event['price'])
        
        if time.time() - window_start >= WINDOW_SECONDS:
            print(f"\n=== Window {time.strftime('%H:%M:%S')} ===")
            
            for symbol, prices in window_data.items():
                avg = sum(prices) / len(prices)
                print(f"{symbol}: avg={avg:.2f} ({len(prices)} events)")
            
            save_window_results(window_data)
            consumer.commit()
            
            window_data.clear()
            window_start = time.time()
    
    #Graceful shutdown
    print("Finalizing last window...")
    
    if window_data:
        save_window_results(window_data)
    
    consumer.commit()
    consumer.close()
    
    print("Consumer closed gracefully.")

if __name__ == "__main__":
    process_with_windows()