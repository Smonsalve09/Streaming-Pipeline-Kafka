import json
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

def process_events():
    print("Starting crypto consumer...")
    
    # Aggregations in memory
    stats = defaultdict(lambda: {
        'count': 0,
        'sum_price': 0.0,
        'sum_quantity': 0.0
    })
    
    for message in consumer:
        event = message.value
        symbol = event['symbol']
        price = event['price']
        quantity = event['quantity']
        
        # Update stats
        stats[symbol]['count'] += 1
        stats[symbol]['sum_price'] += price
        stats[symbol]['sum_quantity'] += quantity
        
        avg_price = stats[symbol]['sum_price'] / stats[symbol]['count']
        total_volume = stats[symbol]['sum_quantity']
        
        print(
            f"{symbol} | avg_price={avg_price:.2f} "
            f"| trades={stats[symbol]['count']} "
            f"| volume={total_volume:.4f}"
        )

if __name__ == "__main__":
    process_events()