import json
from kafka import KafkaProducer
from websocket import WebSocketApp

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)

    event = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "timestamp": data["T"]
    }

    producer.send("crypto-prices", value=event)
    print(event)

ws = WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_message=on_message
)


def produce_events(events_per_second: int = 10):
    print("Starting producer...")
    while True:
        event = ws()
        
        # Usar device_id como key para ordering
        producer.send(
            'crypto-transaction',
            key=event['symbol'],
            value=event
        )
        
        print(f"Sent: {event['symbol']}")
        time.sleep(1 / events_per_second)

if __name__ == "__main__":
    produce_events()