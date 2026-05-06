import json
from kafka import KafkaProducer
from websocket import WebSocketApp

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def on_message(ws, message):
    data = json.loads(message)

    event = {
        "symbol": data["s"],
        "price": float(data["p"]),
        "quantity": float(data["q"]),
        "timestamp": data["T"]
    }

    producer.send(
        "crypto-transaction",
        key=event["symbol"],
        value=event
    )

    print(event)

def on_open(ws):
    print("Connected to Binance WebSocket")

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

ws = WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

if __name__ == "__main__":
    ws.run_forever()