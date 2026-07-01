import json
import time
import requests
from azure.eventhub import EventHubProducerClient, EventData

# --- CONFIGURATION ---
# Replace with your Fabric Eventstream Event Hub Connection String
CONNECTION_STR = "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<key>"
EVENTHUB_NAME = "crypto-stream"
SYMBOL = "BTCUSDT"  # Target trading pair

def get_binance_ticker(symbol):
    """
    Fetches the current price and volume metrics from the Binance Spot API.
    """
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        return {
            "symbol": data["symbol"],
            "price": float(data["price"]),
            "timestamp": int(time.time() * 1000) # Epoch milliseconds
        }
    except Exception as e:
        print(f"Error fetching data from Binance: {e}")
        return None

def stream_ticker_feed():
    """
    Streams live ticker metrics to Azure Event Hub / Fabric Eventstream.
    """
    print(f"Connecting to Event Hub: {EVENTHUB_NAME}...")
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, 
        eventhub_name=EVENTHUB_NAME
    )
    print(f"Streaming started for {SYMBOL}. Press Ctrl+C to exit.")
    
    try:
        while True:
            ticker = get_binance_ticker(SYMBOL)
            if ticker:
                # Package and send payload batch
                batch = producer.create_batch()
                batch.add(EventData(json.dumps(ticker)))
                producer.send_batch(batch)
                print(f"Successfully Sent event: {ticker}")
            time.sleep(2)  # Ingest ticker data every 2 seconds
            
    except KeyboardInterrupt:
        print("\nStopping streaming feed...")
    finally:
        producer.close()
        print("Event Hub Producer closed.")

if __name__ == "__main__":
    stream_ticker_feed()
