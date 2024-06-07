import pandas as pd
import time
from multiprocessing import Process, Queue
from sqlalchemy import create_engine

# WebSocket Client to receive live tick data
def websocket_client(queue):
    # Simulate receiving tick data from WebSocket
    while True:
        # Replace the following line with actual WebSocket tick data fetching
        tick_data = {"symbol": "AAPL", "price": 150, "timestamp": time.time()}
        queue.put(tick_data)
        time.sleep(0.01)  # Simulate data tick every 10ms

# Data processor to aggregate tick data into 1-minute OHLC
def ohlc_aggregator(queue):
    ohlc_data = {}
    engine = create_engine('sqlite:///ohlc_data.db')

    while True:
        try:
            tick_data = queue.get(timeout=1)
            symbol = tick_data['symbol']
            price = tick_data['price']
            timestamp = tick_data['timestamp']

            if symbol not in ohlc_data:
                ohlc_data[symbol] = []

            ohlc_data[symbol].append({'price': price, 'timestamp': timestamp})

            # Every minute, aggregate and save OHLC data
            current_minute = int(time.time() // 60)
            for symbol in list(ohlc_data.keys()):
                data = ohlc_data[symbol]
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('timestamp', inplace=True)

                ohlc_resampled = df['price'].resample('1T').ohlc()

                # Save to database
                ohlc_resampled['symbol'] = symbol
                ohlc_resampled.to_sql('ohlc', engine, if_exists='append')

                # Clear data
                ohlc_data[symbol] = [tick for tick in data if tick['timestamp'] >= current_minute * 60]
        except:
            continue

if __name__ == "__main__":
    queue = Queue()

    # Start WebSocket client
    p1 = Process(target=websocket_client, args=(queue,))
    p1.start()

    # Start OHLC aggregator
    p2 = Process(target=ohlc_aggregator, args=(queue,))
    p2.start()

    p1.join()
    p2.join()
