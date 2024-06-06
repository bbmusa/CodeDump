import traceback
from multiprocessing import Queue
import time
import numpy as np
from datetime import datetime
from db_tools import DataUploader

class getDataTools:
    def __init__(self, settings):
        self.ce = 0
        self.pe = 0
        self.settings = settings
        self.data_dict = {}

    def calculate_ohlc(self):
        for token, trades in self.data_dict.items():
            if trades:
                prices = [trade['ltp'] for trade in trades]
                volumes = [trade['volume'] for trade in trades]
                ois = [trade['oi'] for trade in trades]
                time.sleep(1)
                uploader = DataUploader('optiondata', 'postgres', 'localhost', '12345')
                timestamp = trades[0]['timestamp']
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                ohlc = {
                    'symbol': token,
                    'open': trades[0]['ltp'],
                    'high': max(prices),
                    'low': min(prices),
                    'close': trades[-1]['ltp'],
                    'volume': int(sum(volumes) / len(volumes)),
                    'oi': int(sum(ois) / len(ois)),
                    'timestamp': timestamp
                }
                uploader.insert_data(ohlc)
                uploader.close_connection()
                print(ohlc)
        self.data_dict.clear()

    def get_data(self, queue: Queue):
        while True:
            try:
                if queue.qsize() > 0:
                    data = queue.get(timeout=1)
                    if data['last_price'] > 0:
                        instrument_token = data['instrument_token']
                        last_price = data['last_price']
                        timestamp = data['last_trade_time']
                        timestamp = timestamp.replace(second=0, microsecond=0)
                        oi = data.get('oi', 0)
                        volume = data.get('volume_traded', 0)

                        if instrument_token not in self.data_dict:
                            self.data_dict[instrument_token] = []

                        self.data_dict[instrument_token].append({'ltp': last_price,
                                                                 'timestamp': timestamp,
                                                                 'oi': oi,
                                                                 'volume': volume})
                        current_minute = int(time.time() // 60)
                        if current_minute != getattr(self, 'last_minute', None):
                            self.last_minute = current_minute
                            self.calculate_ohlc()

            except:
                print(traceback.format_exc())
