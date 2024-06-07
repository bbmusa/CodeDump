import traceback
from multiprocessing import Queue
import datetime
from db_tools import DataUploader
from ticker_tools import Ticker_List
import time


class getDataTools:
    def __init__(self, settings):
        self.ce = 0
        self.pe = 0
        self.settings = settings
        self.data_dict = {}
        self.last_trade_time = {}
        self.timeout_seconds = 70
        self.tickerTools = Ticker_List()
        self.isFirst = True

    def calculate_ohlc(self):
        """calculate OHLCV,OI, SYMBOL & save to psql"""
        data_to_process = self.data_dict.copy()
        for token, times in data_to_process.items():
            for timestamp, trades in times.items():
                if trades:
                    prices = [trade['ltp'] for trade in trades]
                    timestamp = trades[0]['timestamp']
                    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    symbol = str(self.tickerTools.symbol_info(instrument_token=token))
                    ohlc = {
                        'ticker': token,
                        'open': trades[0]['ltp'],
                        'high': max(prices),
                        'low': min(prices),
                        'close': trades[-1]['ltp'],
                        'volume': trades[-1]['volume'] - trades[0]['volume'],
                        'oi': trades[-1]['oi'],
                        'timestamp': timestamp,
                        'symbol': symbol
                    }
                    if ohlc['volume'] > 0:
                        print(f"ohlc: {ohlc}")
                        uploader = DataUploader('optiondata', 'postgres', 'localhost', '12345')
                        uploader.insert_data(ohlc)
                        uploader.close_connection()
            self.data_dict.clear()

    def get_data(self, queue: Queue):
        """get data from queue"""
        while True:
            try:
                if queue.qsize() > 0:
                    data = queue.get()
                    if self.isFirst:
                        current_time = data['exchange_timestamp']
                        next_minute = (current_time + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
                        time_to_next_minute = (next_minute - current_time).total_seconds()
                        print(f"time till next minute: {time_to_next_minute}")
                        if time_to_next_minute <= 1 or time_to_next_minute == 60:
                            self.isFirst = False

                    elif data['last_price'] > 0 and not self.isFirst:
                        instrument_token = data['instrument_token']
                        last_price = data['last_price']
                        timestamp = data['exchange_timestamp']
                        timestamp = timestamp.replace(second=0, microsecond=0)
                        oi = data.get('oi', 0)
                        volume = data.get('volume_traded', 0)

                        if instrument_token not in self.data_dict:
                            self.data_dict[instrument_token] = {}

                        if timestamp not in self.data_dict.get(instrument_token, {}):
                            self.data_dict[instrument_token][timestamp] = []

                        current_minute = int(timestamp.timestamp() // 60)
                        if current_minute != getattr(self, 'last_minute', None):
                            self.last_minute = current_minute
                            self.calculate_ohlc()
                        else:
                            self.data_dict[instrument_token][timestamp].append({'ltp': last_price,
                                                                                'timestamp': timestamp,
                                                                                'oi': oi,
                                                                                'volume': volume})
                        self.last_trade_time[instrument_token] = time.time()
            except:
                print(traceback.format_exc())
            self.check_timeouts()

    def check_timeouts(self):
        """handle last ticks"""
        current_time = time.time()
        for instrument_token, last_time in self.last_trade_time.items():
            if current_time - last_time > self.timeout_seconds:
                self.calculate_ohlc()
                self.last_trade_time[instrument_token] = current_time
