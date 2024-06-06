"""
Web socket data streaming
"""
import logging

import pandas as pd
from kiteconnect import KiteTicker, KiteConnect

from ticker_tools import Ticker_List

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('kiteconnect.ticker').setLevel(logging.WARNING)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class OptionDataStreaming:
    def __init__(self, settings, queue):
        kite = KiteConnect(settings.API_KEY, settings.ACCESS_TOKEN)
        self.ticker_list_instance = Ticker_List()
        self.TickerList = self.ticker_list_instance.get_filtered_data(kite)
        self.TickerList = pd.read_csv('TickerList.csv')
        self.TickerList = self.TickerList['instrument_token'].tolist()[:2]
        print(self.TickerList)
        self.kws = KiteTicker(settings.API_KEY, settings.ACCESS_TOKEN, debug=True)
        self.q = queue
        self.assign_callBacks()

    def on_noreconnect(self, ws):
        logging.error("Reconnecting the websocket failed")

    def on_error(ws, code, reason, xs):
        logging.error("closed connection on error: {} {}".format(code, reason))

    def on_reconnect(self, ws, attempt_count):
        logging.debug("Reconnecting the websocket: {}".format(attempt_count))

    def on_ticks(self, ws, ticks):
        for tick in ticks:
            try:
                self.q.put(tick)
            except Exception as e:
                logging.error(f"Failed to Publish: {e}")

    def on_connect(self, ws, response):
        # logging.debug(self.TickerList[:5])
        ws.subscribe(self.TickerList)
        ws.set_mode(ws.MODE_FULL, self.TickerList)

    def on_close(self, ws, code, reason):
        logging.error("closed connection on close: {} {}".format(code, reason))
        ws.stop()

    def assign_callBacks(self):
        # Assign all the callbacks
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_reconnect = self.on_reconnect
        self.kws.connect()
