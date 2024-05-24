import logging
from kiteconnect import KiteTicker
import datetime
import multiprocessing

logging.basicConfig(level=logging.DEBUG)

class KiteTickerHandler:
    def __init__(self, settings, ticker_ids, queue):
        self.kws = KiteTicker(settings.API_KEY, settings.ACCESS_TOKEN)
        logging.debug("KWS connected")
        self.last_processed_time = datetime.datetime.now()
        logging.debug(f"Last Processed Time: {self.last_processed_time}")
        self.data_log = {}
        self.ticker_ids = ticker_ids
        self.queue = queue
        logging.debug(f"Ticker IDs: {self.ticker_ids}")


    def on_error(self, ws, code, reason):
        logging.error("closed connection on error: {} {}".format(code, reason))

    def on_noreconnect(self, ws):
        logging.error("Reconnecting the websocket failed")

    def on_reconnect(self, ws, attempt_count):
        logging.debug("Reconnecting the websocket: {}".format(attempt_count))


    def on_ticks(self, ws, ticks):
        # logging.debug("Ticks are downloading: {}".format(ticks))
        self.queue.put(ticks)

    def on_connect(self, ws, response):
        logging.debug("Connected")
        ws.subscribe(self.ticker_ids)
        ws.set_mode(ws.MODE_QUOTE, self.ticker_ids)

    def on_close(self, ws, code, reason):
        logging.debug(f"Connection closed:")
        self.queue.put(None)
        logging.error("closed connection on close: {} {}".format(code, reason))
        ws.stop()

    def connect(self):
        self.kws.connect(threaded=True)

    def assign_callbacks(self):
        # Setup callbacks
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_noreconnect = self.on_noreconnect
        self.kws.on_reconnect = self.on_reconnect
        self.kws.connect()
