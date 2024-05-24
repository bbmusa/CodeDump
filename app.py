from DataDownloader import KiteTickerHandler
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from pydantic import BaseModel
import multiprocessing
import os
import pandas as pd
from TickerListGenerator import Ticker_List

load_dotenv()

class Setting(BaseModel):
    API_KEY: str = os.getenv('API_KEY')
    API_SECRET: str = os.getenv('API_SECRET')
    REQUEST_TOKEN: str = os.getenv('REQUEST_TOKEN')
    ACCESS_TOKEN: str = os.getenv('ACCESS_TOKEN')
def symbol_info(instrument_token: int):
    data = pd.read_csv("instruments.csv")

    symbol = data[data['instrument_token'] == instrument_token]['tradingsymbol']
    return symbol.iloc[0] if not symbol.empty else None

def process_queue_data(queue):
    try:
        while True:
            data = queue.get()
            for tick in data:
                if tick['last_price'] >= tick['ohlc']['high']:
                    trading_symbol = symbol_info(tick['instrument_token'])
                    print(trading_symbol)
    except Exception as e:
        print(e)



if __name__ == "__main__":
    settings = Setting()
    kite = KiteConnect(api_key=settings.API_KEY)
    # data = kite.generate_session(settings.REQUEST_TOKEN, settings.API_SECRET)
    kite.set_access_token(settings.ACCESS_TOKEN)

    tickerListID = Ticker_List(kite)
    tickerListID = tickerListID.get_filtered_data()
    tickerListID = tickerListID['instrument_token'].tolist()

    queue = multiprocessing.Queue()
    KTH = KiteTickerHandler(settings, tickerListID, queue)
    ticker_process = multiprocessing.Process(target=KTH.assign_callbacks)
    ticker_process.start()

    QueueReader = multiprocessing.Process(target=process_queue_data, args=(queue,))
    QueueReader.start()

    ticker_process.join()
    QueueReader.join()


