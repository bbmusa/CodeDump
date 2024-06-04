from pydantic import BaseModel
import os
from sqlDB import sqlDb
from TickerInstrument import Ticker_List
import redis
import json
import traceback
from multiprocessing import Queue
import logging
from kiteconnect import KiteConnect
import pandas as pd
import datetime

class getDataTools:
    def __init__(self, settings):
        self.ce = 0
        self.pe = 0
        self.highBreak = []
        self.settings = settings
        # self.sql = sqlDb()

    def buy_order(self, OptionType):
        print(f"buy generated for {OptionType}")
        kws = KiteConnect(api_key=self.settings.API_KEY, access_token=self.settings.ACCESS_TOKEN)
        instrument_list = pd.read_csv('TickerList.csv')
        data = instrument_list[instrument_list['instrument_type'] == "CE"]
        data = data.sort_values(by='expiry')
        unique_dates = sorted(data['expiry'].unique())
        expiry = unique_dates[:1]
        expiry_date = datetime.datetime.strptime(expiry[0], '%Y-%m-%d')
        expiry_date = expiry_date.strftime('%d%b%Y').upper()
        symbol_with_expiry = f"NIFTY{expiry_date}{OptionType}"



    def logic(self, data):
        if data['last_price'] > data['ohlc']['high']:
            ticker_instrument = Ticker_List()
            symbol = ticker_instrument.symbol_info(data['token'])
            if symbol not in self.highBreak:
                self.highBreak.append(symbol)
                option_type = symbol[-2:]
                if option_type == "CE":
                    self.ce += 1
                if option_type == "PE":
                    self.pe += 1
            if self.ce >= 5:
                self.buy_order(OptionType = 'CE')

            if self.pe >= 5:
                self.buy_order(OptionType='PE')

    def get_data(self, queue: Queue):
        r = redis.Redis(decode_responses=True)
        while True:
            try:
                if queue.qsize() > 0:
                    data = queue.get()
                    r.set(data['token'], json.dumps(data))
                    self.logic(data)
            except:
                print(traceback.format_exc())