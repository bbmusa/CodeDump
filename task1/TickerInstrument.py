import pandas as pd
import logging

class Ticker_List:
    def __init__(self):
        self.csv_data = pd.read_csv('TickerList.csv')

    def get_filtered_data(self, kite):
        self.kite = kite
        self.data = self.kite.instruments()
        self.data = pd.DataFrame(self.data)
        self.data = self.data[(self.data['segment'] == 'NFO-OPT') & (self.data['name'] == 'NIFTY')]
        self.data = self.data.sort_values(by='expiry')
        unique_dates = sorted(self.data['expiry'].unique())
        top_two_dates = unique_dates[:2]
        self.data = self.data[self.data['expiry'].isin(top_two_dates)]
        # logging.debug("Ticker List downloaded.")
        self.data = self.data
        self.data.to_csv('TickerList.csv')
        return self.data

    def symbol_info(self, instrument_token):
        symbol = self.csv_data[self.csv_data['instrument_token'] == instrument_token]['tradingsymbol']
        if not symbol.empty:
            return symbol.iloc[0]
        else:
            logging.debug(f"{instrument_token} Not in TickerInsrtument csv db")