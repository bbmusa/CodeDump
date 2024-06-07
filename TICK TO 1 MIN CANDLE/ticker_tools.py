import pandas as pd
import logging
import datetime

class Ticker_List:
    def __init__(self):
        self.csv_data = pd.read_csv('TickerList.csv')

    def get_filtered_data(self, kite):
        """generate e1 option list"""
        self.kite = kite
        self.data = self.kite.instruments()
        self.data = pd.DataFrame(self.data)
        self.data = self.data[(self.data['segment'] == 'NFO-OPT') & (self.data['name'] == 'NIFTY')]
        self.data = self.data.sort_values(by='expiry')
        unique_dates = sorted(self.data['expiry'].unique())
        top_two_dates = unique_dates[:2]
        self.data = self.data[self.data['expiry'].isin(top_two_dates)]
        # logging.log("Ticker List downloaded.")
        self.data = self.data
        self.data.to_csv('TickerList.csv')
        return self.data

    def symbol_info(self, instrument_token):
        """get symbol name by matching to csv"""
        symbol = self.csv_data[self.csv_data['instrument_token'] == instrument_token]['tradingsymbol']
        if not symbol.empty:
            return symbol.iloc[0]
        else:
            logging.debug(f"{instrument_token} Not in TickerInsrtument csv db")

    def generate_atm_symbol(self, optionType, kite):
        """generate Nifty atm in symbol nearest 50's """
        instrument_list = pd.read_csv('TickerList.csv')
        data = instrument_list[instrument_list['instrument_type'] == "CE"]
        data = data.sort_values(by='expiry')
        unique_dates = sorted(data['expiry'].unique())
        expiry = unique_dates[:1]
        expiry_date = datetime.datetime.strptime(expiry[0], '%Y-%m-%d')
        expiry_date = expiry_date.strftime(f'%y{6}%d')
        ltp = kite.ltp('NSE:NIFTY 50')
        ltp = ltp['NSE:NIFTY 50']['last_price']
        strike = round(ltp / 50) * 50
        symbol_with_expiry = f"NIFTY24606{strike}{optionType}"
        return symbol_with_expiry

