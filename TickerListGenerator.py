import pandas as pd

class Ticker_List:
    def __init__(self, kite):
        self.data = kite.instruments()
        self.data = pd.DataFrame(self.data)
        self.data = self.data[(self.data['segment'] == 'NFO-OPT') & (self.data['name'] == 'NIFTY')]
        self.data = self.data.sort_values(by='expiry')
        unique_dates = sorted(self.data['expiry'].unique())
        top_two_dates = unique_dates[:2]
        self.data = self.data[self.data['expiry'].isin(top_two_dates)]
        print("Ticker List downloaded.")
        self.data.to_csv('instruments.csv')

    def get_filtered_data(self):
        return self.data