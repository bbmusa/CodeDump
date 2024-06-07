from bs4 import BeautifulSoup as bs
import requests
import pandas as pd

class Screener:
    def __init__(self, symbol):
        self.flag  = True
        self.soup = self.get_soup(value=symbol)

    def get_soup(self, value):
        url =  "https://www.screener.in/company/" + value + "/"
        response = requests.get(url)
        soup = bs(response.content, "html.parser")
        return soup

    def share_Holding_pattern(self):
        quarterly_shp_section = self.soup.find('div', {'id': 'quarterly-shp'})

        shareholding_table = quarterly_shp_section.find('table', {'class': 'data-table'})
        shareholding_data = []
        for row in shareholding_table.find_all('tr'):
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            shareholding_data.append(cols)

        df = pd.DataFrame(shareholding_data)
        df = df.transpose()
        df = df.drop(columns=[0])
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        # Define a dictionary to map the starting strings to the new names
        rename_dict = {
            'Pro': 'Promoter',
            'FII': 'FII',
            'DII': 'DII',
            'Gov': 'Government',
            'Oth': 'Others',
            'Pub': 'Public'
        }

        # Function to apply the renaming logic
        def rename_columns(col_name):
            for key, value in rename_dict.items():
                if col_name.startswith(key):
                    return value
            return col_name  # Return the original name if no match is found

        # Rename the columns using the function
        df.columns = [rename_columns(col) for col in df.columns]
        df['Promoter'] = df['Promoter'].str.replace('%', '').astype(float)
        df['FII'] = df['FII'].str.replace('%', '').astype(float)
        df['Government'] = df['Government'].str.replace('%', '').astype(float)
        df['DII'] = df['DII'].str.replace('%', '').astype(float)
        df['Public'] = df['Public'].str.replace('%', '').astype(float)

        return df