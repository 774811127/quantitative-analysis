import os
import pandas as pd


class MarketData:
    monthly_data_directory = 'data_base\initial_data\monthly_market_data'
    save_directory = 'data_base'
    contracts_df = pd.read_csv('data_base/market_data.csv')

    def read_monthly_data(self):
        file_list = [file for file in os.listdir(
            self.monthly_data_directory) if file.endswith('.xls')]

        for file in file_list:
            file_path = os.path.join(self.monthly_data_directory, file)
            df = pd.read_excel(file_path, header=1, usecols=range(
                0, 14), skiprows=2, skipfooter=5)
            df['Contract'] = df['Contract'].ffill()
            df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
            df_merge = df.merge(self.contracts_df, how='outer', indicator=True)
            df = df.loc[df_merge['_merge'] == 'left_only']
            self.contracts_df = pd.concat([self.contracts_df, df])

    def read_daily_data(self):
        pass

    def save(self):
        self.contracts_df.to_csv('data_base/market_data.csv', index=False)


if __name__ == '__main__':
    market_data = MarketData()
    market_data.read_monthly_data()
    market_data.save()
