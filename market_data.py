import os
import pandas as pd


class MarketData:
    monthly_data_directory = 'database\original_data\monthly_data'
    daily_data_directory = 'database\original_data\daily_data'
    save_directory = 'database'
    file_name = 'market_data.csv'
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
        file_list = [file for file in os.listdir(self.daily_data_directory) if file.endswith('.csv')]

        for file in file_list:
            file_path = os.path.join(self.daily_data_directory, file)
            print('reading', file)
            original_df = pd.read_csv(file_path, on_bad_lines='warn')
            print('\n')


    def save(self):
        file_path = os.path.join(self.save_directory, self.file_name)
        self.contracts_df.to_csv(file_path, index=False)


if __name__ == '__main__':
    market_data = MarketData()
    market_data.read_monthly_data()
    market_data.save()
