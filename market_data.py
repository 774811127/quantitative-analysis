import os
import csv
import re

import pandas as pd


class MarketData:
    monthly_data_directory = r'database\original_data\monthly_data'
    daily_data_directory = r'database\original_data\daily_data'
    save_directory = 'database'
    file_name = 'market_data.csv'
    contracts_df = pd.read_csv('database/market_data.csv')
    # dtypes = {
    #     'Column1': str,  # 将'Column1'设置为字符串类型
    #     'Column2': int,  # 将'Column2'设置为整数类型
    #     'Column3': float,  # 将'Column3'设置为浮点数类型
    #     'Column4': bool,  # 将'Column4'设置为布尔类型
    #     'DateColumn': 'datetime64[ns]'  # 将'DateColumn'设置为日期时间类型
    # }

    def read_monthly_data(self):
        file_list = [file for file in os.listdir(
            self.monthly_data_directory) if file.endswith('.xls')]

        for file in file_list:
            file_path = os.path.join(self.monthly_data_directory, file)
            print('reading', file)
            df = pd.read_excel(file_path, header=1, usecols=range(
                0, 14), skiprows=2, skipfooter=5)
            df['Contract'] = df['Contract'].ffill()
            df = df.astype(str)
            self.contracts_df = self.contracts_df.astype(str)
            df_merge = df.merge(self.contracts_df, how='outer', indicator=True)
            df = df.loc[df_merge['_merge'] == 'left_only']
            self.contracts_df = pd.concat([self.contracts_df, df])

    def read_daily_data(self):
        file_list = [file for file in os.listdir(
            self.daily_data_directory) if file.endswith('.csv')]

        for file in file_list:
            file_path = os.path.join(self.daily_data_directory, file)
            print('reading', file)
            df = self.__read_daily_csv(file_path)
            df = df.astype(str)
            self.contracts_df = self.contracts_df.astype(str)
            # df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
            # self.contracts_df['Date'] = pd.to_datetime(
            #     self.contracts_df['Date'], format='%Y%m%d')
            df_merge = df.merge(self.contracts_df, how='outer', indicator=True)
            df = df.loc[df_merge['_merge'] == 'left_only']
            self.contracts_df = pd.concat([self.contracts_df, df])

    def save(self):
        file_path = os.path.join(self.save_directory, self.file_name)
        self.contracts_df.to_csv(file_path, index=False)

    def __read_daily_csv(self, file_path):
        commodities_path = r'database\commodities.csv'
        commodities_dict = {}

    # 打开文件并读取数据，指定编码方式
        with open(commodities_path, 'r', newline='', encoding='UTF8') as file:
            reader = csv.reader(file)
            # 跳过第一行（表头）
            next(reader)
            # 遍历每一行数据
            for row in reader:
                key = row[0]
                value = row[1]
                commodities_dict[key] = value

        with open(file_path, 'r', encoding='UTF8') as file:
            reader = csv.reader(file)
            read_switch = False
            data_list = []
            for row in reader:
                if not row:
                    match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date)
                    date = match.groups()
                    date = f'{date[0]}{date[1]}{date[2]}'
                    break
                if row[0] == '小计':
                    read_switch = False
                if read_switch:
                    row[0] = commodities_dict[key] + row[0]
                    row.insert(1, None)
                    row.insert(1, None)
                    row.pop()
                    print(len(row))
                    data_list.append(row)
                if row[0] in ['商品名称:' + key for key in commodities_dict]:
                    read_switch = True
                    key = re.search(r'商品名称:\s*(.*)', row[0]).group(1)
                date = row[0]

            for row in data_list:
                row[1] = date

            columns = ['Contract', 'Date', 'pre close', 'Pre settle', 'Open',
                       'High', 'Low', 'Close', 'Settle', 'ch1', 'ch2', 'Volume', 'Amount', 'OI']
            df = pd.DataFrame(data_list, columns=columns)
        return df


if __name__ == '__main__':
    market_data = MarketData()
    # market_data.read_monthly_data()
    # market_data.save()
    market_data.read_daily_data()
    market_data.save()
