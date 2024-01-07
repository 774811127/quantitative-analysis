import os
import pandas as pd


class ContractData:
    directory = 'database\contract_data'
    market_data = r'database\market_data.csv'
    dtypes = {
        'Contract': str,
        'pre close': float,
        'Pre settle': float,
        'Open': float,
        'High': float,
        'Low': float,
        'Close': float,
        'Settle': float,
        'ch1': float,
        'ch2': float,
        'Volume': int,
        'Amount': float,
        'OI': int,
    }

    def __init__(self, contract) -> None:
        self.contract = contract
        self.file_name = contract + '_data.csv'
        self.file_path = os.path.join(self.directory, self.file_name)
        try:
            self.contract_data_df = pd.read_csv(
                self.file_path, header=0, index_col=0)
            self.new_contract_data_df, self.added_indices = self.__read_market_data()
        except FileNotFoundError:
            market_data_df = pd.read_csv(self.market_data, header=0, index_col=1, dtype=self.dtypes, parse_dates=[
                                         'Date'], date_format='%Y-%m-%d')
            self.contract_data_df = market_data_df.loc[market_data_df['Contract'] == contract]
            self.new_contract_data_df = self.contract_data_df
            self.added_indices = self.new_contract_data_df.index

    def __read_market_data(self):
        market_data_df = pd.read_csv(self.market_data, header=0, index_col=1, dtype=self.dtypes, parse_dates=[
            'Date'], date_format='%Y-%m-%d')
        market_data_df = market_data_df.loc[market_data_df['Contract']
                                            == self.contract]

        market_data_df_index = market_data_df.index
        contract_data_df_index = self.contract_data_df.index
        added_indices = market_data_df_index.difference(contract_data_df_index)
        new_contract_data_df = market_data_df.loc[added_indices]
        return new_contract_data_df, added_indices


if __name__ == '__main__':
    contract_au2402 = ContractData('au2402')
    print(contract_au2402.new_contract_data_df)
