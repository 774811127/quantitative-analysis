import os
import pandas as pd


class Contract:
    directory = r'database\contract_data'
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
        market_data_df = pd.read_csv(self.market_data, header=0, index_col=1, dtype=self.dtypes, parse_dates=[
            'Date'], date_format='%Y-%m-%d')
        self.contract_data_df = market_data_df.loc[market_data_df['Contract'] == contract]

    def __calculate_macd(self, fast_period=12, slow_period=26, signal_period=9):
        contract_data_df = self.contract_data_df
        # 计算EMA（指数移动平均线）
        ema_fast = contract_data_df['Close'].ewm(
            span=fast_period, adjust=False).mean()
        ema_slow = contract_data_df['Close'].ewm(
            span=slow_period, adjust=False).mean()

        # 计算MACD线（快慢线之差）
        macd_line = ema_fast - ema_slow

        # 计算信号线（MACD线的EMA）
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()

        # 计算MACD柱状图（MACD线与信号线之差）
        macd_histogram = (macd_line - signal_line) * 2

        # 将MACD计算结果连接到合约数据表中
        macd_df = pd.DataFrame(
            {'MACD DIF': macd_line, 'MACD DEA': signal_line, 'MACD M': macd_histogram})
        #

        return macd_df

    def calculate(self):
        macd_df = self.__calculate_macd()
        self.contract_data_df = pd.merge(
            self.contract_data_df, macd_df, on='Date', how='inner')

    def save(self):
        file_path = os.path.join(self.directory, self.file_name)
        print('saving', self.file_name)
        self.contract_data_df.to_csv(file_path)

    def analysis(self):
        pass

    def __analysis_macd(self):
        pass

if __name__ == '__main__':
    contract_au2402 = Contract('au2402')
    contract_au2402.calculate()
    contract_au2402.save()
