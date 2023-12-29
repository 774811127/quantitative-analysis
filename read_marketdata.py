import pandas as pd
import os

# 指定要读取的Excel文件所在的目录
directory = '/MarketData_Year_2023'

# 获取目录下所有Excel文件的列表
file_list = [f for f in os.listdir(
    directory) if f.endswith('.xlsx') or f.endswith('.xls')]

# 创建一个空的DataFrame来存储所有数据
all_data = pd.DataFrame()

# 依次读取每个Excel文件并将其添加到all_data DataFrame中
for file in file_list:
    file_path = os.path.join(directory, file)
    data = pd.read_excel(file_path)
    all_data = pd.concat([all_data, data])

# 打印合并后的DataFrame
print(all_data)
