import pandas as pd
df_ohlc = pd.read_csv('data/ohlc.csv')

print(df_ohlc.head())

# df_ohlc['tend'] = pd.to_datetime(df_ohlc['tend'])
