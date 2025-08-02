import os
import h5py
import pandas as pd

from Utils.BinanceDataAPI import BinanceDataAPI
from Models.BinanceDataModel import BinanceDataCandle

def main():
    # Example usage of BinanceDataAPI to fetch klines
    try:
        Symbol = "BTCUSDT"
        Interval = "5m"

        candles = BinanceDataAPI.get_klines(
            symbol=Symbol,
            interval=Interval,
            limit=1000
        )

        if not os.path.exists('temp'):
            os.makedirs('temp')

        print(len(candles), "candles fetched.")

        candles = pd.DataFrame([candle.to_dict() for candle in candles])
        # Convert timestamps to datetime
        candles['open_timestamp'] = pd.to_datetime(candles['open_timestamp'], unit='ms')
        candles['close_timestamp'] = pd.to_datetime(candles['close_timestamp'], unit='ms')

        # Set the index to the open timestamp
        candles.set_index('open_timestamp', inplace=True)
        # Sort by open timestamp
        candles.sort_index(inplace=True)

        # save to csv
        candles.to_csv(f'temp/{Symbol}_{Interval}_candles.csv', index=True)

        print(candles)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
