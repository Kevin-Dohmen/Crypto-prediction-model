import os
import h5py
import pandas as pd
import numpy as np

from Utils.BinanceDataAPI import BinanceDataAPI
from Models.BinanceDataModel import BinanceDataCandle
from Models.CandlesSaveModel import CandlesSaveModel

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

        candles: pd.DataFrame = pd.DataFrame([candle.to_dict() for candle in candles])
        # Convert timestamps to datetime

        # Set the index to the open timestamp
        candles.set_index('open_timestamp', inplace=True)
        # Sort by open timestamp
        candles.sort_index(inplace=True)

        saveModel = CandlesSaveModel(candles=candles)

        saveModel.save_to_hdf5(f'temp/{Symbol}_{Interval}_candles.h5')
        saveModel.save_to_csv(f'temp/{Symbol}_{Interval}_candles.csv')

        print(f"Candles saved to temp/{Symbol}_{Interval}_candles.h5")
        with h5py.File(f'temp/{Symbol}_{Interval}_candles.h5', 'r') as hf:
            print("Dataset keys:", list(hf.keys()))
            print("Candle columns:", list(hf['candles'].keys()))


        # print(candles)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
