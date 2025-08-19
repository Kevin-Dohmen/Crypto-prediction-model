import pandas as pd
import h5py as h5

from Models.BinanceDataModel import BinanceDataCandle

class CandlesSaveModel:
    candles: pd.DataFrame

    def __init__(self, candles: pd.DataFrame | list[BinanceDataCandle]):
        if isinstance(candles, pd.DataFrame):
            self.candles = candles
        else:
            self.candles = pd.DataFrame([candle.to_dict() for candle in candles])

    def save_to_csv(self, file_path: str):
        self.candles.to_csv(file_path, index=True)
        print(f"Candles saved to {file_path}")

    def save_to_hdf5(self, file_path: str):
        with h5.File(file_path, 'w') as hf:
            candleGroup = hf.create_group('candles')
            candleGroup.create_dataset('open_timestamp', data=self.candles.index.to_numpy())
            for col in self.candles.columns:
                candleGroup.create_dataset(col, data=self.candles[col].to_numpy())