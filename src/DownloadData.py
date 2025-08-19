import os
import pandas as pd
import time
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

from Utils.BinanceDataAPI import BinanceDataAPI
from Models.BinanceDataModel import BinanceDataCandle
from Models.CandlesSaveModel import CandlesSaveModel

# Configuration constants
CURRENCIES = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "ADAUSDT"
]

INTERVALS = [
    "1m", "5m", "15m", "30m",
    "1h", "2h"
]

START_TIME = 1577833200000  # 01-01-2020 00:00:00 UTC
END_TIME   = 1735686000000  # 01-01-2025 00:00:00 UTC

SAVE_CANDLE_COUNT = None  # Number of candles to save per symbol and interval
MAX_FETCH_ATTEMPTS = 3
BINANCE_LIMIT = 1000

def download_data(
    symbols: List[str],
    intervals: List[str],
    start_time: Optional[int],
    end_time: Optional[int],
    save_candle_count: Optional[int],
    temp_dir: str = 'temp'
) -> None:
    with ThreadPoolExecutor(max_workers=4) as executor:
        for symbol in symbols:
            for interval in intervals:
                executor.submit(
                    _download_symbol,
                    symbol=symbol,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    save_candle_count=save_candle_count,
                    temp_dir=temp_dir
                )

def _download_symbol(
    symbol: str,
    interval: str,
    start_time: Optional[int],
    end_time: Optional[int],
    save_candle_count: Optional[int],
    temp_dir: str = 'temp'
) -> None:
    identifier = f"{symbol}-{interval}"

    print(f"STRT {identifier}")
            
    try:
        candles = _fetch_candles_for_symbol(
            symbol, interval, start_time, end_time, save_candle_count
        )
        
        if not candles:
            print(f"END  {identifier} No candles found for {symbol} at {interval}.")
            return
        
        _save_candles_to_file(candles, symbol, interval, temp_dir)
        
    except Exception as e:
        print(f"ERR  {identifier} Error processing {symbol} at {interval}: {e}")
        return

def _fetch_candles_for_symbol(
    symbol: str,
    interval: str,
    start_time: Optional[int],
    end_time: Optional[int],
    save_candle_count: Optional[int]
) -> List[BinanceDataCandle]:
    """Fetch candles for a specific symbol and interval."""
    candles: List[BinanceDataCandle] = []
    current_start_time = start_time
    fetch_attempts = 0

    identifier = f"{symbol}-{interval}"
    
    while True:
        # Check if we've reached the desired candle count
        if save_candle_count and len(candles) >= save_candle_count:
            break
            
        try:
            # Fetch batch of candles
            batch_candles = BinanceDataAPI.get_klines(
                symbol=symbol,
                interval=interval,
                start_time=current_start_time,
                end_time=end_time,
                limit=BINANCE_LIMIT
            )
            
            if not batch_candles:
                fetch_attempts += 1
                if fetch_attempts >= MAX_FETCH_ATTEMPTS:
                    print(f"No more candles available for {symbol} at {interval}")
                    break
                continue
            
            # Reset fetch attempts on successful fetch
            fetch_attempts = 0
            
            # Filter candles within time range if specified
            if end_time:
                batch_candles = [
                    candle for candle in batch_candles 
                    if candle.open_timestamp <= end_time
                ]
            
            if not batch_candles:
                print(f"END  {identifier} All candles exceed end_time for {symbol} at {interval}")
                break
            
            # Limit candles if save_candle_count is specified
            if save_candle_count:
                remaining_slots = save_candle_count - len(candles)
                if remaining_slots <= 0:
                    break
                batch_candles = batch_candles[:remaining_slots]
            
            candles.extend(batch_candles)
            print(f"     {identifier} Fetched {len(batch_candles)} candles. Total: {len(candles)}")
            
            # Update start time for next batch (add 1ms to avoid duplicate)
            current_start_time = batch_candles[-1].close_timestamp + 1
            
            # If we got fewer candles than the limit, we've reached the end
            if len(batch_candles) < BINANCE_LIMIT:
                break
                
        except ValueError as ve:
            print(f"ERR  {identifier} ValueError for {symbol} at {interval}: {ve}")
            break
        except Exception as e:
            print(f"ERR  {identifier} Error fetching data for {symbol} at {interval}: {e}")
            fetch_attempts += 1
            if fetch_attempts >= MAX_FETCH_ATTEMPTS:
                print(f"ERR  {identifier} Max fetch attempts reached for {symbol} at {interval}")
                break
            time.sleep(1)  # Brief pause before retry
    
    return candles


def _save_candles_to_file(
    candles: List[BinanceDataCandle],
    symbol: str,
    interval: str,
    temp_dir: str
) -> None:
    """Save candles to HDF5 file."""
    # Convert to DataFrame
    candles_df = pd.DataFrame([candle.to_dict() for candle in candles])
    
    # Set index and sort
    candles_df.set_index('open_timestamp', inplace=True)
    candles_df.sort_index(inplace=True)
    
    # Ensure directory exists
    directory = os.path.join(temp_dir, symbol, interval)
    os.makedirs(directory, exist_ok=True)
    
    # Save to file
    file_path = os.path.join(directory, f'{symbol}_{interval}_candles.h5')
    save_model = CandlesSaveModel(candles=candles_df)
    save_model.save_to_hdf5(file_path)
    
    print(f"Saved {len(candles)} candles to {file_path}")

if __name__ == "__main__":
    if not os.path.exists('temp'):
        os.makedirs('temp')

    download_data(
        symbols=CURRENCIES,
        intervals=INTERVALS,
        start_time=START_TIME,
        end_time=END_TIME,
        save_candle_count=SAVE_CANDLE_COUNT,
        temp_dir='temp'
    )
    

