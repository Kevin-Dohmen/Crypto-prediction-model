import requests
import pandas as pd
from Models.BinanceDataModel import BinanceDataCandle

# Validation constants
supported_intervals = [
    "1s",                                   # seconds
    "1m", "3m", "5m", "15m", "30m",         # minutes
    "1h", "2h", "4h", "6h", "8h", "12h",    # hours
    "1d", "3d",                             # days
    "1w",                                   # weeks
    "1M"                                    # months
]

class BinanceDataAPI:
    BASE_URL = "https://api.binance.com/api/v3"

    @staticmethod
    def get_klines(
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = 1000
    ) -> list[BinanceDataCandle]:
        endpoint = f"{BinanceDataAPI.BASE_URL}/klines"

        if interval not in supported_intervals:
            raise ValueError(f"Unsupported interval: {interval}")

        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }

        if start_time is not None:
            params['startTime'] = start_time
        if end_time is not None:
            params['endTime'] = end_time

        response = requests.get(endpoint, params=params)
        response.raise_for_status()

        return [BinanceDataCandle.from_api_response_list(candle) for candle in response.json()]