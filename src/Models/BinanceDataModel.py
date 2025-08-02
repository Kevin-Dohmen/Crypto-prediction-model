from dataclasses import dataclass

@dataclass
class BinanceDataCandle:
    open_timestamp: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    close_timestamp: int
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    unused_field: bool

    @classmethod
    def from_api_response_list(cls, data: list) -> 'BinanceDataCandle':
        return cls(
            open_timestamp                  = int(data[0]),
            open_price                      = float(data[1]),
            high_price                      = float(data[2]),
            low_price                       = float(data[3]),
            close_price                     = float(data[4]),
            volume                          = float(data[5]),
            close_timestamp                 = int(data[6]),
            quote_asset_volume              = float(data[7]),
            number_of_trades                = int(data[8]),
            taker_buy_base_asset_volume     = float(data[9]),
            taker_buy_quote_asset_volume    = float(data[10]),
            unused_field                    = bool(int(data[11]))
        )

    def to_dict(self) -> dict:
        return {
            'open_timestamp'                : self.open_timestamp,
            'open_price'                    : self.open_price,
            'high_price'                    : self.high_price,
            'low_price'                     : self.low_price,
            'close_price'                   : self.close_price,
            'volume'                        : self.volume,
            'close_timestamp'               : self.close_timestamp,
            'quote_asset_volume'            : self.quote_asset_volume,
            'number_of_trades'              : self.number_of_trades,
            'taker_buy_base_asset_volume'   : self.taker_buy_base_asset_volume,
            'taker_buy_quote_asset_volume'  : self.taker_buy_quote_asset_volume,
            'unused_field'                  : self.unused_field
        }
    
    def from_dict(self, data: dict) -> 'BinanceDataCandle':
        return self.__class__(
            open_timestamp                  = data['open_timestamp'],
            open_price                      = data['open_price'],
            high_price                      = data['high_price'],
            low_price                       = data['low_price'],
            close_price                     = data['close_price'],
            volume                          = data['volume'],
            close_timestamp                 = data['close_timestamp'],
            quote_asset_volume              = data['quote_asset_volume'],
            number_of_trades                = data['number_of_trades'],
            taker_buy_base_asset_volume     = data['taker_buy_base_asset_volume'],
            taker_buy_quote_asset_volume    = data['taker_buy_quote_asset_volume'],
            unused_field                    = data['unused_field']
        )