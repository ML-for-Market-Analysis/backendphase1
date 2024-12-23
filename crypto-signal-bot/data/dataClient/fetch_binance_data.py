import os
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
from config.settings import API_KEY, API_SECRET
import pytz

client = Client(API_KEY, API_SECRET)

def fetch_historical_klines(symbol, interval, start, end=None):
    output_data = []
    max_limit = 1000

    if isinstance(start, str):
        start_dt = datetime.utcnow() - timedelta(days=30)
    else:
        start_dt = start

    if end is None:
        end_dt = datetime.utcnow()
    else:
        end_dt = end

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    while True:
        klines = client.get_historical_klines(
            symbol,
            interval,
            start_str=start_ts,
            end_str=end_ts,
            limit=max_limit
        )

        if not klines:
            break

        output_data += klines

        last_kline = klines[-1]
        last_kline_close_time = last_kline[6]
        next_ts = last_kline_close_time + 1

        if next_ts >= end_ts:
            break

        start_ts = next_ts

    return output_data

def to_dataframe(klines):
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time']]

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    if df['timestamp'].dt.tz is None:
        df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
    else:
        df['timestamp'] = df['timestamp'].dt.tz_convert('Europe/Istanbul')

    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    if df['close_time'].dt.tz is None:
        df['close_time'] = df['close_time'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
    else:
        df['close_time'] = df['close_time'].dt.tz_convert('Europe/Istanbul')

    df[['open', 'high', 'low', 'close', 'volume']] = \
        df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    return df

def get_top_symbols(limit=10):
    try:
        tickers = client.get_ticker()
        sorted_tickers = sorted(tickers, key=lambda x: float(x['quoteVolume']), reverse=True)
        top_symbols = [t['symbol'] for t in sorted_tickers if t['symbol'].endswith('USDT')][:limit]
        return top_symbols
    except Exception as e:
        print(f"İşlem çiftleri alınırken hata oluştu: {e}")
        return []

def fetch_and_append_data(symbols):
    interval = '4h'
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=30)

    save_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(save_dir, exist_ok=True)

    for symbol in symbols:
        print(f"[FETCH] {symbol} - 4h verileri çekiliyor...")
        try:
            klines = fetch_historical_klines(symbol, interval, start_time, end_time)
            df = to_dataframe(klines)

            df = df.ffill()

            file_name = os.path.join(save_dir, f"{symbol}_latest.csv")

            if os.path.exists(file_name):
                existing_data = pd.read_csv(file_name)

                existing_data['timestamp'] = pd.to_datetime(existing_data['timestamp'])
                if existing_data['timestamp'].dt.tz is None:
                    existing_data['timestamp'] = existing_data['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Europe/Istanbul')
                else:
                    existing_data['timestamp'] = existing_data['timestamp'].dt.tz_convert('Europe/Istanbul')

                combined_data = pd.concat([existing_data, df]).drop_duplicates(subset='timestamp', keep='last')
                combined_data.to_csv(file_name, index=False)
                print(f"[FETCH] {symbol} verileri güncellendi: {file_name}")
            else:
                df.to_csv(file_name, index=False)
                print(f"[FETCH] {symbol} için yeni dosya oluşturuldu: {file_name}")

            last_row = df.iloc[-1]
            print(f"[FETCH] {symbol}: Son Kapanış Zamanı (GMT+3): {last_row['close_time']}")

        except Exception as e:
            print(f"[FETCH] Hata oluştu! Sembol: {symbol}, Hata Mesajı: {e}")

if __name__ == "__main__":
    top_10_symbols = get_top_symbols(limit=10)
    if top_10_symbols:
        fetch_and_append_data(top_10_symbols)
    else:
        print("[FETCH] İşlem çiftleri alınamadı, veri çekme işlemi iptal edildi.")
