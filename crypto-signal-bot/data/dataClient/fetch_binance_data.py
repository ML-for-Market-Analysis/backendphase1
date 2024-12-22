import os
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
from config.settings import API_KEY, API_SECRET

client = Client(API_KEY, API_SECRET)

def fetch_historical_klines(symbol, interval, start, end=None):
    """
    Belirtilen sembol, zaman aralığı (interval) ve başlangıç/bitiş tarihleri arasında
    tüm kline verilerini döngülü olarak çeker ve tek bir liste olarak geri döndürür.
    """
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
        last_kline_close_time = last_kline[6]  # close_time
        next_ts = last_kline_close_time + 1

        if next_ts >= end_ts:
            break

        start_ts = next_ts

    return output_data

def to_dataframe(klines):
    """
    get_historical_klines ile çekilen ham veriyi pandas DataFrame'e çevirir.
    """
    df = pd.DataFrame(klines, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = \
        df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    return df

def get_top_symbols(limit=10):
    """
    Binance API'den en yüksek hacimli (limit kadar) işlem çiftlerini alır.
    Sadece USDT ile biten işlem çiftlerini filtreler.
    """
    try:
        tickers = client.get_ticker()
        sorted_tickers = sorted(tickers, key=lambda x: float(x['quoteVolume']), reverse=True)
        top_symbols = [t['symbol'] for t in sorted_tickers if t['symbol'].endswith('USDT')][:limit]
        return top_symbols
    except Exception as e:
        print(f"İşlem çiftleri alınırken hata oluştu: {e}")
        return []

def fetch_and_append_data(symbols):
    """
    Her bir sembol için 30 günlük 4 saatlik veriyi döngülü şekilde çekip,
    CSV dosyalarına kaydeder veya ekler.
    """
    interval = '4h'  # <<< 4 saatlik veriler
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=30)

    # CSV'lerin kaydedileceği dizin:
    save_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(save_dir, exist_ok=True)

    for symbol in symbols:
        print(f"[FETCH] {symbol} - 4h verileri çekiliyor...")
        try:
            klines = fetch_historical_klines(symbol, interval, start_time, end_time)
            df = to_dataframe(klines)

            df.fillna(method='ffill', inplace=True)

            file_name = os.path.join(save_dir, f"{symbol}_latest.csv")

            if os.path.exists(file_name):
                existing_data = pd.read_csv(file_name)
                combined_data = pd.concat([existing_data, df]).drop_duplicates(subset='timestamp', keep='last')
                combined_data.to_csv(file_name, index=False)
                print(f"[FETCH] {symbol} verileri güncellendi: {file_name}")
            else:
                df.to_csv(file_name, index=False)
                print(f"[FETCH] {symbol} için yeni dosya oluşturuldu: {file_name}")

        except Exception as e:
            print(f"[FETCH] Hata oluştu ({symbol}): {e}")

if __name__ == "__main__":
    top_10_symbols = get_top_symbols(limit=10)
    if top_10_symbols:
        fetch_and_append_data(top_10_symbols)
    else:
        print("[FETCH] İşlem çiftleri alınamadı, veri çekme işlemi iptal edildi.")
