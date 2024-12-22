import os
import pandas as pd
from binance.client import Client
from config.settings import API_KEY, API_SECRET

# Binance istemcisi oluştur
client = Client(API_KEY, API_SECRET)

def get_top_100_symbols():
    """
    Binance API'den ilk 100 işlem çiftini alır.
    Sadece USDT ile biten işlem çiftlerini filtreler.
    """
    try:
        tickers = client.get_ticker()
        sorted_tickers = sorted(tickers, key=lambda x: float(x['quoteVolume']), reverse=True)
        top_100_symbols = [ticker['symbol'] for ticker in sorted_tickers if ticker['symbol'].endswith('USDT')][:100]
        return top_100_symbols
    except Exception as e:
        print(f"İşlem çiftleri alınırken hata oluştu: {e}")
        return []

def fetch_and_append_data(symbols):
    """
    Binance API'den belirli işlem çiftleri için verileri çeker ve mevcut CSV'lere ekler.
    """
    interval = '1h'  # 1 saatlik veriler
    start_time = "2 hours ago UTC"  # 2 saat öncesinden itibaren

    # CSV'leri kaydedeceğiniz dizin
    save_dir = "data"
    os.makedirs(save_dir, exist_ok=True)  # Eğer `data` dizini yoksa oluştur

    for symbol in symbols:
        file_name = os.path.join(save_dir, f"{symbol}_latest.csv")
        print(f"Veri çekiliyor: {symbol}")
        try:
            # Binance API'den veri çek
            klines = client.get_historical_klines(symbol, interval, start_time)
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                'quote_asset_volume', 'number_of_trades', 'taker_buy_base', 'taker_buy_quote', 'ignore'
            ])
            # İlgili sütunları dönüştür
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

            # Eğer dosya mevcutsa eski verileri yükle ve yeni verileri birleştir
            if os.path.exists(file_name):
                existing_data = pd.read_csv(file_name)
                # Yeni verileri eski verilere ekle ve tekrar kaydet
                combined_data = pd.concat([existing_data, df]).drop_duplicates(subset='timestamp', keep='last')
                combined_data.to_csv(file_name, index=False)
                print(f"{symbol} verileri güncellendi: {file_name}")
            else:
                # Eğer dosya yoksa yeni bir dosya oluştur
                df.to_csv(file_name, index=False)
                print(f"{symbol} için yeni dosya oluşturuldu: {file_name}")

        except Exception as e:
            print(f"Veri çekilirken hata oluştu: {symbol} -> {e}")

# İlk 100 işlem çiftini al
top_100_symbols = get_top_100_symbols()

# Veri çekme fonksiyonunu çağır
if top_100_symbols:
    fetch_and_append_data(top_100_symbols)
else:
    print("İşlem çiftleri alınamadı, veri çekme işlemi iptal edildi.")
