import os
import pandas as pd
from datetime import datetime, timedelta
from binance.client import Client
import time

# Binance API bilgileri
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_API_SECRET"
client = Client(API_KEY, API_SECRET)

def fetch_historical_klines(symbol, interval, start, end=None):
    """
    Belirli bir zaman aralığında (start-end) 4 saatlik (interval='4h') kline verilerini çekiyor.
    start ve end, datetime nesneleri olarak verilebilir. Aksi halde varsayılan 60 günlük aralık kullanılır.
    """
    output_data = []
    max_limit = 1000

    if isinstance(start, datetime):
        start_dt = start
    else:
        start_dt = datetime.utcnow() - timedelta(days=60)  # 60 gün öncesi

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
        # Hiç veri gelmezse döngüyü bitir
        if not klines:
            break

        output_data.extend(klines)

        last_kline = klines[-1]
        last_close_time = last_kline[6]  # Kline formatındaki 7. eleman
        next_ts = last_close_time + 1

        if next_ts >= end_ts:
            break

        start_ts = next_ts
        time.sleep(0.5)  # Rate-limit'e takılmamak için kısa bekleme

    return output_data

def to_dataframe(klines):
    """
    Alınan kline verilerini DataFrame'e çevirip UTC+3 (Europe/Istanbul) formatında ayarlar.
    """
    df = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base', 'taker_buy_quote', 'ignore'
    ])

    # Sadece gerekli sütunları tut
    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time']]

    # Zaman damgalarını pandas datetime'e çevir
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')

    # Önce UTC lokalize, sonra UTC+3 (Europe/Istanbul) dönüştür
    df['open_time'] = (df['open_time']
                           .dt.tz_localize("UTC")
                           .dt.tz_convert("Europe/Istanbul"))
    df['close_time'] = (df['close_time']
                           .dt.tz_localize("UTC")
                           .dt.tz_convert("Europe/Istanbul"))

    # Sayısal sütunları float’a çevir
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    df[numeric_cols] = df[numeric_cols].astype(float)

    return df

def get_top_symbols(limit=50):
    """
    USDT ile biten, en yüksek hacimli işlem çiftlerini çeker.
    """
    try:
        tickers = client.get_ticker()
        # Hacme (quoteVolume) göre büyükten küçüğe sıralıyoruz
        sorted_tickers = sorted(tickers, key=lambda x: float(x['quoteVolume']), reverse=True)
        top_symbols = [t['symbol'] for t in sorted_tickers if t['symbol'].endswith('USDT')][:limit]
        return top_symbols
    except Exception as e:
        print(f"[FETCH] İşlem çiftleri alınırken hata oluştu: {e}")
        return []

def fetch_and_append_data(symbols):
    """
    Seçili semboller için 4 saatlik (4h) verileri 60 gün geriye dönük çekip
    var olan CSV dosyalarıyla birleştirerek kaydeder / günceller.
    """
    interval = '4h'
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=60)  # 60 günlük periyot

    # CSV’lerin kaydedileceği klasör
    save_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(save_dir, exist_ok=True)

    for idx, symbol in enumerate(symbols):
        print(f"[FETCH] {symbol} - 4h verileri çekiliyor... ({idx+1}/{len(symbols)})")

        try:
            klines = fetch_historical_klines(symbol, interval, start_time, end_time)
            if not klines:
                print(f"[FETCH] Veri alınamadı: {symbol}")
                continue

            # DataFrame’e dönüştür
            df = to_dataframe(klines)

            # Eksik hücreleri "forward fill" (FFILL) ile doldur
            df = df.ffill()

            # Kaydedilecek dosya
            file_name = os.path.join(save_dir, f"{symbol}_latest.csv")

            # Dosya önceden varsa, eski veriyle birleştir
            if os.path.exists(file_name):
                existing_data = pd.read_csv(file_name, parse_dates=['open_time', 'close_time'])

                # Okuduktan sonra yine UTC+3’e dönüştürelim (ilk okunuşta "Naive" gelebilir)
                existing_data['open_time'] = (existing_data['open_time']
                                                  .dt.tz_localize("UTC")
                                                  .dt.tz_convert("Europe/Istanbul"))
                existing_data['close_time'] = (existing_data['close_time']
                                                   .dt.tz_localize("UTC")
                                                   .dt.tz_convert("Europe/Istanbul"))

                combined_data = pd.concat([existing_data, df], ignore_index=True)
                combined_data.drop_duplicates(subset='open_time', keep='last', inplace=True)
                combined_data.sort_values('open_time', inplace=True)
                combined_data.to_csv(file_name, index=False)
                print(f"[FETCH] {symbol} verileri güncellendi: {file_name}")
            else:
                # Yeni dosya oluştur
                df.to_csv(file_name, index=False)
                print(f"[FETCH] {symbol} için yeni dosya oluşturuldu: {file_name}")

            # Son satırın kapanış zamanını göstermek
            last_row = df.iloc[-1]
            print(f"[FETCH] {symbol}: Son Kapanış Zamanı (UTC+3): {last_row['close_time']}")

            time.sleep(0.5)  # Rate limit için

        except Exception as e:
            print(f"[FETCH] Hata oluştu! Sembol: {symbol}, Hata Mesajı: {e}")

if __name__ == "__main__":
    # Örnek: en yüksek hacimli 50 USDT sembolünü seç
    top_symbols = get_top_symbols(limit=50)
    if top_symbols:
        fetch_and_append_data(top_symbols)
    else:
        print("[FETCH] İşlem çiftleri alınamadı, veri çekme işlemi iptal edildi.")
