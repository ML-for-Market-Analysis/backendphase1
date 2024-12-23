import os
import pandas as pd
import numpy as np

def calculate_indicators(file_path):
    try:
        # 1) CSV'den open_time, close_time sütunlarını parse edeceğiz
        df = pd.read_csv(file_path, parse_dates=['open_time', 'close_time'])

        # Eger CSV’de 'timestamp' yoksa, parse_dates=['timestamp'] parametresi hata verir.
        # Bu kodda 'timestamp' yerine 'open_time' ve 'close_time' var diye varsayıyoruz.

        required_columns = {'open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume'}
        if not required_columns.issubset(df.columns):
            print(f"Eksik sütunlar: {required_columns - set(df.columns)} -> {file_path}")
            return

        # 2) Eksik değer kontrolü ve doldurma
        if df.isnull().any().any():
            print(f"Veride eksik değer var (ffill uygulanacak): {file_path}")
            df.fillna(method='ffill', inplace=True)

        # 3) Zaman damgasını index olarak ayarla ve sırala
        df.set_index('open_time', inplace=True)
        df.sort_index(inplace=True)

        # (İSTEĞE BAĞLI) Eğer veriniz UTC olarak geliyorsa (ve siz UTC’de tutuyorsanız),
        # ek bir tz_localize / tz_convert yapmanıza gerek kalmaz.
        # df.index = df.index.tz_localize('UTC')  # Örnek: eğer datetime naive ise.

        # 4) 4 saatlik veriyi "7,11,15,19,23" gibi saatlere kaydırmak için:
        #    - 3 saat geri kaydır
        #    - resample('4h')
        #    - indexi tekrar 3 saat ileri al
        # Böylece periyotların kapanışı (sağ sınır) 7,11,15,19,23 vb. olur.

        # 4.1) 3 saat geri kaydır
        df.index = df.index - pd.Timedelta(hours=3)

        # 4.2) 4 saatlik muma dönüştürme
        df_4h = df.resample('4h', label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

        # 4.3) Indexi tekrar 3 saat ileri al
        df_4h.index = df_4h.index + pd.Timedelta(hours=3)

        # open/high/low/close/volume NaN olan 4h satırları at
        df_4h.dropna(subset=['open','high','low','close','volume'], inplace=True)

        # 5) open_time'ı tekrar kolona çevir
        df_4h.reset_index(inplace=True)
        df_4h.rename(columns={'index': 'open_time'}, inplace=True)

        # 6) Boş mu kontrol et
        if df_4h.empty:
            print(f"[INDICATORS] Uyarı: {file_path} -> 4h dataframe boş, indikatör hesaplanamadı.")
            return

        # 7) Teknik indikatör hesaplamaları
        #    (Aşağıdaki fonksiyonların hepsi alt kısımda tanımlanmıştır.)
        df_4h['RSI'] = calculate_rsi(df_4h['close'], 14)
        rsi_5 = calculate_rsi(df_4h['close'], 5)
        df_4h['Inverse_Fisher_RSI'] = inverse_fisher_transform(rsi_5.rolling(window=9).mean())
        df_4h['MACD'], df_4h['MACD_signal'], df_4h['MACD_hist'] = calculate_macd(df_4h['close'], 12, 26, 9)
        df_4h['upper_band'], df_4h['middle_band'], df_4h['lower_band'] = calculate_bollinger_bands(df_4h['close'], 20, 2)
        df_4h = calculate_fibonacci(df_4h)
        df_4h['ADX'] = calculate_adx(df_4h['high'], df_4h['low'], df_4h['close'], 14)
        df_4h['Stochastic_%K'], df_4h['Stochastic_%D'] = calculate_stochastic(df_4h['high'], df_4h['low'], df_4h['close'], 14, 3)
        df_4h['ATR'] = calculate_atr(df_4h['high'], df_4h['low'], df_4h['close'], 14)
        df_4h['ROC'] = calculate_roc(df_4h['close'], 10)
        df_4h['CCI'] = calculate_cci(df_4h['high'], df_4h['low'], df_4h['close'], 20)

        # 8) İşlenmiş veriyi kaydetme
        processed_dir = os.path.join(os.path.dirname(__file__), "../data/processedData")
        os.makedirs(processed_dir, exist_ok=True)
        output_filename = os.path.basename(file_path).replace("_latest.csv", "_processed.csv")
        output_path = os.path.join(processed_dir, output_filename)
        df_4h.to_csv(output_path, index=False)
        print(f"İşlenmiş dosya kaydedildi: {output_path}")

    except Exception as e:
        print(f"Hata oluştu: {file_path} -> {e}")

# Aşağıda kullanılan indikatör fonksiyonları:

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def inverse_fisher_transform(rsi_series):
    transformed = (np.exp(2 * (rsi_series / 100)) - 1) / (np.exp(2 * (rsi_series / 100)) + 1)
    return transformed

def calculate_macd(series, fast_period, slow_period, signal_period):
    ema_fast = series.ewm(span=fast_period, adjust=False).mean()
    ema_slow = series.ewm(span=slow_period, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal = macd.ewm(span=signal_period, adjust=False).mean()
    hist = macd - signal
    return macd, signal, hist

def calculate_bollinger_bands(series, period, std_dev):
    sma = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper_band = sma + (std_dev * std)
    lower_band = sma - (std_dev * std)
    return upper_band, sma, lower_band

def calculate_fibonacci(df):
    high = df['high'].max()
    low = df['low'].min()
    df['Fibonacci_236'] = high - (high - low) * 0.236
    df['Fibonacci_382'] = high - (high - low) * 0.382
    df['Fibonacci_50'] = high - (high - low) * 0.5
    df['Fibonacci_618'] = high - (high - low) * 0.618
    df['Fibonacci_786'] = high - (high - low) * 0.786
    return df

def calculate_adx(high, low, close, period):
    plus_dm = high.diff().clip(lower=0)
    minus_dm = -low.diff().clip(upper=0)
    tr = pd.concat([
        high - low,
        abs(high - close.shift()),
        abs(low - close.shift())
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window=period).mean()
    return adx

def calculate_stochastic(high, low, close, k_period, d_period):
    low_min = low.rolling(window=k_period).min()
    high_max = high.rolling(window=k_period).max()
    k = 100 * (close - low_min) / (high_max - low_min)
    d = k.rolling(window=d_period).mean()
    return k, d

def calculate_atr(high, low, close, period):
    tr = pd.concat([
        high - low,
        abs(high - close.shift()),
        abs(low - close.shift())
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    return atr

def calculate_roc(close, period):
    roc = (close - close.shift(period)) / close.shift(period) * 100
    return roc

def calculate_cci(high, low, close, period):
    tp = (high + low + close) / 3
    sma = tp.rolling(window=period).mean()
    mean_deviation = tp.rolling(window=period).apply(lambda x: np.mean(np.abs(x - x.mean())), raw=True)
    cci = (tp - sma) / (0.015 * mean_deviation)
    return cci


if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), "../data/dataClient/data")
    if not os.path.exists(data_dir):
        print(f"Veri klasörü bulunamadı: {data_dir}")
    else:
        for file_name in os.listdir(data_dir):
            if file_name.endswith("_latest.csv"):
                file_path = os.path.join(data_dir, file_name)
                calculate_indicators(file_path)
