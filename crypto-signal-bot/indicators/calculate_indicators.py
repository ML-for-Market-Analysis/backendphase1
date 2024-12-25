import os
import pandas as pd
import numpy as np
import ta
def calculate_indicators(file_path):
    try:
        # 1) CSV'den open_time, close_time sütunlarını parse et (UTC)
        df = pd.read_csv(file_path, parse_dates=['open_time', 'close_time'])

        required_columns = {'open_time', 'close_time', 'open', 'high', 'low', 'close', 'volume'}
        if not required_columns.issubset(df.columns):
            print(f"Eksik sütunlar: {required_columns - set(df.columns)} -> {file_path}")
            return

        # 2) Eksik değer kontrolü ve doldurma
        if df.isnull().any().any():
            print(f"Veride eksik değer var (ffill uygulanacak): {file_path}")
            df.fillna(method='ffill', inplace=True)

        # 3) Zaman damgasını index olarak ayarla ve sırala (UTC)
        df.set_index('open_time', inplace=True)
        df.sort_index(inplace=True)

        # 4) 4 saatlik verilere dönüştürme (UTC zaman diliminde)
        df_4h = df.resample('4H', label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })

        # 5) Eksik verileri düşür
        df_4h.dropna(subset=['open', 'high', 'low', 'close', 'volume'], inplace=True)
        df_4h.reset_index(inplace=True)

        if df_4h.empty:
            print(f"[INDICATORS] Uyarı: {file_path} -> 4h dataframe boş, indikatör hesaplanamadı.")
            return

        # 6) Teknik indikatör hesaplamaları (ta kütüphanesi ile)
        # RSI
        df_4h['RSI'] = ta.momentum.RSIIndicator(close=df_4h['close'], window=14).rsi()

        # Inverse Fisher RSI (Özelleştirilmiş)
        rsi_5 = ta.momentum.RSIIndicator(close=df_4h['close'], window=5).rsi()
        rsi_5_rolling = rsi_5.rolling(window=9).mean()
        df_4h['Inverse_Fisher_RSI'] = inverse_fisher_transform(rsi_5_rolling)

        # MACD
        macd = ta.trend.MACD(close=df_4h['close'], window_slow=26, window_fast=12, window_sign=9)
        df_4h['MACD'] = macd.macd()
        df_4h['MACD_signal'] = macd.macd_signal()
        df_4h['MACD_hist'] = macd.macd_diff()

        # Bollinger Bands
        bollinger = ta.volatility.BollingerBands(close=df_4h['close'], window=20, window_dev=2)
        df_4h['upper_band'] = bollinger.bollinger_hband()
        df_4h['middle_band'] = bollinger.bollinger_mavg()
        df_4h['lower_band'] = bollinger.bollinger_lband()

        # ADX
        adx = ta.trend.ADXIndicator(high=df_4h['high'], low=df_4h['low'], close=df_4h['close'], window=14)
        df_4h['ADX'] = adx.adx()

        # Stochastic Oscillator
        stochastic = ta.momentum.StochasticOscillator(high=df_4h['high'], low=df_4h['low'], close=df_4h['close'], window=14, smooth_window=3)
        df_4h['Stochastic_%K'] = stochastic.stoch()
        df_4h['Stochastic_%D'] = stochastic.stoch_signal()

        # ATR
        atr = ta.volatility.AverageTrueRange(high=df_4h['high'], low=df_4h['low'], close=df_4h['close'], window=14)
        df_4h['ATR'] = atr.average_true_range()

        # ROC
        roc = ta.momentum.ROCIndicator(close=df_4h['close'], window=10)
        df_4h['ROC'] = roc.roc()

        # CCI
        cci = ta.trend.CCIIndicator(high=df_4h['high'], low=df_4h['low'], close=df_4h['close'], window=20)
        df_4h['CCI'] = cci.cci()

        # Fibonacci seviyeleri (Özelleştirilmiş)
        df_4h = calculate_fibonacci(df_4h, window=50)

        # 7) İşlenmiş veriyi kaydetme
        processed_dir = os.path.join(os.path.dirname(__file__), "../data/processedData")
        os.makedirs(processed_dir, exist_ok=True)
        output_filename = os.path.basename(file_path).replace("_latest.csv", "_processed.csv")
        output_path = os.path.join(processed_dir, output_filename)
        df_4h.to_csv(output_path, index=False)
        print(f"İşlenmiş dosya kaydedildi: {output_path}")

    except Exception as e:
        print(f"Hata oluştu: {file_path} -> {e}")

def inverse_fisher_transform(rsi_series):
    """
    Inverse Fisher Transform uygulayarak RSI değerlerini normalize eder.
    """
    # RSI değerlerini -1 ile 1 arasında normalize et
    normalized_rsi = (rsi_series - 50) / 50
    transformed = (np.exp(2 * normalized_rsi) - 1) / (np.exp(2 * normalized_rsi) + 1)
    return transformed

def calculate_fibonacci(df, window=50):
    """
    Belirli bir pencere boyunca maksimum ve minimum değerleri alarak Fibonacci seviyelerini hesaplar.
    """
    high = df['high'].rolling(window=window).max()
    low = df['low'].rolling(window=window).min()
    df['Fibonacci_236'] = high - (high - low) * 0.236
    df['Fibonacci_382'] = high - (high - low) * 0.382
    df['Fibonacci_50'] = high - (high - low) * 0.5
    df['Fibonacci_618'] = high - (high - low) * 0.618
    df['Fibonacci_786'] = high - (high - low) * 0.786
    return df

if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), "../data/dataClient/data")
    if not os.path.exists(data_dir):
        print(f"Veri klasörü bulunamadı: {data_dir}")
    else:
        for file_name in os.listdir(data_dir):
            if file_name.endswith("_latest.csv"):
                file_path = os.path.join(data_dir, file_name)
                calculate_indicators(file_path)
