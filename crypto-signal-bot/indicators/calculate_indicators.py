import os
import pandas as pd
import numpy as np

def calculate_indicators(file_path):
    """
    Tek bir dosyada teknik indikatörleri hesaplar.
    :param file_path: İşlenecek CSV dosyasının yolu.
    """
    try:
        # Dosyayı oku
        df = pd.read_csv(file_path)

        # Gereken sütunların olup olmadığını kontrol et
        required_columns = {'open', 'high', 'low', 'close', 'volume'}
        if not required_columns.issubset(df.columns):
            print(f"Gerekli sütunlar eksik: {file_path}")
            return

        # RSI Hesapla
        df['RSI'] = calculate_rsi(df['close'], 14)

        # Inverse Fisher RSI
        df['Inverse_Fisher_RSI'] = inverse_fisher_transform(df['RSI'])

        # MACD Hesapla
        df['MACD'], df['MACD_signal'], df['MACD_hist'] = calculate_macd(df['close'], 12, 26, 9)

        # Bollinger Bands Hesapla
        df['upper_band'], df['middle_band'], df['lower_band'] = calculate_bollinger_bands(df['close'], 20, 2)

        # Fibonacci Seviyeleri Hesapla
        df = calculate_fibonacci(df)

        # İşlenmiş dosyayı kaydet
        output_path = file_path.replace("_latest.csv", "_processed.csv")
        df.to_csv(output_path, index=False)
        print(f"İşlenmiş dosya kaydedildi: {output_path}")

    except Exception as e:
        print(f"Hata oluştu: {file_path} -> {e}")

def calculate_rsi(series, period):
    """RSI hesaplama fonksiyonu."""
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=period, min_periods=1).mean()
    avg_loss = pd.Series(loss).rolling(window=period, min_periods=1).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def inverse_fisher_transform(rsi_series):
    """Inverse Fisher RSI dönüşümü."""
    transformed = (np.exp(2 * (rsi_series / 100)) - 1) / (np.exp(2 * (rsi_series / 100)) + 1)
    return transformed

def calculate_macd(series, fast_period, slow_period, signal_period):
    """MACD hesaplama fonksiyonu."""
    ema_fast = series.ewm(span=fast_period, adjust=False).mean()
    ema_slow = series.ewm(span=slow_period, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal = macd.ewm(span=signal_period, adjust=False).mean()
    hist = macd - signal
    return macd, signal, hist

def calculate_bollinger_bands(series, period, std_dev):
    """Bollinger Bands hesaplama fonksiyonu."""
    sma = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper_band = sma + (std_dev * std)
    lower_band = sma - (std_dev * std)
    return upper_band, sma, lower_band

def calculate_fibonacci(df):
    """Fibonacci seviyelerini hesaplar ve DataFrame'e ekler."""
    high = df['high'].max()
    low = df['low'].min()

    df['Fibonacci_236'] = high - (high - low) * 0.236
    df['Fibonacci_382'] = high - (high - low) * 0.382
    df['Fibonacci_50'] = high - (high - low) * 0.5
    df['Fibonacci_618'] = high - (high - low) * 0.618
    df['Fibonacci_786'] = high - (high - low) * 0.786

    return df

if __name__ == "__main__":
    # Data klasöründe işlenmemiş tüm dosyaları tarayın
    data_dir = "data/dataClient/data"
    for file_name in os.listdir(data_dir):
        if file_name.endswith("_latest.csv"):
            file_path = os.path.join(data_dir, file_name)
            calculate_indicators(file_path)
