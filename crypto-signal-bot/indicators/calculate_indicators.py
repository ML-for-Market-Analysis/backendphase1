import os
import pandas as pd
import numpy as np

def calculate_indicators(file_path):
    """
    Tek bir dosyada teknik indikatörleri hesaplar.
    :param file_path: İşlenecek CSV dosyasının yolu.
    """
    try:
        df = pd.read_csv(file_path)

        required_columns = {'open', 'high', 'low', 'close', 'volume'}
        if not required_columns.issubset(df.columns):
            print(f"Gerekli sütunlar eksik: {file_path}")
            return

        # Eksik verileri doldur
        df.ffill(inplace=True)

        # -----------------------------------------------
        # RSI ve diğer indikatörler
        # TradingView ile daha uyumlu sonuç almak için
        # Wilder RSI fonksiyonunu kullanıyoruz.
        # -----------------------------------------------
        df['RSI'] = wilder_rsi(df['close'], period=14)  # "7" yazıyorsunuz, dilediğiniz periyodu verebilirsiniz.
        df['Inverse_Fisher_RSI'] = inverse_fisher_transform(df['RSI'])

        df['MACD'], df['MACD_signal'], df['MACD_hist'] = calculate_macd(df['close'], 12, 26, 9)
        df['upper_band'], df['middle_band'], df['lower_band'] = calculate_bollinger_bands(df['close'], 20, 2)
        df = calculate_fibonacci(df)

        # -----------------------------------------------------------------
        #  İşlenmiş dosyaların kaydedileceği klasör:
        #  Burada "crypto-signal-bot/data/processedData" yolunu kullanıyoruz.
        #  Proje yapınıza göre "../" miktarını düzenleyebilirsiniz.
        # -----------------------------------------------------------------
        processed_dir = os.path.join(os.path.dirname(__file__), "../data/processedData")
        processed_dir = os.path.abspath(processed_dir)
        os.makedirs(processed_dir, exist_ok=True)

        # "_latest.csv" => "_processed.csv"
        output_filename = os.path.basename(file_path).replace("_latest.csv", "_processed.csv")
        output_path = os.path.join(processed_dir, output_filename)

        df.to_csv(output_path, index=False)
        print(f"İşlenmiş dosya kaydedildi: {output_path}")

    except Exception as e:
        print(f"Hata oluştu: {file_path} -> {e}")


# ------------------------------------------------------------
# Wilder RSI fonksiyonu (TradingView ile daha uyumlu)
# ------------------------------------------------------------
def wilder_rsi(prices, period=14):
    """
    Wilder'ın (RMA) yaklaşımıyla RSI hesaplar.
    TradingView'deki "RSI" ile daha uyumlu sonuç alırsınız.
    """
    # Fiyat farkları
    delta = prices.diff()
    # Pozitif (kazanç) ve negatif (kayıp) kısımları ayır
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    # İlk ortalamaları simple average ile başlatıyoruz
    avg_gain = gain.rolling(period).mean().dropna()
    avg_loss = loss.rolling(period).mean().dropna()

    # RSI serisini oluşturmak için boş bir seri:
    rsi_series = pd.Series(index=prices.index, dtype=float)

    # Eğer yeterli satır yoksa erkenden çıkabiliriz
    if len(avg_gain) == 0 or len(avg_loss) == 0:
        return rsi_series

    # avg_gain/avg_loss'ın başladığı ilk index
    start_idx = avg_gain.index[0]

    # İlk RSI değerini hesaplarken
    if avg_loss.loc[start_idx] == 0:
        rsi_series.loc[start_idx] = 100
    else:
        rs = avg_gain.loc[start_idx] / avg_loss.loc[start_idx]
        rsi_series.loc[start_idx] = 100 - (100 / (1 + rs))

    # Şimdi geri kalan satırlar için Wilder (RMA) formülü:
    for i in range(start_idx + 1, len(prices)):
        if i not in prices.index:
            continue

        prev_avg_gain = avg_gain.loc[i-1] if (i-1 in avg_gain.index) else avg_gain.loc[start_idx]
        prev_avg_loss = avg_loss.loc[i-1] if (i-1 in avg_loss.index) else avg_loss.loc[start_idx]

        this_gain = gain.loc[i] if i in gain.index else 0
        this_loss = loss.loc[i] if i in loss.index else 0

        # RMA formülü
        new_avg_gain = (prev_avg_gain * (period - 1) + this_gain) / period
        new_avg_loss = (prev_avg_loss * (period - 1) + this_loss) / period

        avg_gain.loc[i] = new_avg_gain
        avg_loss.loc[i] = new_avg_loss

        if new_avg_loss == 0:
            rsi_series.loc[i] = 100
        else:
            rs = new_avg_gain / new_avg_loss
            rsi_series.loc[i] = 100 - (100 / (1 + rs))

    return rsi_series

def inverse_fisher_transform(rsi_series):
    """
    RSI (0-100) değerini alıp inverse fisher RSI oluşturur.
    """
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


if __name__ == "__main__":
    # Bu kod dosyası "crypto-signal-bot/" altındayken
    # işlenmemiş CSV’leri "../data/dataClient/data" klasöründen,
    # işlenmişleri "../data/processedData" klasörüne kaydediyoruz.
    data_dir = os.path.join(os.path.dirname(__file__), "../data/dataClient/data")
    data_dir = os.path.abspath(data_dir)

    print("İşlenmemiş veri klasörü:", data_dir)
    if not os.path.exists(data_dir):
        print(f"Veri klasörü bulunamadı: {data_dir}")
    else:
        for file_name in os.listdir(data_dir):
            if file_name.endswith("_latest.csv"):
                file_path = os.path.join(data_dir, file_name)
                print(f"İşleniyor: {file_path}")
                calculate_indicators(file_path)
