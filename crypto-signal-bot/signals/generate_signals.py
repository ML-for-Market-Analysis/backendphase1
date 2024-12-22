import sys
import os
import pandas as pd
import numpy as np

# notifications dizinini sys.path'e ekleyelim
sys.path.append(os.path.join(os.path.dirname(__file__), "../notifications"))

# notification.py'den fonksiyonu import et
from notifications.notification import send_telegram_message

def generate_signals():
    """
    İşlenen verilere dayalı olarak alım-satım sinyalleri üretir ve Telegram mesajı gönderir.
    """
    # İşlenmiş CSV'lerin aranacağı klasör
    processed_dir = os.path.join(os.path.dirname(__file__), "../data/processedData")
    processed_dir = os.path.abspath(processed_dir)

    # Üretilen sinyalleri kaydedeceğimiz klasör
    signals_dir = os.path.join(os.path.dirname(__file__), "../signals")
    signals_dir = os.path.abspath(signals_dir)
    os.makedirs(signals_dir, exist_ok=True)

    # Farklı indikatörlere puan ağırlıkları
    weights = {
        'RSI': 35,
        'MACD': 30,
        'Bollinger_Bands': 20,
        'Fibonacci': 10,
        'Volume': 5
    }

    # İşlenmiş dosyaları tarayın (_processed.csv uzantılı)
    for file_name in os.listdir(processed_dir):
        if file_name.endswith("_processed.csv"):
            file_path = os.path.join(processed_dir, file_name)
            print(f"Sinyal üretiliyor: {file_name}")

            try:
                df = pd.read_csv(file_path)

                # Eksik değerler varsa doldur
                if df.isnull().any().any():
                    print(f"NaN değerler bulundu: {file_path}")
                    df = df.fillna(method='ffill')

                # Gerekli hesaplamalar
                df['Volume_Avg'] = df['volume'].rolling(window=14).mean()
                df['signal'] = 'Hold'
                df['score'] = 0

                # İndikatör fonksiyonları (RSI, MACD, vb.)
                df['RSI'] = calculate_rsi(df['close'], 14)
                df['MACD'], df['MACD_signal'], df['MACD_hist'] = calculate_macd(df['close'], 12, 26, 9)
                df['upper_band'], df['middle_band'], df['lower_band'] = calculate_bollinger_bands(df['close'], 20, 2)
                df['Fibonacci_618'], df['Fibonacci_382'] = calculate_fibonacci(df)

                # RSI sinyal puanı
                df.loc[df['RSI'] < 30, 'score'] += weights['RSI']   # Aşırı satım → Buy
                df.loc[df['RSI'] > 70, 'score'] -= weights['RSI']   # Aşırı alım → Sell

                # MACD sinyal puanı
                df.loc[df['MACD'] > df['MACD_signal'], 'score'] += weights['MACD']
                df.loc[df['MACD'] < df['MACD_signal'], 'score'] -= weights['MACD']

                # Bollinger Bands sinyalleri
                df.loc[df['close'] < df['lower_band'], 'score'] += weights['Bollinger_Bands']
                df.loc[df['close'] > df['upper_band'], 'score'] -= weights['Bollinger_Bands']

                # Fibonacci sinyalleri
                df.loc[df['close'] > df['Fibonacci_618'], 'score'] += weights['Fibonacci']
                df.loc[df['close'] < df['Fibonacci_382'], 'score'] -= weights['Fibonacci']

                # Hacim (Volume) sinyalleri
                df.loc[df['volume'] > df['Volume_Avg'], 'score'] += weights['Volume']
                df.loc[df['volume'] < df['Volume_Avg'], 'score'] -= weights['Volume']

                # Nihai sinyal kararı
                df.loc[df['score'] >= 50, 'signal'] = 'Buy'
                df.loc[df['score'] <= -50, 'signal'] = 'Sell'

                # Sinyalleri kaydet
                signal_file_name = file_name.replace("_processed.csv", "_signals.csv")
                signal_file_path = os.path.join(signals_dir, signal_file_name)
                df.to_csv(signal_file_path, index=False)
                print(f"Sinyaller kaydedildi: {signal_file_path}")

                # Telegram mesajı gönder (son satırdaki sinyal)
                latest_signal = df.iloc[-1]
                coin_name = file_name.replace('_processed.csv', '')
                message = (
                    f"Sinyal Üretildi 📊\n"
                    f"Coin: {coin_name}\n"
                    f"Sinyal: {latest_signal['signal']}\n"
                    f"Puan: {latest_signal['score']}\n\n"
                    f"🔍 İndikatörler:\n"
                    f" - RSI: {latest_signal['RSI']:.2f}\n"
                    f" - MACD: {latest_signal['MACD']:.2f} (Signal: {latest_signal['MACD_signal']:.2f})\n"
                    f" - Bollinger: [{latest_signal['lower_band']:.2f}, {latest_signal['upper_band']:.2f}]\n"
                    f" - Fibonacci(0.618): {latest_signal['Fibonacci_618']:.2f}\n"
                    f" - Fibonacci(0.382): {latest_signal['Fibonacci_382']:.2f}\n"
                    f" - Volume: {latest_signal['volume']:.2f} (Avg: {latest_signal['Volume_Avg']:.2f})\n"
                )
                send_telegram_message(message)

            except Exception as e:
                print(f"Hata oluştu ({file_name}): {e}")


def calculate_rsi(series, period=14):
    """RSI hesaplama fonksiyonu."""
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=period, min_periods=1).mean()
    avg_loss = pd.Series(loss).rolling(window=period, min_periods=1).mean()
    rs = avg_gain / (avg_loss + 1e-10)  # Sıfıra bölünmeyi engelle
    return 100 - (100 / (1 + rs))

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
    fibonacci_618 = high - (high - low) * 0.618
    fibonacci_382 = high - (high - low) * 0.382
    return fibonacci_618, fibonacci_382


if __name__ == "__main__":
    generate_signals()
