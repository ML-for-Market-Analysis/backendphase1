import sys
import os
import pandas as pd
import numpy as np

# notifications dizinini sys.path'e ekleyelim
sys.path.append(os.path.join(os.path.dirname(__file__), "../notifications"))

# notification.py'den fonksiyonu import et
from notification import send_telegram_message

def generate_signals():
    """
    İşlenen verilere dayalı olarak alım-satım sinyalleri üretir ve Telegram mesajı gönderir.
    """
    # İşlenmiş CSV'lerin aranacağı klasör
    processed_dir = os.path.join(os.path.dirname(__file__), "../data/processedData")
    processed_dir = os.path.abspath(processed_dir)

    # Tüm sinyalleri kaydedeceğimiz klasör
    signals_data_dir = os.path.join(os.path.dirname(__file__), "data")
    signals_data_dir = os.path.abspath(signals_data_dir)
    os.makedirs(signals_data_dir, exist_ok=True)

    # Farklı indikatörlere puan ağırlıkları
    weights = {
        'RSI': 30,
        'Inverse_Fisher_RSI': 25,
        'MACD': 20,
        'Bollinger_Bands': 15,
        'Fibonacci': 5,
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

                # RSI sinyal puanı
                df.loc[df['RSI'] < 30, 'score'] += weights['RSI']   # Aşırı satım → Buy
                df.loc[df['RSI'] > 70, 'score'] -= weights['RSI']   # Aşırı alım → Sell

                # Inverse Fisher RSI sinyal puanı
                df.loc[df['Inverse_Fisher_RSI'] > 0.8, 'score'] += weights['Inverse_Fisher_RSI']  # Güçlü pozitif momentum
                df.loc[df['Inverse_Fisher_RSI'] < -0.8, 'score'] -= weights['Inverse_Fisher_RSI']  # Güçlü negatif momentum

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
                signal_file_path = os.path.join(signals_data_dir, signal_file_name)
                df.to_csv(signal_file_path, index=False)
                print(f"Sinyaller kaydedildi: {signal_file_path}")

                # Telegram mesajı gönder (son satırdaki sinyal)
                latest_signal = df.iloc[-1]
                coin_name = file_name.replace('_processed.csv', '')

                # Emoji ekleme
                signal_emoji = "🚀" if latest_signal['signal'] == 'Buy' else "📉"

                # Zaman bilgisi ekleme
                timestamp = latest_signal['timestamp']  # Son verinin zamanı
                formatted_time = pd.to_datetime(timestamp).strftime('%Y-%m-%d %H:%M:%S')

                message = (
                    f"Sinyal Üretildi 📊\n"
                    f"Coin: {coin_name}\n"
                    f"Sinyal: {latest_signal['signal']} {signal_emoji} (Buy/Sell/Hold)\n"
                    f"Tarih ve Saat: {formatted_time}\n"
                    f"Puan: {latest_signal['score']}\n\n"
                    f"🔍 İndikatörler:\n"
                    f" - RSI: {latest_signal['RSI']:.2f}\n"
                    f" - Inverse Fisher RSI: {latest_signal['Inverse_Fisher_RSI']:.2f}\n"
                    f" - MACD: {latest_signal['MACD']:.2f} (Signal: {latest_signal['MACD_signal']:.2f})\n"
                    f" - Bollinger: [{latest_signal['lower_band']:.2f}, {latest_signal['upper_band']:.2f}]\n"
                    f" - Fibonacci(0.618): {latest_signal['Fibonacci_618']:.2f}\n"
                    f" - Fibonacci(0.382): {latest_signal['Fibonacci_382']:.2f}\n"
                    f" - Volume: {latest_signal['volume']:.2f} (Avg: {latest_signal['Volume_Avg']:.2f})\n"
                )
                send_telegram_message(message)

            except Exception as e:
                print(f"Hata oluştu ({file_name}): {e}")

if __name__ == "__main__":
    generate_signals()
