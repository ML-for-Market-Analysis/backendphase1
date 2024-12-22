import os
import pandas as pd
from notifications import send_telegram_message  # notification.py'den mesaj gönderme fonksiyonu

def generate_signals():
    """
    İşlenen verilere dayalı olarak alım-satım sinyalleri üretir ve Telegram mesajı gönderir.
    """
    processed_dir = "data/processedData"  # İşlenmiş verilerin olduğu klasör
    signals_dir = "signals"  # Sinyallerin kaydedileceği klasör
    os.makedirs(signals_dir, exist_ok=True)  # Sinyal klasörü yoksa oluştur

    # Ağırlıklar
    weights = {
        'RSI': 35,
        'MACD': 30,
        'Bollinger_Bands': 20,
        'Fibonacci': 10,
        'Volume': 5
    }

    # İşlenmiş dosyaları tarayın
    for file_name in os.listdir(processed_dir):
        if file_name.endswith("_processed.csv"):  # Sadece işlenmiş dosyaları al
            file_path = os.path.join(processed_dir, file_name)
            print(f"Sinyal üretiliyor: {file_name}")

            try:
                # CSV dosyasını yükle
                df = pd.read_csv(file_path)

                # Sinyal sütunu oluştur
                df['signal'] = 'Hold'  # Varsayılan sinyal
                df['score'] = 0  # Puanlama için yeni bir sütun

                # RSI
                df.loc[df['RSI'] < 30, 'score'] += weights['RSI']  # Buy sinyali
                df.loc[df['RSI'] > 70, 'score'] -= weights['RSI']  # Sell sinyali

                # MACD
                df.loc[df['MACD'] > df['MACD_signal'], 'score'] += weights['MACD']  # Buy
                df.loc[df['MACD'] < df['MACD_signal'], 'score'] -= weights['MACD']  # Sell

                # Bollinger Bands
                df.loc[df['close'] < df['lower_band'], 'score'] += weights['Bollinger_Bands']  # Buy
                df.loc[df['close'] > df['upper_band'], 'score'] -= weights['Bollinger_Bands']  # Sell

                # Fibonacci Levels
                df.loc[df['close'] > df['Fibonacci_Levels_level_618'], 'score'] += weights['Fibonacci']  # Strong Buy
                df.loc[df['close'] < df['Fibonacci_Levels_level_382'], 'score'] -= weights['Fibonacci']  # Strong Sell

                # Volume
                df.loc[df['volume'] > df['Volume_Avg'], 'score'] += weights['Volume']  # High Volume Buy
                df.loc[df['volume'] < df['Volume_Avg'], 'score'] -= weights['Volume']  # Low Volume Sell

                # Karar Kriterleri
                df.loc[df['score'] >= 50, 'signal'] = 'Buy'
                df.loc[df['score'] <= -50, 'signal'] = 'Sell'

                # Sinyal dosyasına kaydet
                signal_file_path = os.path.join(signals_dir, f"{file_name.replace('_processed.csv', '_signals.csv')}")
                df.to_csv(signal_file_path, index=False)
                print(f"Sinyaller kaydedildi: {signal_file_path}")

                # Telegram mesajı gönder
                latest_signal = df.iloc[-1]  # En son satırdaki sinyal
                coin_name = file_name.replace('_processed.csv', '')  # Coin ismi
                message = (
                    f"Sinyal Üretildi 📊\n"
                    f"Coin: {coin_name}\n"
                    f"Sinyal: {latest_signal['signal']}\n"
                    f"Puan: {latest_signal['score']}\n\n"
                    f"🔍 İndikatörler:\n"
                    f" - RSI: {latest_signal['RSI']:.2f}\n"
                    f" - MACD: {latest_signal['MACD']:.2f} (Signal: {latest_signal['MACD_signal']:.2f})\n"
                    f" - Bollinger: [{latest_signal['lower_band']:.2f}, {latest_signal['upper_band']:.2f}]\n"
                    f" - Fibonacci Level: {latest_signal['Fibonacci_Levels_level_618']:.2f}\n"
                    f" - Volume: {latest_signal['volume']:.2f} (Avg: {latest_signal['Volume_Avg']:.2f})\n"
                )
                send_telegram_message(message)

            except Exception as e:
                print(f"Hata oluştu ({file_name}): {e}")

if __name__ == "__main__":
    generate_signals()
