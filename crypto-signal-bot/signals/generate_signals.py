import sys
import os
import asyncio
import pandas as pd
import numpy as np

# notifications dizinini sys.path'e ekleyelim
sys.path.append(os.path.join(os.path.dirname(__file__), "../notifications"))

# notification.py'den fonksiyonu import et
from notifications.notification import send_telegram_message

async def generate_signals():
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

    if not os.path.exists(processed_dir):
        print(f"[SIGNALS] İşlenmiş dosya klasörü yok: {processed_dir}")
        return

    last_signal = None  # Önceki sinyali tutmak için değişken

    for file_name in os.listdir(processed_dir):
        if file_name.endswith("_processed.csv"):
            file_path = os.path.join(processed_dir, file_name)
            print(f"[SIGNALS] Sinyal üretiliyor: {file_name}")

            try:
                df = pd.read_csv(file_path)
                print(f"[SIGNALS] Dosya okundu: {file_name}, Veri boyutu: {df.shape}")

                # ------------ MACD Sinyali ------------
                df['macd_signal'] = 'Hold'
                df['buy_signal'] = (df['MACD'] > df['MACD_signal']) & (df['MACD'].shift(1) <= df['MACD_signal'].shift(1))
                df['sell_signal'] = (df['MACD'] < df['MACD_signal']) & (df['MACD'].shift(1) >= df['MACD_signal'].shift(1))

                for i in range(len(df)):
                    if df.loc[i, 'buy_signal']:
                        last_signal = 'Buy'
                        df.loc[i, 'macd_signal'] = 'Buy'
                    elif df.loc[i, 'sell_signal']:
                        last_signal = 'Sell'
                        df.loc[i, 'macd_signal'] = 'Sell'
                    else:
                        df.loc[i, 'macd_signal'] = f"Hold (Hala {last_signal})" if last_signal else 'Hold'

                # ------------ Nihai Sinyali Kaydet ------------
                signal_file_name = file_name.replace("_processed.csv", "_signals.csv")
                df.to_csv(os.path.join(signals_data_dir, signal_file_name), index=False)
                print(f"[SIGNALS] Sinyaller kaydedildi: {signal_file_name}")

                # ------------ Telegram Mesajı ------------
                latest_signal = df.iloc[-1]
                coin_name = file_name.replace('_processed.csv', '')
                macd_signal = latest_signal['macd_signal']

                # Emoji Gösterimleri
                signal_emojis = {
                    'Buy': "🟢",
                    'Sell': "🔴",
                    'Hold': "🔵"
                }

                emoji = signal_emojis.get(macd_signal.split()[0], "🔵")
                formatted_time = latest_signal['open_time'] if 'open_time' in latest_signal else "N/A"
                macd_value = latest_signal['MACD']
                macd_signal_value = latest_signal['MACD_signal']
                macd_hist = latest_signal['MACD_hist']

                # Parantez içi ifadeler ve detaylar
                decision_text = {
                    "🟢": "(Al)",
                    "🔴": "(Sat)",
                    "🔵": "(Tut)"
                }
                decision = decision_text.get(emoji, "(Belirsiz)")

                message = (
                    f"✅ **Al-Sat Sinyali (MACD)**\n"
                    f"Coin: {coin_name}\n"
                    f"Karar: {macd_signal} {emoji} {decision}\n"
                    f"Tarih/Saat: {formatted_time}\n\n"
                    f"🔍 **Detaylar**:\n"
                    f" - MACD: {macd_value:.4f}\n"
                    f" - MACD Signal: {macd_signal_value:.4f}\n"
                    f" - MACD Histogram: {macd_hist:.4f}\n"
                )

                try:
                    await send_telegram_message(message)
                    print(f"[SIGNALS] Mesaj gönderildi: {coin_name}")
                except Exception as e:
                    print(f"[SIGNALS] Mesaj gönderme hatası: {e}")

            except Exception as e:
                print(f"[SIGNALS] Hata oluştu ({file_name}): {e}")

if __name__ == "__main__":
    asyncio.run(generate_signals())
