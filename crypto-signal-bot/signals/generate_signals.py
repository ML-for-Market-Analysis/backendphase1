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

    # Tüm sinyalleri kaydedeceğimiz klasör
    signals_data_dir = os.path.join(os.path.dirname(__file__), "data")
    signals_data_dir = os.path.abspath(signals_data_dir)
    os.makedirs(signals_data_dir, exist_ok=True)

    # İndikatörlere göre ağırlıklar (score hesabında)
    weights = {
        'RSI': 15,
        'MACD': 20,
        'Bollinger_Bands': 10,
        'Fibonacci': 5,
        'ADX': 10,
        'Stochastic_%K': 10,
        'ATR': 10,
        'ROC': 10,
        'CCI': 10
    }

    if not os.path.exists(processed_dir):
        print(f"[SIGNALS] İşlenmiş dosya klasörü yok: {processed_dir}")
        return

    for file_name in os.listdir(processed_dir):
        if file_name.endswith("_processed.csv"):
            file_path = os.path.join(processed_dir, file_name)
            print(f"[SIGNALS] Sinyal üretiliyor: {file_name}")

            try:
                df = pd.read_csv(file_path)
                print(f"[SIGNALS] Dosya okundu: {file_name}, Veri boyutu: {df.shape}")

                # Eksik sütunları ekleyelim
                missing_columns = [col for col in weights.keys() if col not in df.columns]
                print(f"[SIGNALS] Eksik sütunlar: {missing_columns}")
                for col in missing_columns:
                    df[col] = np.nan

                # Boş değerleri 0 ile dolduralım
                df.fillna(0, inplace=True)

                # Ortalama hacim ve skor
                # (Örnek bir ek metrik, isterseniz kullanmayabilirsiniz)
                if 'volume' in df.columns:
                    df['Volume_Avg'] = df['volume'].rolling(window=14).mean().fillna(0)
                else:
                    df['Volume_Avg'] = 0

                df['score'] = 0.0

                # ------------ RSI Hesaplaması ------------
                if 'RSI' in df.columns:
                    df.loc[df['RSI'] < 25, 'score'] += weights['RSI'] * 1.5
                    df.loc[df['RSI'] > 75, 'score'] -= weights['RSI'] * 1.5
                    print(f"[SIGNALS] RSI sinyali hesaplandı.")

                # ------------ MACD ------------
                if 'MACD' in df.columns and 'MACD_signal' in df.columns:
                    df.loc[df['MACD'] > df['MACD_signal'], 'score'] += weights['MACD']
                    df.loc[df['MACD'] < df['MACD_signal'], 'score'] -= weights['MACD']
                    print(f"[SIGNALS] MACD sinyali hesaplandı.")

                # ------------ Bollinger Bands ------------
                # Kodda 'Bollinger_Bands' diye bir key var. Aslında lower_band/upper_band
                # varsa, onlara göre sinyal veriyoruz. "Bollinger_Bands" skoru eklemek için:
                if 'lower_band' in df.columns and 'upper_band' in df.columns:
                    df.loc[df['close'] < df['lower_band'], 'score'] += weights['Bollinger_Bands']
                    df.loc[df['close'] > df['upper_band'], 'score'] -= weights['Bollinger_Bands']
                    print(f"[SIGNALS] Bollinger Bands sinyali hesaplandı.")

                # ------------ Fibonacci Seviyeleri ------------
                # Kodda 'Fibonacci' key var. Örnek olarak Fibonacci_236 / Fibonacci_786 dikkate alınmış:
                if 'close' in df.columns and 'Fibonacci_236' in df.columns:
                    # Yukarı seviyeyi de kullanalım:
                    if 'Fibonacci_786' in df.columns:
                        df.loc[df['close'] < df['Fibonacci_236'], 'score'] += weights['Fibonacci']
                        df.loc[df['close'] > df['Fibonacci_786'], 'score'] -= weights['Fibonacci']
                        print(f"[SIGNALS] Fibonacci sinyali hesaplandı.")
                    else:
                        print(f"[SIGNALS] Uyarı: 'Fibonacci_786' yok, kısıtlı sinyal.")

                # ------------ ADX ------------
                if 'ADX' in df.columns:
                    df.loc[df['ADX'] > 25, 'score'] += weights['ADX']
                    df.loc[df['ADX'] < 20, 'score'] -= weights['ADX']
                    print(f"[SIGNALS] ADX sinyali hesaplandı.")

                # ------------ Stochastic Oscillator ------------
                if 'Stochastic_%K' in df.columns and 'Stochastic_%D' in df.columns:
                    df.loc[df['Stochastic_%K'] > df['Stochastic_%D'], 'score'] += weights['Stochastic_%K']
                    df.loc[df['Stochastic_%K'] < df['Stochastic_%D'], 'score'] -= weights['Stochastic_%K']
                    print(f"[SIGNALS] Stochastic Oscillator sinyali hesaplandı.")

                # ------------ ATR ------------
                if 'ATR' in df.columns:
                    atr_mean = df['ATR'].rolling(window=14).mean().fillna(0)
                    df.loc[df['ATR'] > atr_mean, 'score'] -= weights['ATR'] * 0.5
                    print(f"[SIGNALS] ATR sinyali hesaplandı.")

                # ------------ ROC ------------
                if 'ROC' in df.columns:
                    df.loc[df['ROC'] > 5, 'score'] += weights['ROC'] * 0.5
                    df.loc[df['ROC'] < -5, 'score'] -= weights['ROC'] * 0.5
                    print(f"[SIGNALS] ROC sinyali hesaplandı.")

                # ------------ CCI ------------
                if 'CCI' in df.columns:
                    df.loc[df['CCI'] > 100, 'score'] += weights['CCI']
                    df.loc[df['CCI'] < -100, 'score'] -= weights['CCI']
                    print(f"[SIGNALS] CCI sinyali hesaplandı.")

                # ------------ Nihai Sinyal Kararı ------------
                df['signal'] = 'Hold'
                df.loc[df['score'] >= 70, 'signal'] = 'Strong Buy'
                df.loc[(df['score'] >= 40) & (df['score'] < 70), 'signal'] = 'Buy'
                df.loc[df['score'] <= -70, 'signal'] = 'Strong Sell'
                df.loc[(df['score'] <= -40) & (df['score'] > -70), 'signal'] = 'Sell'
                print(f"[SIGNALS] Nihai sinyal belirlendi.")

                # Sinyalleri kaydet
                signal_file_name = file_name.replace("_processed.csv", "_signals.csv")
                df.to_csv(os.path.join(signals_data_dir, signal_file_name), index=False)
                print(f"[SIGNALS] Sinyaller kaydedildi: {signal_file_name}")

                # ------------------- Telegram Mesajı (Detaylı) -------------------
                latest_signal = df.iloc[-1]
                coin_name = file_name.replace('_processed.csv', '')
                signal_decision = latest_signal['signal']
                score_value = latest_signal['score']
                print(f"[SIGNALS] Telegram mesajı için: Coin: {coin_name}, Signal: {signal_decision}, Score: {score_value}")

                # Emoji Gösterimleri
                signal_emojis = {
                    'Strong Buy': "🟢🟢",
                    'Buy': "🟢",
                    'Hold': "🔵",
                    'Sell': "🔴",
                    'Strong Sell': "🔴🔴"
                }
                signal_emoji = signal_emojis.get(signal_decision, "🔵")

                # Tarih/Saat
                formatted_time = latest_signal['open_time'] if 'open_time' in latest_signal else "N/A"

                # Daha detaylı indikatör mesajları
                detail_lines = []

                # RSI
                if 'RSI' in latest_signal:
                    rsi_val = latest_signal['RSI']
                    rsi_comment = ""
                    if rsi_val < 30:
                        rsi_comment = " (Aşırı Satım)"
                    elif rsi_val > 70:
                        rsi_comment = " (Aşırı Alım)"
                    detail_lines.append(f" - RSI: {rsi_val:.2f}{rsi_comment}")

                # MACD
                if 'MACD' in latest_signal and 'MACD_signal' in latest_signal:
                    macd_val = latest_signal['MACD']
                    macd_sig = latest_signal['MACD_signal']
                    macd_hist = macd_val - macd_sig
                    detail_lines.append(f" - MACD: {macd_val:.2f}, Signal: {macd_sig:.2f}, Hist: {macd_hist:.2f}")

                # Bollinger Bands
                if 'lower_band' in latest_signal and 'upper_band' in latest_signal and 'close' in latest_signal:
                    lb = latest_signal['lower_band']
                    ub = latest_signal['upper_band']
                    cl = latest_signal['close']
                    bb_comment = ""
                    if cl < lb:
                        bb_comment = " (Fiyat alt bandın altında!)"
                    elif cl > ub:
                        bb_comment = " (Fiyat üst bandın üstünde!)"
                    detail_lines.append(f" - Bollinger: LB={lb:.2f}, UB={ub:.2f}{bb_comment}")

                # Fibonacci
                if 'Fibonacci_236' in latest_signal and 'Fibonacci_786' in latest_signal and 'close' in latest_signal:
                    fib_236 = latest_signal['Fibonacci_236']
                    fib_786 = latest_signal['Fibonacci_786']
                    cl = latest_signal['close']
                    fib_comment = ""
                    if cl < fib_236:
                        fib_comment = " (0.236 altında!)"
                    elif cl > fib_786:
                        fib_comment = " (0.786 üstünde!)"
                    detail_lines.append(f" - Fibonacci: 0.236={fib_236:.2f}, 0.786={fib_786:.2f}{fib_comment}")

                # ADX
                if 'ADX' in latest_signal:
                    adx_val = latest_signal['ADX']
                    detail_lines.append(f" - ADX: {adx_val:.2f}")

                # Stochastic
                if 'Stochastic_%K' in latest_signal and 'Stochastic_%D' in latest_signal:
                    k_val = latest_signal['Stochastic_%K']
                    d_val = latest_signal['Stochastic_%D']
                    detail_lines.append(f" - Stoch %K: {k_val:.2f}, %D: {d_val:.2f}")

                # ATR
                if 'ATR' in latest_signal:
                    atr_val = latest_signal['ATR']
                    detail_lines.append(f" - ATR: {atr_val:.2f}")

                # ROC
                if 'ROC' in latest_signal:
                    roc_val = latest_signal['ROC']
                    detail_lines.append(f" - ROC: {roc_val:.2f}")

                # CCI
                if 'CCI' in latest_signal:
                    cci_val = latest_signal['CCI']
                    detail_lines.append(f" - CCI: {cci_val:.2f}")

                # Mesaj Formatı
                message = (
                    f"✅ **Al-Sat Sinyali**\n"
                    f"Coin: {coin_name}\n"
                    f"Karar: {signal_decision} {signal_emoji}\n"
                    f"Tarih/Saat: {formatted_time}\n"
                    f"Puan: {score_value:.2f}\n\n"
                    f"🔍 **İndikatör Detayları**:\n"
                )
                message += "\n".join(detail_lines)

                try:
                    send_telegram_message(message)
                    print(f"[SIGNALS] Mesaj gönderildi: {coin_name}")
                except Exception as e:
                    print(f"[SIGNALS] Mesaj gönderme hatası: {e}")

            except Exception as e:
                print(f"[SIGNALS] Hata oluştu ({file_name}): {e}")

if __name__ == "__main__":
    generate_signals()
