import os
import pandas as pd
import talib

def calculate_indicators():
    """
    Tüm işlem çiftleri için indikatörleri hesaplar ve CSV dosyalarını günceller.
    """
    # Verilerin bulunduğu dizin
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("Data dizini bulunamadı. Lütfen verileri önce çekin.")
        return

    # Data dizinindeki tüm dosyaları tarayın
    for file_name in os.listdir(data_dir):
        if file_name.endswith("_latest.csv"):  # Sadece ilgili dosyalar üzerinde işlem yap
            file_path = os.path.join(data_dir, file_name)
            print(f"İndikatörler hesaplanıyor: {file_name}")

            try:
                # CSV dosyasını yükle
                df = pd.read_csv(file_path)

                # İndikatörler için gerekli sütunların mevcut olup olmadığını kontrol edin
                if not {'close', 'high', 'low', 'volume'}.issubset(df.columns):
                    print(f"Gerekli sütunlar eksik: {file_name}")
                    continue

                # RSI Hesaplama
                df['RSI'] = talib.RSI(df['close'], timeperiod=14)

                # MACD Hesaplama
                df['MACD'], df['MACD_signal'], df['MACD_hist'] = talib.MACD(
                    df['close'], fastperiod=12, slowperiod=26, signalperiod=9
                )

                # Bollinger Bands Hesaplama
                df['upper_band'], df['middle_band'], df['lower_band'] = talib.BBANDS(
                    df['close'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
                )

                # Ekstra: Volume Analysis
                df['volume_change'] = df['volume'].pct_change()  # Hacim değişim oranı

                # Yeni hesaplanan indikatörleri aynı CSV dosyasına kaydet
                df.to_csv(file_path, index=False)
                print(f"{file_name} dosyası güncellendi.")
            except Exception as e:
                print(f"Hata oluştu ({file_name}): {e}")

if __name__ == "__main__":
    calculate_indicators()
