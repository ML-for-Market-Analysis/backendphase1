import os
from data.dataClient.fetch_binance_data import fetch_and_append_data
from indicators.calculate_indicators import calculate_indicators
from signals.generate_signals import generate_signals
from notifications.notification import send_telegram_message


def main():
    """
    Tüm süreçlerin sırasıyla yürütüldüğü ana fonksiyon.
    """
    try:
        print("📊 Veri çekme işlemi başlatılıyor...")
        # İlk 50 işlem çiftini al
        top_50_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT']  # Gerçekten ilk 50'yi buraya ekleyebilirsiniz
        fetch_and_append_data(top_50_symbols)  # Binance API'den verileri çek ve kaydet
        print("✅ Veri çekme tamamlandı.")

        print("📈 İndikatör hesaplama işlemi başlatılıyor...")
        # İşlenmiş verilerle indikatör hesaplama işlemi başlat
        data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "data/dataClient/data"))
        print(f"İndikatör hesaplanacak veri klasörü: {data_dir}")

        if not os.path.exists(data_dir):
            print(f"🚨 Veri klasörü bulunamadı: {data_dir}")
        else:
            for file_name in os.listdir(data_dir):
                if file_name.endswith("_latest.csv"):
                    file_path = os.path.join(data_dir, file_name)
                    print(f"İşleniyor: {file_path}")
                    try:
                        calculate_indicators(file_path)  # İndikatör hesaplama fonksiyonu
                        print(f"✅ İndikatörler hesaplandı: {file_name}")
                    except Exception as e:
                        print(f"🚨 Hata oluştu ({file_name}): {e}")
        print("✅ İndikatör hesaplama tamamlandı.")

        print("🔔 Sinyal üretimi başlatılıyor...")
        generate_signals()  # İşlenmiş verilerle al-sat sinyalleri oluştur
        print("✅ Sinyal üretimi tamamlandı.")

        print("✉️ Bildirim gönderiliyor...")
        send_telegram_message("Tüm süreç başarıyla tamamlandı!")  # Sürecin bittiğini bildiren mesaj
        print("✅ Bildirim gönderildi.")

    except Exception as e:
        # Hata durumunda mesaj gönder
        error_message = f"🚨 Süreçte hata oluştu: {e}"
        print(error_message)
        send_telegram_message(error_message)


if __name__ == "__main__":
    # Çalışma dizinini kontrol et ve ayarla
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(current_dir)

    # Ana işlemleri başlat
    main()
