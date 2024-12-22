import os
from data.dataClient.fetch_binance_data import fetch_and_append_data
from indicators.calculate_indicators import calculate_indicators
from signals.generate_signals import generate_signals
from notifications.notification import send_telegram_messagert 


def main():
    """
    Tüm süreçlerin sırasıyla yürütüldüğü ana fonksiyon.
    """
    try:
        print("📊 Veri çekme işlemi başlatılıyor...")
        fetch_and_save_data()  # Binance API'den verileri çek ve kaydet
        print("✅ Veri çekme tamamlandı.")

        print("📈 İndikatör hesaplama işlemi başlatılıyor...")
        process_all_data()  # İndikatörleri hesapla ve işlenmiş verileri kaydet
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
