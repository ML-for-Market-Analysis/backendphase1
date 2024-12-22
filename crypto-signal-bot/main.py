import os
from data.dataClient.fetch_binance_data import get_top_100_symbols, fetch_and_append_data
from indicators.calculate_indicators import calculate_indicators
from signals.generate_signals import generate_signals
from notifications.send_telegram import send_notification

def main():
    # 1. Binance API'den ilk 100 işlem çiftini al
    print("İlk 100 işlem çifti alınıyor...")
    top_100_symbols = get_top_100_symbols()

    if not top_100_symbols:
        print("İşlem çiftleri alınamadı, program sonlandırılıyor.")
        return

    # 2. Verileri Binance API'den çek ve dosyalara kaydet
    print("Veriler çekiliyor ve güncelleniyor...")
    fetch_and_append_data(top_100_symbols)

    # 3. İndikatörleri hesapla
    print("İndikatörler hesaplanıyor...")
    calculate_indicators()  # İlgili veri dosyalarını kullanarak çalışacak şekilde yapılandırın

    # 4. Alım-satım sinyalleri oluştur
    print("Sinyaller oluşturuluyor...")
    signals = generate_signals()  # Alım-satım sinyalleri oluştur

    # 5. Sinyalleri Telegram botuna gönder
    if signals:
        print("Sinyaller gönderiliyor...")
        send_notification(signals)
    else:
        print("Gönderilecek sinyal bulunamadı.")

if __name__ == "__main__":
    main()
