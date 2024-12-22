import requests

TELEGRAM_TOKEN = "7730853093:AAHZCZOUZMj3q4WYqFY60zoCuSR8IWZphMM"
TELEGRAM_CHAT_ID = "1222350744"

def send_telegram_message(message):
    """
    Telegram botundan mesaj gönderir.
    Hata kontrolü ile başarısız istekleri yakalar.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # HTTP hatalarını kontrol et

        if response.status_code == 200:
            print("Mesaj başarıyla gönderildi.")
            return response.json()
        else:
            print(f"Mesaj gönderilemedi, status kodu: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Mesaj gönderilirken hata oluştu: {e}")
        return None

# Test mesajı gönder
send_telegram_message("Selam 🚀")
