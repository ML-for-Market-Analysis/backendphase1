import requests

# Telegram bot bilgileri
TELEGRAM_TOKEN = "7730853093:AAHZCZOUZMj3q4WYqFY60zoCuSR8IWZphMM"
TELEGRAM_CHAT_ID = "1222350744"

def send_telegram_message(message, parse_mode="Markdown"):
    """
    Telegram botundan mesaj gönderir.
    :param message: Gönderilecek mesaj içeriği (string)
    :param parse_mode: Mesajın formatı (örneğin: 'Markdown', 'HTML')
    :return: Telegram API yanıtı (json)
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": parse_mode  # Mesajın formatı için destek
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Hata durumlarını yakalar
        print("Mesaj başarıyla gönderildi!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Mesaj gönderiminde hata oluştu: {e}")
        return None

# Test mesajı gönder
if __name__ == "__main__":
    test_message = (
        "*Selam 🚀*\n\n"
        "_Bu bir test mesajıdır._\n"
        "`Kod blokları desteklenir.`"
    )
    send_telegram_message(test_message)
