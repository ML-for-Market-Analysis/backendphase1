import requests

TELEGRAM_TOKEN = "7730853093:AAHZCZOUZMj3q4WYqFY60zoCuSR8IWZphMM"
TELEGRAM_CHAT_ID = "1222350744"

def send_telegram_message(message):
    """
    Telegram botundan mesaj gönderir.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    response = requests.post(url, json=payload)
    return response.json()

# Test mesajı gönder
send_telegram_message("Selam 🚀")
