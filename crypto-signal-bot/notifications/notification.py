import requests
import asyncio
from telegram import Update ,Bot
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from telegram.error import RetryAfter, TelegramError

TELEGRAM_TOKEN = "7730853093:AAHZCZOUZMj3q4WYqFY60zoCuSR8IWZphMM"

base_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/"

Whitelist = [
    1222350744, # Edip
    5652200670  # Sercan - Nac - Naxxus
    ]

async def get_chat_ids():
    """
    Tek seferlik güncelleme ile kullanıcı ID'lerini al.
    """
    bot = Bot(token=TELEGRAM_TOKEN)
    updates = await bot.get_updates()

    chat_ids = []
    for update in updates:  # await kaldırıldı çünkü updates zaten çözümlenmiş bir liste.
        if update.message:
            chat_id = update.message.chat_id
            username = update.message.chat.username
            first_name = update.message.chat.first_name
            if chat_id not in chat_ids:
                chat_ids.append(chat_id)
                print(f"Chat ID: {chat_id}, Kullanıcı: {first_name} (@{username})")

    return chat_ids


def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.message.chat_id
    update.message.reply_text(f"Your Chat ID: {chat_id}")


async def send_telegram_message(message):
    """
    Birden fazla kullanıcıya mesaj gönderir.
    Rate limit kontrolü ile paralel işlem yapılır.
    """
    bot = Bot(token=TELEGRAM_TOKEN)
    #chat_ids = await get_chat_ids()  # Tüm chat ID'lerini al
    chat_ids = Whitelist

    tasks = []
    for chat_id in chat_ids:
        tasks.append(send_with_retry(bot, chat_id, message))
    
    print(f"Tasks: {tasks}")


    results = await asyncio.gather(*tasks, return_exceptions=True)

    for chat_id, result in zip(chat_ids, results):
        if isinstance(result, Exception):
            print(f"Mesaj {chat_id} için gönderilemedi: {result}")
        else:
            print(f"Mesaj {chat_id} için başarıyla gönderildi.")


async def send_with_retry(bot, chat_id, message, max_retries=5):
    """
    Rate limit nedeniyle mesaj gönderme işlemini tekrar dener.
    Belirli bir deneme sayısından sonra başarısız olur.
    """
    bot = Bot(token=TELEGRAM_TOKEN)
    retries = 0
    while retries < max_retries:
        try:
            return await bot.send_message(chat_id=chat_id, text=message)
        except RetryAfter as e:
            print(f"Rate limit nedeniyle bekleniyor: {e.retry_after} saniye")
            await asyncio.sleep(e.retry_after)
            retries += 1
        except TelegramError as e:
            print(f"Telegram API hatası: {e}")
            raise e
        except Exception as ex:
            print(f"Bilinmeyen hata oluştu: {ex}")
            raise ex
    print(f"{chat_id} için maksimum deneme sayısına ulaşıldı. İşlem başarısız.")
    raise Exception(f"{chat_id} için işlem başarısız oldu.")

asyncio.run(send_telegram_message("Selam 🚀"))
