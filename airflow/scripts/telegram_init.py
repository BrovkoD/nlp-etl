from telethon.sync import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

with TelegramClient('airflow_session', TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
    print(">> Success")
