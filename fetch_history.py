import os
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
from database import save_proverbs
from parser import clean_message

load_dotenv()

API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
CHANNEL_USERNAME = 'ababaloch'

async def main():
    client = TelegramClient('ababaloch', int(API_ID), API_HASH)
    await client.start()
    
    print(f"Fetching full history from {CHANNEL_USERNAME}...")
    channel = await client.get_entity(CHANNEL_USERNAME)
    
    proverbs = []
    count = 0
    
    # reverse=True -> oldest first
    async for message in client.iter_messages(channel, reverse=True):
        if not message.message:
            continue
            
        entry = {
            "id": message.id,
            "date": message.date.isoformat(),
            "text": clean_message(message.message),
            "views": message.views,
            "forwards": message.forwards
        }
        proverbs.append(entry)
        count += 1
        
        if len(proverbs) >= 100:
            save_proverbs(proverbs)
            proverbs = []
            print(f"Saved {count} proverbs...")

    # Save remaining
    if proverbs:
        save_proverbs(proverbs)
    
    print(f"Done! Successfully saved {count} messages to dataset.")

if __name__ == '__main__':
    asyncio.run(main())
