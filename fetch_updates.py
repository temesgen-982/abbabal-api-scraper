import os
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient
from database import save_proverbs, get_last_message_id
from parser import clean_message

load_dotenv()

API_ID = os.getenv("TG_API_ID")
API_HASH = os.getenv("TG_API_HASH")
CHANNEL_USERNAME = 'ababaloch'

async def fetch_updates():
    client = TelegramClient('ababaloch', int(API_ID), API_HASH)
    await client.start()
    
    last_id = get_last_message_id()
    print(f"Fetching updates since message ID: {last_id}")
    
    channel = await client.get_entity(CHANNEL_USERNAME)
    
    proverbs = []
    count = 0
    
    # min_id ensures we only get messages newer than last_id
    async for message in client.iter_messages(channel, min_id=last_id, reverse=True):
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

    if proverbs:
        save_proverbs(proverbs)
        print(f"Saved {count} new proverbs.")
    else:
        print("No new proverbs found.")
        
    return count

if __name__ == '__main__':
    asyncio.run(fetch_updates())
