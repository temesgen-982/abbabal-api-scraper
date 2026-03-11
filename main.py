import asyncio
import sys
from fetch_updates import fetch_updates

async def main():
    print("--- Starting Cron Job ---")
    try:
        count = await fetch_updates()
        print(f"Cron execution finished. {count} new proverbs processed.")
    except Exception as e:
        print(f"Error during cron execution: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
