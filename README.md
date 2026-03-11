# Abbabal API Scraper

A modular Telegram scraper designed to fetch and maintain a dataset of proverbs from the `@ababaloch` channel.

## 📁 Project Structure

- `main.py`: The entrypoint for automated updates (cron job).
- `fetch_history.py`: Script to retrieve the entire message history from the channel.
- `fetch_updates.py`: Incremental scraper that only fetches messages newer than the last saved ID.
- `database.py`: Handles reading/writing to the SQLite database.
- `parser.py`: Cleans and formats raw message text.
- `data/proverbs.db`: The generated SQLite database.

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.10+
- A Telegram account to get API credentials.

### 2. Setup

1. Clone the repository.
2. Initialize and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install telethon python-dotenv
   ```
3. Configure environment variables:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and add your `TG_API_ID` and `TG_API_HASH` from [my.telegram.org](https://my.telegram.org/apps).

### 3. Usage

#### Initial Fetch

To build the initial dataset from the channel history:

```bash
python3 fetch_history.py
```

#### Daily Updates

To fetch new messages (intended for cron jobs):

```bash
python3 main.py
```

## 🛠️ Automated Updates (Cron)

To run the scraper every day at midnight, add the following to your `crontab -e`:

```cron
0 0 * * * /path/to/abbabal-api/venv/bin/python3 /path/to/abbabal-api/main.py >> /path/to/abbabal-api/cron.log 2>&1
```
