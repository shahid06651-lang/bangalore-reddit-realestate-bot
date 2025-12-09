"""
Bangalore Reddit Leads Bot -> Telegram

What this script does:
- Monitors public Reddit posts for Bangalore rental/sale requirements.
- Extracts info: BHK, Budget, Location, Rent/Sale.
- Sends new leads to your Telegram bot.
- Saves all leads in a CSV file to avoid duplicates.

IMPORTANT:
You must enter your Reddit & Telegram keys in Render environment settings.
Do NOT put real secrets in GitHub.

Requirements:
praw
python-telegram-bot==13.17
pandas
python-dotenv
regex
"""

import os
import re
import time
import csv
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import praw
import pandas as pd
from telegram import Bot

# Load configuration
load_dotenv()
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'BangaloreRealEstateBot/0.1')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL_MINUTES', '3')) * 60

# Subreddits to monitor
SUBREDDITS = [
    'bangalore',
    'Bengaluru',
    'India_RealEstate',
    'realestate',
]

# Keywords meaning user is looking for a flat
KEYWORDS = [
    r'looking for', r'need a', r'require', r'wanted', r'flat for rent',
    r'house for rent', r'flat for sale', r'house for sale', r'seeking',
]

# Patterns to extract budget
BUDGET_PATTERNS = [
    r"\b₹\s?([0-9,\.kK]+)\b",
    r"\bINR\s?([0-9,\.kK]+)\b",
    r"([0-9]{1,3}(?:,?[0-9]{3})+)\s*(?:INR|Rs|₹)",
    r"([0-9]+)\s*(?:k|K)\b",
    r"([0-9]+)\s*(?:lac|lakh|Lakh|lakhs|Lakhs)\b",
    r"([0-9]+\.?[0-9]+)\s*(?:crore|Cr|cr)\b",
]

# BHK detection
BHK_PATTERN = re.compile(r"(\b[1-4]\s?BHK\b|\bstudio\b)", re.I)

# Bangalore locality list
LOCALITIES = [
    'whitefield', 'koramangala', 'hsr', 'hsr layout', 'indiranagar',
    'marathahalli', 'rt nagar', 'yelahanka', 'jayanagar', 'hebbal',
    'malleshwaram', 'banashankari', 'electronic city', 'sarjapur',
    'bellandur', 'rajajinagar', 'ulsoor', 'frazer town',
]

# Output file
OUT_CSV = 'bangalore_reddit_leads.csv'

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Telegram Bot
bot = None
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
    except Exception as e:
        logging.error("Telegram Bot init failed: %s", e)

# Reddit Client
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT,
)

# Create CSV if missing
if not os.path.exists(OUT_CSV):
    with open(OUT_CSV, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'timestamp', 'title', 'text', 'budget', 'bhk', 'locality', 'type', 'link'])

def clean_text(s):
    return re.sub(r"\s+", " ", s).strip() if s else ""

def extract_budget(text):
    for p in BUDGET_PATTERNS:
        m = re.search(p, text, re.I)
        if m:
            return m.group(0)
    return ""

def extract_bhk(text):
    m = BHK_PATTERN.search(text)
    return m.group(0) if m else ""

def extract_locality(text):
    t = text.lower()
    results = [loc.title() for loc in LOCALITIES if loc in t]
    return ", ".join(results)

def classify_type(text):
    t = text.lower()
    if "sale" in t or "buy" in t:
        return "Sale"
    if "rent" in t:
        return "Rent"
    return "Unknown"

def already_seen(pid):
    df = pd.read_csv(OUT_CSV)
    return pid in df['id'].astype(str).values

def save_lead(lead):
    with open(OUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            lead['id'], lead['timestamp'], lead['title'], lead['text'][:400],
            lead['budget'], lead['bhk'], lead['locality'], lead['type'], lead['link']
        ])

def send_telegram(lead):
    if not bot:
        return
    msg = (
        f"🏠 *New Bangalore Lead*\n"
        f"*Type:* {lead['type']}\n"
        f"*BHK:* {lead['bhk'] or 'N/A'}\n"
        f"*Budget:* {lead['budget'] or 'N/A'}\n"
        f"*Locality:* {lead['locality'] or 'N/A'}\n"
        f"*Title:* {lead['title']}\n"
        f"*Link:* {lead['link']}\n"
        f"*Time:* {lead['timestamp']}\n"
    )
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg, parse_mode='Markdown')
    except:
        pass

def poll():
    logging.info("🔍 Monitoring Reddit...")
    subs = reddit.subreddit("+".join(SUBREDDITS))

    for post in subs.stream.submissions(skip_existing=True):
        try:
            title = clean_text(post.title)
            body = clean_text(post.selftext)
            full = f"{title} {body}".lower()

            # Relevance filter
            if not any(k in full for k in KEYWORDS):
                continue

            if already_seen(post.id):
                continue

            lead = {
                "id": post.id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": body,
                "budget": extract_budget(full),
                "bhk": extract_bhk(full),
                "locality": extract_locality(full),
                "type": classify_type(full),
                "link": f"https://reddit.com{post.permalink}",
            }

            save_lead(lead)
            send_telegram(lead)
            logging.info(f"🔥 NEW LEAD: {title}")

        except Exception as e:
            logging.error("Error: %s", e)
            time.sleep(2)

if __name__ == "__main__":
    poll()
