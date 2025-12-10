"""
Bangalore Reddit Leads Bot (Pushshift + RSS) -> Telegram

No Reddit API required.

Environment variables required:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- POLL_INTERVAL_MINUTES  (optional, default=3)

Deploy: python bangalore_reddit_leads_bot.py
"""
import os
import re
import time
import csv
import logging
from datetime import datetime, timezone
import requests
import feedparser
import pandas as pd
from telegram import Bot
from dotenv import load_dotenv

# Load env
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_MINUTES", "3")) * 60

# Subreddits to monitor (you asked "all both" -> these are relevant Bangalore + India housing subs)
SUBREDDITS = [
    "bangalore",
    "Bengaluru",
    "blr_rentals",
    "India_RealEstate",
    "IndiaHousing",
    "RealEstateIndia",
    "rentalindia",
    "India",
]

# Keywords and simple patterns
KEYWORDS = [
    r"looking for", r"looking to rent", r"looking to buy", r"need a", r"need an",
    r"flat for rent", r"flat for sale", r"house for rent", r"house for sale", r"wanted", r"seeking", r"available for rent"
]

# Budget detection patterns
BUDGET_PATTERNS = [
    r"\b₹\s?[0-9,\.kK]+\b",
    r"\bINR\s?[0-9,\.kK]+\b",
    r"[0-9]{1,3}(?:,?[0-9]{3})+\s*(?:INR|Rs|₹)",
    r"\b[0-9]+\s?(?:k|K)\b",
    r"\b[0-9]+\.?[0-9]*\s?(?:lac|lakh|Lakh|lakhs|Lakhs)\b",
    r"\b[0-9]+\.?[0-9]*\s?(?:crore|Cr|cr)\b",
]

BHK_PATTERN = re.compile(r"(\bstudio\b|\b[1-4]\s?BHK\b|\b[1-4]\s?B/R\b)", re.I)

LOCALITIES = [
    'whitefield','koramangala','hsr','hsr layout','indiranagar','marathahalli',
    'rt nagar','yelahanka','jayanagar','hebbal','malleshwaram','banashankari',
    'electronic city','sarjapur','bellandur','rajajinagar','ulsoor','frazer town',
    'white field','sarjapur road','whitefield','white-field'
]

OUT_CSV = "bangalore_reddit_leads.csv"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

# Telegram
bot = None
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
    except Exception as e:
        logging.error("Telegram init failed: %s", e)
        bot = None
else:
    logging.warning("Telegram token or chat id not set. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")

# Ensure CSV exists
if not os.path.exists(OUT_CSV):
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "timestamp", "title", "text", "budget", "bhk", "locality", "type", "link"])

def clean(s):
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
    found = []
    for loc in LOCALITIES:
        if loc in t:
            found.append(loc.title())
    return ", ".join(sorted(set(found)))

def classify_type(text):
    t = text.lower()
    is_rent = any(k in t for k in ["rent", "looking to rent", "flat for rent", "house for rent"])
    is_sale = any(k in t for k in ["sale", "buy", "flat for sale", "house for sale", "looking to buy"])
    if is_rent and is_sale:
        return "Both"
    if is_sale:
        return "Sale"
    if is_rent:
        return "Rent"
    return "Unknown"

def already_seen(pid):
    try:
        df = pd.read_csv(OUT_CSV)
        return str(pid) in df['id'].astype(str).values
    except Exception:
        return False

def save_lead(lead):
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            lead['id'], lead['timestamp'], lead['title'], lead['text'][:400],
            lead['budget'], lead['bhk'], lead['locality'], lead['type'], lead['link']
        ])

def send_telegram(lead):
    if not bot:
        logging.info("Telegram not configured; skipping send.")
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
    except Exception as e:
        logging.error("Failed sending telegram: %s", e)

# Pull from Pushshift (fast) - returns list of recent posts for subreddits
def fetch_pushshift(subreddits, since_ts):
    # Pushshift endpoint
    url = "https://api.pushshift.io/reddit/search/submission/"
    params = {
        "subreddit": ",".join(subreddits),
        "size": 100,
        "after": int(since_ts),
        "sort": "asc"
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get("data", [])
    except Exception as e:
        logging.debug("Pushshift error: %s", e)
        return []

# Fallback: get subreddit RSS new posts
def fetch_rss(subreddit):
    try:
        url = f"https://www.reddit.com/r/{subreddit}/new/.rss"
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries:
            title = entry.title
            link = entry.link
            summary = getattr(entry, "summary", "")
            # create a pseudo id using link
            pid = re.sub(r"\W+", "", link)[-12:]
            items.append({
                "id": pid,
                "title": title,
                "selftext": summary,
                "created_utc": int(time.time()),
                "permalink": link.replace("https://www.reddit.com", "")
            })
        return items
    except Exception as e:
        logging.debug("RSS error: %s", e)
        return []

def process_item(item):
    title = clean(item.get("title") or "")
    selftext = clean(item.get("selftext") or "")
    full = f"{title} {selftext}"
    # quick filter: must contain a keyword or housing words
    if not any(re.search(k, full, re.I) for k in KEYWORDS) and not re.search(r"\b(flat|apartment|house|rent|sale|bhk|studio)\b", full, re.I):
        return None
    pid = item.get("id") or item.get("url") or item.get("permalink") or title[:20]
    link = item.get("full_link") or ("https://reddit.com" + item.get("permalink", "")) if item.get("permalink") else item.get("link", "")
    budget = extract_budget(full)
    bhk = extract_bhk(full)
    locality = extract_locality(full)
    typ = classify_type(full)
    lead = {
        "id": pid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": selftext,
        "budget": budget,
        "bhk": bhk,
        "locality": locality,
        "type": typ,
        "link": link
    }
    return lead

def poll_loop():
    logging.info("Starting Poll Loop (Pushshift + RSS). Subreddits: %s", ", ".join(SUBREDDITS))
    # Use last processed unix timestamp; start from now-5 min to not miss new posts
    last_ts = int(time.time()) - 300
    while True:
        try:
            # 1) Pushshift
            posts = fetch_pushshift(SUBREDDITS, last_ts)
            for p in posts:
                # pushshift returns created_utc
                created = p.get("created_utc", int(time.time()))
                if created > last_ts:
                    last_ts = created
                lead = process_item({
                    "id": p.get("id"),
                    "title": p.get("title"),
                    "selftext": p.get("selftext"),
                    "permalink": p.get("permalink"),
                    "link": "https://reddit.com" + p.get("permalink") if p.get("permalink") else ""
                })
                if not lead:
                    continue
                if already_seen(lead['id']):
                    continue
                save_lead(lead)
                send_telegram(lead)
                logging.info("New lead saved & sent: %s | %s", lead['id'], lead['title'])

            # 2) RSS fallback (scan each subreddit quickly)
            for sub in SUBREDDITS:
                rss_items = fetch_rss(sub)
                for ri in rss_items:
                    lead = process_item(ri)
                    if not lead:
                        continue
                    if already_seen(lead['id']):
                        continue
                    save_lead(lead)
                    send_telegram(lead)
                    logging.info("New lead (RSS) saved & sent: %s | %s", lead['id'], lead['title'])

        except Exception as e:
            logging.exception("Polling loop error: %s", e)

        # sleep
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    poll_loop()
