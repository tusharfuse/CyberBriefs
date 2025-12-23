from fastapi import FastAPI
import feedparser
import sqlite3
import time
import threading
import sys
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import os
import base64
import requests
from openai import OpenAI
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Lock
from contextlib import asynccontextmanager

# =========================================================
# ENV + OPENAI
# =========================================================
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# =========================================================
# GLOBAL CONFIG
# =========================================================
DB_NAME = "news.db"
IMAGE_BASE_DIR = "images"
SCHEDULER_INTERVAL_SECONDS = 15 * 60

os.makedirs(f"{IMAGE_BASE_DIR}/ai", exist_ok=True)
os.makedirs(f"{IMAGE_BASE_DIR}/cyber", exist_ok=True)

pipeline_lock = Lock()
scheduler = BackgroundScheduler()

next_run_time = None
countdown_stop_event = threading.Event()

# =========================================================
# COUNTDOWN TIMER
# =========================================================
def scheduler_countdown():
    global next_run_time
    while not countdown_stop_event.is_set():
        if next_run_time:
            remaining = int((next_run_time - datetime.now()).total_seconds())
            remaining = max(0, remaining)
            m, s = divmod(remaining, 60)
            sys.stdout.write(f"\r⏳ Next pipeline run in {m:02d}:{s:02d} ")
            sys.stdout.flush()
        time.sleep(1)

# =========================================================
# FASTAPI LIFESPAN
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global next_run_time

    print("\n🚀 STARTUP: checking database state")

    if is_db_empty():
        print("📦 Database empty → running bootstrap pipeline")
        bootstrap_pipeline()
    else:
        print("📦 Database already populated → skipping bootstrap")

    next_run_time = datetime.now() + timedelta(seconds=SCHEDULER_INTERVAL_SECONDS)

    threading.Thread(
        target=scheduler_countdown,
        daemon=True
    ).start()

    scheduler.add_job(
        incremental_pipeline,
        "interval",
        seconds=SCHEDULER_INTERVAL_SECONDS,
        max_instances=1,
        coalesce=True
    )
    scheduler.start()

    print("\n⏱️ Scheduler started — runs every 15 minutes\n")

    yield

    countdown_stop_event.set()
    scheduler.shutdown(wait=False)

# =========================================================
# APP INIT
# =========================================================
app = FastAPI(
    title="AI & Cyber News Collector",
    version="2.4.0",
    lifespan=lifespan
)

# =========================================================
# TAGS
# =========================================================
AI_TAGS = {
    "ai", "artificial intelligence", "machine learning", "deep learning",
    "neural network", "llm", "large language model", "gpt", "transformer",
    "computer vision", "nlp", "natural language processing", "generative ai",
    "chatbot", "autonomous systems",
    "ai model", "ai training", "ai inference",
    "ai ethics", "ai safety", "ai alignment",
    "ai regulation", "ai policy",
    "ai chip", "ai hardware", "nvidia", "amd ai",
    "openai", "google deepmind", "anthropic",
    "meta ai", "microsoft ai",
    "ai startup", "ai research",
    "foundation model", "multimodal ai",
    "ai automation", "ai agent", "ai system",
    "reinforcement learning", "self supervised learning",
    "ai benchmark", "ai evaluation", "ai deployment",
    "ai infrastructure", "edge ai", "federated learning",
    "ai cloud", "ai ops", "ml ops",
    "synthetic data", "ai risk", "ai bias",
    "ai governance", "ai compliance",
    "ai security", "ai red teaming", "ai audit",
    "ai explainability", "ai trust",
    "ai decision", "ai optimization", "ai accelerator",
    "ai stack", "ai pipeline", "ai framework",
    "pytorch", "tensorflow"
}
CYBER_TAGS = {
    "cybersecurity", "cyber attack", "hacking", "hacker",
    "malware", "ransomware", "phishing", "ddos",
    "zero day", "zero-day",
    "vulnerability", "exploit", "cve",
    "data breach", "infosec", "information security",
    "threat actor", "apt", "advanced persistent threat",
    "botnet", "spyware", "trojan", "worm",
    "rootkit", "backdoor",
    "security patch", "patching",
    "incident response", "forensics", "digital forensics",
    "soc", "siem", "soar", "ids", "ips",
    "endpoint security", "network security", "cloud security",
    "identity attack", "credential theft",
    "password attack", "brute force",
    "supply chain attack",
    "security advisory", "threat intelligence", "threat hunting",
    "penetration testing", "pentest",
    "red team", "blue team",
    "security breach", "data leak",
    "privacy breach", "regulatory fine",
    "cybercrime", "cyber espionage", "nation state attack",
    "attack surface", "vulnerability disclosure",
    "security flaw", "security bug", "security misconfiguration",
    "credential stuffing",
    "email security", "web security", "application security",
    "iot security", "mobile security", "ics security", "ot security"
}


# =========================================================
# RSS FEEDS
# =========================================================
RSS_FEEDS = {
    "CSO Online": "https://www.csoonline.com/feed",
    "The Hacker News": "https://feeds.feedburner.com/TheHackersNews?format=xml",
    "Help Net Security": "https://www.helpnetsecurity.com/feed/",
    "The Register Security": "https://www.theregister.co.uk/security/headlines.atom",
    "The Guardian Technology": "https://www.theguardian.com/technology/rss",
    "BleepingComputer": "https://www.bleepingcomputer.com/feed/",
    "ZDNET": "https://www.zdnet.com/news/rss.xml",
    "Security Affairs": "https://securityaffairs.co/feed/",
    "IT Security Guru": "https://www.itsecurityguru.org/news/feed",
    "TechRepublic": "https://www.techrepublic.com/rssfeeds/",
    "Cybercrime Magazine": "https://cybersecurityventures.com/feed/",
    "Kaspersky Blog": "https://www.kaspersky.com/blog/feed/",
    "NCSC UK": "https://www.ncsc.gov.uk/feed",
    "Comparitech": "https://www.comparitech.com/blog/feed/",
    "Qualys Blog": "https://www.qualys.com/community/blog/feed/",
    "SentinelOne": "https://www.sentinelone.com/feed/",
    "CyberScoop": "https://www.cyberscoop.com/feed/",
    "Graham Cluley": "https://grahamcluley.com/feed/",
    "SecurityWeek": "https://www.securityweek.com/feed/",
    "Infosecurity Magazine": "https://www.infosecurity-magazine.com/rss/news/",
    "SecPod": "https://www.secpod.com/blog/feed/",
    "Cyber Safe": "https://cybersafe.news/feed/",
    "Krebs on Security": "https://krebsonsecurity.com/feed/",
    "Dark Reading": "https://www.darkreading.com/rss/all.xml",
    "Panda Security": "https://www.pandasecurity.com/en/mediacenter/feed/",
    "Troy Hunt": "http://feeds.feedburner.com/troyhunt",
    "Dr Vikram Sethi": "https://www.drvikramsethi.com/blogs?format=rss",
    "Pwn2Own / ZDI": "https://www.zerodayinitiative.com/blog?format=rss",
    "Darktrace": "https://www.darktrace.com/en/blog/rss/",
    "Jisc": "https://www.jisc.ac.uk/blog/feed",
    "IT Governance UK": "https://www.itgovernance.co.uk/blog/feed"
}

# =========================================================
# DATABASE
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    for table in ("ai_news", "cyber_news"):
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT,
            headline TEXT,
            body TEXT,
            summary TEXT,
            image_path TEXT,
            published_time TEXT,
            article_link TEXT UNIQUE,
            fetched_at TEXT
        )
        """)
    conn.commit()
    conn.close()

def is_db_empty():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ai_news")
    ai_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM cyber_news")
    cyber_count = cur.fetchone()[0]
    conn.close()
    return (ai_count + cyber_count) == 0

init_db()

# =========================================================
# HELPERS
# =========================================================
def clean_html(raw):
    return BeautifulSoup(raw or "", "lxml").get_text(" ", strip=True)

def extract_datetime(entry):
    if hasattr(entry, "published_parsed"):
        return datetime(*entry.published_parsed[:6])
    return None

def classify(text: str) -> str:
    text = text.lower()
    if any(t in text for t in AI_TAGS):
        return "ai"
    return "cyber"

# =========================================================
# PIPELINE STEPS
# =========================================================
def ingest_rss():
    print("\n📥 RSS INGESTION")
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0

    for channel, url in RSS_FEEDS.items():
        feed = feedparser.parse(url)
        print(f"[RSS] {channel}: {len(feed.entries)} entries")

        for e in feed.entries:
            dt = extract_datetime(e)
            if not dt:
                continue

            title = e.get("title", "")
            body = clean_html(e.get("summary", ""))
            category = classify(f"{title} {body}")
            table = "ai_news" if category == "ai" else "cyber_news"

            try:
                cur.execute(f"""
                    INSERT INTO {table}
                    (channel, headline, body, published_time, article_link, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    channel,
                    title,
                    body,
                    dt.isoformat(),
                    e.get("link", ""),
                    datetime.now(timezone.utc).isoformat()
                ))
                conn.commit()
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    conn.close()
    print(f"[RSS] New articles inserted: {inserted}")

# def generate_image_for_article(article_id, summary, category):
#     """Generate a single image for an article based on its summary using OpenAI DALL-E."""
#     try:
#         prompt = f"Create an illustration representing: {summary[:500]}"

#         response = client.images.generate(
#             model="dall-e-3",
#             prompt=prompt,
#             size="1024x1024",
#             quality="standard",
#             n=1,
#         )

#         image_url = response.data[0].url

#         # Download the image
#         image_response = requests.get(image_url)
#         image_response.raise_for_status()

#         path = f"{IMAGE_BASE_DIR}/{category}/{article_id}.png"

#         with open(path, "wb") as f:
#             f.write(image_response.content)

#         # Update DB with image path
#         conn = get_conn()
#         cur = conn.cursor()
#         table = "ai_news" if category == "ai" else "cyber_news"
#         cur.execute(
#             f"UPDATE {table} SET image_path=? WHERE id=?",
#             (path, article_id)
#         )
#         conn.commit()
#         conn.close()

#         print(f"🎨 Image generated for {category} article {article_id}")

#     except Exception as e:
#         print(f"[ERROR] Image generation failed for {category} article {article_id}: {e}")

def generate_summaries(category):
    table = "ai_news" if category == "ai" else "cyber_news"
    label = category.upper()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(f"SELECT id, body FROM {table} WHERE summary IS NULL")
    rows = cur.fetchall()
    total = len(rows)

    if total == 0:
        print(f"🧠 {label} summaries: nothing pending")
        conn.close()
        return

    print(f"\n🧠 {label} summaries: {total} pending")

    completed = 0

    for article_id, body in rows:
        try:
            prompt = f"""
Summarize the following news article into EXACTLY ONE paragraph in ENGLISH.

STRICT RULES (MANDATORY):
- Output must be ONE paragraph only.
- Maximum 2-3 sentences.
- Do NOT use bullet points, headings, lists, or markdown.
- Do NOT add introductions, conclusions, recommendations, or opinions.
- Neutral, factual, news-reporting tone.
- English language only.

Article:
{body[:6000]}
"""

            response = client.chat.completions.create(
                model="gpt-4.1",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )

            summary = response.choices[0].message.content or "Summary not available."
            summary = summary.strip()

            # Hard safety: enforce single paragraph + sentence cap
            summary = " ".join(summary.splitlines())
            sentences = summary.split(". ")
            summary = ". ".join(sentences[:2]).strip()
            if not summary.endswith("."):
                summary += "."

            cur.execute(
                f"UPDATE {table} SET summary=? WHERE id=?",
                (summary, article_id)
            )
            conn.commit()

            # Generate image immediately after summary
            #generate_image_for_article(article_id, summary, category)

            completed += 1
            remaining = total - completed
            print(f"🧠 {label} summary done: {completed}/{total} (remaining {remaining})")

        except Exception as e:
            conn.rollback()
            print(f"[ERROR] {label} summary failed for ID {article_id}: {e}")

    print(f"🧠 {label} summaries complete\n")
    conn.close()

import random

def get_fallback_image(category: str) -> str:
    folder = f"{IMAGE_BASE_DIR}/{category}"
    fallbacks = [
        f for f in os.listdir(folder)
        if f.startswith("fallback_")
    ]
    if not fallbacks:
        return None
    return os.path.join(folder, random.choice(fallbacks))



# =========================================================
# UPDATED IMAGE GENERATION (USING OPENAI DALL-E)
# =========================================================
def generate_images(category):
    table = "ai_news" if category == "ai" else "cyber_news"
    label = category.upper()

    # Fetch pending rows using a short-lived connection
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT id, summary FROM {table}
        WHERE summary IS NOT NULL AND image_path IS NULL
    """)
    rows = cur.fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        print(f"🎨 {label} images: nothing pending")
        return

    print(f"\n🎨 {label} images: {total} pending")

    completed = 0

    for article_id, summary in rows:
        conn = None
        try:
            conn = get_conn()
            cur = conn.cursor()

            # 🚫 DO NOT include summary or news text
            prompt=f'''
Create a cinematic, high-quality digital illustration inspired by the following news summary.
Interpret it visually and symbolically.

Rules:
- Image only
- No text, no letters, no numbers, no words

Summary (for inspiration only):
{summary}
'''

            response = client.images.generate(
                model="dall-e-3",
                prompt=prompt,
                size="1024x1024",
                quality="standard",
                n=1,
            )

            image_url = response.data[0].url
            image_response = requests.get(image_url, timeout=60)
            image_response.raise_for_status()

            path = f"{IMAGE_BASE_DIR}/{category}/{article_id}.png"
            with open(path, "wb") as f:
                f.write(image_response.content)

            cur.execute(
                f"UPDATE {table} SET image_path=? WHERE id=?",
                (path, article_id)
            )
            conn.commit()

            completed += 1
            print(f"🎨 {label} image done: {completed}/{total}")

        except Exception as e:
            print(f"[ERROR] {label} image failed for ID {article_id}: {e}")

            # Assign fallback safely
            try:
                if conn:
                    conn.close()

                conn = get_conn()
                cur = conn.cursor()

                fallback = get_fallback_image(category)
                if fallback:
                    cur.execute(
                        f"UPDATE {table} SET image_path=? WHERE id=?",
                        (fallback, article_id)
                    )
                    conn.commit()
                    print(f"🟡 {label} fallback image assigned for ID {article_id}")

            except Exception as db_err:
                print(f"[DB ERROR] Failed assigning fallback for ID {article_id}: {db_err}")

        finally:
            if conn:
                conn.close()

    print(f"🎨 {label} images complete\n")

# =========================================================
# PIPELINES
# =========================================================
def bootstrap_pipeline():
    print("\n🧱 BOOTSTRAP PIPELINE START")
    ingest_rss()
    generate_summaries("ai")
    generate_summaries("cyber")
    generate_images("ai")
    generate_images("cyber")
    print("🧱 BOOTSTRAP PIPELINE COMPLETE\n")

def incremental_pipeline():
    if not pipeline_lock.acquire(blocking=False):
        print("[PIPELINE] Already running, skipping")
        return

    try:
        print("\n🔁 INCREMENTAL PIPELINE START")
        ingest_rss()
        generate_summaries("ai")
        generate_summaries("cyber")
        generate_images("ai")
        generate_images("cyber")
        print("🔁 INCREMENTAL PIPELINE END\n")
        global next_run_time
        next_run_time = datetime.now() + timedelta(seconds=SCHEDULER_INTERVAL_SECONDS)
    finally:
        pipeline_lock.release()

# =========================================================
# API
# =========================================================
@app.get("/news/cyber")
def get_all_news():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 'cyber' AS type, * FROM cyber_news
        ORDER BY published_time DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]

# =========================================================
# MANUAL IMAGE GENERATION ENDPOINTS
# =========================================================

@app.post("/images/generate/ai", tags=["Images"])
def generate_ai_images():
    """
    Generate images for AI news where summary exists but image is missing
    """
    generate_images("ai")
    return {
        "status": "success",
        "message": "AI image generation triggered"
    }


@app.post("/images/generate/cyber", tags=["Images"])
def generate_cyber_images():
    """
    Generate images for Cyber news where summary exists but image is missing
    """
    generate_images("cyber")
    return {
        "status": "success",
        "message": "Cyber image generation triggered"
    }
