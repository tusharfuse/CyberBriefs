from fastapi import FastAPI, Form, UploadFile, File, Request
import feedparser
import sqlite3
import time
import threading
import sys
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO)
from bs4 import BeautifulSoup
import os
import shutil
import base64
import requests
from openai import OpenAI
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Lock
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse



MAX_UPLOAD_SIZE = 5 * 1024 * 1024  # 5 MB


class LimitUploadSizeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        content_length = request.headers.get("content-length")

        if content_length and int(content_length) > MAX_UPLOAD_SIZE:
            return JSONResponse(
                status_code=413,
                content={"detail": "File too large. Max size allowed is 5 MB"},
            )

        return await call_next(request)


# =========================================================
# ENV + OPENAI
# =========================================================
load_dotenv()
PUBLIC_BASE_URL = os.environ["PUBLIC_BASE_URL"] 
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# =========================================================
# GLOBAL CONFIG
# =========================================================
DB_NAME = "news.db"
IMAGE_BASE_DIR = "images"
SCHEDULER_INTERVAL_SECONDS = 15 * 60

os.makedirs(f"{IMAGE_BASE_DIR}/ai", exist_ok=True)
os.makedirs(f"{IMAGE_BASE_DIR}/cyber", exist_ok=True)

os.makedirs(f"{IMAGE_BASE_DIR}/vikram", exist_ok=True)
os.makedirs(f"{IMAGE_BASE_DIR}/y2ai", exist_ok=True)

pipeline_lock = Lock()
scheduler = BackgroundScheduler()

next_run_time = None
countdown_stop_event = threading.Event()

# =========================================================
# SCHEDULER TIMERS
# =========================================================
PIPELINE_INTERVAL_SECONDS = 15 * 60
HEADLINE_INTERVAL_SECONDS = 2 * 60 * 60  # 2 hours

next_pipeline_run = None
next_headline_run = None

# =========================================================
# AUTH CONFIG
# =========================================================
SECRET_KEY = os.getenv("JWT_SECRET", "CHANGE_ME_SUPER_SECRET")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def get_current_admin(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Dependency to validate JWT token and ensure admin exists
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str | None = payload.get("sub")
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, email FROM admin_users WHERE email = ?", (email,))
    admin = cur.fetchone()
    conn.close()

    if not admin:
        raise credentials_exception

    return dict(admin)


class SocialLinksBase(BaseModel):
    youtube: str | None = None
    facebook: str | None = None
    linkedin: str | None = None


class SocialLinksCreate(BaseModel):
    youtube: str
    facebook: str
    linkedin: str


class SocialLinksUpdate(SocialLinksBase):
    pass


# =========================================================
# COUNTDOWN TIMER
# =========================================================
def scheduler_countdown():
    global next_pipeline_run, next_headline_run

    while not countdown_stop_event.is_set():
        now = datetime.now()

        lines = []

        if next_pipeline_run:
            p_remaining = max(0, int((next_pipeline_run - now).total_seconds()))
            pm, ps = divmod(p_remaining, 60)
            lines.append(f"📰 Pipeline: {pm:02d}:{ps:02d}")

        if next_headline_run:
            h_remaining = max(0, int((next_headline_run - now).total_seconds()))
            hm, hs = divmod(h_remaining, 60)
            hh, hm = divmod(hm, 60)
            lines.append(f"🧠 Headline: {hh:02d}:{hm:02d}:{hs:02d}")

        sys.stdout.write("\r⏳ " + " | ".join(lines) + "   ")
        sys.stdout.flush()

        time.sleep(1)


# =========================================================
# FASTAPI LIFESPAN
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global next_pipeline_run, next_headline_run

    print("\n🚀 STARTUP: checking database state")

    # 1️⃣ Bootstrap DB + pipeline
    if is_db_empty():
        print("📦 Database empty → running bootstrap pipeline")
        bootstrap_pipeline()
    else:
        print("📦 Database already populated → skipping bootstrap")

    # 2️⃣ AI headline bootstrap
    print("\n🧠 Checking AI headline status")

    if not ai_headline_exists():
        print("🧠 No AI headline found → generating first headline")
        generate_ai_headline()
    else:
        print("🧠 AI headline already exists → skipping initial generation")

    # ✅ 3️⃣ INITIALIZE BOTH COUNTDOWN TIMERS (THIS IS STEP 3)
    next_pipeline_run = datetime.now() + timedelta(seconds=PIPELINE_INTERVAL_SECONDS)
    next_headline_run = datetime.now() + timedelta(seconds=HEADLINE_INTERVAL_SECONDS)

    # 4️⃣ Start unified countdown thread
    threading.Thread(target=scheduler_countdown, daemon=True).start()

    scheduler.add_job(
        incremental_pipeline,
        "interval",
        seconds=PIPELINE_INTERVAL_SECONDS,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        generate_ai_headline,
        "interval",
        seconds=HEADLINE_INTERVAL_SECONDS,
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()

    print("\n⏱️ Scheduler started — pipelines + AI headline active\n")

    yield

    # 6️⃣ Shutdown cleanup
    countdown_stop_event.set()
    scheduler.shutdown(wait=False)


# =========================================================
# APP INIT
# =========================================================
app = FastAPI(title="AI & Cyber News Collector", version="2.4.0", lifespan=lifespan)
# app.add_middleware(LimitUploadSizeMiddleware)



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # GET, POST, PUT, DELETE, etc.
    allow_headers=["*"],  # Authorization, Content-Type, etc.
)


app.mount("/images", StaticFiles(directory="images"), name="images")


# =========================================================
# TAGS
# =========================================================
AI_TAGS = {
    "ai",
    "artificial intelligence",
    "machine learning",
    "deep learning",
    "neural network",
    "llm",
    "large language model",
    "gpt",
    "transformer",
    "computer vision",
    "nlp",
    "natural language processing",
    "generative ai",
    "chatbot",
    "autonomous systems",
    "ai model",
    "ai training",
    "ai inference",
    "ai ethics",
    "ai safety",
    "ai alignment",
    "ai regulation",
    "ai policy",
    "ai chip",
    "ai hardware",
    "nvidia",
    "amd ai",
    "openai",
    "google deepmind",
    "anthropic",
    "meta ai",
    "microsoft ai",
    "ai startup",
    "ai research",
    "foundation model",
    "multimodal ai",
    "ai automation",
    "ai agent",
    "ai system",
    "reinforcement learning",
    "self supervised learning",
    "ai benchmark",
    "ai evaluation",
    "ai deployment",
    "ai infrastructure",
    "edge ai",
    "federated learning",
    "ai cloud",
    "ai ops",
    "ml ops",
    "synthetic data",
    "ai risk",
    "ai bias",
    "ai governance",
    "ai compliance",
    "ai security",
    "ai red teaming",
    "ai audit",
    "ai explainability",
    "ai trust",
    "ai decision",
    "ai optimization",
    "ai accelerator",
    "ai stack",
    "ai pipeline",
    "ai framework",
    "pytorch",
    "tensorflow",
}
CYBER_TAGS = {
    "cybersecurity",
    "cyber attack",
    "hacking",
    "hacker",
    "malware",
    "ransomware",
    "phishing",
    "ddos",
    "zero day",
    "zero-day",
    "vulnerability",
    "exploit",
    "cve",
    "data breach",
    "infosec",
    "information security",
    "threat actor",
    "apt",
    "advanced persistent threat",
    "botnet",
    "spyware",
    "trojan",
    "worm",
    "rootkit",
    "backdoor",
    "security patch",
    "patching",
    "incident response",
    "forensics",
    "digital forensics",
    "soc",
    "siem",
    "soar",
    "ids",
    "ips",
    "endpoint security",
    "network security",
    "cloud security",
    "identity attack",
    "credential theft",
    "password attack",
    "brute force",
    "supply chain attack",
    "security advisory",
    "threat intelligence",
    "threat hunting",
    "penetration testing",
    "pentest",
    "red team",
    "blue team",
    "security breach",
    "data leak",
    "privacy breach",
    "regulatory fine",
    "cybercrime",
    "cyber espionage",
    "nation state attack",
    "attack surface",
    "vulnerability disclosure",
    "security flaw",
    "security bug",
    "security misconfiguration",
    "credential stuffing",
    "email security",
    "web security",
    "application security",
    "iot security",
    "mobile security",
    "ics security",
    "ot security",
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
    "IT Governance UK": "https://www.itgovernance.co.uk/blog/feed",
}


# =========================================================
# DATABASE
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


# def get_conn():
#     conn = sqlite3.connect(DB_NAME, timeout=30, check_same_thread=False)
#     conn.row_factory = sqlite3.Row
#     conn.execute("PRAGMA journal_mode=WAL;")
#     conn.execute("PRAGMA synchronous=NORMAL;")
#     return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    for table in ("ai_news", "cyber_news"):
        cur.execute(
            f"""
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
        """
        )

    # ✅ NEW: Vikram Sethi Blogs table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vikram_blogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            headline TEXT NOT NULL,
            body TEXT NOT NULL,
            image_path TEXT NOT NULL,
            published_time TEXT NOT NULL,
            original_link TEXT,
            author TEXT DEFAULT 'Dr. Vikram Sethi',
            created_at TEXT
        )
    """
    )

    # ✅ Y2AI Newsletter table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS y2ai_newsletter (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            headline TEXT NOT NULL,
            body TEXT NOT NULL,
            image_path TEXT NOT NULL,
            published_time TEXT NOT NULL,
            original_link TEXT,
            author TEXT DEFAULT 'Y2AI',
            created_at TEXT
        )
    """
    )

    # ADmin Login Table
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS admin_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT
    )
"""
    )

    # ✅ Social Media Links Table (single-row config)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS social_links (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            youtube TEXT,
            facebook TEXT,
            linkedin TEXT,
            updated_at TEXT
        )
        """
    )

    cur.execute(
        """
        INSERT OR IGNORE INTO social_links
        (id, youtube, facebook, linkedin, updated_at)
        VALUES (1, '', '', '', '')
        """
    )

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


def init_ai_headline_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_headline (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            headline TEXT,
            generated_at TEXT
        )
    """
    )
    cur.execute(
        """
        INSERT OR IGNORE INTO ai_headline (id, headline, generated_at)
        VALUES (1, '', '')
    """
    )
    conn.commit()
    conn.close()


init_db()
init_ai_headline_table()


# =========================================================
# HELPERS
# =========================================================
def clean_html(raw):
    return BeautifulSoup(raw or "", "lxml").get_text(" ", strip=True)


def extract_datetime(entry):
    if hasattr(entry, "published_parsed") and entry.published_parsed and len(entry.published_parsed) >= 6:
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
                cur.execute(
                    f"""
                    INSERT INTO {table}
                    (channel, headline, body, published_time, article_link, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        channel,
                        title,
                        body,
                        dt.isoformat(),
                        e.get("link", ""),
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
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
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=1.0,
            )

            content = response.choices[0].message.content
            summary = content.strip() if content else "Summary not available."

            # Hard safety: enforce single paragraph + sentence cap
            summary = " ".join(summary.splitlines())
            sentences = summary.split(". ")
            summary = ". ".join(sentences[:2]).strip()
            if not summary.endswith("."):
                summary += "."

            cur.execute(
                f"UPDATE {table} SET summary=? WHERE id=?", (summary, article_id)
            )
            conn.commit()

            # Generate image immediately after summary
            # generate_image_for_article(article_id, summary, category)

            completed += 1
            remaining = total - completed
            print(
                f"🧠 {label} summary done: {completed}/{total} (remaining {remaining})"
            )

        except Exception as e:
            conn.rollback()
            print(f"[ERROR] {label} summary failed for ID {article_id}: {e}")

    print(f"🧠 {label} summaries complete\n")
    conn.close()


import random


def get_fallback_image(category: str) -> str | None:
    folder = f"{IMAGE_BASE_DIR}/{category}"
    fallbacks = [f for f in os.listdir(folder) if f.startswith("fallback_")]
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
    cur.execute(
        f"""
        SELECT id, summary FROM {table}
        WHERE summary IS NOT NULL AND image_path IS NULL
    """
    )
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
            prompt = f"""
Create a cinematic, high-quality digital illustration inspired by the following news summary.
Interpret it visually and symbolically.

Rules:
- Image only
- No text, no letters, no numbers, no words

Summary (for inspiration only):
{summary}
"""

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
                f"UPDATE {table} SET image_path=? WHERE id=?", (path, article_id)
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
                        (fallback, article_id),
                    )
                    conn.commit()
                    print(f"🟡 {label} fallback image assigned for ID {article_id}")

            except Exception as db_err:
                print(
                    f"[DB ERROR] Failed assigning fallback for ID {article_id}: {db_err}"
                )

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

    generate_summaries("cyber")

    generate_images("cyber")
    print("🧱 BOOTSTRAP PIPELINE COMPLETE\n")


def incremental_pipeline():
    global next_pipeline_run

    if not pipeline_lock.acquire(blocking=False):
        print("[PIPELINE] Already running, skipping")
        return

    try:
        print("\n🔁 INCREMENTAL PIPELINE START")

        ingest_rss()
        generate_summaries("cyber")
        generate_images("cyber")

        print("🔁 INCREMENTAL PIPELINE END\n")

        # ✅ STEP 4 — UPDATE PIPELINE COUNTDOWN TIMER
        next_pipeline_run = datetime.now() + timedelta(
            seconds=PIPELINE_INTERVAL_SECONDS
        )

    finally:
        pipeline_lock.release()


# =========================================================
# API
# =========================================================
# =========================================================
# MANUAL IMAGE GENERATION ENDPOINTS
# =========================================================


@app.post("/cyberbriefs/images/generate/ai", tags=["Images"])
def generate_ai_images():
    """
    Generate images for AI news where summary exists but image is missing
    """
    generate_images("ai")
    return {"status": "success", "message": "AI image generation triggered"}


@app.post("/cyberbriefs/images/generate/cyber", tags=["Images"])
def generate_cyber_images():
    """
    Generate images for Cyber news where summary exists but image is missing
    """
    generate_images("cyber")
    return {"status": "success", "message": "Cyber image generation triggered"}


def count_missing(category: str):
    table = "ai_news" if category == "ai" else "cyber_news"
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT
            SUM(summary IS NULL OR TRIM(summary) = '') AS missing_summary,
            SUM(image_path IS NULL OR TRIM(image_path) = '') AS missing_image
        FROM {table}
    """
    )

    row = cur.fetchone()
    conn.close()

    return {"missing_summary": row[0] or 0, "missing_image": row[1] or 0}


@app.get("/cyberbriefs/news/cyber/headline", tags=["AI Headline"])
def get_ai_headline(request: Request):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT headline, generated_at
        FROM ai_headline
        WHERE id = 1
    """)
    row = cur.fetchone()
    conn.close()

    if not row or not row["headline"]:
        return {
            "headline": "Latest cyber security developments unfolding worldwide",
            "generated_at": None,
            "source": "fallback",
        }

    return {
        "headline": row["headline"],
        "generated_at": row["generated_at"] or None,
        "source": None,
    }



@app.post("/cyberbriefs/pipeline/heal/cyber", tags=["Pipeline"])
def heal_cyber_pipeline():
    """
    Heal pipeline by generating missing summaries and images
    ONLY for Cyber news and report progress stats.
    """

    if not pipeline_lock.acquire(blocking=False):
        return {"status": "skipped", "message": "Pipeline already running"}

    try:
        print("\n🛠️ CYBER PIPELINE HEAL START")

        # ---- BEFORE COUNTS ----
        before = count_missing("cyber")

        # ---- HEAL STEPS (CYBER ONLY) ----
        generate_summaries("cyber")
        generate_images("cyber")

        # ---- AFTER COUNTS ----
        after = count_missing("cyber")

        print("🛠️ CYBER PIPELINE HEAL COMPLETE\n")

        return {
            "status": "success",
            "category": "cyber",
            "before": before,
            "after": after,
            "done": {
                "summaries_generated": before["missing_summary"]
                - after["missing_summary"],
                "images_generated": before["missing_image"] - after["missing_image"],
            },
        }

    finally:
        pipeline_lock.release()


@app.get("/cyberbriefs/cyber/channels", tags=["Channels+Images"])
def get_channels():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT channel, image_path
        FROM cyber_news
        WHERE summary IS NOT NULL
          AND summary != ''
          AND image_path IS NOT NULL
          AND image_path != ''
        GROUP BY channel
        ORDER BY MAX(published_time) DESC
    """
    )

    rows = cur.fetchall()
    conn.close()

    return [{"channel": row["channel"], "image": row["image_path"]} for row in rows]


@app.get("/cyberbriefs/news/cyber/channel/{channel_name}", tags=["Selected Channel News"])
def get_news_by_channel(channel_name: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, headline, summary, image_path, published_time
        FROM cyber_news
        WHERE channel = ?
          AND summary IS NOT NULL
          AND summary != ''
          AND image_path IS NOT NULL
          AND image_path != ''
        ORDER BY published_time DESC
    """,
        (channel_name,),
    )

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "headline": row["headline"],
            "summary": row["summary"],
            "image": row["image_path"],
            "published_time": row["published_time"],
        }
        for row in rows
    ]


### Recent News endpoint ###
@app.get("/cyberbriefs/news/cyber/recent", tags=["Recent Cyber News"])
def get_all_cyber_news():
    """
    Fetch all Cyber news ordered from most recent to least recent.
    Only returns items with summary + image (UI-safe feed).
    """

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id,
            channel,
            headline,
            summary,
            image_path,
            published_time
        FROM cyber_news
        WHERE summary IS NOT NULL
          AND TRIM(summary) != ''
          AND image_path IS NOT NULL
          AND TRIM(image_path) != ''
        ORDER BY published_time DESC
    """
    )

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "channel": row["channel"],
            "headline": row["headline"],
            "summary": row["summary"],
            "image": row["image_path"],
            "published_time": row["published_time"],
        }
        for row in rows
    ]


@app.get("/cyberbriefs/news/cyber/{news_id}", tags=["Whole News Detail with id"])
def get_news_detail(news_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM cyber_news
        WHERE id = ?
    """,
        (news_id,),
    )

    row = cur.fetchone()
    conn.close()

    if not row:
        return {"error": "News not found"}

    return dict(row)


###Function to check headline 1 exists or not###
def ai_headline_exists() -> bool:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT headline
        FROM ai_headline
        WHERE id = 1
    """
    )
    row = cur.fetchone()
    conn.close()

    return bool(row and row["headline"])


## Headline generation endpoint After every 2 hours
def generate_ai_headline():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT headline, summary
        FROM cyber_news
        WHERE summary IS NOT NULL
        ORDER BY published_time DESC
        LIMIT 10
    """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        ai_headline = "Latest cyber security developments unfolding worldwide"
    else:
        context = "\n".join(f"- {row['headline']}: {row['summary']}" for row in rows)

        prompt = f"""
You are a cybersecurity news editor.

Generate ONE powerful, concise headline (max 15 words)
that summarizes the most important recent cyber security developments.

Rules:
- One line only
- No quotes
- No emojis
- Professional, news-style tone

Recent news:
{context}
"""

        try:
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
            )

            ai_headline = response.choices[0].message.content.strip()
        except Exception as e:
            print(f"[ERROR] AI headline generation failed: {e}")
            ai_headline = "Latest cyber security developments unfolding worldwide"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE ai_headline
        SET headline = ?, generated_at = ?
        WHERE id = 1
    """,
        (ai_headline, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

    print(f"🧠 AI HEADLINE UPDATED: {ai_headline}")
    global next_headline_run
    next_headline_run = datetime.now() + timedelta(seconds=HEADLINE_INTERVAL_SECONDS)


@app.get("/cyberbriefs/social-links", tags=["Social Media"])
def get_social_links():
    """
    Public endpoint – fetch social media links
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT youtube, facebook, linkedin, updated_at FROM social_links WHERE id = 1"
    )
    row = cur.fetchone()
    conn.close()

    return dict(row) if row else {}


@app.post("/cyberbriefs/social-links", tags=["Social Media"])
def create_social_links(
    data: SocialLinksCreate,
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE social_links
        SET youtube = ?, facebook = ?, linkedin = ?, updated_at = ?
        WHERE id = 1
        """,
        (
            data.youtube,
            data.facebook,
            data.linkedin,
            datetime.now(timezone.utc).isoformat(),
        ),
    )

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "message": "Social media links saved",
    }


@app.put("/cyberbriefs/social-links", tags=["Social Media"])
def update_social_links(
    data: SocialLinksUpdate,
):
    fields = []
    values = []

    if data.youtube is not None:
        fields.append("youtube = ?")
        values.append(data.youtube)

    if data.facebook is not None:
        fields.append("facebook = ?")
        values.append(data.facebook)

    if data.linkedin is not None:
        fields.append("linkedin = ?")
        values.append(data.linkedin)

    if not fields:
        raise HTTPException(status_code=400, detail="No fields provided")

    fields.append("updated_at = ?")
    values.append(datetime.now(timezone.utc).isoformat())

    values.append(1)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"UPDATE social_links SET {', '.join(fields)} WHERE id = ?",
        values,
    )
    conn.commit()
    conn.close()

    return {
        "status": "success",
        "message": "Social media links updated",
    }


@app.delete("/cyberbriefs/social-links", tags=["Social Media"])
def delete_social_links():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE social_links
        SET youtube = '', facebook = '', linkedin = '', updated_at = ?
        WHERE id = 1
        """,
        (datetime.now(timezone.utc).isoformat(),),
    )

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "message": "All social media links cleared",
    }


from typing import Optional
from pydantic import BaseModel


class RichContentUpdate(BaseModel):
    headline: Optional[str] = None
    body: Optional[str] = None
    image_path: Optional[str] = None
    published_time: Optional[str] = None
    original_link: Optional[str] = None


import re
import uuid
import base64
from bs4 import BeautifulSoup

def process_body_images(
    body_html: str, blog_id: int, base_folder: str = "vikram"
) -> str:
    print(f"🔄 Processing body images for {base_folder}/{blog_id}")
    soup = BeautifulSoup(body_html, "html.parser")

    image_dir = f"{IMAGE_BASE_DIR}/{base_folder}/{blog_id}"

    if os.path.isfile(image_dir):
        os.remove(image_dir)

    os.makedirs(image_dir, exist_ok=True)

    img_tags = soup.find_all("img")
    print(f"📸 Found {len(img_tags)} img tags in body")

    for idx, img in enumerate(img_tags, start=1):
        src = img.get("src", "")

        if src.startswith("data:image"):
            try:
                header, encoded = src.split(",", 1)
                mime = header.split(";")[0].split(":")[1]
                ext = mime.split("/")[-1]

                image_bytes = base64.b64decode(encoded)
                filename = f"body_{idx}_{uuid.uuid4().hex[:8]}.{ext}"
                disk_path = os.path.join(image_dir, filename)

                with open(disk_path, "wb") as f:
                    f.write(image_bytes)

                img["src"] = (
                    f"{PUBLIC_BASE_URL}/images/{base_folder}/{blog_id}/{filename}"
                )

            except Exception as e:
                print(f"❌ Failed image {idx}: {e}")
                img.decompose()

    return str(soup)

@app.post("/cyberbriefs/news/blogs/vikram", tags=["Vikram Blogs"])
def create_vikram_blog(
    headline: str = Form(...),
    body: str = Form(...),
    published_time: str = Form(...),
    original_link: str | None = Form(None),
    image: UploadFile = File(...),
):
    import shutil

    conn = get_conn()
    cur = conn.cursor()

    # 1️⃣ Insert blog (temporary body + empty image)
    cur.execute(
        """
        INSERT INTO vikram_blogs
        (headline, body, image_path, published_time, original_link, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            headline,
            body,  # temporary, will update after processing
            "",
            published_time,
            original_link,
            datetime.now(timezone.utc).isoformat(),
        ),
    )

    blog_id = cur.lastrowid
    conn.commit()

    # 2️⃣ Save cover image (SAFE)
    ext = os.path.splitext(image.filename)[1].lower()
    if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
        ext = ".png"

    filename = f"cover_{blog_id}{ext}"

    cover_dir = f"{IMAGE_BASE_DIR}/vikram"
    os.makedirs(cover_dir, exist_ok=True)

    cover_disk_path = os.path.join(cover_dir, filename)

    # 🚑 If a directory exists with same name, remove it
    if os.path.isdir(cover_disk_path):
        shutil.rmtree(cover_disk_path)

    with open(cover_disk_path, "wb") as f:
        f.write(image.file.read())

    cover_image_url = f"/images/vikram/{filename}"

    # 3️⃣ Process embedded images inside body
    processed_body = process_body_images(body, blog_id)

    # 4️⃣ Update blog with final body + cover image
    cur.execute(
        """
        UPDATE vikram_blogs
        SET body = ?, image_path = ?
        WHERE id = ?
        """,
        (processed_body, cover_image_url, blog_id),
    )

    conn.commit()
    conn.close()

    return {
        "id": blog_id,
        "headline": headline,
        "body": processed_body,
        "image_path": cover_image_url,
        "published_time": published_time,
        "original_link": original_link,
    }

@app.get("/cyberbriefs/news/blogs/vikram", tags=["Vikram Blogs"])
def list_vikram_blogs():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            headline,
            body,
            image_path,
            author,
            published_time
        FROM vikram_blogs
        ORDER BY published_time DESC
    """)

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "headline": row["headline"],
            "body": row["body"],
            "image_path": row["image_path"],
            "author": row["author"] or "Dr. Vikram Sethi",
            "published_time": row["published_time"],
        }
        for row in rows
    ]
@app.get("/cyberbriefs/news/blogs/vikram/{id}", tags=["Vikram Blogs"])
def get_vikram_blog(id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            headline,
            body,
            image_path,
            author,
            published_time,
            original_link,
            created_at
        FROM vikram_blogs
        WHERE id = ?
    """, (id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Blog not found")

    return {
        "id": row["id"],
        "headline": row["headline"],
        "body": row["body"],
        "image_path": row["image_path"],
        "author": row["author"] or "Dr. Vikram Sethi",
        "published_time": row["published_time"],
        "original_link": row["original_link"],
        "created_at": row["created_at"],
    }

from typing import Optional
from fastapi import Form, UploadFile, File
@app.put("/cyberbriefs/news/blogs/vikram/{id}", tags=["Vikram Blogs"])
def update_vikram_blog(
    id: int,
    headline: Optional[str] = Form(None),
    body: Optional[str] = Form(None),
    published_time: Optional[str] = Form(None),
    original_link: Optional[str] = Form(None),
    image: Optional[UploadFile] = File(None),
):
    conn = get_conn()
    cur = conn.cursor()

    # 1️⃣ Check existence
    cur.execute(
        "SELECT image_path FROM vikram_blogs WHERE id = ?",
        (id,),
    )
    existing = cur.fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Blog not found")

    old_image_path = existing["image_path"]

    fields = []
    values = []

    # 2️⃣ Text fields
    if headline is not None:
        fields.append("headline = ?")
        values.append(headline)

    if body is not None:
        fields.append("body = ?")
        values.append(body)

    if published_time is not None:
        fields.append("published_time = ?")
        values.append(published_time)

    if original_link is not None:
        fields.append("original_link = ?")
        values.append(original_link)

    # 3️⃣ Cover image update (optional)
    if image:
        import shutil

        ext = os.path.splitext(image.filename)[1].lower()
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            ext = ".png"

        filename = f"cover_{id}{ext}"

        cover_dir = f"{IMAGE_BASE_DIR}/vikram"
        os.makedirs(cover_dir, exist_ok=True)

        disk_path = os.path.join(cover_dir, filename)

        # 🚑 If a directory exists with same name, remove it
        if os.path.isdir(disk_path):
            shutil.rmtree(disk_path)

        with open(disk_path, "wb") as f:
            f.write(image.file.read())

        # ✅ DEFINE image_url (THIS WAS MISSING)
        image_url = f"/images/vikram/{filename}"

        fields.append("image_path = ?")
        values.append(image_url)

        # optional cleanup
        if old_image_path and old_image_path != image_url:
            try:
                old_disk = old_image_path.lstrip("/")
                if os.path.exists(old_disk):
                    os.remove(old_disk)
            except Exception:
                pass

    if not fields:
        conn.close()
        raise HTTPException(status_code=400, detail="No fields provided for update")

    # 4️⃣ Execute UPDATE
    values.append(id)
    cur.execute(
        f"UPDATE vikram_blogs SET {', '.join(fields)} WHERE id = ?",
        values,
    )

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "message": "Vikram blog updated successfully",
        "id": id,
    }


@app.delete("/cyberbriefs/news/blogs/vikram/{id}", tags=["Vikram Blogs"])
def delete_vikram_blog(id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM vikram_blogs WHERE id = ?", (id,))
    if not cur.fetchone():
        conn.close()
        return {"error": "Blog not found"}

    cur.execute("DELETE FROM vikram_blogs WHERE id = ?", (id,))
    conn.commit()
    conn.close()

    return {"status": "success", "message": "Vikram blog deleted"}


###Y2AI NEWSLETTER ENDPOINTS###
from pydantic import BaseModel

@app.post("/cyberbriefs/newsletter/y2ai", tags=["Y2AI Newsletter"])
def create_y2ai_newsletter(
    headline: str = Form(...),
    body: str = Form(...),
    published_time: str = Form(...),
    original_link: str | None = Form(None),
    image: UploadFile = File(...),
):
    import shutil

    # 1️⃣ Insert newsletter (temporary body + empty image)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO y2ai_newsletter
        (headline, body, image_path, published_time, original_link, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            headline,
            body,
            "",
            published_time,
            original_link,
            datetime.now(timezone.utc).isoformat(),
        ),
    )

    newsletter_id = cur.lastrowid
    conn.commit()
    conn.close()

    # 2️⃣ Save cover image (SAFE)
    ext = os.path.splitext(image.filename)[1].lower()
    if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
        ext = ".png"

    filename = f"cover_{newsletter_id}{ext}"

    cover_dir = f"{IMAGE_BASE_DIR}/y2ai"
    os.makedirs(cover_dir, exist_ok=True)

    cover_disk_path = os.path.join(cover_dir, filename)

    # 🚑 If a directory exists with same name, remove it
    if os.path.isdir(cover_disk_path):
        shutil.rmtree(cover_disk_path)

    with open(cover_disk_path, "wb") as f:
        f.write(image.file.read())

    cover_image_url = f"/images/y2ai/{filename}"

    # 3️⃣ Process embedded images in body
    processed_body = process_body_images(
        body_html=body,
        blog_id=newsletter_id,
        base_folder="y2ai",
    )

    # 4️⃣ Update newsletter with final body + cover image
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE y2ai_newsletter
        SET body = ?, image_path = ?
        WHERE id = ?
        """,
        (processed_body, cover_image_url, newsletter_id),
    )

    conn.commit()
    conn.close()

    return {
        "id": newsletter_id,
        "headline": headline,
        "body": processed_body,
        "image_path": cover_image_url,
        "published_time": published_time,
        "original_link": original_link,
    }


@app.get("/cyberbriefs/newsletter/y2ai", tags=["Y2AI Newsletter"])
def list_y2ai_newsletters():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, headline,body, image_path, published_time
        FROM y2ai_newsletter
        ORDER BY published_time DESC
    """
    )

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": row["id"],
            "headline": row["headline"],
            "body": row["body"],
            "image": row["image_path"],
            "published_time": row["published_time"],
        }
        for row in rows
    ]


@app.get("/cyberbriefs/newsletter/y2ai/{newsletter_id}", tags=["Y2AI Newsletter"])
def get_y2ai_newsletter(newsletter_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT *
        FROM y2ai_newsletter
        WHERE id = ?
    """,
        (newsletter_id,),
    )

    row = cur.fetchone()
    conn.close()

    if not row:
        return {"error": "Newsletter not found"}

    return dict(row)

@app.put("/cyberbriefs/newsletter/y2ai/{id}", tags=["Y2AI Newsletter"])
def update_y2ai_newsletter(
    id: int,
    headline: str | None = Form(None),
    body: str | None = Form(None),
    published_time: str | None = Form(None),
    original_link: str | None = Form(None),
    image: UploadFile | None = File(None),
):
    import shutil

    conn = get_conn()
    cur = conn.cursor()

    # 1️⃣ Check newsletter exists
    cur.execute("SELECT image_path FROM y2ai_newsletter WHERE id = ?", (id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Newsletter not found")

    old_cover_image = row["image_path"]

    fields = []
    values = []

    # 2️⃣ Text fields
    if headline is not None:
        fields.append("headline = ?")
        values.append(headline)

    if published_time is not None:
        fields.append("published_time = ?")
        values.append(published_time)

    if original_link is not None:
        fields.append("original_link = ?")
        values.append(original_link)

    # 3️⃣ BODY WITH EMBEDDED IMAGES
    if body is not None:
        processed_body = process_body_images(
            body_html=body,
            blog_id=id,
            base_folder="y2ai",
        )
        fields.append("body = ?")
        values.append(processed_body)

    # 4️⃣ COVER IMAGE REPLACE (OPTIONAL, SAFE)
    if image:
        ext = os.path.splitext(image.filename)[1].lower()
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            ext = ".png"

        filename = f"cover_{id}{ext}"

        cover_dir = f"{IMAGE_BASE_DIR}/y2ai"
        os.makedirs(cover_dir, exist_ok=True)

        cover_disk_path = os.path.join(cover_dir, filename)

        # 🚑 If a directory exists with same name, remove it
        if os.path.isdir(cover_disk_path):
            shutil.rmtree(cover_disk_path)

        with open(cover_disk_path, "wb") as f:
            f.write(image.file.read())

        cover_url = f"/images/y2ai/{filename}"

        fields.append("image_path = ?")
        values.append(cover_url)

        # Optional: delete old cover if different
        if old_cover_image and old_cover_image != cover_url:
            try:
                old_disk = old_cover_image.lstrip("/")
                if os.path.exists(old_disk):
                    os.remove(old_disk)
            except Exception as e:
                print("⚠️ Failed deleting old cover image:", e)

    if not fields:
        conn.close()
        raise HTTPException(status_code=400, detail="No fields provided for update")

    # 5️⃣ Execute update
    values.append(id)
    cur.execute(
        f"UPDATE y2ai_newsletter SET {', '.join(fields)} WHERE id = ?",
        values,
    )

    conn.commit()
    conn.close()

    return {
        "status": "success",
        "message": "Y2AI newsletter updated",
        "id": id,
    }

@app.delete("/cyberbriefs/newsletter/y2ai/{id}", tags=["Y2AI Newsletter"])
def delete_y2ai_newsletter(id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM y2ai_newsletter WHERE id = ?", (id,))
    if not cur.fetchone():
        conn.close()
        return {"error": "Newsletter not found"}

    cur.execute("DELETE FROM y2ai_newsletter WHERE id = ?", (id,))
    conn.commit()
    conn.close()

    return {"status": "success", "message": "Y2AI newsletter deleted"}


##ADmin User Management Endpoints##
def hash_password(password: str) -> str:
    return pwd_context.hash(password[:72])


def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password[:72], hashed)


import re
from fastapi import HTTPException, status


def validate_password(password: str):
    if len(password) < 8:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must be at least 8 characters long",
        )

    if not re.search(r"[a-z]", password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must contain at least one lowercase letter",
        )

    if not re.search(r"[A-Z]", password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must contain at least one uppercase letter",
        )

    if not re.search(r"\d", password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must contain at least one digit",
        )

    if not re.search(r"[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>/?]", password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must contain at least one special character",
        )


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_admin(token: str = Depends(oauth2_scheme)) -> dict:
    """
    Dependency to validate JWT token and ensure admin exists
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str | None = payload.get("sub")
        if email is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, email FROM admin_users WHERE email = ?", (email,))
    admin = cur.fetchone()
    conn.close()

    if not admin:
        raise credentials_exception

    return dict(admin)


@app.post("/cyberbriefs/auth/signup", tags=["Auth"])
def admin_signup(email: str = Form(...), password: str = Form(...)):
    # 🔒 Validate password strength FIRST
    validate_password(password)

    # 🔐 Hash password (bcrypt limit safe)
    hashed_password = hash_password(password[:72])

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO admin_users (email, password_hash, created_at)
            VALUES (?, ?, ?)
        """,
            (email.lower(), hashed_password, datetime.now(timezone.utc).isoformat()),
        )

        conn.commit()
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Admin already exists")
    finally:
        conn.close()

    return {"status": "success", "message": "Admin registered successfully"}


@app.post("/cyberbriefs/auth/login", tags=["Auth"])
def admin_login(email: str = Form(...), password: str = Form(...)):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, password_hash FROM admin_users WHERE email = ?", (email,))
    row = cur.fetchone()
    conn.close()

    if not row or not verify_password(password, row["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    token = create_access_token({"sub": email})

    return {
        "access_token": token,
        "token_type": "bearer",
        "message": "Login successful",
    }
