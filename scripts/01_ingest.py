# scripts/01_ingest.py
# Weekly ingest: Instagram (Apify), X (Apify), Reddit (RSS), News (RSS), Google Trends (Apify) → big JSON
# Outputs:
#   data/raw/*.jsonl per source
#   data/all_YYYY-MM-DD.json combined

import os
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

import yaml
import feedparser
import orjson
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)
CONFIG_DIR_ENV = os.getenv("CONFIG_DIR", "")
if CONFIG_DIR_ENV:
    config_dir_candidate = Path(CONFIG_DIR_ENV)
    if not config_dir_candidate.is_absolute():
        config_dir_candidate = (ROOT / config_dir_candidate).resolve()
else:
    config_dir_candidate = ROOT / "config"
CONFIG_DIR = config_dir_candidate


# ---------- helpers ----------

def date_stamp() -> str:
    # UTC date stamp for filenames
    return datetime.utcnow().strftime("%Y-%m-%d")


def since_days(days: int) -> str:
    return (datetime.utcnow() - timedelta(days=days)).date().isoformat()


def save_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for r in rows:
            f.write(orjson.dumps(r) + b"\n")


def load_config_yaml(name: str):
    cfg_path = CONFIG_DIR / name
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing configuration file: {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def apify_run(actor_id: str, input_payload: dict):
    """
    Run an Apify actor synchronously and return dataset items.
    Accept any 2xx status; handle JSON array or NDJSON.
    """
    import requests
    if not APIFY_TOKEN:
        raise RuntimeError("APIFY_TOKEN is not set in .env")
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items?token={APIFY_TOKEN}"
    r = requests.post(url, json=input_payload, timeout=300)
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Apify error {r.status_code}: {r.text}")
    try:
        return r.json()
    except Exception:
        txt = r.text.strip()
        if not txt:
            return []
        if txt.startswith("["):
            return orjson.loads(txt)
        return [orjson.loads(line) for line in txt.splitlines() if line.strip()]


# ---------- sources ----------

def ingest_instagram(creators, hashtags):
    """
    Instagram creators via apify/instagram-scraper (posts),
    hashtags via apify/instagram-hashtag-scraper.
    """
    outrows = []

    # Creators → posts
    for u in tqdm(creators, desc="IG creators"):
        try:
            items = apify_run("apify~instagram-scraper", {
                "directUrls": [f"https://www.instagram.com/{u}"],
                "resultsType": "posts",
                "resultsLimit": 30,
                "onlyPostsNewerThan": "14 days",
                "addParentData": False
            })
            for it in items:
                outrows.append({
                    "platform": "instagram",
                    "kind": "post",
                    "author": u,
                    "url": it.get("url") or it.get("postUrl"),
                    "ts": it.get("timestamp") or it.get("takenAt"),
                    "text": it.get("caption"),
                    "likes": it.get("likesCount"),
                    "comments": it.get("commentsCount"),
                    "shares": None,
                    "hashtags": it.get("hashtags"),
                })
        except Exception as e:
            print(f"[WARN] IG creator {u}: {e}")

    # Hashtags → posts
    for h in tqdm(hashtags, desc="IG hashtags"):
        try:
            items = apify_run("apify~instagram-hashtag-scraper", {
                "hashtags": [h],
                "resultsLimit": 50
            })
            for it in items:
                outrows.append({
                    "platform": "instagram",
                    "kind": "hashtag",
                    "tag": h,
                    "url": it.get("url"),
                    "ts": it.get("firstCommentAt") or it.get("timestamp"),
                    "text": it.get("caption"),
                    "likes": it.get("likesCount"),
                    "comments": it.get("commentsCount"),
                    "shares": None,
                    "hashtags": it.get("hashtags"),
                })
        except Exception as e:
            print(f"[WARN] IG hashtag #{h}: {e}")

    save_jsonl(RAW / f"instagram_{date_stamp()}.jsonl", outrows)


def ingest_x(search_terms, tweet_language="en", days_back=7, max_items=150, sort="Latest"):
    """
    X via Apify Actor xtdata/twitter-x-scraper.
    Maps fields per actor's Output Example:
      full_text, created_at, favorite_count, retweet_count, reply_count, author.screen_name
    """
    rows = []
    try:
        start = since_days(days_back)
        payload = {
            "searchTerms": search_terms,   # e.g., ["non-alcoholic beer", "NA beer", "hopwater"]
            "tweetLanguage": tweet_language,
            "start": start,
            "end": date_stamp(),
            "sort": sort,                  # "Latest" or "Top"
            "maxItems": max_items,
            "includeSearchTerms": False
        }
        items = apify_run("xtdata~twitter-x-scraper", payload)

        def g(obj, path, default=None):
            cur = obj
            for k in path.split("."):
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    return default
            return cur

        for it in items:
            rows.append({
                "platform": "x",
                "url": it.get("url") or it.get("twitterUrl"),
                "author": g(it, "author.screen_name") or g(it, "author.name"),
                "ts": it.get("created_at") or it.get("date") or it.get("timeParsed"),
                "text": it.get("full_text") or "",
                "likes": it.get("favorite_count", 0),
                "comments": it.get("reply_count", 0),
                "shares": it.get("retweet_count", 0),
            })
    except Exception as e:
        print(f"[WARN] X ingest (Apify) failed: {e}")

    save_jsonl(RAW / f"x_{date_stamp()}.jsonl", rows)



def ingest_reddit(subs):
    """
    Reddit via 'new' RSS feeds (no auth), using a browser-like User-Agent.
    """
    import requests
    rows = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RedditRSSFetcher/1.0"}
    for s in tqdm(subs, desc="Reddit RSS"):
        try:
            url = f"https://www.reddit.com/r/{s}/new/.rss?limit=50"
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            feed = feedparser.parse(r.content)
            for e in feed.entries:
                rows.append({
                    "platform": "reddit",
                    "subreddit": s,
                    "url": e.link,
                    "ts": e.get("published"),
                    "title": e.get("title"),
                    "text": e.get("summary", ""),
                    "likes": None,
                    "comments": None,
                    "shares": None,
                })
        except Exception as e:
            print(f"[WARN] Reddit r/{s}: {e}")
    save_jsonl(RAW / f"reddit_{date_stamp()}.jsonl", rows)


def ingest_news(rss_list):
    rows = []
    for url in tqdm(rss_list, desc="News RSS"):
        try:
            feed = feedparser.parse(url)
            for e in feed.entries:
                rows.append({
                    "platform": "news",
                    "source": url,
                    "url": e.get("link"),
                    "ts": e.get("published", ""),
                    "title": e.get("title"),
                    "text": e.get("summary") or e.get("description") or "",
                    "likes": None,
                    "comments": None,
                    "shares": None,
                })
        except Exception as e:
            print(f"[WARN] News {url}: {e}")
    save_jsonl(RAW / f"news_{date_stamp()}.jsonl", rows)


def ingest_trends(terms, geo="US", time_range="now 7-d"):
    """
    Google Trends via local pytrends with:
      - batching (<=5 terms)
      - jittered exponential backoff on 429/5xx
      - per-day disk cache to avoid repeat calls
      - optional proxy via HTTP(S)_PROXY env vars
    Writes: data/raw/trends_YYYY-MM-DD.jsonl
    """
    import os, hashlib, json, time, random
    from pathlib import Path
    from pytrends.request import TrendReq
    from requests.exceptions import HTTPError

    # cache dir (per-day, per-geo, per-time_range, per-term-group)
    CACHE = (ROOT / "data" / "cache" / "trends")
    CACHE.mkdir(parents=True, exist_ok=True)

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    # Optional proxy picked up from environment if present
    proxies = {}
    for k in ("HTTP_PROXY","HTTPS_PROXY","http_proxy","https_proxy"):
        v = os.getenv(k)
        if v:
            if "http" in k.lower():
                proxies["http"] = v
            if "https" in k.lower():
                proxies["https"] = v

    pytrends = TrendReq(
        hl='en-US', tz=0,
        requests_args={
            "headers": {"User-Agent": "Mozilla/5.0"},
            **({"proxies": proxies} if proxies else {})
        },
        retries=0,             # we’ll implement our own backoff
        backoff_factor=0.0
    )

    rows = []

    # helper to read/write cache
    def cache_path(group):
        key = json.dumps({
            "d": date_stamp(),
            "geo": geo,
            "range": time_range,
            "terms": group
        }, sort_keys=True)
        h = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return CACHE / f"{h}.json"

    def load_cache(cp):
        if cp.exists():
            try:
                return json.loads(cp.read_text(encoding="utf-8"))
            except Exception:
                return None

    def save_cache(cp, data):
        try:
            cp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    # jittered backoff runner
    def build_and_fetch(group):
        # try cache first
        cp = cache_path(group)
        cached = load_cache(cp)
        if cached is not None:
            return cached

        attempt = 0
        while True:
            try:
                pytrends.build_payload(group, timeframe=time_range, geo=geo)
                df = pytrends.interest_over_time()
                # normalise to records
                out = []
                if not df.empty:
                    dfr = df.reset_index()
                    for _, r in dfr.iterrows():
                        ts = r["date"].isoformat()
                        for term in group:
                            val = r.get(term)
                            if val is None:
                                continue
                            out.append({"term": term, "ts": ts, "value": int(val) if not (val != val) else 0})
                save_cache(cp, out)
                return out
            except HTTPError as ex:
                code = getattr(ex.response, "status_code", None)
                attempt += 1
                # backoff up to ~60s with jitter
                wait = min(60, (2 ** attempt) + random.uniform(0.0, 1.0))
                if attempt >= 6:
                    # give up after ~1+2+4+8+16+32 ≈ 63s total
                    return [{"term": t, "ts": None, "value": None, "error": f"HTTP {code} after retries"} for t in group]
                time.sleep(wait)
            except Exception as ex:
                # non-HTTP errors: one short retry then give up
                attempt += 1
                if attempt > 2:
                    return [{"term": t, "ts": None, "value": None, "error": str(ex)} for t in group]
                time.sleep(2 + random.uniform(0, 1.5))

    # process in groups of 5 with small inter-group pause
    for group in tqdm(list(chunks(terms, 5)), desc="Google Trends"):
        out = build_and_fetch(group)
        rows.extend([{"platform": "trends", **rec} for rec in out])
        # polite spacing between groups
        time.sleep(1.0 + random.uniform(0, 0.75))

    save_jsonl(RAW / f"trends_{date_stamp()}.jsonl", rows)




# ---------- main ----------

if __name__ == "__main__":
    creators_cfg = load_config_yaml("creators.yaml")
    hashtags_cfg = load_config_yaml("hashtags.yaml")
    sources_cfg = load_config_yaml("sources.yaml")

    # Instagram
    ig_creators = creators_cfg.get("instagram", [])
    ig_hashtags = hashtags_cfg.get("instagram", [])
    try:
        ingest_instagram(ig_creators, ig_hashtags)
    except Exception as e:
        print(f"[WARN] Instagram ingest failed: {e}")

    # X via Apify
    x_cfg = sources_cfg.get("x", {})
    try:
        ingest_x(
            x_cfg.get("search_terms", []),
            tweet_language=x_cfg.get("tweet_language", "en"),
            days_back=int(x_cfg.get("days_back", 7)),
            max_items=int(x_cfg.get("max_items", 150)),
            sort=x_cfg.get("sort", "Latest"),
        )
    except Exception as e:
        print(f"[WARN] X ingest failed: {e}")

    # Reddit
    try:
        ingest_reddit(sources_cfg.get("reddit", {}).get("subreddits", []))
    except Exception as e:
        print(f"[WARN] Reddit ingest failed: {e}")

    # News RSS
    try:
        ingest_news(sources_cfg.get("news_rss", []))
    except Exception as e:
        print(f"[WARN] News ingest failed: {e}")

    # Google Trends via Apify (US)
    try:
        ingest_trends(
            sources_cfg.get("trends_terms", []),
            geo=sources_cfg.get("trends_geo", "US"),
            time_range=sources_cfg.get("trends_time_range", "now 7-d"),
        )
    except Exception as e:
        print(f"[WARN] Trends ingest failed: {e}")

    # Combine today's files (UTC date)
    combined = []
    for p in sorted(RAW.glob(f"*_{date_stamp()}.jsonl")):
        with open(p, "rb") as f:
            for line in f:
                try:
                    combined.append(orjson.loads(line))
                except Exception:
                    pass

    out_path = ROOT / "data" / f"all_{date_stamp()}.json"
    with open(out_path, "wb") as f:
        f.write(orjson.dumps(combined, option=orjson.OPT_INDENT_2))

    print(f"Wrote {out_path} with {len(combined)} items.")
