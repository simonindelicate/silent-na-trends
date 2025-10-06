import os, time, json, subprocess, feedparser, requests, orjson
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import yaml
from tqdm import tqdm
from pytrends.request import TrendReq

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

def save_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        for r in rows:
            f.write(orjson.dumps(r) + b"\n")

def apify_run(actor_id, input_payload):
    # simple POST to start an actor and wait for dataset items
    r = requests.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs?token={APIFY_TOKEN}",
        json=input_payload, timeout=120
    )
    r.raise_for_status()
    rid = r.json()["data"]["id"]
    # poll for finish
    while True:
        s = requests.get(f"https://api.apify.com/v2/actor-runs/{rid}?token={APIFY_TOKEN}").json()["data"]["status"]
        if s in ("SUCCEEDED","FAILED","ABORTED","TIMED-OUT"): break
        time.sleep(5)
    if s != "SUCCEEDED": return []
    # fetch items
    items = requests.get(
        f"https://api.apify.com/v2/datasets/{rid}/items?token={APIFY_TOKEN}"
    ).json()
    return items

def ingest_instagram(creators, hashtags):
    # Apify actors (IDs current at time of writing):
    # Instagram Profile Scraper: apify/instagram-profile-scraper
    # Instagram Hashtag Scraper: apify/instagram-hashtag-scraper
    outrows=[]
    for u in tqdm(creators, desc="IG creators"):
        items = apify_run("apify~instagram-profile-scraper", {
            "profiles": [u], "resultsLimit": 30
        })
        for it in items:
            outrows.append({
                "platform":"instagram","kind":"post","author":u,
                "url": it.get("url"), "ts": it.get("firstCommentAt") or it.get("timestamp"),
                "text": it.get("caption"), "likes": it.get("likesCount"),
                "comments": it.get("commentsCount"), "hashtags": it.get("hashtags")})
    for h in tqdm(hashtags, desc="IG hashtags"):
        items = apify_run("apify~instagram-hashtag-scraper", {
            "hashtags": [h], "resultsLimit": 50
        })
        for it in items:
            outrows.append({
                "platform":"instagram","kind":"hashtag","tag":h,
                "url": it.get("url"), "ts": it.get("firstCommentAt") or it.get("timestamp"),
                "text": it.get("caption"), "likes": it.get("likesCount"),
                "comments": it.get("commentsCount"), "hashtags": it.get("hashtags")})
    save_jsonl(RAW / f"instagram_{date_stamp()}.jsonl", outrows)

def ingest_x(search_queries):
    # snscrape (no API key)
    rows=[]
    for q in tqdm(search_queries, desc="X search"):
        cmd = f'snscrape --jsonl twitter-search "{q} since:{since_days(7)}"'
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, text=True, encoding="utf-8")
        for line in p.stdout:
            j = orjson.loads(line)
            rows.append({
                "platform":"x","url":j["url"],"author":j["user"]["username"],
                "ts": j["date"], "text": j.get("rawContent",""),
                "likes": j.get("likeCount",0),"comments": j.get("replyCount",0),
                "shares": j.get("retweetCount",0)
            })
    save_jsonl(RAW / f"x_{date_stamp()}.jsonl", rows)

def ingest_reddit(subs):
    import praw
    # You can also avoid auth via pushshift mirrors; PRAW script app is straightforward.
    # If you don't want API keys, skip and pull RSS for "new" pages—adequate for weekly.
    rows=[]
    for s in subs:
        feed = feedparser.parse(f"https://www.reddit.com/r/{s}/new/.rss")
        for e in feed.entries:
            rows.append({
                "platform":"reddit","subreddit":s,"url":e.link,"ts":e.published,
                "title":e.title,"text":e.get("summary",""),"likes":None,"comments":None
            })
    save_jsonl(RAW / f"reddit_{date_stamp()}.jsonl", rows)

def ingest_news(rss_list):
    rows=[]
    for url in rss_list:
        feed = feedparser.parse(url)
        for e in feed.entries:
            rows.append({
                "platform":"news","source":url,"url":e.link,"ts":e.get("published",""),
                "title":e.title,"text": (e.get("summary") or e.get("description") or "")
            })
    save_jsonl(RAW / f"news_{date_stamp()}.jsonl", rows)

def ingest_trends(terms):
    pytrends = TrendReq(hl='en-GB', tz=0)
    rows=[]
    for t in terms:
        pytrends.build_payload([t], timeframe='now 7-d', geo='GB')  # or 'US'/''
        df = pytrends.interest_over_time().reset_index()
        for _, r in df.iterrows():
            rows.append({"platform":"trends","term":t,"ts":r["date"].isoformat(),"value":int(r[t])})
    save_jsonl(RAW / f"trends_{date_stamp()}.jsonl", rows)

def date_stamp():
    return datetime.utcnow().strftime("%Y-%m-%d")
def since_days(d):
    return (datetime.utcnow()-timedelta(days=d)).date().isoformat()

if __name__ == "__main__":
    creators = yaml.safe_load(open(ROOT/"config/creators.yaml"))["instagram"]
    hashtags = yaml.safe_load(open(ROOT/"config/hashtags.yaml"))["instagram"]
    cfg = yaml.safe_load(open(ROOT/"config/sources.yaml"))
    ingest_instagram(creators, hashtags)
    ingest_x(cfg["x"]["search_queries"])
    ingest_reddit(cfg["reddit"]["subreddits"])
    ingest_news(cfg["news_rss"])
    ingest_trends(cfg["trends_terms"])
    # Combine all raw files into one big JSON (array)
    big = []
    for p in sorted((RAW).glob(f"*_{date_stamp()}.jsonl")):
        with open(p,"rb") as f:
            for line in f: big.append(orjson.loads(line))
    with open(ROOT/"data"/f"all_{date_stamp()}.json","wb") as f:
        f.write(orjson.dumps(big, option=orjson.OPT_INDENT_2))
