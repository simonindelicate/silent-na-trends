# scripts/02_prepare_context.py
# Create balanced, deduped, LLM-ready context covering all platforms

import orjson, re, hashlib, statistics, math
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTDIR = DATA / "context"
OUTDIR.mkdir(parents=True, exist_ok=True)

def latest_all_json():
    files = sorted(DATA.glob("all_*.json"))
    if not files:
        raise FileNotFoundError("No data/all_YYYY-MM-DD.json found. Run 01_ingest.py first.")
    return files[-1]

def load_all():
    return orjson.loads(open(latest_all_json(), "rb").read())

def norm_text(t):
    return re.sub(r"\s+", " ", (t or "")).strip()

def hash_text(t):
    return hashlib.md5(norm_text(t).encode("utf-8")).hexdigest()

def prepare(rows):
    """Deduplicate and normalise text/fields"""
    seen = set()
    cleaned = []
    for r in rows:
        url = r.get("url")
        key = url or hash_text(r.get("text") or r.get("title") or "")
        if key in seen:
            continue
        seen.add(key)
        cleaned.append({
            "platform": (r.get("platform") or "").lower(),
            "ts": r.get("ts"),
            "author": r.get("author") or r.get("subreddit") or "",
            "url": url,
            "title": norm_text(r.get("title") or ""),
            "text": norm_text(r.get("text") or r.get("caption") or ""),
            "likes": r.get("likes"),
            "comments": r.get("comments"),
            "shares": r.get("shares"),
            "hashtags": r.get("hashtags"),
            "tag": r.get("tag"),
            "term": r.get("term"),
            "value": r.get("value"),
        })
    return cleaned

def safe_nonneg_int(x):
    try:
        if x is None: return 0
        v = int(float(x))
        return v if v > 0 else 0
    except Exception:
        return 0

def trend_score(rows):
    """Compute engagement z-score across entire dataset"""
    bases = []
    for r in rows:
        likes = safe_nonneg_int(r.get("likes"))
        comments = safe_nonneg_int(r.get("comments"))
        shares = safe_nonneg_int(r.get("shares"))
        base = likes + 2 * comments + 2 * shares
        s = math.log1p(max(0, base))
        bases.append(s)
    mu = statistics.fmean(bases) if bases else 0.0
    sd = statistics.pstdev(bases) if len(bases) > 1 else 0.0
    out = []
    for r, s in zip(rows, bases):
        z = (s - mu) / (sd if sd > 0 else 1.0)
        r["score"] = round(z, 3)
        out.append(r)
    return out

def ngram_slang(rows, min_len=3, top_k=50):
    """Collect frequent n-grams from social-style posts"""
    texts = [(r.get("text") or "") for r in rows if r.get("platform") in ("instagram","x","reddit")]
    text = " ".join(texts).lower()
    text = re.sub(r"http\S+|[@#]\w+|\d+"," ",text)
    toks = [t for t in re.findall(r"[a-z][a-z'\-]+", text) if len(t)>=min_len]
    stop = set(("the a an and or for with this that about from into over under your our their you we they of in on at to is are be was were been being will would can could should it its it's im i'm".split()))
    toks = [t for t in toks if t not in stop]
    grams=[]
    for n in (1,2,3):
        grams.extend(" ".join(toks[i:i+n]) for i in range(len(toks)-n+1))
    cnt = Counter(grams)
    return [{"term":t,"count":c} for t,c in cnt.most_common(top_k)]

def representative_sample(rows, n_per_platform=60):
    """Guarantee representation from each platform."""
    by_platform = defaultdict(list)
    for r in rows:
        by_platform[r.get("platform")].append(r)
    sample = []
    for p, items in by_platform.items():
        sorted_items = sorted(items, key=lambda r: r.get("score",0), reverse=True)
        sample.extend(sorted_items[:n_per_platform])
    return sample

if __name__ == "__main__":
    rows = prepare(load_all())
    rows = trend_score(rows)

    # Balanced representation
    balanced = representative_sample(rows, n_per_platform=60)

    ctx = {
        "summary": {
            "total_items": len(rows),
            "by_platform": {p: sum(1 for r in rows if r["platform"]==p) for p in set(r["platform"] for r in rows)}
        },
        "top_posts": balanced,
        "slang_candidates": ngram_slang(rows),
        "reddit_posts": [r for r in rows if r.get("platform")=="reddit"],
        "news_articles": [r for r in rows if r.get("platform")=="news"],
    }

    out_path = OUTDIR / "context.json"
    with open(out_path, "wb") as f:
        f.write(orjson.dumps(ctx, option=orjson.OPT_INDENT_2))
    print(f"Wrote {out_path} with {len(balanced)} representative posts.")
