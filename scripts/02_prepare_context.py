import orjson, re, hashlib, statistics, math
from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ALL = max((ROOT/"data").glob("all_*.json"))  # latest run
OUTDIR = ROOT/"data"/"context"
OUTDIR.mkdir(parents=True, exist_ok=True)

def load_all():
    return orjson.loads(open(ALL,"rb").read())

def norm_text(t): 
    return re.sub(r"\s+", " ", (t or "")).strip()

def hash_text(t): 
    return hashlib.md5(norm_text(t).encode("utf-8")).hexdigest()

def prepare(rows):
    # dedupe
    seen=set(); cleaned=[]
    for r in rows:
        key = r.get("url") or hash_text(r.get("text") or r.get("title") or "")
        if key in seen: continue
        seen.add(key)
        cleaned.append({
            "platform": r.get("platform"),
            "ts": r.get("ts"),
            "author": r.get("author") or r.get("subreddit"),
            "url": r.get("url"),
            "title": r.get("title"),
            "text": norm_text(r.get("text") or r.get("caption") or ""),
            "likes": r.get("likes"),
            "comments": r.get("comments"),
            "shares": r.get("shares"),
            "hashtags": r.get("hashtags"),
            "tag": r.get("tag"),
            "term": r.get("term"),
            "value": r.get("value")
        })
    return cleaned

def ngram_slang(rows, min_len=3, top_k=50):
    texts=[(r["text"] or "") for r in rows if r["platform"] in ("instagram","x","reddit")]
    text=" ".join(texts).lower()
    text=re.sub(r"http\S+|[@#]\w+|\d+"," ",text)
    toks=[t for t in re.findall(r"[a-z][a-z'\-]+", text) if len(t)>=min_len]
    # remove boring words
    stop=set("""the a an and or for with this that about from into over under your our their you we they of in on at to is are be was were been being will would can could should it its it's im i'm""".split())
    toks=[t for t in toks if t not in stop]
    grams=[]
    for n in (1,2,3):
        seq=[" ".join(toks[i:i+n]) for i in range(len(toks)-n+1)]
        grams.extend(seq)
    cnt=Counter(grams)
    return [{"term":t,"count":c} for t,c in cnt.most_common(top_k)]

def trend_score(rows):
    # Simple platform-agnostic score: log(1+likes+2*comments+2*shares)
    scores=[]
    for r in rows:
        base = (r.get("likes") or 0) + 2*(r.get("comments") or 0) + 2*(r.get("shares") or 0)
        s = math.log1p(base)
        scores.append(s)
    mu = statistics.fmean(scores) if scores else 0
    sd = statistics.pstdev(scores) if len(scores)>1 else 1
    for r,s in zip(rows,scores):
        r["score"] = round((s - mu) / (sd or 1), 3)
    return rows

if __name__=="__main__":
    rows=prepare(load_all())
    rows=trend_score(rows)
    # compile LLM context in compact jsonl chunks (<= ~10k tokens each)
    rows_sorted = sorted(rows, key=lambda r: r.get("score",0), reverse=True)
    ctx = {
      "top_posts": rows_sorted[:120],
      "slang_candidates": ngram_slang(rows),
      "trends_timeseries": [r for r in rows if r["platform"]=="trends"]
    }
    with open(OUTDIR/"context.json","wb") as f:
        f.write(orjson.dumps(ctx, option=orjson.OPT_INDENT_2))
