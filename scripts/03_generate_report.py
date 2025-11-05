# scripts/03_generate_report.py
# Generate a weekly brief Markdown file from data/context/context.json using OpenAI.

import os
from datetime import datetime
from pathlib import Path
import orjson
from dotenv import load_dotenv

load_dotenv()

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")  # set to a model your key has
ROOT = Path(__file__).resolve().parents[1]
CTX_PATH = ROOT / "data" / "context" / "context.json"
RUN_ID = os.getenv("RUN_ID")

def resolve_output_dir() -> Path:
    """Return the output directory for the current run (defaulting to shared outputs)."""
    if RUN_ID:
        base = ROOT / "data" / "runs" / RUN_ID / "outputs"
    else:
        base = ROOT / "data" / "outputs"
    base.mkdir(parents=True, exist_ok=True)
    return base

OUT_DIR = resolve_output_dir()
LATEST_DIR = ROOT / "data" / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)

SYSTEM_PROMPT = (
    "You are a senior social strategist for a non-alcoholic beer brand serving a US audience. "
    "Write in warm, professional, approachable UK English (friendly, not stiff; never slangy). "
    "Be specific and evidence-led, but use smooth transitions and complete sentences. "
    "Avoid health claims and ‘sober shaming’. Use only the supplied context."
)

USER_PROMPT = """You are given structured context JSON (summary, top_posts, slang_candidates, reddit_posts, news_articles).

Produce a well-structured weekly report with these sections:

# Headline Summary (2 short paragraphs)
- What’s moving and why, in plain language. Name the biggest patterns.

## Top Trends (8 items)
For each:
- Title (friendly, descriptive)
- Why it matters (2–3 sentences; cite cross-platform presence or creator weight)
- 2–3 example links

## Slang & Phrases to Watch (10 items)
- Term — one-line gloss; add an example URL if present.

## Content Plan (5 themes)
For each theme:
- Rationale (2–3 sentences, grounded in the posts)
- Hook (10–12 words)
- Two formats (e.g., Reel, Carousel, TikTok) each with 3–6 beat bullets
- On-screen text (one line) + Caption (one sentence)
- 3–5 hashtags (blend brand + trend; UK English)
- Compliance notes (brief)

## Notables
- Product launches or notable creator posts (bulleted with links).

Tone: friendly, helpful, confident. Avoid clipped telegraph style."""

def load_context():
    if not CTX_PATH.exists():
        raise FileNotFoundError(f"Context not found: {CTX_PATH}. Run scripts/02_prepare_context.py first.")
    return orjson.loads(CTX_PATH.read_bytes())

def call_openai(system_text: str, user_text: str, context_json: dict, max_out_tokens=6000) -> str:
    if not OPENAI_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set in .env")

    from openai import OpenAI
    from openai import BadRequestError

    client = OpenAI(api_key=OPENAI_KEY)
    ctx_str = orjson.dumps(context_json).decode("utf-8")
    messages = [
        {"role": "system", "content": system_text},
        {"role": "user", "content": user_text},
        {"role": "user", "content": f"Context JSON:\n```json\n{ctx_str}\n```"},
    ]

    # Prefer Responses API (GPT-5 family). Try max_output_tokens then max_completion_tokens.
    try:
        resp = client.responses.create(
            model=MODEL,
            input=messages,
            max_output_tokens=max_out_tokens,
        )
        return resp.output_text
    except TypeError:
        try:
            resp = client.responses.create(
                model=MODEL,
                input=messages,
                max_completion_tokens=max_out_tokens,
            )
            return resp.output_text
        except Exception:
            pass
    except BadRequestError as e:
        if "must use the chat.completions endpoint" not in str(e):
            raise

    # Fallback: Chat Completions (non-GPT-5 models)
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=messages,
            max_completion_tokens=max_out_tokens,
        )
        return resp.choices[0].message.content
    except BadRequestError as e:
        if "Use 'max_tokens' instead" not in str(e):
            raise
    resp = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        max_tokens=max_out_tokens,
    )
    return resp.choices[0].message.content

def save_markdown(text: str, path: Path):
    path.write_text(text, encoding="utf-8")


def write_latest_copy(text: str, out_dir: Path, latest_dir: Path, filename: str):
    """Update convenience copies in the run directory and shared latest directory."""
    latest_path = out_dir / filename
    latest_path.write_text(text, encoding="utf-8")
    shared_latest = latest_dir / filename
    shared_latest.write_text(text, encoding="utf-8")

def main():
    ctx = load_context()
    md = call_openai(SYSTEM_PROMPT, USER_PROMPT, ctx, max_out_tokens=6000)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    md_filename = f"weekly_brief_{timestamp}.md"
    md_path = OUT_DIR / md_filename
    save_markdown(md, md_path)
    write_latest_copy(md, OUT_DIR, LATEST_DIR, "weekly_brief.md")
    print(f"Wrote markdown → {md_path}")
    print("Next: run scripts/04_markdown_to_docx.py to create the DOCX.")

if __name__ == "__main__":
    main()
