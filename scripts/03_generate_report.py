import os, orjson
from pathlib import Path
from dotenv import load_dotenv
from jinja2 import Template
from pptx import Presentation
from pptx.util import Pt

load_dotenv()
OPENAI_KEY = os.getenv("OPENAI_API_KEY")

ROOT = Path(__file__).resolve().parents[1]
CTX = orjson.loads(open(ROOT/"data/context/context.json","rb").read())
OUT = ROOT/"data"/"outputs"
OUT.mkdir(parents=True, exist_ok=True)

SYSTEM = """You are a senior UK social strategist for a non-alcoholic beer brand.
Write concise, factual analysis. UK English. Avoid hype. No health claims."""

USER_TMPL = Template("""
Using the attached JSON context:
- Identify the 8 most significant *trends* (explain the signal: growth, cross-platform traction, or creator weight).
- List 10 *slang/phrases* to watch with one-line glosses and example links if present.
- Provide 5 *content themes for next week* (each with: 1-sentence rationale grounded in observed posts; a 10–12 word hook; two post formats with beat-by-beat outline; suggested on-screen text; 3–5 hashtags blending brand + trend; compliance notes).
- Flag any *product launches* or notable creator posts we should acknowledge.
Return clean Markdown with headings.

Data notes:
Top posts include fields: platform, ts, author, url, text, score (z). Treat z≥1.0 as strong.
Trends_timeseries are Google Trends values for GB over 7 days (0–100).
""")

def call_openai(system, user, context_json):
    from openai import OpenAI
    client = OpenAI(api_key=OPENAI_KEY)
    content_str = orjson.dumps(context_json).decode("utf-8")
    msg = client.chat.completions.create(
        model="gpt-5-thinking",  # or "gpt-5" if preferred
        messages=[
            {"role":"system","content":system},
            {"role":"user","content":user},
            {"role":"user","content":f"Context JSON:\n```json\n{content_str}\n```"}
        ],
        temperature=0.2,
        max_tokens=3000,
    )
    return msg.choices[0].message.content

def make_deck(markdown_text, out_path):
    prs = Presentation()
    # very light: title + one slide per '## Trend'
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "Weekly NA Beer Brief"
    slide.placeholders[1].text = "Auto-generated summary"
    for block in markdown_text.split("\n## "):
        if not block.strip(): continue
        title = block.split("\n",1)[0][:70]
        body = block.split("\n",1)[1] if "\n" in block else ""
        s = prs.slides.add_slide(prs.slide_layouts[1])
        s.shapes.title.text = title
        s.placeholders[1].text = body[:1500]
        for shape in s.placeholders:
            try:
                shape.text_frame.word_wrap = True
                shape.text_frame.paragraphs[0].font.size = Pt(18)
            except: pass
    prs.save(out_path)

if __name__=="__main__":
    user_prompt = USER_TMPL.render()
    md = call_openai(SYSTEM, user_prompt, CTX)
    (OUT/"weekly_brief.md").write_text(md, encoding="utf-8")
    make_deck(md, OUT/"weekly_brief.pptx")
