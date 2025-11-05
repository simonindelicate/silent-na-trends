# ui_app.py
# Streamlit UI to run the NA-trends pipeline with progress + optional Google Drive upload.
# - Loads .env explicitly from repo root
# - Lets you create/select RUN_IDs so outputs are not overwritten
# - Streams logs from each step
# - Optional: uploads weekly_brief.docx to Google Drive as a Google Doc (service account)

import os
import sys
import time
import subprocess
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv, set_key, find_dotenv

# ---------- Paths / env loading ----------

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"
RUNS_ROOT = ROOT / "data" / "runs"
RUNS_ROOT.mkdir(parents=True, exist_ok=True)
LATEST_DIR = ROOT / "data" / "latest"
LATEST_DIR.mkdir(parents=True, exist_ok=True)

ENV_PATH = ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=False)
else:
    alt = find_dotenv(usecwd=True)
    if alt:
        load_dotenv(dotenv_path=alt, override=False)

# ---------- Helpers ----------

def mask(v: str) -> str:
    if not v:
        return ""
    if len(v) <= 8:
        return "*" * len(v)
    return v[:4] + "*" * (len(v) - 8) + v[-4:]

def run_script(script_relpath: str, args=None, env_extra=None):
    """Run a Python script and stream stdout/stderr lines to the UI."""
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)

    cmd = [sys.executable, str(SCRIPTS / script_relpath)]
    if args:
        cmd += args

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        env=env,
    )
    for line in iter(process.stdout.readline, ""):
        yield line.rstrip()
    process.stdout.close()
    rc = process.wait()
    if rc != 0:
        raise RuntimeError(f"{script_relpath} failed with exit code {rc}")

def list_runs():
    return sorted([p.name for p in RUNS_ROOT.iterdir() if p.is_dir()])

def latest_output(run_id: str, name: str):
    out_dir = RUNS_ROOT / run_id / "outputs"
    if not out_dir.exists():
        return None
    p = out_dir / name
    return p if p.exists() else None

def upload_docx_to_drive_as_gdoc(docx_path: Path, folder_id: str, service_account_file: str):
    """Upload .docx and convert to Google Doc in Drive (returns file id + link)."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    scopes = ["https://www.googleapis.com/auth/drive.file"]
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    file_metadata = {
        "name": docx_path.stem,
        "mimeType": "application/vnd.google-apps.document",
        "parents": [folder_id] if folder_id else None,
    }
    media = MediaFileUpload(
        str(docx_path),
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        resumable=True,
    )
    created = drive.files().create(body=file_metadata, media_body=media, fields="id, webViewLink").execute()
    return created.get("id"), created.get("webViewLink")

# ---------- UI ----------

st.set_page_config(page_title="NA Trends Runner", layout="wide")
st.title("NA Trends Runner")
st.caption(f".env loaded from: {ENV_PATH if ENV_PATH.exists() else '(not found)'}")

# Sidebar: Environment editor (writes to .env and os.environ)
st.sidebar.header("Settings (.env)")
with st.sidebar.form("env_form", clear_on_submit=False):
    apify = st.text_input("APIFY_TOKEN", os.getenv("APIFY_TOKEN", ""), type="password")
    openai_key = st.text_input("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""), type="password")
    openai_model = st.text_input("OPENAI_MODEL", os.getenv("OPENAI_MODEL", "gpt-5"))
    enable_trends = st.selectbox("ENABLE_TRENDS", ["0", "1"], index=0 if os.getenv("ENABLE_TRENDS", "0") == "0" else 1)
    sa_file = st.text_input("GOOGLE_SERVICE_ACCOUNT_FILE", os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", ""))
    drive_folder = st.text_input("GOOGLE_DRIVE_FOLDER_ID", os.getenv("GOOGLE_DRIVE_FOLDER_ID", ""))
    saved = st.form_submit_button("Save & Apply")
    if saved:
        if not ENV_PATH.exists():
            ENV_PATH.touch()
        set_key(str(ENV_PATH), "APIFY_TOKEN", apify)
        set_key(str(ENV_PATH), "OPENAI_API_KEY", openai_key)
        set_key(str(ENV_PATH), "OPENAI_MODEL", openai_model)
        set_key(str(ENV_PATH), "ENABLE_TRENDS", enable_trends)
        set_key(str(ENV_PATH), "GOOGLE_SERVICE_ACCOUNT_FILE", sa_file)
        set_key(str(ENV_PATH), "GOOGLE_DRIVE_FOLDER_ID", drive_folder)
        os.environ.update({
            "APIFY_TOKEN": apify,
            "OPENAI_API_KEY": openai_key,
            "OPENAI_MODEL": openai_model,
            "ENABLE_TRENDS": enable_trends,
            "GOOGLE_SERVICE_ACCOUNT_FILE": sa_file,
            "GOOGLE_DRIVE_FOLDER_ID": drive_folder,
        })
        st.success("Saved to .env and applied to this session.")

# Run management: create/select RUN_ID
st.sidebar.header("Run")
existing = list_runs()
create_new = st.sidebar.button("Create new run (timestamp)")
if create_new or not existing:
    run_id = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime())
    st.sidebar.info(f"New run created: {run_id}")
else:
    run_id = st.sidebar.selectbox("Select an existing run", existing[::-1], index=0)

st.write(f"**Active RUN_ID:** `{run_id}`")

# Environment / Drive / Outputs panels
col1, col2, col3 = st.columns(3)

with col1:
    st.write("### Environment")
    env_view = {
        "APIFY_TOKEN": bool(os.getenv("APIFY_TOKEN")),
        "OPENAI_API_KEY": bool(os.getenv("OPENAI_API_KEY")),
        "ENABLE_TRENDS": os.getenv("ENABLE_TRENDS", "0"),
        "OPENAI_MODEL": os.getenv("OPENAI_MODEL", ""),
        "APIFY_TOKEN(sample)": mask(os.getenv("APIFY_TOKEN", "")),
        "OPENAI_API_KEY(sample)": mask(os.getenv("OPENAI_API_KEY", "")),
    }
    st.json(env_view)

with col2:
    st.write("### Optional Google Drive")
    drive_view = {
        "GOOGLE_SERVICE_ACCOUNT_FILE": bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")),
        "GOOGLE_DRIVE_FOLDER_ID": bool(os.getenv("GOOGLE_DRIVE_FOLDER_ID")),
    }
    st.json(drive_view)

with col3:
    st.write("### Outputs")
    md = latest_output(run_id, "weekly_brief.md")
    docx = latest_output(run_id, "weekly_brief.docx")
    st.json({
        "weekly_brief.md": str(md) if md else None,
        "weekly_brief.docx": str(docx) if docx else None,
    })

st.divider()

# Buttons
b1 = st.button("Step 1 — Ingest")
b2 = st.button("Step 2 — Prepare Context")
b3 = st.button("Step 3 — Generate Report (MD + DOCX)")
b_all = st.button("Run All")

log = st.empty()
prog = st.progress(0, text="Idle")

def stream_task(name, gen, pct):
    prog.progress(pct, text=name)
    buf = []
    for line in gen:
        buf.append(line)
        if len(buf) > 300:
            buf = buf[-300:]
        log.code("\n".join(buf))
    prog.progress(pct, text=f"{name} ✓")

def do_ingest(rid: str):
    stream_task(f"Ingest [{rid}]", run_script("01_ingest.py", env_extra={"RUN_ID": rid}), 33)

def do_prepare(rid: str):
    stream_task(f"Prepare [{rid}]", run_script("02_prepare_context.py", env_extra={"RUN_ID": rid}), 66)

def do_report(rid: str):
    stream_task(f"Report [{rid}]", run_script("03_generate_report.py", env_extra={"RUN_ID": rid}), 95)

def maybe_upload(rid: str):
    docx = latest_output(rid, "weekly_brief.docx")
    if not docx:
        st.warning("No weekly_brief.docx found to upload.")
        return
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")
    if not sa_file or not Path(sa_file).exists() or not folder_id:
        st.info("Drive upload skipped (set GOOGLE_SERVICE_ACCOUNT_FILE and GOOGLE_DRIVE_FOLDER_ID).")
        return
    try:
        file_id, link = upload_docx_to_drive_as_gdoc(docx, folder_id, sa_file)
        st.success(f"Uploaded to Drive as Google Doc: {link}")
    except Exception as e:
        st.error(f"Drive upload failed: {e}")

# Handlers
if b1:
    try:
        do_ingest(run_id)
    except Exception as e:
        st.error(str(e))
if b2:
    try:
        do_prepare(run_id)
    except Exception as e:
        st.error(str(e))
if b3:
    try:
        do_report(run_id)
        maybe_upload(run_id)
        prog.progress(100, text="Done")
    except Exception as e:
        st.error(str(e))
if b_all:
    try:
        do_ingest(run_id)
        do_prepare(run_id)
        do_report(run_id)
        maybe_upload(run_id)
        prog.progress(100, text="Done")
    except Exception as e:
        st.error(str(e))

st.divider()
st.write("### Latest outputs for active run")
md = latest_output(run_id, "weekly_brief.md")
docx = latest_output(run_id, "weekly_brief.docx")
if md:
    st.download_button("Download weekly_brief.md", md.read_bytes(), file_name=md.name, mime="text/markdown")
if docx:
    st.download_button("Download weekly_brief.docx", docx.read_bytes(), file_name=docx.name, mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
