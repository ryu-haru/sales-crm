import sqlite3, os, json, gzip, logging, threading
from pathlib import Path

logger = logging.getLogger(__name__)

# On Vercel /tmp is writable; locally fall back to the project directory
_IS_VERCEL = os.environ.get("VERCEL") == "1"
DB_PATH = Path("/tmp/sales.db") if _IS_VERCEL else Path(os.environ.get("DATA_DIR", str(Path(__file__).parent))) / "sales.db"

# GitHub Gist used to persist contacts across serverless cold-starts
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GIST_ID      = os.environ.get("CONTACTS_GIST_ID", "")

# ── DB download (companies, 660 MB gzip) ────────────────────────────────────
# The download is kicked off in a background thread so the function can
# return a "loading" page immediately without hitting the 10s timeout.
# /tmp persists across warm invocations, so subsequent requests are instant.

_download_lock = threading.Lock()
_download_started = False


def is_db_ready() -> bool:
    """True when the companies table exists and has data."""
    return DB_PATH.exists() and DB_PATH.stat().st_size > 1_000_000


def _do_download():
    """Background thread: download + decompress the DB into /tmp/sales.db."""
    global _download_started
    with _download_lock:
        if is_db_ready():
            return  # Another thread or warm instance already finished
        url = os.environ.get(
            "DB_DOWNLOAD_URL",
            "https://github.com/ryu-haru/sales-crm/releases/download/v1.0/sales.db.gz",
        )
        logger.info("[db] background download started: %s", url)
        try:
            import urllib.request, shutil
            tmp_gz = Path("/tmp/sales.db.gz")
            with urllib.request.urlopen(url, timeout=300) as resp, open(tmp_gz, "wb") as f:
                shutil.copyfileobj(resp, f)
            logger.info("[db] download done, decompressing…")
            db_tmp = Path("/tmp/sales.db.tmp")
            with gzip.open(tmp_gz, "rb") as gz, open(db_tmp, "wb") as out:
                shutil.copyfileobj(gz, out)
            db_tmp.rename(DB_PATH)
            tmp_gz.unlink(missing_ok=True)
            logger.info("[db] ready: %s MB", DB_PATH.stat().st_size // 1_048_576)
            # After DB is ready, ensure contacts table + Gist restore run
            _init_contacts()
        except Exception as exc:
            logger.error("[db] download failed: %s", exc)
            _download_started = False  # Allow retry on next request


def ensure_db_background():
    """Kick off the DB download in a daemon thread (non-blocking)."""
    global _download_started
    if is_db_ready() or _download_started:
        return
    _download_started = True
    t = threading.Thread(target=_do_download, daemon=True)
    t.start()


# ── Gist helpers ─────────────────────────────────────────────────────────────

def _gist_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _load_contacts_from_gist() -> list[dict]:
    """Fetch contacts JSON from the Gist; return [] on any error."""
    if not GITHUB_TOKEN or not GIST_ID:
        return []
    try:
        import urllib.request
        req = urllib.request.Request(
            f"https://api.github.com/gists/{GIST_ID}",
            headers=_gist_headers(),
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        content = data["files"].get("contacts.json", {}).get("content", "[]")
        return json.loads(content)
    except Exception as exc:
        logger.warning("[gist] load failed: %s", exc)
        return []


def _save_contacts_to_gist(rows: list[dict]):
    """Push the full contacts table to the Gist as contacts.json."""
    if not GITHUB_TOKEN or not GIST_ID:
        return
    try:
        import urllib.request
        body = json.dumps(
            {"files": {"contacts.json": {"content": json.dumps(rows, ensure_ascii=False)}}},
            ensure_ascii=False,
        ).encode()
        req = urllib.request.Request(
            f"https://api.github.com/gists/{GIST_ID}",
            data=body,
            headers={**_gist_headers(), "Content-Type": "application/json"},
            method="PATCH",
        )
        with urllib.request.urlopen(req, timeout=8):
            pass
    except Exception as exc:
        logger.warning("[gist] save failed: %s", exc)


# ── Core DB helpers ──────────────────────────────────────────────────────────

_SCHEMA = """
    CREATE TABLE IF NOT EXISTS companies (
        id         INTEGER PRIMARY KEY,
        法人番号   TEXT,
        名称       TEXT NOT NULL,
        都道府県   TEXT,
        市区町村   TEXT,
        丁目番地等 TEXT,
        業種       TEXT DEFAULT ''
    );
    CREATE INDEX IF NOT EXISTS idx_pref     ON companies(都道府県);
    CREATE INDEX IF NOT EXISTS idx_city     ON companies(市区町村);
    CREATE INDEX IF NOT EXISTS idx_name     ON companies(名称);
    CREATE INDEX IF NOT EXISTS idx_industry ON companies(業種);

    CREATE TABLE IF NOT EXISTS contacts (
        id           INTEGER PRIMARY KEY,
        company_id   INTEGER NOT NULL,
        ステータス   TEXT DEFAULT '未対応',
        担当者       TEXT DEFAULT '',
        メール       TEXT DEFAULT '',
        電話         TEXT DEFAULT '',
        業種         TEXT DEFAULT '',
        メモ         TEXT DEFAULT '',
        最終連絡日   TEXT DEFAULT '',
        更新日時     DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (company_id) REFERENCES companies(id)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_contact_company ON contacts(company_id);
    CREATE INDEX IF NOT EXISTS idx_status ON contacts(ステータス);
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _init_contacts():
    """Create contacts table and restore from Gist. Safe to call after DB file exists."""
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(_SCHEMA)
    conn.commit()
    if _IS_VERCEL:
        existing = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        if existing == 0:
            gist_rows = _load_contacts_from_gist()
            if gist_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO contacts
                      (id, company_id, ステータス, 担当者, メール, 電話, 業種, メモ, 最終連絡日, 更新日時)
                    VALUES
                      (:id, :company_id, :ステータス, :担当者, :メール, :電話, :業種, :メモ, :最終連絡日, :更新日時)
                    """,
                    gist_rows,
                )
                conn.commit()
                logger.info("[gist] restored %d contacts", len(gist_rows))
    conn.close()


def init_db():
    """
    On Vercel: kick off DB download in background thread, create a minimal
    stub DB immediately so the app serves a "loading" page without crashing.
    Locally: apply schema to existing DB_PATH.
    """
    if _IS_VERCEL:
        if not is_db_ready():
            # Create a minimal stub so SQLite connections don't fail
            stub = sqlite3.connect(DB_PATH)
            stub.executescript(_SCHEMA)
            stub.commit()
            stub.close()
            # Kick off the real download in the background
            ensure_db_background()
        else:
            # DB already present (warm instance) — just ensure schema
            _init_contacts()
    else:
        # Local dev: DB file must already exist; just apply schema
        if DB_PATH.exists():
            conn = sqlite3.connect(DB_PATH)
            conn.executescript(_SCHEMA)
            conn.commit()
            conn.close()


def sync_contacts_to_gist():
    """Dump the entire contacts table to the Gist. Call after every write."""
    if not _IS_VERCEL or not GITHUB_TOKEN or not GIST_ID:
        return
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT id, company_id, ステータス, 担当者, メール, 電話, 業種, メモ, 最終連絡日, 更新日時 FROM contacts"
        ).fetchall()
        conn.close()
        _save_contacts_to_gist([dict(r) for r in rows])
    except Exception as exc:
        logger.warning("[gist] sync_contacts_to_gist failed: %s", exc)
