import sqlite3, os
from pathlib import Path

_data_dir = Path(os.environ.get("DATA_DIR", str(Path(__file__).parent)))
DB_PATH = _data_dir / "sales.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS companies (
            id          INTEGER PRIMARY KEY,
            法人番号    TEXT,
            名称        TEXT NOT NULL,
            都道府県    TEXT,
            市区町村    TEXT,
            丁目番地等  TEXT,
            業種        TEXT DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_pref    ON companies(都道府県);
        CREATE INDEX IF NOT EXISTS idx_city    ON companies(市区町村);
        CREATE INDEX IF NOT EXISTS idx_name    ON companies(名称);
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
    """)
    conn.commit()
    conn.close()
