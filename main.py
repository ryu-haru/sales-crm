from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional
import csv, io, sqlite3
from datetime import datetime
from database import get_db, init_db, DB_PATH

app = FastAPI(title="営業管理CRM")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

STATUSES = ["未対応", "営業中", "営業済み", "提携済み", "NG", "保留"]
PAGE_SIZE = 50

@app.on_event("startup")
def startup():
    init_db()

# ── ユーティリティ ──────────────────────────────

def search_companies(pref="", city="", keyword="", industry="", status="", page=1):
    conn = get_db()
    conditions, params = [], []

    if pref:
        conditions.append("c.都道府県 = ?")
        params.append(pref)
    if city:
        conditions.append("c.市区町村 LIKE ?")
        params.append(f"%{city}%")
    if keyword:
        conditions.append("c.名称 LIKE ?")
        params.append(f"%{keyword}%")
    if industry:
        conditions.append("(c.業種 LIKE ? OR COALESCE(ct.業種,'') LIKE ?)")
        params.extend([f"%{industry}%", f"%{industry}%"])
    if status:
        if status == "未対応":
            conditions.append("ct.ステータス IS NULL")
        else:
            conditions.append("ct.ステータス = ?")
            params.append(status)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * PAGE_SIZE

    sql_count = f"""
        SELECT COUNT(*) FROM companies c
        LEFT JOIN contacts ct ON c.id = ct.company_id
        {where}
    """
    total = conn.execute(sql_count, params).fetchone()[0]

    sql = f"""
        SELECT c.id, c.名称, c.都道府県, c.市区町村, c.丁目番地等,
               COALESCE(ct.業種, c.業種) AS 業種,
               COALESCE(ct.ステータス, '未対応') AS ステータス,
               COALESCE(ct.担当者, '') AS 担当者,
               COALESCE(ct.最終連絡日, '') AS 最終連絡日,
               COALESCE(ct.メモ, '') AS メモ
        FROM companies c
        LEFT JOIN contacts ct ON c.id = ct.company_id
        {where}
        ORDER BY ct.更新日時 DESC NULLS LAST, c.名称
        LIMIT {PAGE_SIZE} OFFSET {offset}
    """
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows, total

def get_prefs():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT 都道府県 FROM companies ORDER BY 都道府県").fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]

def get_stats():
    conn = get_db()
    stats = {}
    for s in STATUSES:
        if s == "未対応":
            total = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
            touched = conn.execute("SELECT COUNT(*) FROM contacts WHERE ステータス != '未対応'").fetchone()[0]
            stats[s] = total - touched
        else:
            stats[s] = conn.execute(
                "SELECT COUNT(*) FROM contacts WHERE ステータス = ?", (s,)
            ).fetchone()[0]
    conn.close()
    return stats

# ── ルート ──────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    pref: str = "", city: str = "", keyword: str = "",
    industry: str = "", status: str = "", page: int = 1
):
    rows, total = search_companies(pref, city, keyword, industry, status, page)
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    prefs = get_prefs()
    stats = get_stats()
    _c = sqlite3.connect(DB_PATH) if DB_PATH.exists() else None
    db_ready = bool(_c and _c.execute("SELECT COUNT(*) FROM companies").fetchone()[0] > 0)
    if _c:
        _c.close()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "rows": rows, "total": total,
        "page": page, "total_pages": total_pages,
        "pref": pref, "city": city, "keyword": keyword,
        "industry": industry, "status": status,
        "prefs": prefs, "statuses": STATUSES, "stats": stats,
        "db_ready": db_ready,
    })

@app.get("/company/{company_id}", response_class=HTMLResponse)
async def company_detail(request: Request, company_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT c.*, COALESCE(ct.ステータス,'未対応') AS ステータス, "
        "COALESCE(ct.担当者,'') AS 担当者, COALESCE(ct.メール,'') AS メール, "
        "COALESCE(ct.電話,'') AS 電話, COALESCE(ct.業種,c.業種,'') AS 業種_ct, "
        "COALESCE(ct.メモ,'') AS メモ, COALESCE(ct.最終連絡日,'') AS 最終連絡日 "
        "FROM companies c LEFT JOIN contacts ct ON c.id = ct.company_id "
        "WHERE c.id = ?", (company_id,)
    ).fetchone()
    conn.close()
    if not row:
        return HTMLResponse("Not found", 404)
    return templates.TemplateResponse("company.html", {
        "request": request, "c": row, "statuses": STATUSES
    })

@app.post("/company/{company_id}/update")
async def update_company(
    company_id: int,
    status: str = Form("未対応"),
    担当者: str = Form(""),
    メール: str = Form(""),
    電話: str = Form(""),
    業種: str = Form(""),
    メモ: str = Form(""),
    最終連絡日: str = Form(""),
):
    conn = get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO contacts(company_id, ステータス, 担当者, メール, 電話, 業種, メモ, 最終連絡日, 更新日時)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(company_id) DO UPDATE SET
            ステータス=excluded.ステータス,
            担当者=excluded.担当者,
            メール=excluded.メール,
            電話=excluded.電話,
            業種=excluded.業種,
            メモ=excluded.メモ,
            最終連絡日=excluded.最終連絡日,
            更新日時=excluded.更新日時
    """, (company_id, status, 担当者, メール, 電話, 業種, メモ, 最終連絡日, now))
    conn.commit()
    conn.close()
    # Back to search
    referer = f"/company/{company_id}"
    return RedirectResponse(referer, status_code=303)

@app.post("/bulk-update")
async def bulk_update(
    ids: str = Form(""),
    status: str = Form(""),
    担当者: str = Form(""),
):
    if not ids or not status:
        return RedirectResponse("/", 303)
    conn = get_db()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for cid in ids.split(","):
        cid = cid.strip()
        if not cid.isdigit():
            continue
        conn.execute("""
            INSERT INTO contacts(company_id, ステータス, 担当者, 更新日時)
            VALUES(?,?,?,?)
            ON CONFLICT(company_id) DO UPDATE SET
                ステータス=excluded.ステータス,
                担当者=CASE WHEN excluded.担当者 != '' THEN excluded.担当者 ELSE 担当者 END,
                更新日時=excluded.更新日時
        """, (int(cid), status, 担当者, now))
    conn.commit()
    conn.close()
    return RedirectResponse("/", 303)

@app.get("/export")
async def export_csv(
    pref: str = "", city: str = "", keyword: str = "",
    industry: str = "", status: str = ""
):
    rows, _ = search_companies(pref, city, keyword, industry, status, page=1)
    # Get all (no pagination)
    conn = get_db()
    conditions, params = [], []
    if pref:
        conditions.append("c.都道府県 = ?"); params.append(pref)
    if city:
        conditions.append("c.市区町村 LIKE ?"); params.append(f"%{city}%")
    if keyword:
        conditions.append("c.名称 LIKE ?"); params.append(f"%{keyword}%")
    if industry:
        conditions.append("(c.業種 LIKE ? OR COALESCE(ct.業種,'') LIKE ?)")
        params.extend([f"%{industry}%", f"%{industry}%"])
    if status:
        if status == "未対応":
            conditions.append("ct.ステータス IS NULL")
        else:
            conditions.append("ct.ステータス = ?"); params.append(status)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    all_rows = conn.execute(f"""
        SELECT c.名称, c.都道府県, c.市区町村, c.丁目番地等,
               COALESCE(ct.業種, c.業種,'') AS 業種,
               COALESCE(ct.ステータス,'未対応') AS ステータス,
               COALESCE(ct.担当者,'') AS 担当者,
               COALESCE(ct.メール,'') AS メール,
               COALESCE(ct.電話,'') AS 電話,
               COALESCE(ct.最終連絡日,'') AS 最終連絡日,
               COALESCE(ct.メモ,'') AS メモ
        FROM companies c LEFT JOIN contacts ct ON c.id = ct.company_id
        {where} ORDER BY c.名称
    """, params).fetchall()
    conn.close()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["会社名","都道府県","市区町村","住所","業種","ステータス","担当者","メール","電話","最終連絡日","メモ"])
    for r in all_rows:
        w.writerow(list(r))

    output = "﻿" + buf.getvalue()  # BOM for Excel
    return StreamingResponse(
        iter([output.encode("utf-8")]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=sales_list.csv"}
    )

@app.get("/health")
def health():
    return {"status": "ok"}
