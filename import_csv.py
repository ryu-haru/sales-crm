"""
import_csv.py - houjin_companies.csv を sales.db にインポート（初回のみ）
Usage: python import_csv.py [--csv /path/to/houjin_companies.csv]
"""
import csv, sqlite3, sys, time, argparse, os
from pathlib import Path
from database import DB_PATH, init_db

# 社名から業種を推定するキーワードマップ
INDUSTRY_MAP = [
    (["システム","ソフト","IT","テック","デジタル","ネット","ウェブ","クラウド","AI","DX"], "IT・ソフトウェア"),
    (["建設","建築","工務","土木","設備","施工","リフォーム","内装"], "建設・建築"),
    (["不動産","住宅","マンション","アパート","仲介","賃貸"], "不動産"),
    (["医院","病院","クリニック","歯科","調剤","薬局","介護","福祉","ケア"], "医療・介護"),
    (["学校","塾","教育","予備校","スクール","研修","保育"], "教育"),
    (["飲食","レストラン","カフェ","食堂","居酒屋","ラーメン","寿司","弁当"], "飲食"),
    (["製造","工業","金属","鉄鋼","化学","プラスチック","部品","機械"], "製造"),
    (["物流","運送","倉庫","配送","トラック","フォワード"], "物流・運送"),
    (["保険","生命","損害","金融","銀行","証券","投資","ファンド","FP"], "金融・保険"),
    (["広告","マーケ","PR","プロモ","デザイン","クリエイティブ","印刷"], "広告・マーケティング"),
    (["人材","採用","派遣","リクルート","就職","転職","HR"], "人材"),
    (["コンサル","経営","会計","税理","監査","法律","行政書"], "コンサル・士業"),
    (["小売","販売","ショップ","ストア","商事","商社"], "商社・小売"),
    (["通信","テレコム","モバイル","携帯","光通信"], "通信"),
    (["農業","農産","林業","漁業","畜産"], "農林水産"),
    (["旅行","ホテル","観光","宿泊","トラベル"], "旅行・ホテル"),
    (["エネルギー","電力","ガス","石油","再生可能"], "エネルギー"),
]

def guess_industry(name: str) -> str:
    for keywords, industry in INDUSTRY_MAP:
        if any(k in name for k in keywords):
            return industry
    return ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=os.environ.get("CSV_PATH", r"C:\Users\Owner\.company\secretary\notes\houjin_companies.csv"))
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        sys.exit(1)

    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-256000")  # 256MB cache

    # Check if already imported
    count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    if count > 0:
        print(f"Already imported: {count:,} rows. Skipping.")
        conn.close()
        return

    print(f"Importing {csv_path} ...")
    start = time.time()
    batch = []
    total = 0
    BATCH_SIZE = 10000

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("名称", "").strip()
            if not name:
                continue
            industry = guess_industry(name)
            batch.append((
                row.get("法人番号", ""),
                name,
                row.get("都道府県", ""),
                row.get("市区町村", ""),
                row.get("丁目番地等", ""),
                industry,
            ))
            if len(batch) >= BATCH_SIZE:
                conn.executemany(
                    "INSERT INTO companies(法人番号,名称,都道府県,市区町村,丁目番地等,業種) VALUES(?,?,?,?,?,?)",
                    batch
                )
                conn.commit()
                total += len(batch)
                batch = []
                elapsed = time.time() - start
                print(f"  {total:,}件 ({elapsed:.0f}s)", end="\r")

    if batch:
        conn.executemany(
            "INSERT INTO companies(法人番号,名称,都道府県,市区町村,丁目番地等,業種) VALUES(?,?,?,?,?,?)",
            batch
        )
        conn.commit()
        total += len(batch)

    elapsed = time.time() - start
    print(f"\n完了: {total:,}社 / {elapsed:.1f}秒")
    conn.close()

if __name__ == "__main__":
    main()
