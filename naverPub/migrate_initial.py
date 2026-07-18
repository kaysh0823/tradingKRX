"""
로컬 kor_stock_db → VPS naverpub 최초 이관 헬퍼.

권장: mysqldump/CSV (deploy/README.md 명령어).
이 스크립트는 CSV export/import 경로를 제공합니다.
최소 400거래일 ohlcv 이력이 RS·신고가 계산에 필요합니다.
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import (
    LOCAL_DB_HOST,
    LOCAL_DB_NAME,
    LOCAL_DB_PASSWORD,
    LOCAL_DB_PORT,
    LOCAL_DB_USER,
    db_config,
)
from db import connect, ensure_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("migrate")

# 로컬 테이블 → (export SQL 컬럼 매핑 힌트)
LOCAL_OHLCV = "krx_ohlcv"
LOCAL_PDF = "krx_etf_pdf"
LOCAL_INDEX = "krx_index_ohlcv"


def local_connect():
    return pymysql.connect(
        host=LOCAL_DB_HOST,
        port=LOCAL_DB_PORT,
        user=LOCAL_DB_USER,
        passwd=LOCAL_DB_PASSWORD,
        db=LOCAL_DB_NAME,
        charset="utf8mb4",
    )


def export_ohlcv_csv(out: Path, min_rows_hint: int = 400) -> Path:
    """로컬 krx_ohlcv → CSV (최근 충분 이력)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    con = local_connect()
    try:
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT ticker, date, open, high, low, close, volume,
                       close*volume AS trading_value, NULL AS mcap, NULL AS chg_pct,
                       NULL AS name, NULL AS market
                FROM `{LOCAL_OHLCV}`
                WHERE date >= (
                    SELECT d FROM (
                        SELECT DISTINCT date AS d FROM `{LOCAL_OHLCV}`
                        ORDER BY date DESC LIMIT {int(min_rows_hint) + 20}
                    ) t ORDER BY d ASC LIMIT 1
                )
                ORDER BY date, ticker
                """
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        log.info("export ohlcv %s rows → %s", len(rows), out)
    finally:
        con.close()
    return out


def export_pdf_csv(out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    con = local_connect()
    try:
        with con.cursor() as cur:
            # 한글 컬럼명 대응
            cur.execute(f"SHOW COLUMNS FROM `{LOCAL_PDF}`")
            cols_info = [r[0] for r in cur.fetchall()]
            log.info("local pdf columns: %s", cols_info)
            cur.execute(f"SELECT * FROM `{LOCAL_PDF}` ORDER BY 1, 2")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        log.info("export pdf %s rows → %s", len(rows), out)
    finally:
        con.close()
    return out


def export_index_csv(out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    con = local_connect()
    try:
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT ticker, date, close FROM `{LOCAL_INDEX}`
                WHERE ticker IN ('1001','2001')
                ORDER BY date, ticker
                """
            )
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(rows)
        log.info("export index %s rows → %s", len(rows), out)
    finally:
        con.close()
    return out


BATCH_SIZE = 1000


def _nan_to_none(df):
    """pymysql은 NaN을 받지 않으므로 None으로 치환."""
    import pandas as pd

    return df.astype(object).where(pd.notnull(df), None)


def _pad_code(v) -> object:
    if v is None:
        return None
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    if s.isdigit():
        return s.zfill(6)
    return s


def _executemany_batches(con, sql: str, rows: list, label: str) -> int:
    if not rows:
        return 0
    n = 0
    with con.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            cur.executemany(sql, batch)
            con.commit()
            n += len(batch)
            log.info("… %s %s / %s", label, n, len(rows))
    return n


def import_ohlcv_csv(path: Path) -> int:
    ensure_schema()
    import pandas as pd

    df = pd.read_csv(path)
    df = _nan_to_none(df)
    rows = []
    for r in df.to_dict(orient="records"):
        tk = r.get("ticker")
        rows.append(
            (
                _pad_code(tk) if tk is not None else None,
                r.get("date"),
                r.get("name"),
                r.get("market"),
                r.get("open"),
                r.get("high"),
                r.get("low"),
                r.get("close"),
                r.get("volume"),
                r.get("trading_value"),
                r.get("mcap"),
                r.get("chg_pct"),
            )
        )
    sql = """
    INSERT INTO ohlcv
      (ticker, date, name, market, open, high, low, close,
       volume, trading_value, mcap, chg_pct)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      open=VALUES(open), high=VALUES(high), low=VALUES(low),
      close=VALUES(close), volume=VALUES(volume),
      trading_value=VALUES(trading_value)
    """
    con = connect()
    try:
        n = _executemany_batches(con, sql, rows, "ohlcv")
    finally:
        con.close()
    log.info("import ohlcv %s rows", n)
    return n


def import_index_csv(path: Path) -> int:
    ensure_schema()
    import pandas as pd

    df = pd.read_csv(path)
    df = _nan_to_none(df)
    rows = [
        (str(r["ticker"]) if r.get("ticker") is not None else None, r.get("date"), r.get("close"))
        for r in df.to_dict(orient="records")
    ]
    sql = """
    INSERT INTO index_ohlcv (ticker, date, close)
    VALUES (%s,%s,%s)
    ON DUPLICATE KEY UPDATE close=VALUES(close)
    """
    con = connect()
    try:
        n = _executemany_batches(con, sql, rows, "index")
    finally:
        con.close()
    log.info("import index %s rows", n)
    return n


def import_pdf_csv(path: Path) -> int:
    """로컬 한글 컬럼 CSV를 etf_pdf 스키마로 매핑."""
    ensure_schema()
    import pandas as pd

    df = pd.read_csv(path)
    colmap = {
        "ETF코드": "etf_ticker",
        "ETF명": "etf_name",
        "수집일자": "date",
        "티커": "ticker",
        "구성종목명": "name",
        "계약수": "shares",
        "금액": "amount",
        "시가총액": "mcap",
        "시가총액기준 구성비중": "weight",
        "비중": "weight",
    }
    df = df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})
    df = _nan_to_none(df)
    rows = []
    for r in df.to_dict(orient="records"):
        rows.append(
            (
                _pad_code(r.get("etf_ticker")),
                r.get("etf_name"),
                r.get("date"),
                r.get("ticker"),
                r.get("name"),
                r.get("shares"),
                r.get("amount"),
                r.get("mcap"),
                r.get("weight"),
            )
        )
    sql = """
    INSERT INTO etf_pdf
      (etf_ticker, etf_name, date, ticker, name, shares, amount, mcap, weight)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE weight=VALUES(weight), name=VALUES(name)
    """
    con = connect()
    try:
        n = _executemany_batches(con, sql, rows, "pdf")
    finally:
        con.close()
    log.info("import pdf %s rows", n)
    return n


def main():
    p = argparse.ArgumentParser()
    p.add_argument("action", choices=["export", "import-ohlcv", "import-index", "import-pdf", "export-all"])
    p.add_argument("--dir", default=str(ROOT / "migrate_data"))
    p.add_argument("--file", help="import 대상 CSV")
    p.add_argument("--min-days", type=int, default=400)
    args = p.parse_args()
    d = Path(args.dir)
    d.mkdir(parents=True, exist_ok=True)

    if args.action == "export":
        export_ohlcv_csv(d / "ohlcv.csv", args.min_days)
    elif args.action == "export-all":
        export_ohlcv_csv(d / "ohlcv.csv", args.min_days)
        export_index_csv(d / "index_ohlcv.csv")
        export_pdf_csv(d / "etf_pdf.csv")
    elif args.action == "import-ohlcv":
        import_ohlcv_csv(Path(args.file or d / "ohlcv.csv"))
    elif args.action == "import-index":
        import_index_csv(Path(args.file or d / "index_ohlcv.csv"))
    elif args.action == "import-pdf":
        import_pdf_csv(Path(args.file or d / "etf_pdf.csv"))


if __name__ == "__main__":
    main()
