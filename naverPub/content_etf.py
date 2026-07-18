"""
액티브 ETF: ETF별 PDF 보유 비중 표.

날짜 스냅샷 규칙 (휴장 후퇴):
  - 전일   = 당일보다 엄격히 이전인 etf_pdf 영업일 중 최대
  - 1주전  = (당일 캘린더 -7일) 이하인 영업일 중 최대
            (그 날이 휴장이면 더 이전 영업일로 후퇴)
  - 2주전  = (당일 캘린더 -14일) 이하인 영업일 중 최대 (동일 후퇴)

컬럼: 순위|종목명|티커|{당일}|{전일}|{1주전}|{2주전}|전일대비|1주전대비|2주전대비|10일 상승률
"""
from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from db import engine

log = logging.getLogger("naverPub.content_etf")

CASH_RE = re.compile(
    r"원화현금|현금|예탁금|콜론|CALL\s*MONEY|Cash|CASH|원화예금",
    re.IGNORECASE,
)

CHG_PREV = "전일 대비"
CHG_1W = "1주 전 대비"
CHG_2W = "2주 전 대비"
RET_10D = "10일 상승률"


def _load_etf_targets() -> list[dict]:
    import json

    p = Path(__file__).resolve().parent / "etf_targets.json"
    if not p.is_file():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    out = []
    seen = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        tk = str(item.get("ticker", "")).strip()
        if not tk or tk in seen:
            continue
        if tk.isdigit():
            tk = tk.zfill(6)
        seen.add(tk)
        out.append(
            {
                "ticker": tk,
                "name": str(item.get("name", "")).strip() or tk,
                "sector": item.get("sector"),
            }
        )
    return out


def _pdf_dates_upto(eng, end: date, n: int = 60) -> list[date]:
    df = pd.read_sql(
        "SELECT DISTINCT date FROM etf_pdf WHERE date <= %s ORDER BY date DESC LIMIT %s",
        eng,
        params=(end, n),
    )
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["date"]).dt.date.tolist())


def _biz_on_or_before(available: list[date], target: date) -> Optional[date]:
    """
    target 당일 포함, available(오름차순 영업일)에서 target 이하 최대일.
    없으면 None. 휴장일이면 자연스럽게 이전 영업일로 후퇴.
    """
    cands = [d for d in available if d <= target]
    return cands[-1] if cands else None


def _prev_biz(available: list[date], cur: date) -> Optional[date]:
    """직전 영업일 = cur 미만 최대."""
    cands = [d for d in available if d < cur]
    return cands[-1] if cands else None


def resolve_etf_snapshot_dates(cur_d: date, available: list[date]) -> dict:
    """
    Returns keys: cur, prev, w1, w2 (Optional[date]).
    1주전 = cur-7d 이하 영업일, 2주전 = cur-14d 이하 영업일.
    """
    return {
        "cur": cur_d,
        "prev": _prev_biz(available, cur_d),
        "w1": _biz_on_or_before(available, cur_d - timedelta(days=7)),
        "w2": _biz_on_or_before(available, cur_d - timedelta(days=14)),
    }


def _load_etf_day(eng, etf_ticker: str, d: date) -> pd.DataFrame:
    df = pd.read_sql(
        """
        SELECT ticker, name, weight
        FROM etf_pdf
        WHERE etf_ticker = %s AND date = %s
        """,
        eng,
        params=(etf_ticker, d),
    )
    if df.empty:
        return df
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["name"] = df["name"].astype(str)
    return df


def _is_cash_row(name: object, ticker: object = None) -> bool:
    nm = "" if name is None or (isinstance(name, float) and np.isnan(name)) else str(name)
    if CASH_RE.search(nm):
        return True
    tk = "" if ticker is None else str(ticker).strip()
    if tk.upper() in ("CASH", "KRW", "CASH-KRW", "원화현금"):
        return True
    return False


def _weight_map(df: pd.DataFrame) -> dict[str, float]:
    if df is None or df.empty:
        return {}
    out = {}
    for r in df.itertuples(index=False):
        if _is_cash_row(r.name, r.ticker):
            continue
        if r.weight is None or (isinstance(r.weight, float) and not np.isfinite(r.weight)):
            continue
        out[str(r.ticker)] = float(r.weight)
    return out


def _name_map(df: pd.DataFrame) -> dict[str, str]:
    if df is None or df.empty:
        return {}
    out = {}
    for r in df.itertuples(index=False):
        if _is_cash_row(r.name, r.ticker):
            continue
        out[str(r.ticker)] = str(r.name) if r.name is not None else str(r.ticker)
    return out


def _ret_10d_map(eng, as_of: date, tickers: list[str]) -> dict[str, float]:
    """구성종목 최근 10거래일 수익률(%). close[t]/close[t-10]-1."""
    if not tickers:
        return {}
    dates = pd.read_sql(
        "SELECT DISTINCT date FROM ohlcv WHERE date <= %s ORDER BY date DESC LIMIT %s",
        eng,
        params=(as_of, 12),
    )
    if dates.empty or len(dates) < 11:
        return {}
    biz = sorted(pd.to_datetime(dates["date"]).dt.date.tolist())
    if biz[-1] != as_of:
        # as_of에 OHLCV가 없으면 맵 비움
        return {}
    base_d = biz[-11]  # 10거래일 전 종가
    out: dict[str, float] = {}
    for i in range(0, len(tickers), 400):
        chunk = tickers[i : i + 400]
        ph = ",".join(["%s"] * len(chunk))
        df = pd.read_sql(
            f"""
            SELECT ticker, date, close FROM ohlcv
            WHERE date IN (%s, %s) AND ticker IN ({ph})
            """,
            eng,
            params=(as_of, base_d, *chunk),
        )
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        piv = df.pivot_table(index="ticker", columns="date", values="close", aggfunc="last")
        if as_of not in piv.columns or base_d not in piv.columns:
            continue
        for tk, row in piv.iterrows():
            c0 = row.get(as_of)
            c1 = row.get(base_d)
            if c0 is None or c1 is None or not np.isfinite(c0) or not np.isfinite(c1) or c1 <= 0:
                continue
            out[str(tk)] = round((float(c0) / float(c1) - 1.0) * 100.0, 2)
    return out


def _fmt_date(d: Optional[date]) -> str:
    if d is None:
        return "-"
    return d.isoformat()


def _pad_ticker(tk: str) -> str:
    t = str(tk).strip()
    if t.isdigit():
        return t.zfill(6)
    return t


def _chg(cur_w: Optional[float], base_w: Optional[float], *, base_pdf_ok: bool, cur_pdf_ok: bool) -> float:
    """
    당일비중 − 기준일비중.
    PDF 자체가 없으면 nan('-').
    미보유는 0으로 간주해 변화량 산출 (편입=+당일, 편출=-기준).
    """
    if not base_pdf_ok or not cur_pdf_ok:
        return np.nan
    c = 0.0 if cur_w is None or not np.isfinite(cur_w) else float(cur_w)
    b = 0.0 if base_w is None or not np.isfinite(base_w) else float(base_w)
    return round(c - b, 2)


def _disp_weight(w: Optional[float], pdf_ok: bool) -> float:
    """표시용 비중: PDF 없거나 미보유 → nan('-')."""
    if not pdf_ok:
        return np.nan
    if w is None or not np.isfinite(w):
        return np.nan
    return float(w)


def build_etf_pdf_table(
    eng,
    etf_ticker: str,
    etf_name: str,
    snaps: dict,
) -> tuple[pd.DataFrame, dict]:
    """
    snaps: {cur, prev, w1, w2} Optional[date]
    Returns (df, meta).
    df columns (표시 순서):
      순위, 종목명, 티커, {cur}, {prev}, {w1}, {w2},
      전일 대비, 1주 전 대비, 2주 전 대비, 10일 상승률
    + _status (편입/편출/"", 렌더용)
    """
    cur_d = snaps["cur"]
    d_prev, d_w1, d_w2 = snaps["prev"], snaps["w1"], snaps["w2"]

    cur_df = _load_etf_day(eng, etf_ticker, cur_d)
    if cur_df.empty:
        return pd.DataFrame(), {
            "prev_available": False,
            "note": f"{_fmt_date(cur_d)} 데이터 없음",
            "footnotes": [f"{_fmt_date(cur_d)} 데이터 없음"],
            "col_dates": {},
        }

    def _day(d: Optional[date]):
        if d is None:
            return None, {}, {}, False
        df = _load_etf_day(eng, etf_ticker, d)
        ok = not df.empty
        return d, _weight_map(df) if ok else {}, _name_map(df) if ok else {}, ok

    _, w_cur, n_cur, ok_cur = _day(cur_d)
    _, w_prev, n_prev, ok_prev = _day(d_prev)
    _, w_w1, n_w1, ok_w1 = _day(d_w1)
    _, w_w2, n_w2, ok_w2 = _day(d_w2)

    col_cur = _fmt_date(cur_d)
    # 헤더는 실제 날짜. 날짜 객체가 없으면 플레이스홀더(데이터 전부 '-')
    col_prev = _fmt_date(d_prev) if d_prev else "전일(없음)"
    col_w1 = _fmt_date(d_w1) if d_w1 else "1주전(없음)"
    col_w2 = _fmt_date(d_w2) if d_w2 else "2주전(없음)"
    used = {col_cur}
    if col_prev in used:
        col_prev = f"{col_prev}(전일)"
    used.add(col_prev)
    if col_w1 in used:
        col_w1 = f"{col_w1}(1주전)"
    used.add(col_w1)
    if col_w2 in used:
        col_w2 = f"{col_w2}(2주전)"

    footnotes = []
    if d_prev is None or not ok_prev:
        footnotes.append(
            f"{_fmt_date(d_prev) if d_prev else '전일'} 데이터 없음"
            if d_prev
            else "전일 데이터 없음"
        )
    if d_w1 is None or not ok_w1:
        footnotes.append(
            f"{_fmt_date(d_w1) if d_w1 else '1주전'} 데이터 없음"
        )
    if d_w2 is None or not ok_w2:
        footnotes.append(
            f"{_fmt_date(d_w2) if d_w2 else '2주전'} 데이터 없음"
        )

    prev_available = bool(d_prev and ok_prev)

    tickers_all = set(w_cur) | set(w_w1) | set(w_w2)
    if prev_available:
        tickers_all |= set(w_prev)

    ret10_map = _ret_10d_map(eng, cur_d, list(tickers_all))

    active_rows = []
    exit_rows = []
    for tk in tickers_all:
        nm = n_cur.get(tk) or n_prev.get(tk) or n_w1.get(tk) or n_w2.get(tk) or tk
        if _is_cash_row(nm, tk):
            continue

        in_cur = tk in w_cur
        in_prev = tk in w_prev if prev_available else False
        wc = w_cur.get(tk) if in_cur else None
        wp = w_prev.get(tk) if in_prev else None
        w1 = w_w1.get(tk) if tk in w_w1 else None
        w2 = w_w2.get(tk) if tk in w_w2 else None

        status = ""
        if prev_available:
            if in_cur and not in_prev:
                status = "편입"
            elif in_prev and not in_cur:
                status = "편출"

        row = {
            "종목명": nm,
            "티커": _pad_ticker(tk),
            col_cur: _disp_weight(wc, ok_cur),
            col_prev: _disp_weight(wp, ok_prev),
            col_w1: _disp_weight(w1, ok_w1),
            col_w2: _disp_weight(w2, ok_w2),
            CHG_PREV: _chg(wc, wp, base_pdf_ok=ok_prev, cur_pdf_ok=ok_cur),
            CHG_1W: _chg(wc, w1, base_pdf_ok=ok_w1, cur_pdf_ok=ok_cur),
            CHG_2W: _chg(wc, w2, base_pdf_ok=ok_w2, cur_pdf_ok=ok_cur),
            RET_10D: ret10_map.get(tk, np.nan),
            "_status": status,
            "_w0": float(wc) if (wc is not None and np.isfinite(wc)) else -1e9,
        }
        # 편출만 전일대비가 있어도 당일 표시는 '-'
        if status == "편출":
            row[col_cur] = np.nan
            exit_rows.append(row)
        else:
            active_rows.append(row)

    active_rows.sort(key=lambda r: r["_w0"], reverse=True)
    exit_rows.sort(key=lambda r: abs(r.get(CHG_PREV) or 0), reverse=True)

    rows = []
    rank = 0
    for r in active_rows:
        if np.isfinite(r.get("_w0", np.nan)) and r["_w0"] > 0:
            rank += 1
            r["순위"] = rank
        else:
            r["순위"] = pd.NA
        rows.append(r)
    for r in exit_rows:
        r["순위"] = pd.NA
        rows.append(r)

    if not rows:
        return pd.DataFrame(), {
            "prev_available": prev_available,
            "note": "구성종목 없음",
            "footnotes": footnotes,
            "col_dates": {},
        }

    out = pd.DataFrame(rows)
    cols = [
        "순위",
        "종목명",
        "티커",
        col_cur,
        col_prev,
        col_w1,
        col_w2,
        CHG_PREV,
        CHG_1W,
        CHG_2W,
        RET_10D,
        "_status",
    ]
    out = out[cols]
    out["순위"] = out["순위"].astype("Int64")

    note = None if prev_available else "전일 데이터 없음"
    meta = {
        "prev_available": prev_available,
        "date_cur": cur_d,
        "date_prev": d_prev,
        "date_w1": d_w1,
        "date_w2": d_w2,
        "col_cur": col_cur,
        "col_prev": col_prev,
        "col_w1": col_w1,
        "col_w2": col_w2,
        "ok_prev": ok_prev,
        "ok_w1": ok_w1,
        "ok_w2": ok_w2,
        "note": note,
        "footnotes": footnotes,
        "n_in": int((out["_status"] == "편입").sum()),
        "n_out": int((out["_status"] == "편출").sum()),
    }
    return out, meta


def build_all_etf(as_of: Optional[date] = None) -> dict:
    eng = engine()
    if as_of is None:
        mx = pd.read_sql("SELECT MAX(date) AS d FROM etf_pdf", eng)
        if mx.empty or pd.isna(mx.iloc[0]["d"]):
            return {
                "by_etf": [],
                "prev_available": False,
                "note": "전일 데이터 없음",
                "date_cur": None,
                "date_prev": None,
            }
        as_of = pd.to_datetime(mx.iloc[0]["d"]).date()

    dates = _pdf_dates_upto(eng, as_of, 60)
    if not dates:
        return {
            "by_etf": [],
            "prev_available": False,
            "note": "전일 데이터 없음",
            "date_cur": None,
            "date_prev": None,
        }

    cur_d = as_of if as_of in dates else dates[-1]
    snaps = resolve_etf_snapshot_dates(cur_d, dates)
    prev_available = snaps["prev"] is not None

    if not prev_available:
        log.warning("ETF PDF 전일 데이터 없음 (기준=%s) — 편입/편출 판정 생략", cur_d)

    targets = _load_etf_targets()
    if not targets:
        db_etfs = pd.read_sql(
            """
            SELECT DISTINCT etf_ticker AS ticker, etf_name AS name
            FROM etf_pdf WHERE date = %s ORDER BY etf_ticker
            """,
            eng,
            params=(cur_d,),
        )
        targets = [
            {"ticker": str(r.ticker), "name": str(r.name), "sector": None}
            for r in db_etfs.itertuples(index=False)
        ]

    by_etf = []
    for t in targets:
        tk = t["ticker"]
        df, meta = build_etf_pdf_table(eng, tk, t["name"], snaps)
        if df.empty:
            continue
        by_etf.append(
            {
                "etf_ticker": tk,
                "etf_name": t["name"],
                "sector": t.get("sector"),
                "df": df,
                **{k: meta.get(k) for k in (
                    "date_cur", "date_prev", "date_w1", "date_w2",
                    "col_cur", "col_prev", "col_w1", "col_w2",
                    "prev_available", "note", "footnotes",
                    "n_in", "n_out", "ok_prev", "ok_w1", "ok_w2",
                )},
            }
        )

    note = None if prev_available else "전일 데이터 없음"
    log.info(
        "ETF PDF 표 %d종 (당일=%s, 전일=%s, 1주전=%s, 2주전=%s)%s",
        len(by_etf),
        snaps["cur"],
        snaps["prev"],
        snaps["w1"],
        snaps["w2"],
        f" [{note}]" if note else "",
    )
    return {
        "by_etf": by_etf,
        "prev_available": prev_available,
        "note": note,
        "date_cur": snaps["cur"],
        "date_prev": snaps["prev"],
        "date_w1": snaps["w1"],
        "date_w2": snaps["w2"],
        "snaps": snaps,
    }
