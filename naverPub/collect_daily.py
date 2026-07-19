"""
일별 수집: 전종목 OHLCV(MDCSTAT01501) + 지수 + 액티브 ETF PDF + RS/talent.
종목별 개별 크롤링 금지 — 하루 CSV 1장으로 전종목 커버.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

import krx_client as krx
from config import SLEEP_SEC
from db import connect, ensure_schema, engine

log = logging.getLogger("naverPub.collect")

PERIODS = [10, 20, 50, 120, 200]
TALENT_WINDOW = 120
TALENT_UP = 0.10


def _num(s: pd.Series) -> pd.Series:
    """숫자 파싱. 단독 '-' 만 결측, 부호(-1.23)는 유지."""
    t = s.astype(str).str.replace(",", "", regex=False).str.strip()
    t = t.mask(t.isin(["-", "", "nan", "None", "NaN", "<NA>"]))
    return pd.to_numeric(t, errors="coerce")


def _pad_ticker(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    num = t.str.fullmatch(r"\d+", na=False)
    return t.where(~num, t.str.zfill(6))


def parse_ohlcv_csv(df: pd.DataFrame, day_str: str) -> pd.DataFrame:
    """KRX [12001] CSV → ohlcv 적재용 DataFrame."""
    d = df.copy()
    d.columns = d.columns.str.replace(" ", "")
    rename = {
        "종목코드": "ticker",
        "종목명": "name",
        "시장구분": "market",
        "시가": "open",
        "고가": "high",
        "저가": "low",
        "종가": "close",
        "거래량": "volume",
        "거래대금": "trading_value",
        "시가총액": "mcap",
        "등락률": "chg_pct",
    }
    d = d.rename(columns={k: v for k, v in rename.items() if k in d.columns})
    need = ["ticker", "close"]
    for c in need:
        if c not in d.columns:
            raise ValueError(f"OHLCV CSV에 '{c}' 컬럼 없음: {list(d.columns)}")
    d["ticker"] = _pad_ticker(d["ticker"])
    for c in ("open", "high", "low", "close", "volume", "trading_value", "mcap", "chg_pct"):
        if c in d.columns:
            d[c] = _num(d[c])
        else:
            d[c] = np.nan
    if "name" not in d.columns:
        d["name"] = None
    if "market" not in d.columns:
        d["market"] = None
    else:
        d["market"] = d["market"].astype(str).str.strip()
        d["market"] = d["market"].replace(
            {"유가증권": "KOSPI", "KOSPI": "KOSPI", "코스닥": "KOSDAQ", "KOSDAQ": "KOSDAQ"}
        )
        # 이미 'KOSPI'/'KOSDAQ'/'KONEX' 등인 경우 유지
        d.loc[d["market"].str.contains("코스닥|KOSDAQ", case=False, na=False), "market"] = "KOSDAQ"
        d.loc[d["market"].str.contains("유가|KOSPI", case=False, na=False), "market"] = "KOSPI"
    d["date"] = datetime.strptime(day_str, "%Y%m%d").date()
    return d[
        [
            "ticker",
            "date",
            "name",
            "market",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trading_value",
            "mcap",
            "chg_pct",
        ]
    ].dropna(subset=["ticker", "close"])


def upsert_ohlcv(df: pd.DataFrame) -> dict:
    """
    Returns {"total": N, "inserted": N, "updated": N}
    inserted = 해당일에 없던 티커, updated = 이미 있던 티커(재적재).
    """
    empty = {"total": 0, "inserted": 0, "updated": 0}
    if df is None or df.empty:
        return empty

    day = df["date"].iloc[0]
    tickers = [str(t) for t in df["ticker"].tolist()]
    existing: set[str] = set()
    con = connect()
    try:
        with con.cursor() as cur:
            # 기존 적재 티커 조회 (배치)
            for i in range(0, len(tickers), 500):
                chunk = tickers[i : i + 500]
                ph = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"SELECT ticker FROM ohlcv WHERE date=%s AND ticker IN ({ph})",
                    (day, *chunk),
                )
                existing.update(str(r[0]) for r in cur.fetchall())

        inserted = sum(1 for t in tickers if t not in existing)
        updated = len(tickers) - inserted

        sql = """
        INSERT INTO ohlcv
          (ticker, date, name, market, open, high, low, close, volume, trading_value, mcap, chg_pct)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          name=VALUES(name), market=VALUES(market),
          open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
          volume=VALUES(volume), trading_value=VALUES(trading_value),
          mcap=VALUES(mcap), chg_pct=VALUES(chg_pct)
        """
        rows = []
        for r in df.itertuples(index=False):
            rows.append(
                tuple(None if (isinstance(v, float) and np.isnan(v)) else v for v in r)
            )
        with con.cursor() as cur:
            for i in range(0, len(rows), 1000):
                cur.executemany(sql, rows[i : i + 1000])
            cur.executemany(
                """
                INSERT INTO tickers (ticker, name, market, updated_at)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  name=VALUES(name), market=VALUES(market), updated_at=VALUES(updated_at)
                """,
                [
                    (r.ticker, r.name, r.market, r.date)
                    for r in df.itertuples(index=False)
                ],
            )
        con.commit()
        return {"total": len(rows), "inserted": inserted, "updated": updated}
    finally:
        con.close()


def upsert_index(
    ticker: str,
    day: date,
    close: float,
    open_: Optional[float] = None,
    high: Optional[float] = None,
    low: Optional[float] = None,
) -> None:
    con = connect()
    try:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO index_ohlcv (ticker, date, open, high, low, close)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  close=VALUES(close),
                  open=COALESCE(VALUES(open), open),
                  high=COALESCE(VALUES(high), high),
                  low=COALESCE(VALUES(low), low)
                """,
                (ticker, day, open_, high, low, close),
            )
        con.commit()
    finally:
        con.close()


def _preview_bytes(content: bytes, n: int = 200) -> str:
    try:
        return content[:n].decode("EUC-KR", errors="replace")
    except Exception:
        return repr(content[:n])


def _parse_index_ohlc_df(df: pd.DataFrame) -> pd.DataFrame:
    """MDCSTAT00301 CSV → date/open/high/low/close."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = out.columns.str.replace(" ", "", regex=False)
    date_col = next((c for c in ("일자", "날짜", "date") if c in out.columns), None)
    if date_col is None or "종가" not in out.columns:
        return pd.DataFrame()
    rows = pd.DataFrame(
        {
            "date": pd.to_datetime(out[date_col], errors="coerce").dt.date,
            "close": _num(out["종가"]),
            "open": _num(out["시가"]) if "시가" in out.columns else np.nan,
            "high": _num(out["고가"]) if "고가" in out.columns else np.nan,
            "low": _num(out["저가"]) if "저가" in out.columns else np.nan,
        }
    )
    rows = rows.dropna(subset=["date", "close"])
    return rows.sort_values("date").reset_index(drop=True)


def collect_index(session, day_str: str) -> None:
    """KOSPI(1001)·KOSDAQ(2001) OHLC 적재. MDCSTAT00301."""
    day = datetime.strptime(day_str, "%Y%m%d").date()
    specs = [
        ("1001", "1", "001", "KOSPI"),
        ("2001", "2", "001", "KOSDAQ"),
    ]
    for code, ind, ind2, label in specs:
        try:
            df, raw = krx.download_index_csv(session, day_str, ind, ind2)
            parsed = _parse_index_ohlc_df(df)
            if parsed.empty:
                log.warning(
                    "지수 %s 응답 비어 있음 (bytes=%d). 원문 앞200자: %s",
                    label,
                    len(raw) if raw else 0,
                    _preview_bytes(raw or b""),
                )
                continue
            row = parsed.iloc[-1]
            close = float(row["close"])
            if not np.isfinite(close):
                log.warning(
                    "지수 %s 종가 파싱 실패. 원문 앞200자: %s",
                    label,
                    _preview_bytes(raw or b""),
                )
                continue

            def _opt(v):
                try:
                    x = float(v)
                    return x if np.isfinite(x) else None
                except (TypeError, ValueError):
                    return None

            upsert_index(
                code,
                day,
                close,
                open_=_opt(row.get("open")),
                high=_opt(row.get("high")),
                low=_opt(row.get("low")),
            )
            log.info(
                "지수 %s(%s) O=%.2f H=%.2f L=%.2f C=%.2f",
                label,
                code,
                _opt(row.get("open")) or float("nan"),
                _opt(row.get("high")) or float("nan"),
                _opt(row.get("low")) or float("nan"),
                close,
            )
        except Exception as e:
            log.warning("지수 %s 수집 실패: %s", label, e)


def index_ohl_needs_backfill(lookback_calendar_days: int = 280) -> bool:
    """최근 구간에 close는 있으나 open이 NULL인 행이 있으면 True."""
    eng = engine()
    df = pd.read_sql(
        """
        SELECT COUNT(*) AS c FROM index_ohlcv
        WHERE date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
          AND close IS NOT NULL
          AND (open IS NULL OR high IS NULL OR low IS NULL)
        """,
        eng,
        params=(lookback_calendar_days,),
    )
    return int(df.iloc[0]["c"] or 0) > 0


def backfill_index_ohl(session, end_day: date, calendar_days: int = 560) -> dict:
    """
    코스피/코스닥 지수 OHL 일회성 백필.
    MDCSTAT00301 기간 조회(최근 calendar_days ≈ 370거래일+) → 기존 행 UPDATE/INSERT.
    캔들 MA120 워밍업(250+120)에 충분한 기간.
    """
    from datetime import timedelta

    start_day = end_day - timedelta(days=calendar_days)
    start_str = start_day.strftime("%Y%m%d")
    end_str = end_day.strftime("%Y%m%d")
    specs = [
        ("1001", "1", "001", "KOSPI"),
        ("2001", "2", "001", "KOSDAQ"),
    ]
    out = {"start": start_str, "end": end_str, "rows": {}}
    for code, ind, ind2, label in specs:
        try:
            df, raw = krx.download_index_csv(
                session, end_str, ind, ind2, start_str=start_str
            )
            parsed = _parse_index_ohlc_df(df)
            n = 0
            for r in parsed.itertuples(index=False):
                close = float(r.close)
                if not np.isfinite(close):
                    continue

                def _opt(v):
                    try:
                        x = float(v)
                        return x if np.isfinite(x) else None
                    except (TypeError, ValueError):
                        return None

                upsert_index(
                    code,
                    r.date,
                    close,
                    open_=_opt(r.open),
                    high=_opt(r.high),
                    low=_opt(r.low),
                )
                n += 1
            out["rows"][label] = n
            log.info("지수 OHL 백필 %s: %d행 (%s~%s)", label, n, start_str, end_str)
            time.sleep(SLEEP_SEC)
        except Exception as e:
            out["rows"][label] = 0
            log.warning("지수 OHL 백필 %s 실패: %s", label, e)
    return out


def load_etf_targets(path: Optional[Path] = None) -> list[dict]:
    """
    etf_targets.json → [{"sector", "ticker", "name"}, ...]
    형식 오류 항목은 스킵. sector 없으면 None.
    """
    import json

    p = path if path else Path(__file__).resolve().parent / "etf_targets.json"
    if not p.is_file():
        log.warning("etf_targets.json 없음: %s", p)
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("etf_targets.json 읽기 실패: %s", e)
        return []
    if not isinstance(data, list):
        log.warning("etf_targets.json 형식 오류(list 필요)")
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        tk = str(item.get("ticker", "")).strip()
        if not tk:
            continue
        if tk.isdigit():
            tk = tk.zfill(6)
        if tk in seen:
            continue
        seen.add(tk)
        sector = item.get("sector")
        if sector is not None:
            sector = str(sector).strip() or None
        nm = str(item.get("name", "")).strip() or tk
        out.append({"sector": sector, "ticker": tk, "name": nm})
    return out


def _date_has_rows(table: str, day: date, date_col: str = "date") -> bool:
    con = connect()
    try:
        with con.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM `{table}` WHERE `{date_col}` = %s LIMIT 1",
                (day,),
            )
            return cur.fetchone() is not None
    finally:
        con.close()


def db_coverage_for_day(day: date) -> dict[str, bool]:
    """기준일 데이터 존재 여부 (OHLCV·지수·PDF)."""
    return {
        "ohlcv": _date_has_rows("ohlcv", day),
        "index": _date_has_rows("index_ohlcv", day),
        "pdf": _date_has_rows("etf_pdf", day),
    }


def ohlcv_needs_enrichment(day: date) -> bool:
    """기준일 trading_value/mcap/market 중 NULL이 있으면 True."""
    con = connect()
    try:
        with con.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) AS n,
                  SUM(trading_value IS NULL) AS tv_null,
                  SUM(mcap IS NULL) AS mcap_null,
                  SUM(market IS NULL OR market='') AS mkt_null
                FROM ohlcv WHERE date=%s
                """,
                (day,),
            )
            row = cur.fetchone()
            if not row or not row[0]:
                return False
            return int(row[1] or 0) > 0 or int(row[2] or 0) > 0 or int(row[3] or 0) > 0
    finally:
        con.close()


def enrich_ohlcv_fields_from_krx(session, day_str: str) -> dict:
    """
    MDCSTAT01501 재수신 → 기준일 NULL 필드만 UPDATE로 채움 (이미 값 있으면 유지).
    tickers.market 갱신 후, 이력 ohlcv의 market NULL을 tickers로 백필.
    """
    stats = {"updated": 0, "market_backfill": 0, "downloaded": 0}
    raw_df, raw_bytes = krx.download_ohlcv_csv(session, day_str)
    if raw_df is None or raw_df.empty:
        log.warning(
            "OHLCV enrich %s KRX 0행 (bytes=%d) %s",
            day_str,
            len(raw_bytes) or 0,
            _preview_bytes(raw_bytes or b""),
        )
        return stats
    parsed = parse_ohlcv_csv(raw_df, day_str)
    stats["downloaded"] = len(parsed)
    if parsed.empty:
        return stats

    day = parsed["date"].iloc[0]
    con = connect()
    try:
        sql = """
        UPDATE ohlcv SET
          trading_value = COALESCE(trading_value, %s),
          mcap = COALESCE(mcap, %s),
          market = COALESCE(NULLIF(market,''), %s),
          name = COALESCE(NULLIF(name,''), %s),
          chg_pct = COALESCE(%s, chg_pct)
        WHERE ticker=%s AND date=%s
          AND (
            trading_value IS NULL OR mcap IS NULL
            OR market IS NULL OR market='' OR name IS NULL OR name=''
            OR chg_pct IS NULL
            OR %s IS NOT NULL
          )
        """
        rows = []
        ticker_rows = []
        now = datetime.now()
        for r in parsed.itertuples(index=False):
            tv = None if (isinstance(r.trading_value, float) and np.isnan(r.trading_value)) else r.trading_value
            mc = None if (isinstance(r.mcap, float) and np.isnan(r.mcap)) else r.mcap
            chg = None if (isinstance(r.chg_pct, float) and np.isnan(r.chg_pct)) else r.chg_pct
            mkt = None if (r.market is None or str(r.market) in ("", "nan", "None")) else str(r.market)
            nm = None if (r.name is None or str(r.name) in ("", "nan", "None")) else str(r.name)
            # chg twice: SET COALESCE(%s,...) and WHERE ... OR %s IS NOT NULL (부호 보정 덮어쓰기)
            rows.append((tv, mc, mkt, nm, chg, str(r.ticker), day, chg))
            ticker_rows.append((str(r.ticker), nm, mkt, now))

        with con.cursor() as cur:
            for i in range(0, len(rows), 1000):
                cur.executemany(sql, rows[i : i + 1000])
            stats["updated"] = cur.rowcount if len(rows) <= 1000 else len(rows)
            cur.executemany(
                """
                INSERT INTO tickers (ticker, name, market, updated_at)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  name=COALESCE(VALUES(name), name),
                  market=COALESCE(VALUES(market), market),
                  updated_at=VALUES(updated_at)
                """,
                ticker_rows,
            )
            # 이관분 등 이력 market NULL → tickers 매핑으로 일괄 백필
            cur.execute(
                """
                UPDATE ohlcv o
                INNER JOIN tickers t ON o.ticker = t.ticker
                SET o.market = t.market
                WHERE (o.market IS NULL OR o.market = '')
                  AND t.market IS NOT NULL AND t.market <> ''
                """
            )
            stats["market_backfill"] = cur.rowcount
        con.commit()
        log.info(
            "OHLCV enrich %s download=%d field_upd≈%d market_hist_backfill=%d",
            day_str,
            stats["downloaded"],
            stats["updated"],
            stats["market_backfill"],
        )
    finally:
        con.close()
    return stats


def backfill_etf_pdf_sectors() -> int:
    """etf_targets.json 섹터로 etf_pdf.sector NULL 행 일괄 UPDATE."""
    targets = load_etf_targets()
    if not targets:
        return 0
    con = connect()
    try:
        n = 0
        with con.cursor() as cur:
            for item in targets:
                sec = item.get("sector")
                tk = item.get("ticker")
                if not sec or not tk:
                    continue
                cur.execute(
                    """
                    UPDATE etf_pdf SET sector=%s
                    WHERE etf_ticker=%s AND (sector IS NULL OR sector='')
                    """,
                    (sec, tk),
                )
                n += cur.rowcount
        con.commit()
        if n:
            log.info("etf_pdf sector 백필 %d행 (etf_targets.json)", n)
        return n
    finally:
        con.close()


def ensure_derived_metrics(target: date, force_talent: bool = True) -> dict:
    """
    원천 수집 스킵과 무관하게, 기준일 rs/talent 없으면 계산.
    talent는 정의 변경(일수) 반영을 위해 기본 재계산.
    """
    out = {"rs_rows": 0, "talent_rows": 0, "errors": []}
    if not _date_has_rows("rs", target):
        try:
            out["rs_rows"] = compute_rs_for_date(target)
            log.info("파생 RS 계산 %s → %d행", target, out["rs_rows"])
        except Exception as e:
            out["errors"].append(f"rs: {e}")
            log.warning("RS 오류: %s", e)
    else:
        log.info("RS %s 이미 존재 → 스킵", target)
    if force_talent or not _date_has_rows("talent", target):
        try:
            out["talent_rows"] = compute_talent_for_date(target)
            log.info("파생 talent 계산 %s → %d행", target, out["talent_rows"])
        except Exception as e:
            out["errors"].append(f"talent: {e}")
            log.warning("Talent 오류: %s", e)
    else:
        log.info("talent %s 이미 존재 → 스킵", target)
    return out


def upsert_etf_pdf(rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO etf_pdf
      (etf_ticker, etf_name, sector, date, ticker, name, shares, amount, mcap, weight)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      etf_name=VALUES(etf_name), sector=VALUES(sector), name=VALUES(name),
      shares=VALUES(shares), amount=VALUES(amount),
      mcap=VALUES(mcap), weight=VALUES(weight)
    """
    con = connect()
    try:
        with con.cursor() as cur:
            cur.executemany(sql, rows)
        con.commit()
        return len(rows)
    finally:
        con.close()


def collect_etf_pdf(session, day_str: str, targets: list[dict]) -> int:
    """etf_targets 목록만 수집. ISIN 없거나 응답 빈 경우 스킵. sector 없으면 NULL."""
    day = datetime.strptime(day_str, "%Y%m%d").date()
    try:
        isin_map = krx.build_isin_map(session)
    except Exception as e:
        log.warning("ISIN 맵 실패: %s", e)
        isin_map = {}
    total = 0
    for item in targets:
        code = item.get("ticker") or ""
        name = item.get("name") or code
        sector = item.get("sector")  # may be None
        if not code:
            continue
        isin = isin_map.get(code)
        if not isin:
            isin = isin_map.get(code.upper()) or isin_map.get(code.lower())
        if not isin:
            log.warning("ISIN 없음 → 스킵: %s %s", code, name)
            continue
        try:
            pdf = krx.fetch_pdf(session, isin, day_str)
        except Exception as e:
            log.warning("PDF 실패 %s → 스킵: %s", code, e)
            continue
        if pdf is None or pdf.empty:
            log.info("PDF 빈 응답 → 스킵: %s", code)
            continue
        rows = []
        for _, r in pdf.iterrows():
            tk = str(r.get("티커", "")).strip()
            if tk.isdigit():
                tk = tk.zfill(6)
            w = r.get("시가총액기준 구성비중")
            try:
                w = float(str(w).replace(",", "")) if w is not None else None
            except (TypeError, ValueError):
                w = None

            def _f(x):
                try:
                    return (
                        float(str(x).replace(",", ""))
                        if x is not None and str(x) not in ("", "-")
                        else None
                    )
                except (TypeError, ValueError):
                    return None

            rows.append(
                (
                    code,
                    name,
                    sector,
                    day,
                    tk,
                    r.get("구성종목명"),
                    _f(r.get("계약수")),
                    _f(r.get("금액")),
                    _f(r.get("시가총액")),
                    w,
                )
            )
        n = upsert_etf_pdf(rows)
        total += n
        log.info("PDF [%s] %s %s → %d행", sector or "-", code, name, n)
        time.sleep(SLEEP_SEC)
    return total


# ── RS (벡터화, ffill limit=20) ────────────────────────────────

def _period_returns(close_df: pd.DataFrame, period: int) -> pd.DataFrame:
    past = close_df.shift(period)
    ret = (close_df / past - 1.0) * 100.0
    return ret.where(close_df.notna() & past.notna() & (past > 0))


def _index_period_returns(index_close: pd.Series, period: int) -> pd.Series:
    past = index_close.shift(period)
    ret = (index_close / past - 1.0) * 100.0
    valid = index_close.notna() & past.notna() & (past > 0)
    out = ret.where(valid, 0.0)
    return out.where(index_close.notna())


def _percentile_rank_rs(momentum: pd.DataFrame) -> pd.DataFrame:
    ranks = momentum.rank(axis=1, method="min", pct=True) * 100.0
    valid_n = momentum.notna().sum(axis=1)
    zero_n = (momentum == 0).sum(axis=1)
    all_zero = (valid_n > 0) & (zero_n == valid_n)
    if all_zero.any():
        fill50 = pd.DataFrame(50.0, index=momentum.index, columns=momentum.columns)
        ranks = ranks.mask(all_zero, fill50).where(momentum.notna())
    return ranks


def compute_market_rs_rows(
    close_wide: pd.DataFrame,
    index_close: pd.Series,
    process_dates: list,
    market_type: str,
    periods: list,
) -> list:
    if close_wide.empty or not process_dates or index_close.empty:
        return []
    idx_dates = set(index_close.dropna().index)
    proc = [d for d in process_dates if d in close_wide.index and d in idx_dates]
    if not proc:
        return []
    close_raw = close_wide
    valid_mask = close_raw.loc[proc].notna()
    close_filled = close_raw.ffill(limit=20)
    rs_long = {}
    for period in periods:
        past = close_filled.shift(period)
        past_ok = past.notna() & (past > 0)
        stock_ret = _period_returns(close_filled, period)
        idx_ret = _index_period_returns(index_close, period)
        rel = stock_ret.sub(idx_ret, axis=0)
        rel = rel.where(past_ok, 0.0)
        rel = rel.where(close_raw.notna())
        rs = _percentile_rank_rs(rel.loc[proc])
        rs_long[period] = rs.where(valid_mask).stack().dropna()
    if not rs_long or rs_long[periods[0]].empty:
        return []
    merged = pd.DataFrame(rs_long).reset_index()
    merged.columns = ["date", "ticker"] + list(periods)
    return [
        (ticker, d, market_type, float(r10), float(r20), float(r50), float(r120), float(r200))
        for d, ticker, r10, r20, r50, r120, r200 in merged[
            ["date", "ticker", 10, 20, 50, 120, 200]
        ].itertuples(index=False, name=None)
    ]


def save_rs_rows(rows: list) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO rs (ticker, date, market_type, rs_10, rs_20, rs_50, rs_120, rs_200)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      market_type=VALUES(market_type),
      rs_10=VALUES(rs_10), rs_20=VALUES(rs_20), rs_50=VALUES(rs_50),
      rs_120=VALUES(rs_120), rs_200=VALUES(rs_200)
    """
    con = connect()
    try:
        with con.cursor() as cur:
            for i in range(0, len(rows), 2000):
                cur.executemany(sql, rows[i : i + 2000])
        con.commit()
        return len(rows)
    finally:
        con.close()


def compute_rs_for_date(target: date) -> int:
    """target 일자 RS 계산·upsert. 최소 lookback 220거래일 로드."""
    eng = engine()
    lookback_dates = pd.read_sql(
        """
        SELECT DISTINCT date FROM ohlcv
        WHERE date <= %s ORDER BY date DESC LIMIT 220
        """,
        eng,
        params=(target,),
    )
    if lookback_dates.empty:
        return 0
    lookback_dates["date"] = pd.to_datetime(lookback_dates["date"]).dt.date
    load_start = lookback_dates["date"].min()

    ohlcv = pd.read_sql(
        """
        SELECT ticker, date, close, market FROM ohlcv
        WHERE date >= %s AND date <= %s
        """,
        eng,
        params=(load_start, target),
    )
    if ohlcv.empty:
        return 0
    ohlcv["date"] = pd.to_datetime(ohlcv["date"]).dt.date
    ohlcv["ticker"] = ohlcv["ticker"].astype(str)

    idx = pd.read_sql(
        """
        SELECT ticker, date, close FROM index_ohlcv
        WHERE ticker IN ('1001','2001') AND date >= %s AND date <= %s
        """,
        eng,
        params=(load_start, target),
    )
    if idx.empty:
        log.warning("index_ohlcv 없음 — RS 스킵")
        return 0
    idx["date"] = pd.to_datetime(idx["date"]).dt.date

    total = 0
    for market, idx_code in (("KOSPI", "1001"), ("KOSDAQ", "2001")):
        sub = ohlcv[ohlcv["market"] == market]
        if sub.empty:
            # market 미매핑 시 전체로 fallback 하지 않음
            continue
        close_wide = sub.pivot(index="date", columns="ticker", values="close").sort_index()
        ix = (
            idx.loc[idx["ticker"] == idx_code]
            .set_index("date")["close"]
            .sort_index()
            .astype(float)
        )
        rows = compute_market_rs_rows(close_wide, ix, [target], market, PERIODS)
        total += save_rs_rows(rows)
        log.info("RS %s %s → %d행", market, target, len(rows))
    return total


def compute_talent_for_date(target: date) -> int:
    eng = engine()
    lookback = pd.read_sql(
        """
        SELECT DISTINCT date FROM ohlcv
        WHERE date <= %s ORDER BY date DESC LIMIT %s
        """,
        eng,
        params=(target, TALENT_WINDOW + 5),
    )
    if lookback.empty:
        return 0
    lookback["date"] = pd.to_datetime(lookback["date"]).dt.date
    load_start = lookback["date"].min()
    df = pd.read_sql(
        """
        SELECT ticker, date, open, close FROM ohlcv
        WHERE date >= %s AND date <= %s
        """,
        eng,
        params=(load_start, target),
    )
    if df.empty:
        return 0
    df["date"] = pd.to_datetime(df["date"]).dt.date
    rows = []
    for tk, g in df.groupby("ticker"):
        g = g.sort_values("date").tail(TALENT_WINDOW)
        op = pd.to_numeric(g["open"], errors="coerce")
        cl = pd.to_numeric(g["close"], errors="coerce")
        m = op.notna() & cl.notna() & (op > 0)
        if not m.any():
            continue
        r = (cl[m] / op[m]) - 1.0
        tal = int((r >= TALENT_UP).sum())
        rows.append((str(tk), target, float(tal)))
    if not rows:
        return 0
    con = connect()
    try:
        with con.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO talent (ticker, date, talent_pct)
                VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE talent_pct=VALUES(talent_pct)
                """,
                rows,
            )
        con.commit()
        return len(rows)
    finally:
        con.close()


def collect_daily(
    session=None,
    day_str: Optional[str] = None,
    force: bool = False,
) -> dict:
    """
    일별 수집 오케스트레이션.
    - 기준일은 업종분류(종가 유효)로만 판별. force도 휴장일을 기준일로 쓰지 않음.
    - force: 휴장이어도 전거래일 기준으로 콘텐츠 재생성(수집은 미적재분만).
    - 이미 DB에 있는 날짜의 OHLCV/지수/PDF는 각각 스킵.
    """
    ensure_schema()
    own_session = session is None
    if own_session:
        session = krx.login_session()

    today = date.today().strftime("%Y%m%d")
    result = {
        "biz_day": None,
        "today": today,
        "holiday": False,
        "need_collect": False,
        "skipped": False,
        "ohlcv_downloaded": 0,
        "ohlcv_rows": 0,
        "ohlcv_inserted": 0,
        "ohlcv_updated": 0,
        "pdf_rows": 0,
        "rs_rows": 0,
        "talent_rows": 0,
        "coverage": {},
        "errors": [],
    }
    try:
        if day_str and day_str.isdigit() and len(day_str) == 8:
            try:
                if krx._probe_sector_biz_day(session, day_str):
                    biz = day_str
                else:
                    log.warning("--date %s 는 업종분류 무효 → 최근 유효 거래일로 대체", day_str)
                    biz = krx.find_latest_biz_day(session)
            except Exception:
                biz = krx.find_latest_biz_day(session)
        else:
            biz = krx.find_latest_biz_day(session)

        result["biz_day"] = biz
        result["holiday"] = krx.is_holiday_today(biz)
        target = datetime.strptime(biz, "%Y%m%d").date()
        cov = db_coverage_for_day(target)
        result["coverage"] = cov
        need = not (cov["ohlcv"] and cov["index"] and cov["pdf"])
        result["need_collect"] = need

        log.info(
            "오늘=%s, 기준 거래일=%s, 수집 필요 여부=%s (ohlcv=%s index=%s pdf=%s)%s",
            today,
            biz,
            "Y" if need else "N",
            "Y" if cov["ohlcv"] else "N",
            "Y" if cov["index"] else "N",
            "Y" if cov["pdf"] else "N",
            " [휴장→전거래일]" if result["holiday"] else "",
        )
        _ = force  # reserved; force는 아래 enrich/콘텐츠 재생성에 사용

        if not need:
            result["skipped"] = True
            log.info("이미 적재됨(%s) — OHLCV·지수·PDF 수집 스킵 (콘텐츠는 DB 기준)", biz)
        else:
            if cov["ohlcv"]:
                log.info("OHLCV %s 이미 적재됨 → 스킵", biz)
            else:
                raw_df, raw_bytes = krx.download_ohlcv_csv(session, biz)
                downloaded = 0 if raw_df is None else len(raw_df)
                result["ohlcv_downloaded"] = downloaded
                if downloaded == 0:
                    log.warning(
                        "OHLCV %s KRX 응답 0행 (bytes=%d). 원문 앞200자: %s",
                        biz,
                        len(raw_bytes) if raw_bytes else 0,
                        _preview_bytes(raw_bytes or b""),
                    )
                    result["errors"].append(f"ohlcv: KRX empty for {biz}")
                else:
                    ohlcv = parse_ohlcv_csv(raw_df, biz)
                    parsed = len(ohlcv)
                    if parsed == 0:
                        log.warning(
                            "OHLCV %s 다운로드 %d행 → 파싱 후 0행. cols=%s",
                            biz,
                            downloaded,
                            list(raw_df.columns) if raw_df is not None else [],
                        )
                    stats = upsert_ohlcv(ohlcv)
                    result["ohlcv_rows"] = stats["total"]
                    result["ohlcv_inserted"] = stats["inserted"]
                    result["ohlcv_updated"] = stats["updated"]
                    log.info(
                        "OHLCV %s 다운로드=%d 파싱=%d → upsert 신규=%d 갱신=%d",
                        biz,
                        downloaded,
                        parsed,
                        stats["inserted"],
                        stats["updated"],
                    )

            if cov["index"]:
                log.info("지수 %s 이미 적재됨 → 스킵", biz)
            else:
                try:
                    collect_index(session, biz)
                except Exception as e:
                    result["errors"].append(f"index: {e}")
                    log.warning("지수 수집 오류: %s", e)

            if cov["pdf"]:
                log.info("ETF PDF %s 이미 적재됨 → 스킵", biz)
            else:
                try:
                    etfs = load_etf_targets()
                    log.info("ETF PDF 대상 %d종 (etf_targets.json)", len(etfs))
                    result["pdf_rows"] = collect_etf_pdf(session, biz, etfs)
                except Exception as e:
                    result["errors"].append(f"etf_pdf: {e}")
                    log.warning("ETF PDF 오류: %s", e)

        # 과거 close-only 행 → OHL 백필 (일회성, 스킵 경로에서도 실행)
        try:
            if force or index_ohl_needs_backfill():
                bf = backfill_index_ohl(session, target, calendar_days=560)
                result["index_ohl_backfill"] = bf
                log.info("지수 OHL 백필 완료: %s", bf.get("rows"))
        except Exception as e:
            result["errors"].append(f"index_ohl_backfill: {e}")
            log.warning("지수 OHL 백필 오류: %s", e)

        # 이관분 등: 원천 행은 있어도 tv/mcap/market NULL → KRX로 보정
        # --force 시 chg_pct 부호 보정(과거 '-' 제거 파서)을 위해 enrich 재실행
        if cov["ohlcv"] or result.get("ohlcv_rows"):
            if force or ohlcv_needs_enrichment(target):
                try:
                    enr = enrich_ohlcv_fields_from_krx(session, biz)
                    result["ohlcv_enriched"] = enr
                except Exception as e:
                    result["errors"].append(f"ohlcv_enrich: {e}")
                    log.warning("OHLCV 필드 보정 오류: %s", e)

        try:
            result["sector_backfill"] = backfill_etf_pdf_sectors()
        except Exception as e:
            result["errors"].append(f"sector_backfill: {e}")
            log.warning("ETF sector 백필 오류: %s", e)

        # 원천 스킵과 무관 — rs/talent 없으면 반드시 계산
        derived = ensure_derived_metrics(target)
        result["rs_rows"] = derived["rs_rows"]
        result["talent_rows"] = derived["talent_rows"]
        result["errors"].extend(derived.get("errors") or [])

        return result
    finally:
        if own_session:
            session.close()
