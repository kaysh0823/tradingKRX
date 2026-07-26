"""
시장 스냅샷 (지표별 · 코스피→코스닥):
1) 거래대금순위  2) 에너지배율  3) RS Top50
4) 주가위치 Top50  5) Talent Top50  6) 신고가
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from db import engine

log = logging.getLogger("naverPub.content_market")

ENERGY_COLORS = {
    ">=3": "#c62828",
    ">=1.5": "#f57f17",
    ">=0.7": "#2e7d32",
    ">=0.3": "#1565c0",
    "else": "#9e9e9e",
}
# 방향(상승/하락) tanh 가중: energy × (1 + tanh(수익률%/K))
ENERGY_DIR_K = 15.0

TALENT_UP = 0.10
TALENT_MCAP_MIN = 500_000_000_000  # 시총 5,000억원 이상
MCAP_COL = "시총(조원)"
TALENT_UD_COLS = ("20일 ↑/↓", "50일 ↑/↓", "120일 ↑/↓")
MARKETS = ("KOSPI", "KOSDAQ")
MARKET_LABELS = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}
PRICE_POS_WINDOW = 120  # 정렬·기본 컬럼(고가·저가)
PRICE_POS_COL = "주가위치"  # = 120일 (고가·저가)
PRICE_POS_CLOSE_WINDOWS = (20, 50)  # 종가 기준 추가 컬럼
HIGH_WINDOWS = [200, 120, 50]  # 종가 신고가 최장구간 우선
RET_3D_COL = "3일상승률"


def energy_ratio_font_color(er: float) -> str:
    if not np.isfinite(er):
        return ENERGY_COLORS["else"]
    if er >= 3.0:
        return ENERGY_COLORS[">=3"]
    if er >= 1.5:
        return ENERGY_COLORS[">=1.5"]
    if er >= 0.7:
        return ENERGY_COLORS[">=0.7"]
    if er >= 0.3:
        return ENERGY_COLORS[">=0.3"]
    return ENERGY_COLORS["else"]


def _latest_date(eng) -> Optional[date]:
    df = pd.read_sql("SELECT MAX(date) AS d FROM ohlcv", eng)
    if df.empty or pd.isna(df.iloc[0]["d"]):
        return None
    return pd.to_datetime(df.iloc[0]["d"]).date()


def _trading_dates(eng, end: date, n: int) -> list[date]:
    df = pd.read_sql(
        "SELECT DISTINCT date FROM ohlcv WHERE date <= %s ORDER BY date DESC LIMIT %s",
        eng,
        params=(end, n),
    )
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["date"]).dt.date.tolist())


def diagnose_source_data(as_of: date) -> dict:
    """콘텐츠 생성 전 기준일 소스 현황 (로그/점검용)."""
    eng = engine()
    out: dict = {"as_of": str(as_of)}

    def _scalar(sql: str, params=()):
        df = pd.read_sql(sql, eng, params=params)
        if df.empty:
            return 0
        return int(df.iloc[0, 0] or 0)

    out["ohlcv_rows"] = _scalar("SELECT COUNT(*) AS c FROM ohlcv WHERE date=%s", (as_of,))
    out["tv_nn"] = _scalar(
        "SELECT COUNT(*) AS c FROM ohlcv WHERE date=%s AND trading_value IS NOT NULL",
        (as_of,),
    )
    out["mcap_nn"] = _scalar(
        "SELECT COUNT(*) AS c FROM ohlcv WHERE date=%s AND mcap IS NOT NULL",
        (as_of,),
    )
    out["market_nn"] = _scalar(
        "SELECT COUNT(*) AS c FROM ohlcv WHERE date=%s AND market IN ('KOSPI','KOSDAQ')",
        (as_of,),
    )
    out["rs_rows"] = _scalar("SELECT COUNT(*) AS c FROM rs WHERE date=%s", (as_of,))
    out["etf_pdf_rows"] = _scalar("SELECT COUNT(*) AS c FROM etf_pdf WHERE date=%s", (as_of,))
    out["etf_sector_null"] = _scalar(
        """
        SELECT COUNT(*) AS c FROM etf_pdf
        WHERE date=%s AND (sector IS NULL OR sector='')
        """,
        (as_of,),
    )
    log.info(
        "소스진단 %s | ohlcv=%s tv_nn=%s mcap_nn=%s market_nn=%s rs=%s etf_pdf=%s sector_null=%s",
        as_of,
        out["ohlcv_rows"],
        out["tv_nn"],
        out["mcap_nn"],
        out["market_nn"],
        out["rs_rows"],
        out["etf_pdf_rows"],
        out["etf_sector_null"],
    )
    return out


def _load_ohlcv_range(
    eng,
    load_start: date,
    end: date,
    cols: str,
    prefer_listed: bool = True,
) -> pd.DataFrame:
    """
    prefer_listed=True면 KOSPI/KOSDAQ 우선.
    이관 데이터처럼 market이 전부 NULL이면 필터 없이 로드 (신고가 등).
    """
    sql_listed = f"""
        SELECT {cols} FROM ohlcv
        WHERE date >= %s AND date <= %s
          AND market IN ('KOSPI','KOSDAQ')
    """
    df = pd.read_sql(sql_listed, eng, params=(load_start, end))
    if not df.empty or not prefer_listed:
        return df
    return pd.read_sql(
        f"SELECT {cols} FROM ohlcv WHERE date >= %s AND date <= %s",
        eng,
        params=(load_start, end),
    )


def _apply_day_chg(day0: pd.DataFrame, prev: pd.DataFrame) -> pd.Series:
    """
    당일상승률(%):
    1) KRX 등락률(chg_pct)이 있고 전일종가 대비 부호·규모가 일치하면 그대로
    2) 아니면 (당일종가/전일종가 - 1) * 100
    """
    m = day0.merge(
        prev.rename(columns={"close": "prev_close"})[["ticker", "prev_close"]],
        on="ticker",
        how="left",
    )
    calc = np.where(
        m["prev_close"].notna() & (m["prev_close"] > 0) & m["close"].notna(),
        (m["close"] / m["prev_close"] - 1.0) * 100.0,
        np.nan,
    )
    raw = pd.to_numeric(m["chg_pct"], errors="coerce")
    # 과거 파서 버그로 부호가 빠진 경우 → 계산값 우선
    use_raw = (
        raw.notna()
        & np.isfinite(calc)
        & (np.sign(raw.fillna(0)) == np.sign(pd.Series(calc).fillna(0)))
        & (np.abs(raw - calc) < 0.15)
    )
    # 전일 없어 계산 불가하면 raw 사용
    use_raw = use_raw | (raw.notna() & ~np.isfinite(calc))
    out = np.where(use_raw, raw, calc)
    return pd.Series(out, index=day0.index)


def _select_cols(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    """
    preferred에 있는 컬럼만, preferred 순서로 반환.
    date/market/chg_pct/trading_value/mcap 등 raw 컬럼은 노출하지 않음.
    """
    if df is None or df.empty:
        return df
    cols: list[str] = []
    for c in preferred:
        if c in df.columns and c not in cols:
            cols.append(c)
    return df[cols]


def _close_map_on(df: pd.DataFrame, d: date) -> dict:
    if df.empty or d is None:
        return {}
    sub = df[df["date"] == d][["ticker", "close"]].drop_duplicates("ticker")
    return {str(r["ticker"]): float(r["close"]) for _, r in sub.iterrows() if pd.notna(r["close"])}


def _n_day_ret_pct(cur_close, base_close) -> float:
    """기준일 종가 / N거래일전 종가 − 1 (%). 불가 시 nan."""
    try:
        c = float(cur_close)
        b = float(base_close)
    except (TypeError, ValueError):
        return np.nan
    if not np.isfinite(c) or not np.isfinite(b) or b <= 0:
        return np.nan
    return round((c / b - 1.0) * 100.0, 2)


def build_tv_rank(
    as_of: Optional[date] = None,
    top_n: int = 50,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """해당 시장 당일 거래대금 내림차순 TopN (+ 당일·3일 상승률)."""
    eng = engine()
    as_of = as_of or _latest_date(eng)
    if as_of is None:
        return pd.DataFrame()
    dates = _trading_dates(eng, as_of, 4)  # 당일+전일+3일전
    if len(dates) < 1:
        return pd.DataFrame()
    d0 = dates[-1]
    prev_d = dates[-2] if len(dates) >= 2 else None
    d3 = dates[0] if len(dates) >= 4 else None

    df = _load_ohlcv_range(
        eng,
        dates[0],
        d0,
        "ticker, date, name, market, close, chg_pct, trading_value, mcap",
    )
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ("close", "chg_pct", "trading_value", "mcap"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if market:
        df = df[df["market"] == market]
        if df.empty:
            return pd.DataFrame()

    day0 = df[df["date"] == d0].copy()
    if day0.empty:
        return pd.DataFrame()

    if prev_d is not None:
        prev = df[df["date"] == prev_d][["ticker", "close"]].copy()
        day0["당일상승률"] = _apply_day_chg(day0, prev).values
    else:
        day0["당일상승률"] = day0["chg_pct"]

    base3 = _close_map_on(df, d3) if d3 is not None else {}
    day0[RET_3D_COL] = [
        _n_day_ret_pct(r["close"], base3.get(str(r["ticker"]))) for _, r in day0.iterrows()
    ]

    top = day0.nlargest(top_n, "trading_value").copy()
    top = top.sort_values("trading_value", ascending=False, na_position="last").reset_index(drop=True)
    top.insert(0, "순위", range(1, len(top) + 1))
    top["거래대금(억)"] = (top["trading_value"] / 1e8).round(1)
    top[MCAP_COL] = (top["mcap"] / 1e12).round(2)
    out = top.rename(columns={"ticker": "티커", "name": "종목명", "close": "현재가"})
    return _select_cols(
        out,
        [
            "순위",
            "티커",
            "종목명",
            "현재가",
            "당일상승률",
            RET_3D_COL,
            "거래대금(억)",
            MCAP_COL,
        ],
    )


def build_energy_rank(
    as_of: Optional[date] = None,
    top_tv: int = 50,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    해당 시장 당일 거래대금 상위 top_tv → 방향반영 3일 에너지배율 내림차순.
    에너지배율 = (거래대금 시장내 비중) / (시총 시장내 비중)
    방향반영 = energy × (1 + tanh(수익률% / ENERGY_DIR_K))
      · 당일: 당일등락률(%)
      · 3일: 기준일종가 / 3거래일전 종가 - 1 (%)
    market 미지정 시 KOSPI+KOSDAQ 합산(하위 호환, 비권장).
    """
    eng = engine()
    as_of = as_of or _latest_date(eng)
    if as_of is None:
        return pd.DataFrame()
    # 최근 3거래일(대금 합) + 3거래일전 종가(누적수익률 분모) → 최대 4일
    dates4 = _trading_dates(eng, as_of, 4)
    if len(dates4) < 1:
        return pd.DataFrame()
    d0 = dates4[-1]
    days = dates4[-3:] if len(dates4) >= 3 else dates4
    load_start = dates4[0]
    prev_d = dates4[-2] if len(dates4) >= 2 else None
    base_d = dates4[0] if len(dates4) >= 4 else None  # 3거래일 전

    df = _load_ohlcv_range(
        eng,
        load_start,
        d0,
        "ticker, date, name, market, close, chg_pct, trading_value, mcap",
    )
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ("close", "chg_pct", "trading_value", "mcap"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "market" not in df.columns:
        df["market"] = "ALL"
    df["market"] = df["market"].fillna("ALL").replace("", "ALL")
    if market:
        df = df[df["market"] == market]
        if df.empty:
            return pd.DataFrame()

    day0 = df[df["date"] == d0].copy()
    if day0.empty:
        return pd.DataFrame()

    if prev_d is not None:
        prev = df[df["date"] == prev_d][["ticker", "close"]].copy()
        day0["당일상승률"] = _apply_day_chg(day0, prev).values
    else:
        day0["당일상승률"] = day0["chg_pct"]

    mkt_tv = day0.groupby("market")["trading_value"].transform("sum")
    mkt_mcap = day0.groupby("market")["mcap"].transform("sum")
    day0["tv_pct"] = np.where(mkt_tv > 0, day0["trading_value"] / mkt_tv * 100.0, np.nan)
    day0["mcap_pct"] = np.where(
        (mkt_mcap > 0) & day0["mcap"].notna(),
        day0["mcap"] / mkt_mcap * 100.0,
        np.nan,
    )
    day0["energy_1d"] = np.where(
        day0["mcap_pct"] > 0,
        day0["tv_pct"] / day0["mcap_pct"],
        np.nan,
    )
    day0["tv_rank"] = (
        day0.groupby("market")["trading_value"].rank(ascending=False, method="min").astype(int)
    )

    tv3 = (
        df[df["date"].isin(days)]
        .groupby(["ticker", "market"], as_index=False)["trading_value"]
        .sum()
        .rename(columns={"trading_value": "tv_3d"})
    )
    mkt_tv3 = (
        df[df["date"].isin(days)].groupby("market")["trading_value"].sum().rename("mkt_tv_3d")
    )
    day0 = day0.merge(tv3, on=["ticker", "market"], how="left")
    day0 = day0.merge(mkt_tv3, on="market", how="left")
    day0["tv_3d_pct"] = np.where(
        day0["mkt_tv_3d"] > 0, day0["tv_3d"] / day0["mkt_tv_3d"] * 100.0, np.nan
    )
    day0["energy_3d"] = np.where(
        day0["mcap_pct"] > 0,
        day0["tv_3d_pct"] / day0["mcap_pct"],
        np.nan,
    )

    # --- 방향 반영 (방법A: tanh, K=ENERGY_DIR_K) ---
    ret_1d = pd.to_numeric(day0["당일상승률"], errors="coerce")
    dir_1d = np.tanh(ret_1d / ENERGY_DIR_K)
    dir_1d = dir_1d.where(np.isfinite(dir_1d), 0.0)
    day0["energy_1d"] = day0["energy_1d"] * (1.0 + dir_1d)

    # 3일 누적수익률(%) = 기준일종가 / 3거래일전 종가 - 1. 분모 없으면 방향계수=0
    if base_d is not None:
        base_closes = (
            df[df["date"] == base_d][["ticker", "close"]]
            .drop_duplicates("ticker")
            .rename(columns={"close": "_close_3d_base"})
        )
        day0 = day0.merge(base_closes, on="ticker", how="left")
    else:
        day0["_close_3d_base"] = np.nan
    base_ok = (
        day0["_close_3d_base"].notna()
        & (day0["_close_3d_base"] > 0)
        & day0["close"].notna()
    )
    ret_3d = np.where(
        base_ok,
        (day0["close"] / day0["_close_3d_base"] - 1.0) * 100.0,
        np.nan,
    )
    dir_3d = np.where(
        np.isfinite(ret_3d),
        np.tanh(np.asarray(ret_3d, dtype=float) / ENERGY_DIR_K),
        0.0,
    )
    day0["energy_3d"] = day0["energy_3d"] * (1.0 + dir_3d)
    day0[RET_3D_COL] = [
        round(float(x), 2) if x is not None and np.isfinite(x) else np.nan for x in ret_3d
    ]
    day0 = day0.drop(columns=["_close_3d_base"], errors="ignore")

    # 시장 내 거래대금 상위 → 방향반영 3일 에너지배율 순
    top = day0.nlargest(top_tv, "trading_value").copy()
    top = top.sort_values("energy_3d", ascending=False, na_position="last").reset_index(drop=True)
    top.insert(0, "순위", range(1, len(top) + 1))
    top["거래대금(억)"] = (top["trading_value"] / 1e8).round(1)
    top["거래대금순위"] = top["tv_rank"].astype(int)
    top["시총(조원)"] = (top["mcap"] / 1e12).round(2)
    top["당일에너지배율"] = pd.to_numeric(top["energy_1d"], errors="coerce").round(2)
    top["3일에너지배율"] = pd.to_numeric(top["energy_3d"], errors="coerce").round(2)
    out = top.rename(
        columns={
            "ticker": "티커",
            "name": "종목명",
            "close": "현재가",
        }
    )
    return _select_cols(
        out,
        [
            "순위",
            "티커",
            "종목명",
            "현재가",
            "당일상승률",
            RET_3D_COL,
            "거래대금(억)",
            "거래대금순위",
            "당일에너지배율",
            "3일에너지배율",
            "시총(조원)",
        ],
    )


def _build_new_highs_df(
    as_of: Optional[date],
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    종가 신고가: 당일 종가가 직전 N거래일 종가 최고를 경신.
    최장 달성 구간만 표시 + 구간대비(%·종가 최저 대비). market 지정 시 해당 시장만.
    """
    eng = engine()
    as_of = as_of or _latest_date(eng)
    if as_of is None:
        return pd.DataFrame()
    days = _trading_dates(eng, as_of, 260)
    if len(days) < 50:
        return pd.DataFrame()
    load_start = days[0]
    df = _load_ohlcv_range(
        eng,
        load_start,
        as_of,
        "ticker, date, name, market, close, chg_pct, trading_value, mcap",
        prefer_listed=True,
    )
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ("close", "chg_pct", "trading_value", "mcap"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if market and "market" in df.columns:
        df = df[df["market"] == market]
        if df.empty:
            return pd.DataFrame()

    prev_d = days[-2] if len(days) >= 2 else None
    prev_map = {}
    if prev_d is not None:
        prev_map = df[df["date"] == prev_d].set_index("ticker")["close"].to_dict()

    rows = []
    windows = list(HIGH_WINDOWS)
    for tk, g in df.groupby("ticker"):
        g = g.sort_values("date")
        if g["date"].iloc[-1] != as_of:
            continue
        cur_c = g["close"].iloc[-1]
        if not np.isfinite(cur_c):
            continue

        label = None
        win = None
        for w in windows:
            hist = g.iloc[:-1].tail(w)
            if len(hist) < w:
                continue
            if cur_c > float(hist["close"].max()):
                label = f"{w}일 신고가"
                win = w
                break
        if label is None or win is None:
            continue

        period = g.tail(win)
        base = float(period["close"].min())
        period_ret = (cur_c / base - 1.0) * 100.0 if base > 0 else np.nan

        prev_c = prev_map.get(tk)
        if prev_c and prev_c > 0:
            day_chg = (cur_c / float(prev_c) - 1.0) * 100.0
        else:
            day_chg = g["chg_pct"].iloc[-1]
        if day_chg is not None and np.isfinite(day_chg):
            day_chg = round(float(day_chg), 2)

        ret_3d = np.nan
        if len(g) >= 4:
            c3 = float(g["close"].iloc[-4])
            ret_3d = _n_day_ret_pct(cur_c, c3)

        last = g.iloc[-1]
        mcap = last.get("mcap")
        mcap_jo = (
            round(float(mcap) / 1e12, 2)
            if mcap is not None and np.isfinite(float(mcap or np.nan))
            else np.nan
        )
        rows.append(
            {
                "티커": tk,
                "종목명": last["name"],
                "현재가": last["close"],
                "당일상승률": day_chg,
                RET_3D_COL: ret_3d,
                "거래대금(억)": round(float(last["trading_value"] or 0) / 1e8, 1),
                "달성구간": label,
                "구간대비(%)": round(float(period_ret), 2) if np.isfinite(period_ret) else np.nan,
                "시총(조원)": mcap_jo,
                "_win": float(win),
            }
        )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out = out.sort_values("_win", ascending=False).drop(columns=["_win"]).reset_index(drop=True)
    return _select_cols(
        out,
        [
            "티커",
            "종목명",
            "현재가",
            "당일상승률",
            RET_3D_COL,
            "거래대금(억)",
            "달성구간",
            "구간대비(%)",
            "시총(조원)",
        ],
    )


def build_new_highs(as_of: Optional[date] = None, market: Optional[str] = None) -> pd.DataFrame:
    return _build_new_highs_df(as_of, market=market)


def _daily_ret_flags(close: pd.Series) -> tuple[pd.Series, pd.Series]:
    """일간 등락률 기준 +10%↑ / -10%↓ 여부."""
    cl = pd.to_numeric(close, errors="coerce")
    prev = cl.shift(1)
    ret = (cl / prev) - 1.0
    valid = prev.notna() & (prev > 0) & cl.notna()
    up = (ret >= TALENT_UP) & valid
    down = (ret <= -TALENT_UP) & valid
    return up, down


def build_rs_rank(
    as_of: Optional[date] = None,
    top_n: int = 50,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    RS(rs_20~200 산술평균) 상위 top_n. 시총 >= 5,000억원.
    현재가 옆에 당일상승률·3일상승률 포함.
    """
    eng = engine()
    as_of = as_of or _latest_date(eng)
    if as_of is None:
        return pd.DataFrame()
    if market:
        rs = pd.read_sql(
            """
            SELECT r.ticker, r.rs_20, r.rs_50, r.rs_120, r.rs_200,
                   o.name, o.close, o.mcap, o.market, o.chg_pct
            FROM rs r
            LEFT JOIN ohlcv o ON o.ticker=r.ticker AND o.date=r.date
            WHERE r.date = %s
              AND o.mcap >= %s
              AND o.market = %s
            """,
            eng,
            params=(as_of, TALENT_MCAP_MIN, market),
        )
    else:
        rs = pd.read_sql(
            """
            SELECT r.ticker, r.rs_20, r.rs_50, r.rs_120, r.rs_200,
                   o.name, o.close, o.mcap, o.chg_pct
            FROM rs r
            LEFT JOIN ohlcv o ON o.ticker=r.ticker AND o.date=r.date
            WHERE r.date = %s
              AND o.mcap >= %s
            """,
            eng,
            params=(as_of, TALENT_MCAP_MIN),
        )
    if rs.empty:
        return pd.DataFrame()
    for c in ("rs_20", "rs_50", "rs_120", "rs_200", "close", "mcap", "chg_pct"):
        if c in rs.columns:
            rs[c] = pd.to_numeric(rs[c], errors="coerce")
    rs["rs_avg"] = rs[["rs_20", "rs_50", "rs_120", "rs_200"]].mean(axis=1)
    rs = rs.sort_values("rs_avg", ascending=False).head(top_n).reset_index(drop=True)
    rs["rs_avg"] = pd.to_numeric(rs["rs_avg"], errors="coerce").round(2)
    rs[MCAP_COL] = (rs["mcap"] / 1e12).round(2)

    dates = _trading_dates(eng, as_of, 4)
    prev_d = dates[-2] if len(dates) >= 2 else None
    d3 = dates[0] if len(dates) >= 4 else None
    tickers = [str(t) for t in rs["ticker"].tolist()]
    hist = pd.DataFrame()
    if tickers and (prev_d is not None or d3 is not None):
        load_start = dates[0]
        ph = ",".join(["%s"] * len(tickers))
        hist = pd.read_sql(
            f"""
            SELECT ticker, date, close FROM ohlcv
            WHERE date >= %s AND date <= %s AND ticker IN ({ph})
            """,
            eng,
            params=(load_start, as_of, *tickers),
        )
        if not hist.empty:
            hist["date"] = pd.to_datetime(hist["date"]).dt.date
            hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
            hist["ticker"] = hist["ticker"].astype(str)

    if prev_d is not None and not hist.empty:
        prev = hist[hist["date"] == prev_d][["ticker", "close"]].copy()
        day0 = rs.copy()
        day0["ticker"] = day0["ticker"].astype(str)
        rs["당일상승률"] = _apply_day_chg(day0, prev).values
    else:
        rs["당일상승률"] = rs.get("chg_pct")

    base3 = _close_map_on(hist, d3) if d3 is not None and not hist.empty else {}
    rs[RET_3D_COL] = [
        _n_day_ret_pct(r["close"], base3.get(str(r["ticker"]))) for _, r in rs.iterrows()
    ]

    rs.insert(0, "순위", range(1, len(rs) + 1))
    out = rs.rename(
        columns={
            "ticker": "티커",
            "name": "종목명",
            "close": "현재가",
        }
    )
    return _select_cols(
        out,
        [
            "순위",
            "티커",
            "종목명",
            "현재가",
            "당일상승률",
            RET_3D_COL,
            "rs_20",
            "rs_50",
            "rs_120",
            "rs_200",
            "rs_avg",
            MCAP_COL,
        ],
    )


def build_talent_rank(
    as_of: Optional[date] = None,
    top_n: int = 50,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    Talent 순위 Top50 (시장별).
    대상: 시총 >= 5,000억원 (+ market 필터).
    talent 지수 = (n20/20)*0.5 + (n50/50)*0.3 + (n120/120)*0.2
    """
    eng = engine()
    as_of = as_of or _latest_date(eng)
    if as_of is None:
        return pd.DataFrame()

    if market:
        day0 = pd.read_sql(
            """
            SELECT ticker, name, close, chg_pct, mcap, market
            FROM ohlcv
            WHERE date = %s AND mcap >= %s AND market = %s
            """,
            eng,
            params=(as_of, TALENT_MCAP_MIN, market),
        )
    else:
        day0 = pd.read_sql(
            """
            SELECT ticker, name, close, chg_pct, mcap
            FROM ohlcv
            WHERE date = %s AND mcap >= %s
              AND market IN ('KOSPI','KOSDAQ')
            """,
            eng,
            params=(as_of, TALENT_MCAP_MIN),
        )
        if day0.empty:
            day0 = pd.read_sql(
                """
                SELECT ticker, name, close, chg_pct, mcap
                FROM ohlcv
                WHERE date = %s AND mcap >= %s
                """,
                eng,
                params=(as_of, TALENT_MCAP_MIN),
            )
    if day0.empty:
        return pd.DataFrame()

    for c in ("close", "chg_pct", "mcap"):
        day0[c] = pd.to_numeric(day0[c], errors="coerce")
    tickers = [str(t) for t in day0["ticker"].tolist()]

    dates = _trading_dates(eng, as_of, 130)
    if len(dates) < 20:
        return pd.DataFrame()
    load_start = dates[0]

    frames = []
    for i in range(0, len(tickers), 400):
        chunk = tickers[i : i + 400]
        ph = ",".join(["%s"] * len(chunk))
        frames.append(
            pd.read_sql(
                f"""
                SELECT ticker, date, close FROM ohlcv
                WHERE date >= %s AND date <= %s AND ticker IN ({ph})
                """,
                eng,
                params=(load_start, as_of, *chunk),
            )
        )
    hist = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if hist.empty:
        return pd.DataFrame()
    hist["date"] = pd.to_datetime(hist["date"]).dt.date
    hist["close"] = pd.to_numeric(hist["close"], errors="coerce")
    hist["ticker"] = hist["ticker"].astype(str)

    prev_d = dates[-2] if len(dates) >= 2 else None
    prev_map = {}
    if prev_d is not None:
        prev_map = (
            hist[hist["date"] == prev_d].set_index("ticker")["close"].to_dict()
        )

    c20, c50, c120 = TALENT_UD_COLS
    rows = []
    for tk, g in hist.groupby("ticker"):
        g = g.sort_values("date")
        if g["date"].iloc[-1] != as_of:
            continue
        up, down = _daily_ret_flags(g["close"])
        n20 = int(up.tail(20).sum()) if len(up) >= 1 else 0
        n50 = int(up.tail(50).sum()) if len(up) >= 1 else 0
        n120 = int(up.tail(120).sum()) if len(up) >= 1 else 0
        d20 = int(down.tail(20).sum()) if len(down) >= 1 else 0
        d50 = int(down.tail(50).sum()) if len(down) >= 1 else 0
        d120 = int(down.tail(120).sum()) if len(down) >= 1 else 0
        idx = (n20 / 20.0) * 0.5 + (n50 / 50.0) * 0.3 + (n120 / 120.0) * 0.2
        rows.append(
            {
                "ticker": tk,
                "talent 지수": round(float(idx), 3),
                c20: f"{n20}/{d20}",
                c50: f"{n50}/{d50}",
                c120: f"{n120}/{d120}",
            }
        )
    if not rows:
        return pd.DataFrame()

    tal = pd.DataFrame(rows)
    day0 = day0.copy()
    day0["ticker"] = day0["ticker"].astype(str)
    m = day0.merge(tal, on="ticker", how="inner")
    if m.empty:
        return pd.DataFrame()

    def _chg_row(r):
        prev = prev_map.get(r["ticker"])
        if prev and prev > 0 and pd.notna(r["close"]):
            return round((float(r["close"]) / float(prev) - 1.0) * 100.0, 2)
        return r["chg_pct"]

    m["당일상승률"] = m.apply(_chg_row, axis=1)
    d3 = dates[-4] if len(dates) >= 4 else None
    base3 = _close_map_on(hist, d3) if d3 is not None else {}
    m[RET_3D_COL] = [
        _n_day_ret_pct(r["close"], base3.get(str(r["ticker"]))) for _, r in m.iterrows()
    ]
    m[MCAP_COL] = (m["mcap"] / 1e12).round(2)
    m = m.sort_values("talent 지수", ascending=False).head(top_n).reset_index(drop=True)
    m.insert(0, "순위", range(1, len(m) + 1))
    out = m.rename(columns={"ticker": "티커", "name": "종목명", "close": "현재가"})
    return _select_cols(
        out,
        [
            "순위",
            "티커",
            "종목명",
            "현재가",
            "당일상승률",
            RET_3D_COL,
            "talent 지수",
            c20,
            c50,
            c120,
            MCAP_COL,
        ],
    )


def build_price_position_rank(
    as_of: Optional[date] = None,
    top_n: int = 50,
    market: Optional[str] = None,
) -> pd.DataFrame:
    """
    주가위치 Top50 (정렬=120일 고가·저가 위치).
    - 주가위치(120일): (종가−120일고가최저)/(120일고가최고−최저) — 기존 고·저가
    - 20일/50일 주가위치: (종가−N일종가최저)/(N일종가최고−최저) — 종가 기준
    """
    eng = engine()
    as_of = as_of or _latest_date(eng)
    if as_of is None:
        return pd.DataFrame()

    if market:
        day0 = pd.read_sql(
            """
            SELECT ticker, name, close, chg_pct, mcap, market
            FROM ohlcv
            WHERE date = %s AND mcap >= %s AND market = %s
            """,
            eng,
            params=(as_of, TALENT_MCAP_MIN, market),
        )
    else:
        day0 = pd.read_sql(
            """
            SELECT ticker, name, close, chg_pct, mcap, market
            FROM ohlcv
            WHERE date = %s AND mcap >= %s
              AND market IN ('KOSPI','KOSDAQ')
            """,
            eng,
            params=(as_of, TALENT_MCAP_MIN),
        )
    if day0.empty:
        return pd.DataFrame()

    for c in ("close", "chg_pct", "mcap"):
        day0[c] = pd.to_numeric(day0[c], errors="coerce")
    tickers = [str(t) for t in day0["ticker"].tolist()]

    need = max(PRICE_POS_WINDOW, max(PRICE_POS_CLOSE_WINDOWS, default=0)) + 5
    dates = _trading_dates(eng, as_of, need)
    if len(dates) < PRICE_POS_WINDOW:
        return pd.DataFrame()
    load_start = dates[0]
    win120 = set(dates[-PRICE_POS_WINDOW:])

    frames = []
    for i in range(0, len(tickers), 400):
        chunk = tickers[i : i + 400]
        ph = ",".join(["%s"] * len(chunk))
        frames.append(
            pd.read_sql(
                f"""
                SELECT ticker, date, high, low, close FROM ohlcv
                WHERE date >= %s AND date <= %s AND ticker IN ({ph})
                """,
                eng,
                params=(load_start, as_of, *chunk),
            )
        )
    hist = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if hist.empty:
        return pd.DataFrame()
    hist["date"] = pd.to_datetime(hist["date"]).dt.date
    for c in ("high", "low", "close"):
        hist[c] = pd.to_numeric(hist[c], errors="coerce")
    hist["ticker"] = hist["ticker"].astype(str)

    prev_d = dates[-2] if len(dates) >= 2 else None
    prev_map = {}
    if prev_d is not None:
        prev_map = hist[hist["date"] == prev_d].set_index("ticker")["close"].to_dict()

    def _pos_hl(win: pd.DataFrame, cur: float) -> float:
        hi = float(win["high"].max())
        lo = float(win["low"].min())
        if not np.isfinite(hi) or not np.isfinite(lo) or not np.isfinite(cur):
            return np.nan
        denom = hi - lo
        if denom <= 0:
            return np.nan
        return float(np.clip((cur - lo) / denom, 0.0, 1.0))

    def _pos_close(win: pd.DataFrame, cur: float) -> float:
        hi = float(win["close"].max())
        lo = float(win["close"].min())
        if not np.isfinite(hi) or not np.isfinite(lo) or not np.isfinite(cur):
            return np.nan
        denom = hi - lo
        if denom <= 0:
            return np.nan
        return float(np.clip((cur - lo) / denom, 0.0, 1.0))

    rows = []
    for tk, g in hist.groupby("ticker"):
        g = g.sort_values("date")
        if g["date"].iloc[-1] != as_of:
            continue
        cur = float(g["close"].iloc[-1])
        win = g[g["date"].isin(win120)]
        if len(win) < PRICE_POS_WINDOW:
            continue
        pos120 = _pos_hl(win, cur)
        if not np.isfinite(pos120):
            continue
        row = {"ticker": tk, PRICE_POS_COL: round(pos120, 2)}
        for n in PRICE_POS_CLOSE_WINDOWS:
            if len(dates) < n:
                row[f"{n}일 주가위치"] = np.nan
                continue
            wdates = set(dates[-n:])
            wn = g[g["date"].isin(wdates)]
            if len(wn) < n:
                row[f"{n}일 주가위치"] = np.nan
            else:
                p = _pos_close(wn, cur)
                row[f"{n}일 주가위치"] = round(p, 2) if np.isfinite(p) else np.nan
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    pos_df = pd.DataFrame(rows)
    day0 = day0.copy()
    day0["ticker"] = day0["ticker"].astype(str)
    m = day0.merge(pos_df, on="ticker", how="inner")
    if m.empty:
        return pd.DataFrame()

    def _chg_row(r):
        prev = prev_map.get(r["ticker"])
        if prev and prev > 0 and pd.notna(r["close"]):
            return round((float(r["close"]) / float(prev) - 1.0) * 100.0, 2)
        return r["chg_pct"]

    m["당일상승률"] = m.apply(_chg_row, axis=1)
    d3 = dates[-4] if len(dates) >= 4 else None
    base3 = _close_map_on(hist, d3) if d3 is not None else {}
    m[RET_3D_COL] = [
        _n_day_ret_pct(r["close"], base3.get(str(r["ticker"]))) for _, r in m.iterrows()
    ]
    m[MCAP_COL] = (m["mcap"] / 1e12).round(2)
    m = m.sort_values(PRICE_POS_COL, ascending=False).head(top_n).reset_index(drop=True)
    m.insert(0, "순위", range(1, len(m) + 1))
    out = m.rename(columns={"ticker": "티커", "name": "종목명", "close": "현재가"})
    preferred = [
        "순위",
        "티커",
        "종목명",
        "현재가",
        "당일상승률",
        RET_3D_COL,
        "20일 주가위치",
        "50일 주가위치",
        PRICE_POS_COL,
        MCAP_COL,
    ]
    for c in preferred:
        if c not in out.columns:
            out[c] = np.nan
    return _select_cols(out, preferred)


def build_all_market(as_of: Optional[date] = None) -> dict[str, dict[str, pd.DataFrame]]:
    """시장별 {tv, energy, rs, pos, talent, high}."""
    out: dict[str, dict[str, pd.DataFrame]] = {}
    for mkt in MARKETS:
        log.info("시장 스냅샷 산출 %s", mkt)
        out[mkt] = {
            "tv": build_tv_rank(as_of, market=mkt),
            "energy": build_energy_rank(as_of, market=mkt),
            "rs": build_rs_rank(as_of, market=mkt),
            "pos": build_price_position_rank(as_of, market=mkt),
            "talent": build_talent_rank(as_of, market=mkt),
            "high": build_new_highs(as_of, market=mkt),
        }
    return out
