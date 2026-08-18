# -*- coding: utf-8 -*-
"""
picking 결합 가중치 Rank IC·워크포워드 백테스트 (로컬 root DB / kor_stock_db).

팩터: tv5(5일 누적거래대금), RS, 주가위치, talent120.
유니버스: 시장별 보통주·전역제외 + 시총>=3,000억 (거래대금 TopN 절단 없음).

목적함수: 전구간 mean Spearman Rank IC 최대화.
검증: K=5 expanding 워크포워드. WEIGHT_SETS 는 수정하지 않고 추천값만 출력한다.
사이즈중립: RUN_MODE size_neutral/both 시 tv5·tv5_turn 등 raw vs within-mcap IC.
"""
from __future__ import annotations

import itertools
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


def _find_repo_root():
    """env_config.find_repo_root 와 동일 규칙 (import 전용 인라인)."""
    markers = ("env_config.py", ".env", ".git")

    def _is_root(p: Path) -> bool:
        return any((p / m).exists() for m in markers)

    def _walk_up(start: Path):
        try:
            start = Path(start).expanduser().resolve()
        except Exception:
            return None
        if not start.exists():
            return None
        if start.is_file():
            start = start.parent
        for p in [start, *start.parents]:
            if _is_root(p):
                return p
        return None

    tried = []
    seen = set()
    _nl = chr(10)
    _hint = _nl + "REPO_ROOT 환경변수를 리포 루트로 지정하거나 F5로 실행하세요"

    env_root = os.environ.get("REPO_ROOT", "").strip()
    if env_root:
        er = Path(env_root).expanduser()
        try:
            er = er.resolve()
        except Exception as e:
            raise RuntimeError(
                "REPO_ROOT 경로를 해석할 수 없습니다: {!r} ({}){}".format(
                    env_root, e, _hint
                )
            ) from e
        tried.append(str(er))
        if not er.is_dir():
            raise RuntimeError(
                "REPO_ROOT 가 디렉터리가 아닙니다: {}{}".format(er, _hint)
            )
        if _is_root(er):
            return er
        found = _walk_up(er)
        if found:
            return found
        raise RuntimeError(
            "REPO_ROOT={} 에서 마커(env_config.py / .env / .git)를 찾지 못했습니다.{}".format(
                er, _hint
            )
        )

    starts = []
    try:
        here = Path(__file__).resolve()
        starts.append(here if here.is_dir() else here.parent)
    except NameError:
        pass
    try:
        import inspect

        for fi in inspect.stack():
            fn = getattr(fi, "filename", None) or ""
            if not fn or fn.startswith("<"):
                continue
            try:
                p = Path(fn).resolve()
            except Exception:
                continue
            if p.suffix.lower() == ".py" and p.is_file():
                starts.append(p.parent)
    except Exception:
        pass
    starts.append(Path.cwd())
    for item in sys.path:
        if not item or item == ".":
            continue
        try:
            p = Path(item)
            if p.is_dir():
                starts.append(p)
        except Exception:
            continue

    for c in starts:
        try:
            key = str(Path(c).expanduser().resolve())
        except Exception:
            key = str(c)
        if key in seen:
            continue
        seen.add(key)
        tried.append(key)
        found = _walk_up(Path(c))
        if found:
            return found

    raise RuntimeError(
        "프로젝트 루트를 찾지 못했습니다 (env_config.py / .env / .git)."
        + _nl
        + "탐색 후보:"
        + _nl
        + "  - "
        + (_nl + "  - ").join(tried)
        + _hint
    )


_ROOT = _find_repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from env_config import load_project_env, db_url, db_connect_kwargs  # noqa: E402

load_project_env()

from exclusions import filter_common_stock_df, filter_tickers  # noqa: E402
from indicators_core import rs_avg, talent_up_share  # noqa: E402

log = logging.getLogger("backtest_picking_local")

# ── 결합 가중치: 로컬 tv5 실험 (에너지배율 → tv5). 추천만 출력, 자동 반영 안 함 ─
MIN_MCAP = 300_000_000_000  # 3,000억 (as_of 재구성 시총)
METRIC_COLS = ("tv5", "RS", "주가위치", "talent")
# v4 단기 탐색 후보. 이 목록은 단일 IC 선별용이며, 통과 팩터만 h별 격자에 사용한다.
FLOW_CODES = ("6000", "3000", "3100", "1000", "7050", "foreign", "8000")
FLOW_FACTORS = tuple(f"flow_{code}" for code in FLOW_CODES)
SHORT_FACTOR_COLS = (
    *FLOW_FACTORS,
    "rev5",
    "div5",
    "RS",
    "주가위치",
    "talent",
    "tv5_turn",
)
SHORT_HOLDING_DAYS = (1, 5, 10, 20)
SHORT_IC_CSV = Path(__file__).resolve().parent / "backtest_short_factors_ic.csv"
SHORT_GRID_CSV = Path(__file__).resolve().parent / "backtest_short_factors_grid.csv"
SHORT_IC_THRESHOLD = 0.02
SHORT_GRID_MAX_FACTORS = 5
WEIGHT_SETS: dict[str, dict[str, float]] = {
    "long": {
        "RS": 0.50,
        "주가위치": 0.30,
        "talent": 0.10,
        "tv5": 0.10,
    },
    "short": {
        "주가위치": 0.40,
        "RS": 0.20,
        "talent": 0.20,
        "tv5": 0.20,
    },
}
# 격자 밖 진단용 (가중치 합에 미포함)
DIAG_FACTORS = ("tv5_turn",)

MARKETS = ("KOSPI", "KOSDAQ")
PRICE_POS_WINDOW = 120
HOLDING_DAYS = SHORT_HOLDING_DAYS
WEIGHT_STEP = 0.1
WARMUP_DAYS = max(PRICE_POS_WINDOW + 10, 130)
EVAL_INTERVAL = 5  # 느리면 10으로 늘려 평가일 수 조절
WF_BLOCKS = 5
MIN_IC_STOCKS = 30
HORIZON_KIND = {1: "h1", 5: "h5", 20: "h20", 50: "h50"}
HORIZON_BASELINE = {1: "short", 5: "short", 50: "long"}
OUTPUT_CSV = Path(__file__).resolve().parent / "backtest_picking_ic_tv5.csv"
DECILE_CSV = Path(__file__).resolve().parent / "decile_analysis_tv5.csv"
SIZE_NEUTRAL_CSV = Path(__file__).resolve().parent / "decile_size_neutral_tv5.csv"
# Spyder F5: "backtest" | "size_neutral" | "both"
RUN_MODE = "both"
PANEL_CACHE_DIR = Path(__file__).resolve().parent / "cache"
# 지표·유니버스 정의 변경 시 값을 올리거나 FORCE_REBUILD_PANEL_CACHE=True로 재생성.
PANEL_CACHE_VERSION = "rank_ic_v4_short"
FORCE_REBUILD_PANEL_CACHE = False
N_DECILES = 10
N_MCAP_TERCILES = 3
N_SIZE_QUINTILES = 5
SIZE_NEUTRAL_H = (20, 50)
SIZE_NEUTRAL_IC_FACTORS = ("tv5", "tv5_turn", "talent", "RS", "주가위치")
SIZE_NEUTRAL_QUINTILE_FACTORS = ("tv5",)
DECILE_CONSOLE_H = (20, 50)
DECILE_CONSOLE_FACTORS = ("tv5", "talent")
MCAP_BUCKET_LABELS = {1: "small", 2: "mid", 3: "large"}  # 1=시총최저 … 3=최고
PCT_COLS = tuple(f"pct_{c}" for c in METRIC_COLS)
DIAG_PCT_COLS = tuple(f"pct_{c}" for c in DIAG_FACTORS)
PANEL_CACHE_REQUIRED_COLS = (
    PCT_COLS
    + DIAG_PCT_COLS
    + tuple(f"pct_{c}" for c in SHORT_FACTOR_COLS)
    + ("tv5", "tv5_turn", "r5", "rev5", "div5", "시가총액")
    + FLOW_FACTORS
)
FACTOR_PANEL_COLS = [
    "시장",
    "티커",
    "종목명",
    "현재가",
    "시가총액",
    "tv5",
    "RS",
    "주가위치",
    "talent",
    "tv5_turn",
    "pct_tv5",
    "pct_RS",
    "pct_주가위치",
    "pct_talent",
    "pct_tv5_turn",
    "r5",
    "rev5",
    "div5",
    *FLOW_FACTORS,
    "pct_rev5",
    "pct_div5",
    *(f"pct_{c}" for c in FLOW_FACTORS),
]

_MARKET_ALIASES = {
    "KOSPI": {"KOSPI", "코스피", "유가증권", "KOSPI시장"},
    "KOSDAQ": {"KOSDAQ", "코스닥", "KOSDAQ시장"},
}


def make_engine():
    return create_engine(db_url(), pool_pre_ping=True)


def _norm_market(v) -> str:
    s = str(v or "").strip().upper()
    if s in ("KOSPI", "KOSDAQ"):
        return s
    raw = str(v or "").strip()
    for canon, aliases in _MARKET_ALIASES.items():
        if raw in aliases or s in {a.upper() for a in aliases}:
            return canon
    if "KOSDAQ" in s or "코스닥" in raw:
        return "KOSDAQ"
    if "KOSPI" in s or "코스피" in raw or "유가" in raw:
        return "KOSPI"
    return s


def _percentile_0_100(series: pd.Series) -> pd.Series:
    """유효값을 0~100 백분위로 변환하고 결측은 중립값 50으로 채운다."""
    s = pd.to_numeric(series, errors="coerce").astype(float)
    s = s.where(np.isfinite(s), np.nan)
    valid = s.notna()
    n = int(valid.sum())
    out = pd.Series(50.0, index=s.index, dtype=float)
    if n <= 0:
        return out
    if n == 1:
        out.loc[valid] = 50.0
        return out
    ranks = s.loc[valid].rank(method="average", ascending=True)
    out.loc[valid] = (ranks - 1.0) / float(n - 1) * 100.0
    return out.clip(0.0, 100.0)


def _percentile_0_100_nullable(series: pd.Series) -> pd.Series:
    """유효값만 0~100 백분위; 데이터 미가용은 NaN으로 보존한다."""
    s = pd.to_numeric(series, errors="coerce").astype(float)
    s = s.where(np.isfinite(s), np.nan)
    out = pd.Series(np.nan, index=s.index, dtype=float)
    valid = s.notna()
    n = int(valid.sum())
    if n == 1:
        out.loc[valid] = 50.0
    elif n > 1:
        ranks = s.loc[valid].rank(method="average", ascending=True)
        out.loc[valid] = (ranks - 1.0) / float(n - 1) * 100.0
    return out.clip(0.0, 100.0)


def _trading_dates(eng, end: date, n: int) -> list[date]:
    df = pd.read_sql(
        """
        SELECT DISTINCT date FROM krx_ohlcv
        WHERE date <= %s
        ORDER BY date DESC
        LIMIT %s
        """,
        eng,
        params=(end, int(n)),
    )
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["date"]).dt.date.tolist())


def _latest_ohlcv_date(eng) -> Optional[date]:
    df = pd.read_sql("SELECT MAX(date) AS d FROM krx_ohlcv", eng)
    if df.empty or pd.isna(df.iloc[0]["d"]):
        return None
    return pd.to_datetime(df.iloc[0]["d"]).date()


def _load_common_tickers(eng, market: str) -> pd.DataFrame:
    """최신 krx_ticker 보통주 + 시장 + 전역제외."""
    mkt = _norm_market(market)
    tick = pd.read_sql(
        """
        SELECT 종목코드 AS ticker, 종목명 AS name, 시장구분 AS market, 시가총액
        FROM krx_ticker
        WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
          AND 종목구분 = '보통주'
        """,
        eng,
    )
    if tick.empty:
        return tick
    tick["ticker"] = tick["ticker"].astype(str).str.strip().str.zfill(6)
    tick["name"] = tick["name"].astype(str)
    tick["market"] = tick["market"].map(_norm_market)
    tick = tick[tick["market"] == mkt].copy()
    tick = filter_common_stock_df(tick, "ticker", "name")
    keep = set(filter_tickers(tick["ticker"].tolist()))
    tick = tick[tick["ticker"].isin(keep)].copy()
    return tick.reset_index(drop=True)


def _nonempty_frames(frames: list[pd.DataFrame]) -> list[pd.DataFrame]:
    """pd.concat 전 빈/전부-NA 프레임 제외 (FutureWarning 방지)."""
    out: list[pd.DataFrame] = []
    for f in frames:
        if f is None or getattr(f, "empty", True):
            continue
        try:
            if bool(f.isna().all().all()):
                continue
        except Exception:
            pass
        out.append(f)
    return out


def _ensure_trading_value(hist: pd.DataFrame) -> pd.DataFrame:
    """trading_value 정리. 없으면 close*volume. (krx_ohlcv.mcap 은 사용하지 않음)"""
    d = hist.copy()
    for c in ("open", "high", "low", "close", "volume", "trading_value"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    if "trading_value" not in d.columns:
        d["trading_value"] = np.nan
    tv_missing = d["trading_value"].isna() | (d["trading_value"] <= 0)
    if tv_missing.any() and {"close", "volume"}.issubset(d.columns):
        d.loc[tv_missing, "trading_value"] = (
            d.loc[tv_missing, "close"] * d.loc[tv_missing, "volume"]
        )
    # 일자별 mcap 컬럼은 최신 며칠만 있어 쓰지 않는다.
    if "mcap" in d.columns:
        d = d.drop(columns=["mcap"])
    return d


def _load_ohlcv_hist(
    eng,
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for i in range(0, len(tickers), 400):
        chunk = tickers[i : i + 400]
        ph = ",".join(["%s"] * len(chunk))
        try:
            frames.append(
                pd.read_sql(
                    f"""
                    SELECT date, ticker, open, high, low, close, volume, trading_value
                    FROM krx_ohlcv
                    WHERE date >= %s AND date <= %s
                      AND ticker IN ({ph})
                    """,
                    eng,
                    params=(start, end, *chunk),
                )
            )
        except Exception:
            frames.append(
                pd.read_sql(
                    f"""
                    SELECT date, ticker, open, high, low, close, volume
                    FROM krx_ohlcv
                    WHERE date >= %s AND date <= %s
                      AND ticker IN ({ph})
                    """,
                    eng,
                    params=(start, end, *chunk),
                )
            )
    frames = _nonempty_frames(frames)
    if not frames:
        return pd.DataFrame()
    hist = pd.concat(frames, ignore_index=True)
    if hist.empty:
        return hist
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.date
    hist["ticker"] = hist["ticker"].astype(str).str.strip().str.zfill(6)
    return _ensure_trading_value(hist)


def load_mcap_latest(eng) -> pd.Series:
    """krx_ticker 최신 기준일 종목별 시가총액 (양 시장 보통주·전역제외)."""
    frames = []
    for mkt in MARKETS:
        t = _load_common_tickers(eng, mkt)
        if t is None or t.empty:
            continue
        frames.append(t[["ticker", "시가총액"]])
    frames = _nonempty_frames(frames)
    if not frames:
        return pd.Series(dtype=float)
    df = pd.concat(frames, ignore_index=True)
    df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    df["시가총액"] = pd.to_numeric(df["시가총액"], errors="coerce")
    df = df.dropna(subset=["ticker"]).drop_duplicates("ticker", keep="last")
    return df.set_index("ticker")["시가총액"].astype(float)


def last_valid_close(close_wide: pd.DataFrame) -> pd.Series:
    """종가 와이드에서 종목별 마지막 유효 종가."""
    if close_wide is None or close_wide.empty:
        return pd.Series(dtype=float)
    return pd.to_numeric(close_wide.ffill().iloc[-1], errors="coerce")


def reconstruct_mcap_at(
    as_of: date,
    tickers: list[str] | pd.Index,
    close_wide: pd.DataFrame,
    mcap_latest: pd.Series,
    close_latest: pd.Series,
) -> pd.Series:
    """
    per-date 시총 재구성 (krx_ohlcv.mcap 미사용).

    mcap_t = 시가총액_최신 × (close_t / close_최신)

    주식수 상수 가정(증자·분할 무시) — 유니버스 게이트·에너지 분모의 근사.
    """
    idx = pd.Index([str(t).strip().zfill(6) if str(t).strip().isdigit() else str(t) for t in tickers])
    if close_wide is None or close_wide.empty or as_of not in close_wide.index:
        return pd.Series(np.nan, index=idx, dtype=float)
    close_t = pd.to_numeric(close_wide.loc[as_of].reindex(idx), errors="coerce")
    ml = pd.to_numeric(mcap_latest.reindex(idx), errors="coerce")
    cl = pd.to_numeric(close_latest.reindex(idx), errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        out = ml * (close_t / cl.replace(0, np.nan))
    return out.astype(float)


def _rs_min_date(eng) -> Optional[date]:
    df = pd.read_sql("SELECT MIN(date) AS d FROM krx_relative_strength", eng)
    if df.empty or pd.isna(df.iloc[0]["d"]):
        return None
    return pd.to_datetime(df.iloc[0]["d"]).date()


def _investor_min_date(eng) -> Optional[date]:
    """투자자 수급 테이블 가용 시작일 (단일팩터 표본 주석용)."""
    try:
        df = pd.read_sql("SELECT MIN(date) AS d FROM krx_investor_trade_krx", eng)
    except Exception as e:
        log.warning("투자자 수급 가용일 조회 실패: %s", e)
        return None
    if df.empty or pd.isna(df.iloc[0]["d"]):
        return None
    return pd.to_datetime(df.iloc[0]["d"]).date()


def build_factor_panel_local(
    engine,
    as_of: Optional[date],
    market: str,
    *,
    close_wide: Optional[pd.DataFrame] = None,
    mcap_latest: Optional[pd.Series] = None,
    close_latest: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    로컬 tv5 광의 유니버스 팩터 패널.

    유니버스: 보통주·시장·전역제외 + as_of 재구성 시총 >= MIN_MCAP (3,000억).
    (거래대금 TopN 선별 없음 — tv5 자체를 팩터로 쓰므로 표본 절단 금지)

    팩터: tv5, RS, 주가위치, talent(120).
    진단: tv5_turn = tv5 / mcap, pct_tv5_turn.
    백분위는 해당 시장 유니버스 내 0~100 (결측=50).
    """
    if _norm_market(market) not in MARKETS:
        raise ValueError(f"unsupported market: {market}")
    market = _norm_market(market)

    as_of = as_of or _latest_ohlcv_date(engine)
    if as_of is None:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    as_of = pd.Timestamp(as_of).date()

    tick = _load_common_tickers(engine, market)
    if tick.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    name_map = dict(zip(tick["ticker"], tick["name"]))
    all_tickers = tick["ticker"].tolist()

    if close_wide is None:
        close_wide = load_close_wide(engine)
    if mcap_latest is None:
        mcap_latest = load_mcap_latest(engine)
    if close_latest is None:
        close_latest = last_valid_close(close_wide)

    dates = _trading_dates(engine, as_of, max(PRICE_POS_WINDOW + 10, 130))
    if not dates:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    d0 = dates[-1]
    hist = _load_ohlcv_hist(engine, all_tickers, dates[0], d0)
    if hist.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)

    day0 = hist[hist["date"] == d0].drop_duplicates("ticker", keep="last").copy()
    if day0.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    day0["name"] = day0["ticker"].map(name_map)
    day0["market"] = market
    day0["mcap"] = reconstruct_mcap_at(
        d0,
        day0["ticker"].tolist(),
        close_wide,
        mcap_latest,
        close_latest,
    ).to_numpy()

    tv5_days = dates[-5:]
    tv5 = (
        hist[hist["date"].isin(tv5_days)]
        .groupby("ticker", as_index=False)["trading_value"]
        .sum(min_count=1)
        .rename(columns={"trading_value": "tv5"})
    )
    day0 = day0.merge(tv5, on="ticker", how="left")

    # 시총 하한 유니버스 (거래대금 상위 절단 없음)
    universe = day0[
        day0["mcap"].notna()
        & np.isfinite(day0["mcap"])
        & (day0["mcap"] >= float(MIN_MCAP))
    ].copy()
    if universe.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    universe_tickers = set(universe["ticker"])

    universe["tv5_turn"] = universe["tv5"] / universe["mcap"].replace(0, np.nan)
    # r5: as_of 포함 직전 5거래일 수익률.
    # 섹터 초과 반전은 rev5=-(r5-r5_sec)로 정의한다.
    base_d = dates[-6] if len(dates) >= 6 else None
    if base_d is not None:
        base_close = (
            hist.loc[hist["date"] == base_d, ["ticker", "close"]]
            .drop_duplicates("ticker", keep="last")
            .rename(columns={"close": "_close_5d_base"})
        )
        universe = universe.merge(base_close, on="ticker", how="left")
        universe["r5"] = universe["close"] / universe["_close_5d_base"].replace(0, np.nan) - 1.0
    else:
        universe["r5"] = np.nan

    sector = pd.read_sql(
        "SELECT ticker, sector_cd FROM krx_ticker_sector",
        engine,
    )
    if not sector.empty:
        sector["ticker"] = sector["ticker"].astype(str).str.strip().str.zfill(6)
        sector = sector.drop_duplicates("ticker", keep="last")
        universe = universe.merge(sector, on="ticker", how="left")
    else:
        universe["sector_cd"] = np.nan
    r5_mkt = float(universe["r5"].mean())
    r5_sec = universe.groupby("sector_cd", dropna=True)["r5"].transform("mean")
    universe["r5_sec"] = r5_sec.fillna(r5_mkt)
    universe["rev5"] = -(universe["r5"] - universe["r5_sec"])

    rs = pd.read_sql(
        """
        SELECT r.ticker, r.rs_20d, r.rs_50d, r.rs_120d, r.rs_200d
        FROM krx_relative_strength r
        INNER JOIN (
            SELECT ticker, MAX(date) AS max_date
            FROM krx_relative_strength
            WHERE date <= %s
              AND UPPER(TRIM(market_type)) = %s
            GROUP BY ticker
        ) latest
          ON latest.ticker = r.ticker AND latest.max_date = r.date
        WHERE UPPER(TRIM(r.market_type)) = %s
        """,
        engine,
        params=(as_of, market, market),
    )
    if not rs.empty:
        rs["ticker"] = rs["ticker"].astype(str).str.strip().str.zfill(6)
        rs["RS"] = rs_avg(frame=rs, cols=("rs_20d", "rs_50d", "rs_120d", "rs_200d"))
        universe = universe.merge(rs[["ticker", "RS"]], on="ticker", how="left")
    else:
        universe["RS"] = np.nan

    u_hist = hist[hist["ticker"].isin(universe_tickers)].copy()
    win120_dates = set(dates[-PRICE_POS_WINDOW:]) if len(dates) >= PRICE_POS_WINDOW else set()
    pos_map: dict[str, float] = {}
    talent_map: dict[str, float] = {}
    for tk, g in u_hist.groupby("ticker"):
        g = g.sort_values("date")
        if g.empty or g["date"].iloc[-1] != d0:
            continue
        close = pd.to_numeric(g["close"], errors="coerce")
        talent_map[tk] = float(talent_up_share(close, 120))

        if not win120_dates:
            continue
        win = g[g["date"].isin(win120_dates)]
        if len(win) < PRICE_POS_WINDOW:
            continue
        cur = float(g["close"].iloc[-1])
        hi = float(win["high"].max())
        lo = float(win["low"].min())
        denom = hi - lo
        if np.isfinite(cur) and np.isfinite(hi) and np.isfinite(lo) and denom > 0:
            pos_map[tk] = float(np.clip((cur - lo) / denom, 0.0, 1.0))

    universe["주가위치"] = universe["ticker"].map(pos_map)
    universe["talent"] = universe["ticker"].map(talent_map)

    # div5: 거래대금 회전율 백분위 - r5 백분위. 거래↑·가격정체일수록 높다.
    universe["_pct_turn5"] = _percentile_0_100(universe["tv5_turn"])
    universe["_pct_r5"] = _percentile_0_100(universe["r5"])
    universe["div5"] = universe["_pct_turn5"] - universe["_pct_r5"]

    # 투자자 수급: 최근 5거래일 순매수대금 / as_of 재구성 시총.
    # FOREIGN은 9000+9001 합산, 나머지는 단일 투자자 코드.
    investor_start = dates[-5]
    inv = pd.read_sql(
        """
        SELECT ticker, invst_tp_cd, SUM(net_val) AS net_val
        FROM krx_investor_trade_krx
        WHERE date >= %s AND date <= %s
          AND invst_tp_cd IN ('6000','3000','3100','1000','7050','9000','9001','8000')
        GROUP BY ticker, invst_tp_cd
        """,
        engine,
        params=(investor_start, d0),
    )
    for code in FLOW_CODES:
        universe[f"flow_{code}"] = np.nan
    if not inv.empty:
        inv["ticker"] = inv["ticker"].astype(str).str.strip().str.zfill(6)
        inv["net_val"] = pd.to_numeric(inv["net_val"], errors="coerce")
        for code in ("6000", "3000", "3100", "1000", "7050", "8000"):
            values = inv.loc[inv["invst_tp_cd"].astype(str) == code].groupby("ticker")["net_val"].sum()
            universe[f"flow_{code}"] = universe["ticker"].map(values) / universe["mcap"].replace(0, np.nan)
        foreign = (
            inv.loc[inv["invst_tp_cd"].astype(str).isin(["9000", "9001"])]
            .groupby("ticker")["net_val"]
            .sum()
        )
        universe["flow_foreign"] = (
            universe["ticker"].map(foreign) / universe["mcap"].replace(0, np.nan)
        )

    for raw_col in ("tv5", "RS", "주가위치", "talent", "tv5_turn", "rev5", "div5"):
        universe[f"pct_{raw_col}"] = _percentile_0_100(universe[raw_col])
    for raw_col in FLOW_FACTORS:
        # 투자자 데이터 미가용 기간은 중립값으로 채우지 않아 IC 표본에서 제외한다.
        universe[f"pct_{raw_col}"] = _percentile_0_100_nullable(universe[raw_col])

    out = universe.rename(
        columns={
            "market": "시장",
            "ticker": "티커",
            "name": "종목명",
            "close": "현재가",
            "mcap": "시가총액",
        }
    )
    for col in FACTOR_PANEL_COLS:
        if col not in out.columns:
            out[col] = np.nan
    return out[FACTOR_PANEL_COLS].reset_index(drop=True)


# ── 백테스트 루프 (naverPub/backtest_picking.py 와 동일) ───────────────────


def generate_weight_grid(step: float = WEIGHT_STEP) -> list[dict[str, float]]:
    n = int(round(1.0 / float(step)))
    out: list[dict[str, float]] = []
    for a, b, c in itertools.product(range(n + 1), repeat=3):
        d = n - a - b - c
        if d < 0:
            continue
        vals = (a * step, b * step, c * step, d * step)
        out.append({col: round(float(v), 10) for col, v in zip(METRIC_COLS, vals)})
    return out


def weights_to_array(weights: Iterable[dict[str, float]]) -> np.ndarray:
    return np.asarray(
        [[float(w.get(c, 0.0)) for c in METRIC_COLS] for w in weights],
        dtype=float,
    )


def format_weights(w: dict[str, float]) -> str:
    return " / ".join(f"{c}={float(w[c]):.1f}" for c in METRIC_COLS)


def ic_stats(values: np.ndarray | list[float]) -> tuple[float, float, int]:
    """IC 시계열 → (meanIC, IC_IR=mean/std, 유효 관측치 수)."""
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    n = int(len(x))
    if n <= 0:
        return float("nan"), float("nan"), 0
    mean_ic = float(x.mean())
    if n < 2:
        return mean_ic, float("nan"), n
    sd = float(x.std(ddof=1))
    ic_ir = mean_ic / sd if np.isfinite(sd) and sd > 0 else float("nan")
    return mean_ic, float(ic_ir), n


def _mean_ic_by_weight(ic_matrix: np.ndarray) -> np.ndarray:
    """열(가중치)별 NaN 무시 meanIC."""
    if ic_matrix.size == 0:
        return np.array([], dtype=float)
    valid_n = np.isfinite(ic_matrix).sum(axis=0)
    sums = np.nansum(ic_matrix, axis=0)
    return np.divide(
        sums,
        valid_n,
        out=np.full(ic_matrix.shape[1], np.nan, dtype=float),
        where=valid_n > 0,
    )


def load_close_wide(eng=None) -> pd.DataFrame:
    """krx_ohlcv 종가 와이드(date×ticker). 보통주·전역제외 유니버스."""
    eng = eng or make_engine()
    log.info("종가 와이드 적재 시작 (krx_ohlcv)")
    t0 = time.time()
    frames: list[pd.DataFrame] = []
    for mkt in MARKETS:
        tick = _load_common_tickers(eng, mkt)
        if tick.empty:
            continue
        tickers = tick["ticker"].tolist()
        for i in range(0, len(tickers), 400):
            chunk = tickers[i : i + 400]
            ph = ",".join(["%s"] * len(chunk))
            frames.append(
                pd.read_sql(
                    f"""
                    SELECT ticker, date, close
                    FROM krx_ohlcv
                    WHERE close IS NOT NULL
                      AND ticker IN ({ph})
                    """,
                    eng,
                    params=tuple(chunk),
                )
            )
    frames = _nonempty_frames(frames)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "close"])
    wide = df.pivot_table(index="date", columns="ticker", values="close", aggfunc="last")
    wide = wide.sort_index()
    log.info(
        "종가 와이드 완료: dates=%d tickers=%d (%.1fs)",
        len(wide),
        wide.shape[1],
        time.time() - t0,
    )
    return wide


def trading_dates_from_wide(close_wide: pd.DataFrame) -> list[date]:
    return [pd.Timestamp(d).date() for d in close_wide.index.tolist()]


def evaluation_dates(
    dates: list[date],
    interval: int = EVAL_INTERVAL,
    warmup: int = WARMUP_DAYS,
) -> list[date]:
    """warmup 이후 interval 거래일 간격 평가일. 최소 h 전방 종가가 있는 날짜까지."""
    step = max(1, int(interval))
    last_i = len(dates) - min(HOLDING_DAYS)
    return [dates[i] for i in range(int(warmup), last_i, step)]


def build_panel_at(
    eng,
    as_of: date,
    *,
    close_wide: pd.DataFrame,
    mcap_latest: pd.Series,
    close_latest: pd.Series,
) -> pd.DataFrame:
    frames = [
        build_factor_panel_local(
            eng,
            as_of,
            mkt,
            close_wide=close_wide,
            mcap_latest=mcap_latest,
            close_latest=close_latest,
        )
        for mkt in MARKETS
    ]
    frames = _nonempty_frames(frames)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def panel_cache_namespace(eng, close_wide: pd.DataFrame) -> str:
    """
    캐시 네임스페이스.

    수동 PANEL_CACHE_VERSION + 최신 OHLCV일 + 최신 ticker 기준일이 달라지면
    과거 패널도 자동으로 별도 캐시를 사용한다.
    """
    close_ref = (
        pd.Timestamp(close_wide.index.max()).strftime("%Y%m%d")
        if close_wide is not None and not close_wide.empty
        else "none"
    )
    try:
        ref = pd.read_sql("SELECT MAX(기준일) AS d FROM krx_ticker", eng)
        ticker_ref = (
            pd.Timestamp(ref.iloc[0]["d"]).strftime("%Y%m%d")
            if not ref.empty and pd.notna(ref.iloc[0]["d"])
            else "none"
        )
    except Exception:
        ticker_ref = "unknown"
    return f"{PANEL_CACHE_VERSION}_ohlcv{close_ref}_ticker{ticker_ref}"


def force_panel_cache_rebuild() -> bool:
    """상수 또는 환경변수 PICKING_REBUILD_PANEL_CACHE=1 로 캐시 강제 무효화."""
    env = os.environ.get("PICKING_REBUILD_PANEL_CACHE", "").strip().lower()
    return bool(FORCE_REBUILD_PANEL_CACHE or env in {"1", "true", "yes", "y", "on"})


def precompute_panels(
    eng,
    eval_dates: list[date],
    *,
    close_wide: pd.DataFrame,
    mcap_latest: pd.Series,
    close_latest: pd.Series,
    cache_dir: Path | str = PANEL_CACHE_DIR,
    cache_namespace: str = PANEL_CACHE_VERSION,
    force_rebuild: bool = False,
) -> dict[date, pd.DataFrame]:
    uniq = sorted(set(eval_dates))
    out: dict[date, pd.DataFrame] = {}
    first3_empty: list[bool] = []
    cache_root = Path(cache_dir) / str(cache_namespace)
    cache_root.mkdir(parents=True, exist_ok=True)
    cache_hits = 0
    cache_builds = 0
    for i, d in enumerate(uniq, start=1):
        t0 = time.time()
        cache_path = cache_root / f"panels_{pd.Timestamp(d).strftime('%Y%m%d')}.pkl"
        panel: Optional[pd.DataFrame] = None
        source = "build"
        if cache_path.exists() and not force_rebuild:
            try:
                cached = pd.read_pickle(cache_path)
                if isinstance(cached, pd.DataFrame) and all(
                    c in cached.columns for c in PANEL_CACHE_REQUIRED_COLS
                ):
                    panel = cached
                    source = "cache"
                    cache_hits += 1
                else:
                    log.warning("패널 캐시 형식 불일치, 재계산: %s", cache_path)
            except Exception as e:
                log.warning("패널 캐시 로드 실패, 재계산: %s (%s)", cache_path, e)
        if panel is None:
            panel = build_panel_at(
                eng,
                d,
                close_wide=close_wide,
                mcap_latest=mcap_latest,
                close_latest=close_latest,
            )
            try:
                tmp_path = cache_path.with_suffix(".tmp.pkl")
                panel.to_pickle(tmp_path)
                tmp_path.replace(cache_path)
            except Exception as e:
                log.warning("패널 캐시 저장 실패: %s (%s)", cache_path, e)
            cache_builds += 1
        out[d] = panel
        n_rows = len(panel)
        log.info(
            "panel %d/%d as_of=%s rows=%d source=%s (%.1fs)",
            i,
            len(uniq),
            d,
            n_rows,
            source,
            time.time() - t0,
        )
        if i <= 3:
            first3_empty.append(n_rows == 0)
            if i == 3 and all(first3_empty):
                msg = (
                    "첫 3개 리밸일 패널이 연속 rows=0 — 시총 재구성/유니버스 로딩을 점검하고 중단합니다. "
                    f"dates={uniq[:3]}"
                )
                log.error(msg)
                raise RuntimeError(msg)
    log.info(
        "패널 캐시 요약: hit=%d build=%d dir=%s force_rebuild=%s",
        cache_hits,
        cache_builds,
        cache_root,
        force_rebuild,
    )
    return out


def _spearman_ic_all_weights(
    pct_values: np.ndarray,
    fwd_rank: np.ndarray,
    weight_matrix: np.ndarray,
) -> np.ndarray:
    """한 평가일의 286개 composite와 사전 계산된 fwd 순위 간 Spearman IC."""
    scores = pct_values @ weight_matrix.T
    # 각 후보 composite 순위만 재계산. 동점은 평균순위.
    comp_rank = pd.DataFrame(scores).rank(axis=0, method="average").to_numpy(dtype=float)
    x = comp_rank - comp_rank.mean(axis=0, keepdims=True)
    y = np.asarray(fwd_rank, dtype=float)
    y = y - y.mean()
    numerator = np.sum(x * y[:, None], axis=0)
    denominator = np.sqrt(np.sum(x * x, axis=0) * np.sum(y * y))
    return np.divide(
        numerator,
        denominator,
        out=np.full(weight_matrix.shape[0], np.nan, dtype=float),
        where=denominator > 0,
    )


def evaluate_ic_on_dates(
    *,
    eval_dates: list[date],
    h: int,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
    weight_list: list[dict[str, float]],
) -> tuple[np.ndarray, np.ndarray]:
    """
    평가일 × 가중치 Rank IC 행렬.

    각 t의 fwd 순위는 한 번만 계산하고, 286개 composite 순위만 재계산한다.
    """
    weight_matrix = weights_to_array(weight_list)
    out = np.full((len(eval_dates), len(weight_list)), np.nan, dtype=float)
    stock_counts = np.zeros(len(eval_dates), dtype=int)

    for row_i, t in enumerate(eval_dates):
        i0 = date_to_idx.get(t)
        if i0 is None or i0 + h >= len(dates):
            continue
        t_h = dates[i0 + h]
        panel = panels.get(t)
        if panel is None or panel.empty or "티커" not in panel.columns:
            continue

        sub = panel.copy()
        sub["티커"] = sub["티커"].astype(str)
        sub = sub[sub["티커"].isin(close_wide.columns)].drop_duplicates("티커")
        if sub.empty:
            continue
        tickers = sub["티커"].tolist()
        close_t = pd.to_numeric(close_wide.loc[t, tickers], errors="coerce")
        close_h = pd.to_numeric(close_wide.loc[t_h, tickers], errors="coerce")
        valid = (
            close_t.notna()
            & close_h.notna()
            & np.isfinite(close_t)
            & np.isfinite(close_h)
            & (close_t > 0)
            & (close_h > 0)
        )
        if int(valid.sum()) < MIN_IC_STOCKS:
            continue

        valid_tickers = close_t.index[valid].tolist()
        sub = sub.set_index("티커").reindex(valid_tickers)
        for col in PCT_COLS:
            if col not in sub.columns:
                sub[col] = 50.0
            else:
                sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(50.0)
        pct_values = sub[list(PCT_COLS)].to_numpy(dtype=float)
        fwd = (close_h.loc[valid_tickers] / close_t.loc[valid_tickers] - 1.0).to_numpy(dtype=float)
        finite = np.isfinite(fwd) & np.all(np.isfinite(pct_values), axis=1)
        if int(finite.sum()) < MIN_IC_STOCKS:
            continue

        pct_values = pct_values[finite]
        fwd_rank = pd.Series(fwd[finite]).rank(method="average").to_numpy(dtype=float)
        out[row_i, :] = _spearman_ic_all_weights(pct_values, fwd_rank, weight_matrix)
        stock_counts[row_i] = int(finite.sum())

        if (row_i + 1) % 20 == 0 or row_i == len(eval_dates) - 1:
            log.info(
                "  IC 평가 h=%d %d/%d (stocks=%d)",
                h,
                row_i + 1,
                len(eval_dates),
                stock_counts[row_i],
            )
    return out, stock_counts


def _weight_index(weight_list: list[dict[str, float]], target: dict[str, float]) -> int:
    target_arr = np.array([float(target.get(c, 0.0)) for c in METRIC_COLS], dtype=float)
    grid = weights_to_array(weight_list)
    return int(np.argmin(np.sum(np.abs(grid - target_arr[None, :]), axis=1)))


def _best_weight_order(ic_matrix: np.ndarray) -> list[int]:
    means = _mean_ic_by_weight(ic_matrix)
    if means.size == 0 or not np.isfinite(means).any():
        return list(range(ic_matrix.shape[1]))
    return np.argsort(-np.where(np.isfinite(means), means, -np.inf)).astype(int).tolist()


def summarize_ic_horizon(
    *,
    h: int,
    ic_matrix: np.ndarray,
    weight_list: list[dict[str, float]],
    eval_dates: list[date],
) -> list[dict]:
    """전구간 최적·baseline·Top5와 K=5 expanding 워크포워드 결과."""
    kind = HORIZON_KIND[int(h)]
    valid_rows = np.isfinite(ic_matrix).any(axis=1)
    work = ic_matrix[valid_rows, :]
    work_dates = [d for d, ok in zip(eval_dates, valid_rows) if ok]
    if work.shape[0] == 0:
        log.warning("h=%d 유효 IC 관측치가 없습니다.", h)

    order = _best_weight_order(work)
    best_j = int(order[0]) if order else 0
    rows: list[dict] = []
    wf_rows: list[dict] = []
    selected_weights: list[np.ndarray] = []
    test_means: list[float] = []

    def _row(
        row_type: str,
        rank: int,
        j: int,
        values: np.ndarray,
        *,
        wf_step: int = 0,
        note: str = "",
    ) -> dict:
        mean_ic, ic_ir, n_obs = ic_stats(values)
        w = weight_list[j]
        row = {
            "h": int(h),
            "kind": kind,
            "type": row_type,
            "rank": int(rank),
            "wf_step": int(wf_step),
            "weights": format_weights(w),
            "meanIC": mean_ic,
            "IC_IR": ic_ir,
            "n_obs": int(n_obs),
            "note": note,
        }
        for col in METRIC_COLS:
            row[f"w_{col}"] = float(w[col])
        return row

    # K=5 블록, expanding train: block 1..k → test block k+1.
    if work.shape[0] >= WF_BLOCKS:
        blocks = [np.asarray(b, dtype=int) for b in np.array_split(np.arange(work.shape[0]), WF_BLOCKS)]
        for step in range(1, WF_BLOCKS):
            train_idx = np.concatenate(blocks[:step])
            test_idx = blocks[step]
            train_order = _best_weight_order(work[train_idx, :])
            selected_j = int(train_order[0]) if train_order else 0
            test_values = work[test_idx, selected_j]
            test_mean, _, _ = ic_stats(test_values)
            if np.isfinite(test_mean):
                test_means.append(test_mean)
            selected_weights.append(weights_to_array([weight_list[selected_j]])[0])
            train_start = work_dates[int(train_idx[0])]
            train_end = work_dates[int(train_idx[-1])]
            test_start = work_dates[int(test_idx[0])]
            test_end = work_dates[int(test_idx[-1])]
            wf_rows.append(
                _row(
                    "wf_step",
                    step,
                    selected_j,
                    test_values,
                    wf_step=step,
                    note=(
                        f"train={train_start}~{train_end}; "
                        f"test={test_start}~{test_end}; train meanIC 최대"
                    ),
                )
            )
    else:
        log.warning("h=%d 워크포워드 K=%d에 필요한 관측치 부족: %d", h, WF_BLOCKS, work.shape[0])

    wf_mean = float(np.mean(test_means)) if test_means else float("nan")
    wf_std = float(np.std(test_means, ddof=1)) if len(test_means) >= 2 else float("nan")
    if selected_weights:
        selected_arr = np.vstack(selected_weights)
        stability_mean = selected_arr.mean(axis=0)
        stability_std = (
            selected_arr.std(axis=0, ddof=1)
            if len(selected_arr) >= 2
            else np.full(len(METRIC_COLS), np.nan)
        )
        stability = ", ".join(
            f"{col}={mu:.2f}±{sd:.2f}"
            for col, mu, sd in zip(METRIC_COLS, stability_mean, stability_std)
        )
    else:
        stability = "-"

    best_note = (
        f"전구간 meanIC 최대 추천; WF test meanIC={wf_mean:.4f}±{wf_std:.4f}; "
        f"선정 가중 안정성 {stability}; WEIGHT_SETS 미변경"
    )
    rows.append(_row("best", 1, best_j, work[:, best_j], note=best_note))

    baseline_key = HORIZON_BASELINE.get(int(h))
    if baseline_key is not None:
        base_j = _weight_index(weight_list, WEIGHT_SETS[baseline_key])
        rows.append(
            _row(
                "baseline",
                0,
                base_j,
                work[:, base_j],
                note=f"현재 WEIGHT_SETS['{baseline_key}']",
            )
        )

    # 각 지평에서 지표별 단일 신호의 방향성과 크기를 직접 확인.
    for rank, factor in enumerate(METRIC_COLS, start=1):
        single_w = {c: 1.0 if c == factor else 0.0 for c in METRIC_COLS}
        single_j = _weight_index(weight_list, single_w)
        rows.append(
            _row(
                "single_factor",
                rank,
                single_j,
                work[:, single_j],
                note=f"h={h} 단일가중 점검: {factor}=1.0",
            )
        )

    for rank, j in enumerate(order[:5], start=1):
        rows.append(
            _row(
                "is_top5",
                rank,
                int(j),
                work[:, int(j)],
                note="전구간 meanIC 상위5(과최적 판단용)",
            )
        )
    rows.extend(wf_rows)
    return rows


def prepare_eval_context(
    close_wide: Optional[pd.DataFrame] = None,
    eng=None,
) -> dict:
    """종가·평가일·캐시 패널 준비 (IC 백테스트·십분위 분석 공용)."""
    eng = eng or make_engine()
    if close_wide is None:
        close_wide = load_close_wide(eng)
    if close_wide is None or close_wide.empty:
        raise RuntimeError("종가 데이터가 비어 있습니다.")

    close_wide = close_wide.copy()
    close_wide.index = pd.to_datetime(close_wide.index, errors="coerce").date
    close_wide = close_wide[~pd.isna(close_wide.index)].sort_index()

    mcap_latest = load_mcap_latest(eng)
    close_latest = last_valid_close(close_wide)
    log.info(
        "시총 재구성 준비: mcap_latest=%d close_latest=%d (krx_ohlcv.mcap 미사용)",
        int(mcap_latest.notna().sum()),
        int(close_latest.notna().sum()),
    )
    cache_namespace = panel_cache_namespace(eng, close_wide)
    rebuild_cache = force_panel_cache_rebuild()
    log.info(
        "패널 캐시: namespace=%s rebuild=%s "
        "(정의 변경 시 PANEL_CACHE_VERSION 상향 또는 PICKING_REBUILD_PANEL_CACHE=1)",
        cache_namespace,
        rebuild_cache,
    )

    rs_min = _rs_min_date(eng)
    investor_min = _investor_min_date(eng)
    log.info(
        "투자자 수급 데이터 가용 시작일: %s (flow_* IC는 이 날짜 이후 평가일만 n_obs 산출)",
        investor_min or "없음",
    )
    all_dates = trading_dates_from_wide(close_wide)
    if rs_min is not None:
        dates = [d for d in all_dates if d >= rs_min]
        log.info(
            "RS 가용일 기준 창 시작: %s (전체 OHLCV %d일 → %d일)",
            rs_min,
            len(all_dates),
            len(dates),
        )
    else:
        dates = all_dates
        log.warning("krx_relative_strength MIN(date) 없음 — OHLCV 전체 거래일 사용")

    if len(dates) <= WARMUP_DAYS + max(HOLDING_DAYS):
        raise RuntimeError(
            f"거래일 부족: {len(dates)}일 (워밍업 {WARMUP_DAYS} + 보유 필요)"
        )
    date_to_idx = {d: i for i, d in enumerate(dates)}
    log.info(
        "백테스트 창: %s ~ %s (%d거래일, warmup=%d)",
        dates[WARMUP_DAYS],
        dates[-1],
        len(dates) - WARMUP_DAYS,
        WARMUP_DAYS,
    )

    eval_dates = evaluation_dates(dates, EVAL_INTERVAL, WARMUP_DAYS)
    if not eval_dates:
        raise RuntimeError("평가일이 없습니다.")
    log.info(
        "평가일: %d개 (%d거래일 간격, 중첩 허용) %s ~ %s",
        len(eval_dates),
        EVAL_INTERVAL,
        eval_dates[0],
        eval_dates[-1],
    )

    panels = precompute_panels(
        eng,
        eval_dates,
        close_wide=close_wide,
        mcap_latest=mcap_latest,
        close_latest=close_latest,
        cache_namespace=cache_namespace,
        force_rebuild=rebuild_cache,
    )
    return {
        "eng": eng,
        "close_wide": close_wide,
        "dates": dates,
        "date_to_idx": date_to_idx,
        "eval_dates": eval_dates,
        "panels": panels,
        "investor_min": investor_min,
    }


def _assign_bins(values: pd.Series, n_bins: int) -> pd.Series:
    """값 → 1(최저)…n_bins(최고). 동점은 first rank로 균등 분할."""
    v = pd.to_numeric(values, errors="coerce")
    ok = v.notna() & np.isfinite(v)
    out = pd.Series(np.nan, index=values.index, dtype=float)
    if int(ok.sum()) < n_bins:
        return out
    ranks = v[ok].rank(method="first", ascending=True)
    n = int(ok.sum())
    out.loc[ok] = np.ceil(ranks.to_numpy(dtype=float) / n * n_bins).clip(1, n_bins)
    return out


def _assign_deciles(values: pd.Series, n_deciles: int = N_DECILES) -> pd.Series:
    """백분위/원시값 → D1(최저)…D10(최고)."""
    return _assign_bins(values, n_deciles)


def _spearman_ic_series(x: pd.Series, y: pd.Series) -> float:
    """Spearman Rank IC (평균순위). 표본 부족·상수열이면 NaN."""
    xx = pd.to_numeric(x, errors="coerce")
    yy = pd.to_numeric(y, errors="coerce")
    mask = xx.notna() & yy.notna() & np.isfinite(xx) & np.isfinite(yy)
    if int(mask.sum()) < MIN_IC_STOCKS:
        return float("nan")
    xr = xx[mask].rank(method="average")
    yr = yy[mask].rank(method="average")
    if float(xr.std(ddof=0)) <= 0 or float(yr.std(ddof=0)) <= 0:
        return float("nan")
    return float(xr.corr(yr))


def _pct_within_groups(values: pd.Series, groups: pd.Series) -> pd.Series:
    """그룹(시총 버킷) 내부에서 0~100 백분위 재계산."""
    out = pd.Series(np.nan, index=values.index, dtype=float)
    v = pd.to_numeric(values, errors="coerce")
    g = groups.reindex(values.index)
    for _, idx in v.groupby(g, dropna=True).groups.items():
        out.loc[idx] = _percentile_0_100(v.loc[idx])
    return out


def _day_panel_with_fwd(
    *,
    t: date,
    h: int,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
) -> Optional[pd.DataFrame]:
    """평가일 t 패널 + h일 전방수익. 유효 종목만 (index=티커)."""
    i0 = date_to_idx.get(t)
    if i0 is None or i0 + h >= len(dates):
        return None
    t_h = dates[i0 + h]
    panel = panels.get(t)
    if panel is None or panel.empty or "티커" not in panel.columns:
        return None

    sub = panel.copy()
    sub["티커"] = sub["티커"].astype(str)
    sub = sub[sub["티커"].isin(close_wide.columns)].drop_duplicates("티커")
    if sub.empty:
        return None

    tickers = sub["티커"].tolist()
    close_t = pd.to_numeric(close_wide.loc[t, tickers], errors="coerce")
    close_h = pd.to_numeric(close_wide.loc[t_h, tickers], errors="coerce")
    valid = (
        close_t.notna()
        & close_h.notna()
        & np.isfinite(close_t)
        & np.isfinite(close_h)
        & (close_t > 0)
        & (close_h > 0)
    )
    if int(valid.sum()) < MIN_IC_STOCKS:
        return None

    valid_tickers = close_t.index[valid].tolist()
    sub = sub.set_index("티커").reindex(valid_tickers)
    fwd = (close_h.loc[valid_tickers] / close_t.loc[valid_tickers] - 1.0).astype(float)
    sub = sub.copy()
    sub["fwd"] = fwd
    sub["시가총액"] = pd.to_numeric(sub.get("시가총액"), errors="coerce")
    for col in PCT_COLS + DIAG_PCT_COLS:
        if col not in sub.columns:
            sub[col] = np.nan
        else:
            sub[col] = pd.to_numeric(sub[col], errors="coerce")
    for col in METRIC_COLS + DIAG_FACTORS:
        if col not in sub.columns:
            sub[col] = np.nan
        else:
            sub[col] = pd.to_numeric(sub[col], errors="coerce")
    return sub


def evaluate_deciles_on_dates(
    *,
    eval_dates: list[date],
    h: int,
    factor: str,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
) -> pd.DataFrame:
    """
    평가일마다 factor 백분위로 십분위 분할 후,
    십분위별 평균 전방수익·초과수익·중앙값(시총/talent/tv5)을 산출하고 전일 평균.
    """
    pct_col = f"pct_{factor}"
    day_rows: list[dict] = []

    for t in eval_dates:
        sub = _day_panel_with_fwd(
            t=t,
            h=h,
            dates=dates,
            date_to_idx=date_to_idx,
            close_wide=close_wide,
            panels=panels,
        )
        if sub is None or pct_col not in sub.columns:
            continue

        pct = pd.to_numeric(sub[pct_col], errors="coerce")
        fwd = pd.to_numeric(sub["fwd"], errors="coerce")
        mcap = pd.to_numeric(sub["시가총액"], errors="coerce")
        talent = pd.to_numeric(sub["talent"], errors="coerce")
        tv5 = pd.to_numeric(sub["tv5"], errors="coerce")

        mask = pct.notna() & np.isfinite(pct) & fwd.notna() & np.isfinite(fwd)
        if int(mask.sum()) < MIN_IC_STOCKS:
            continue

        pct = pct[mask]
        fwd = fwd[mask]
        mcap = mcap[mask]
        talent = talent[mask]
        tv5 = tv5[mask]

        dec = _assign_deciles(pct)
        uni_mean = float(fwd.mean())
        for d in range(1, N_DECILES + 1):
            bucket = dec == d
            n = int(bucket.sum())
            if n <= 0:
                continue
            mean_fwd = float(fwd[bucket].mean())
            day_rows.append(
                {
                    "h": int(h),
                    "factor": factor,
                    "decile": int(d),
                    "mean_fwd_ret": mean_fwd,
                    "excess_ret": mean_fwd - uni_mean,
                    "n": n,
                    "median_mcap": float(mcap[bucket].median())
                    if mcap[bucket].notna().any()
                    else float("nan"),
                    "median_talent": float(talent[bucket].median())
                    if talent[bucket].notna().any()
                    else float("nan"),
                    "median_tv5": float(tv5[bucket].median())
                    if tv5[bucket].notna().any()
                    else float("nan"),
                }
            )

    if not day_rows:
        return pd.DataFrame(
            columns=[
                "h",
                "factor",
                "decile",
                "mean_fwd_ret",
                "excess_ret",
                "n",
                "median_mcap",
                "median_talent",
                "median_tv5",
            ]
        )

    daily = pd.DataFrame(day_rows)
    agg = (
        daily.groupby(["h", "factor", "decile"], as_index=False)
        .agg(
            mean_fwd_ret=("mean_fwd_ret", "mean"),
            excess_ret=("excess_ret", "mean"),
            n=("n", "mean"),
            median_mcap=("median_mcap", "mean"),
            median_talent=("median_talent", "mean"),
            median_tv5=("median_tv5", "mean"),
        )
        .sort_values(["h", "factor", "decile"])
        .reset_index(drop=True)
    )
    return agg


def evaluate_size_neutral_quintiles(
    *,
    eval_dates: list[date],
    h: int,
    factor: str,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
) -> pd.DataFrame:
    """
    시총 3분위(대/중/소) 안에서 factor 5분위 →
    버킷×분위 평균 전방수익·버킷평균 대비 초과수익 (사이즈 중립).
    """
    pct_col = f"pct_{factor}"
    day_rows: list[dict] = []

    for t in eval_dates:
        sub = _day_panel_with_fwd(
            t=t,
            h=h,
            dates=dates,
            date_to_idx=date_to_idx,
            close_wide=close_wide,
            panels=panels,
        )
        if sub is None or pct_col not in sub.columns:
            continue

        pct = pd.to_numeric(sub[pct_col], errors="coerce")
        fwd = pd.to_numeric(sub["fwd"], errors="coerce")
        mcap = pd.to_numeric(sub["시가총액"], errors="coerce")
        mask = (
            pct.notna()
            & np.isfinite(pct)
            & fwd.notna()
            & np.isfinite(fwd)
            & mcap.notna()
            & np.isfinite(mcap)
            & (mcap > 0)
        )
        if int(mask.sum()) < MIN_IC_STOCKS:
            continue

        pct = pct[mask]
        fwd = fwd[mask]
        mcap = mcap[mask]
        mcap_bin = _assign_bins(mcap, N_MCAP_TERCILES)
        if mcap_bin.isna().all():
            continue

        for mb in range(1, N_MCAP_TERCILES + 1):
            in_mb = mcap_bin == mb
            if int(in_mb.sum()) < N_SIZE_QUINTILES:
                continue
            pct_b = pct[in_mb]
            fwd_b = fwd[in_mb]
            q = _assign_bins(pct_b, N_SIZE_QUINTILES)
            bucket_mean = float(fwd_b.mean())
            bucket_label = MCAP_BUCKET_LABELS[mb]
            for qi in range(1, N_SIZE_QUINTILES + 1):
                in_q = q == qi
                n = int(in_q.sum())
                if n <= 0:
                    continue
                mean_fwd = float(fwd_b[in_q].mean())
                day_rows.append(
                    {
                        "h": int(h),
                        "factor": factor,
                        "mcap_bucket": bucket_label,
                        "quintile": int(qi),
                        "mean_fwd_ret": mean_fwd,
                        "excess_vs_bucket": mean_fwd - bucket_mean,
                        "n": n,
                    }
                )

    empty_cols = [
        "h",
        "factor",
        "mcap_bucket",
        "quintile",
        "mean_fwd_ret",
        "excess_vs_bucket",
        "n",
    ]
    if not day_rows:
        return pd.DataFrame(columns=empty_cols)

    daily = pd.DataFrame(day_rows)
    bucket_order = list(MCAP_BUCKET_LABELS.values())
    agg = (
        daily.groupby(["h", "factor", "mcap_bucket", "quintile"], as_index=False)
        .agg(
            mean_fwd_ret=("mean_fwd_ret", "mean"),
            excess_vs_bucket=("excess_vs_bucket", "mean"),
            n=("n", "mean"),
        )
        .sort_values(["h", "factor", "mcap_bucket", "quintile"])
        .reset_index(drop=True)
    )
    agg["mcap_bucket"] = pd.Categorical(
        agg["mcap_bucket"], categories=bucket_order, ordered=True
    )
    return agg.sort_values(["h", "factor", "mcap_bucket", "quintile"]).reset_index(drop=True)


def evaluate_size_neutral_ic(
    *,
    eval_dates: list[date],
    h: int,
    factor: str,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
) -> dict:
    """
    원본 pct Spearman IC vs 시총 버킷 내 재백분위 Spearman IC (전구간 meanIC).
    """
    pct_col = f"pct_{factor}"
    raw_ics: list[float] = []
    neut_ics: list[float] = []

    for t in eval_dates:
        sub = _day_panel_with_fwd(
            t=t,
            h=h,
            dates=dates,
            date_to_idx=date_to_idx,
            close_wide=close_wide,
            panels=panels,
        )
        if sub is None or pct_col not in sub.columns:
            continue

        pct = pd.to_numeric(sub[pct_col], errors="coerce")
        fwd = pd.to_numeric(sub["fwd"], errors="coerce")
        mcap = pd.to_numeric(sub["시가총액"], errors="coerce")
        # 원본 IC: 유효 pct·fwd
        raw_ic = _spearman_ic_series(pct, fwd)
        if np.isfinite(raw_ic):
            raw_ics.append(raw_ic)

        mask = (
            pct.notna()
            & np.isfinite(pct)
            & fwd.notna()
            & np.isfinite(fwd)
            & mcap.notna()
            & np.isfinite(mcap)
            & (mcap > 0)
        )
        if int(mask.sum()) < MIN_IC_STOCKS:
            continue
        mcap_bin = _assign_bins(mcap[mask], N_MCAP_TERCILES)
        if mcap_bin.isna().any() and int(mcap_bin.notna().sum()) < MIN_IC_STOCKS:
            continue
        # 버킷 내 재백분위 (원본 pct 또는 raw factor — pct가 시장내 순위이므로
        # 동일 방향 유지; 버킷 내에서는 원본 pct 순위로 재스케일)
        pct_neu = _pct_within_groups(pct[mask], mcap_bin)
        neut_ic = _spearman_ic_series(pct_neu, fwd[mask])
        if np.isfinite(neut_ic):
            neut_ics.append(neut_ic)

    raw_arr = np.asarray(raw_ics, dtype=float)
    neut_arr = np.asarray(neut_ics, dtype=float)
    raw_mean, raw_ir, raw_n = ic_stats(raw_arr)
    neut_mean, neut_ir, neut_n = ic_stats(neut_arr)
    return {
        "h": int(h),
        "factor": factor,
        "meanIC_raw": raw_mean,
        "IC_IR_raw": raw_ir,
        "n_obs_raw": int(raw_n),
        "meanIC_size_neutral": neut_mean,
        "IC_IR_size_neutral": neut_ir,
        "n_obs_size_neutral": int(neut_n),
        "delta_meanIC": (
            float(neut_mean - raw_mean)
            if np.isfinite(neut_mean) and np.isfinite(raw_mean)
            else float("nan")
        ),
    }


def _print_size_neutral_ic_table(ic_df: pd.DataFrame) -> None:
    """콘솔: 원본 meanIC vs 사이즈중립 meanIC."""
    if ic_df is None or ic_df.empty:
        log.warning("사이즈중립 IC 결과가 비어 콘솔 표를 건너뜁니다.")
        return
    show = ic_df.copy()
    for c in ("meanIC_raw", "meanIC_size_neutral", "delta_meanIC", "IC_IR_raw", "IC_IR_size_neutral"):
        if c in show.columns:
            show[c] = show[c].map(lambda x: f"{x:.4f}" if np.isfinite(x) else "-")
    cols = [
        "h",
        "factor",
        "meanIC_raw",
        "meanIC_size_neutral",
        "delta_meanIC",
        "n_obs_raw",
        "n_obs_size_neutral",
    ]
    print()
    print("=== Size-neutral IC: raw meanIC vs within-mcap-bucket pct meanIC ===")
    print(show[cols].to_string(index=False))


def _print_size_neutral_quintile_tables(result: pd.DataFrame) -> None:
    """콘솔: h∈{20,50} × tv5 × 시총버킷 5분위 초과수익."""
    if result is None or result.empty:
        return
    for h in SIZE_NEUTRAL_H:
        for factor in SIZE_NEUTRAL_QUINTILE_FACTORS:
            part = result[(result["h"] == h) & (result["factor"] == factor)].copy()
            if part.empty:
                continue
            print()
            print(
                f"=== Size-neutral quintile h={h} | {factor} "
                f"(Q1=최저~Q5=최고, excess=버킷평균 대비) ==="
            )
            for bucket in ("large", "mid", "small"):
                b = part[part["mcap_bucket"] == bucket].copy()
                if b.empty:
                    continue
                show = b[["quintile", "mean_fwd_ret", "excess_vs_bucket", "n"]].copy()
                show["mean_fwd_ret"] = show["mean_fwd_ret"].map(lambda x: f"{x * 100:.2f}%")
                show["excess_vs_bucket"] = show["excess_vs_bucket"].map(
                    lambda x: f"{x * 100:.2f}%"
                )
                show["n"] = show["n"].map(lambda x: f"{x:.1f}")
                show = show.rename(
                    columns={
                        "quintile": "Q",
                        "mean_fwd_ret": "mean_fwd",
                        "excess_vs_bucket": "excess_vs_bucket",
                    }
                )
                print(f"  [{bucket}]")
                print(show.to_string(index=False))


def evaluate_single_pct_ic_on_dates(
    *,
    eval_dates: list[date],
    h: int,
    pct_col: str,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
) -> np.ndarray:
    """단일 pct 컬럼의 평가일별 Spearman IC 시계열 (격자 밖 진단용)."""
    out = np.full(len(eval_dates), np.nan, dtype=float)
    for row_i, t in enumerate(eval_dates):
        sub = _day_panel_with_fwd(
            t=t,
            h=h,
            dates=dates,
            date_to_idx=date_to_idx,
            close_wide=close_wide,
            panels=panels,
        )
        if sub is None or pct_col not in sub.columns:
            continue
        out[row_i] = _spearman_ic_series(sub[pct_col], sub["fwd"])
    return out


def run_size_neutral_analysis(
    close_wide: Optional[pd.DataFrame] = None,
    output_csv: Path | str = SIZE_NEUTRAL_CSV,
    eng=None,
    ctx: Optional[dict] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    시총 3분위 × 지표 5분위 + 사이즈중립 IC.
    CSV: decile_size_neutral_tv5.csv / 콘솔에 IC 비교표·tv5 분위.
    """
    ctx = ctx or prepare_eval_context(close_wide=close_wide, eng=eng)
    close_wide = ctx["close_wide"]
    dates = ctx["dates"]
    date_to_idx = ctx["date_to_idx"]
    eval_dates = ctx["eval_dates"]
    panels = ctx["panels"]

    q_parts: list[pd.DataFrame] = []
    ic_rows: list[dict] = []
    for h in SIZE_NEUTRAL_H:
        for factor in SIZE_NEUTRAL_IC_FACTORS:
            log.info("=== 사이즈중립 진단 h=%d factor=%s ===", h, factor)
            t0 = time.time()
            if factor in SIZE_NEUTRAL_QUINTILE_FACTORS:
                q_parts.append(
                    evaluate_size_neutral_quintiles(
                        eval_dates=eval_dates,
                        h=h,
                        factor=factor,
                        dates=dates,
                        date_to_idx=date_to_idx,
                        close_wide=close_wide,
                        panels=panels,
                    )
                )
            ic_rows.append(
                evaluate_size_neutral_ic(
                    eval_dates=eval_dates,
                    h=h,
                    factor=factor,
                    dates=dates,
                    date_to_idx=date_to_idx,
                    close_wide=close_wide,
                    panels=panels,
                )
            )
            log.info(
                "h=%d %s 사이즈중립 완료 (%.1fs) rawIC=%.4f neutIC=%.4f",
                h,
                factor,
                time.time() - t0,
                ic_rows[-1]["meanIC_raw"],
                ic_rows[-1]["meanIC_size_neutral"],
            )

    quintile_df = (
        pd.concat(q_parts, ignore_index=True)
        if q_parts
        else pd.DataFrame(
            columns=[
                "h",
                "factor",
                "mcap_bucket",
                "quintile",
                "mean_fwd_ret",
                "excess_vs_bucket",
                "n",
            ]
        )
    )
    ic_df = pd.DataFrame(ic_rows)
    if not ic_df.empty:
        ic_df = ic_df.sort_values(["h", "factor"]).reset_index(drop=True)

    out_path = Path(output_csv)
    quintile_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info("사이즈중립 CSV 저장: %s (%d행)", out_path, len(quintile_df))

    ic_path = out_path.with_name(out_path.stem + "_ic.csv")
    ic_df.to_csv(ic_path, index=False, encoding="utf-8-sig")
    log.info("사이즈중립 IC CSV 저장: %s (%d행)", ic_path, len(ic_df))

    _print_size_neutral_ic_table(ic_df)
    _print_size_neutral_quintile_tables(quintile_df)
    return quintile_df, ic_df


def _print_decile_tables(result: pd.DataFrame) -> None:
    """콘솔: h∈{20,50} × tv5·talent 십분위 표."""
    if result is None or result.empty:
        log.warning("십분위 결과가 비어 콘솔 표를 건너뜁니다.")
        return
    for h in DECILE_CONSOLE_H:
        for factor in DECILE_CONSOLE_FACTORS:
            part = result[(result["h"] == h) & (result["factor"] == factor)].copy()
            if part.empty:
                log.warning("콘솔 표 없음: h=%d factor=%s", h, factor)
                continue
            show = part[
                [
                    "decile",
                    "mean_fwd_ret",
                    "excess_ret",
                    "n",
                    "median_mcap",
                    "median_talent",
                    "median_tv5",
                ]
            ].copy()
            show["mean_fwd_ret"] = show["mean_fwd_ret"].map(lambda x: f"{x * 100:.2f}%")
            show["excess_ret"] = show["excess_ret"].map(lambda x: f"{x * 100:.2f}%")
            show["n"] = show["n"].map(lambda x: f"{x:.1f}")
            show["median_mcap"] = show["median_mcap"].map(
                lambda x: f"{x / 1e8:.0f}억" if np.isfinite(x) else "-"
            )
            show["median_talent"] = show["median_talent"].map(
                lambda x: f"{x:.3f}" if np.isfinite(x) else "-"
            )
            show["median_tv5"] = show["median_tv5"].map(
                lambda x: f"{x / 1e8:.0f}억" if np.isfinite(x) else "-"
            )
            show = show.rename(
                columns={
                    "decile": "D",
                    "mean_fwd_ret": "mean_fwd",
                    "excess_ret": "excess",
                    "median_mcap": "med_mcap",
                    "median_talent": "med_talent",
                    "median_tv5": "med_tv5",
                }
            )
            print()
            print(f"=== Decile h={h} | {factor} (D1=최저 ~ D10=최고) ===")
            print(show.to_string(index=False))


def run_decile_analysis(
    close_wide: Optional[pd.DataFrame] = None,
    output_csv: Path | str = DECILE_CSV,
    eng=None,
    ctx: Optional[dict] = None,
) -> pd.DataFrame:
    """캐시 패널 재사용 십분위 분석 → decile_analysis_tv5.csv."""
    ctx = ctx or prepare_eval_context(close_wide=close_wide, eng=eng)
    close_wide = ctx["close_wide"]
    dates = ctx["dates"]
    date_to_idx = ctx["date_to_idx"]
    eval_dates = ctx["eval_dates"]
    panels = ctx["panels"]

    parts: list[pd.DataFrame] = []
    for h in HOLDING_DAYS:
        for factor in METRIC_COLS:
            log.info("=== 십분위 분석 h=%d factor=%s ===", h, factor)
            t0 = time.time()
            part = evaluate_deciles_on_dates(
                eval_dates=eval_dates,
                h=h,
                factor=factor,
                dates=dates,
                date_to_idx=date_to_idx,
                close_wide=close_wide,
                panels=panels,
            )
            log.info(
                "h=%d %s 완료: rows=%d (%.1fs)",
                h,
                factor,
                len(part),
                time.time() - t0,
            )
            parts.append(part)

    result = (
        pd.concat(parts, ignore_index=True)
        if parts
        else pd.DataFrame(
            columns=[
                "h",
                "factor",
                "decile",
                "mean_fwd_ret",
                "excess_ret",
                "n",
                "median_mcap",
                "median_talent",
                "median_tv5",
            ]
        )
    )
    out_path = Path(output_csv)
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info("십분위 CSV 저장: %s (%d행)", out_path, len(result))
    _print_decile_tables(result)
    return result


def run_backtest(
    close_wide: Optional[pd.DataFrame] = None,
    output_csv: Path | str = OUTPUT_CSV,
    eng=None,
    ctx: Optional[dict] = None,
) -> pd.DataFrame:
    ctx = ctx or prepare_eval_context(close_wide=close_wide, eng=eng)
    close_wide = ctx["close_wide"]
    dates = ctx["dates"]
    date_to_idx = ctx["date_to_idx"]
    eval_dates = ctx["eval_dates"]
    panels = ctx["panels"]

    weight_list = generate_weight_grid(WEIGHT_STEP)
    log.info("가중치 격자: %d개 (step=%.1f, 합=1)", len(weight_list), WEIGHT_STEP)

    ic_by_h: dict[int, np.ndarray] = {}
    for h in HOLDING_DAYS:
        log.info("=== 보유기간 h=%d Rank IC 평가 시작 ===", h)
        t0 = time.time()
        ic_by_h[h], stock_counts = evaluate_ic_on_dates(
            eval_dates=eval_dates,
            h=h,
            dates=dates,
            date_to_idx=date_to_idx,
            close_wide=close_wide,
            panels=panels,
            weight_list=weight_list,
        )
        valid_dates = int(np.isfinite(ic_by_h[h]).any(axis=1).sum())
        valid_stock_counts = stock_counts[stock_counts > 0]
        median_stocks = (
            float(np.median(valid_stock_counts)) if len(valid_stock_counts) else float("nan")
        )
        log.info(
            "h=%d IC 평가 완료: dates=%d median_stocks=%.0f (%.1fs)",
            h,
            valid_dates,
            median_stocks,
            time.time() - t0,
        )

    rows: list[dict] = []
    for h in HOLDING_DAYS:
        kind = HORIZON_KIND[h]
        part = summarize_ic_horizon(
            h=h,
            ic_matrix=ic_by_h[h],
            weight_list=weight_list,
            eval_dates=eval_dates,
        )
        rows.extend(part)

        best = next(r for r in part if r["type"] == "best")
        wf = [r for r in part if r["type"] == "wf_step"]
        wf_means = [float(r["meanIC"]) for r in wf if np.isfinite(r["meanIC"])]
        wf_mean = float(np.mean(wf_means)) if wf_means else float("nan")
        wf_std = float(np.std(wf_means, ddof=1)) if len(wf_means) >= 2 else float("nan")
        base = next((r for r in part if r["type"] == "baseline"), None)
        if base is not None:
            log.info(
                "[%s h=%d] 추천 %s | full meanIC=%.4f IC_IR=%.3f | "
                "WF test meanIC=%.4f±%.4f | baseline meanIC=%.4f IC_IR=%.3f",
                kind,
                h,
                best["weights"],
                best["meanIC"],
                best["IC_IR"],
                wf_mean,
                wf_std,
                base["meanIC"],
                base["IC_IR"],
            )
        else:
            log.info(
                "[%s h=%d 참고] 추천 %s | full meanIC=%.4f IC_IR=%.3f | "
                "WF test meanIC=%.4f±%.4f",
                kind,
                h,
                best["weights"],
                best["meanIC"],
                best["IC_IR"],
                wf_mean,
                wf_std,
            )
        log.info("  %s", best["note"])
        for r in (x for x in part if x["type"] == "single_factor"):
            log.info(
                "  h=%d 단일가중 %s | full meanIC=%.4f IC_IR=%.3f n=%d",
                h,
                r["weights"],
                r["meanIC"],
                r["IC_IR"],
                r["n_obs"],
            )
        # 격자 밖 진단: tv5_turn 단일 IC
        for diag in DIAG_FACTORS:
            pct_col = f"pct_{diag}"
            diag_ic = evaluate_single_pct_ic_on_dates(
                eval_dates=eval_dates,
                h=h,
                pct_col=pct_col,
                dates=dates,
                date_to_idx=date_to_idx,
                close_wide=close_wide,
                panels=panels,
            )
            mean_ic, ic_ir, n_obs = ic_stats(diag_ic)
            diag_row = {
                "h": int(h),
                "kind": kind,
                "type": "single_factor_diag",
                "rank": 0,
                "wf_step": 0,
                "weights": f"{diag}=1.0 (격자 밖)",
                "meanIC": mean_ic,
                "IC_IR": ic_ir,
                "n_obs": int(n_obs),
                "note": f"h={h} 진단 단일신호: {diag} (시총 정규화 회전율)",
            }
            for col in METRIC_COLS:
                diag_row[f"w_{col}"] = 0.0
            rows.append(diag_row)
            log.info(
                "  h=%d 진단 %s | full meanIC=%.4f IC_IR=%.3f n=%d",
                h,
                diag,
                mean_ic,
                ic_ir,
                n_obs,
            )
        for r in wf:
            log.info(
                "  WF step%d: %s | test meanIC=%.4f IC_IR=%.3f n=%d",
                r["wf_step"],
                r["weights"],
                r["meanIC"],
                r["IC_IR"],
                r["n_obs"],
            )

    result = pd.DataFrame(rows)
    out_path = Path(output_csv)
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info("결과 CSV 저장: %s (%d행)", out_path, len(result))
    return result


def _generate_weight_grid_for(factors: tuple[str, ...], step: float = WEIGHT_STEP) -> list[np.ndarray]:
    """임의 n팩터의 step 단순체 격자."""
    units = int(round(1.0 / step))
    rows: list[np.ndarray] = []

    def _walk(left: int, k: int, prefix: list[int]) -> None:
        if k == 1:
            rows.append(np.asarray([*prefix, left], dtype=float) * step)
            return
        for v in range(left + 1):
            _walk(left - v, k - 1, [*prefix, v])

    _walk(units, len(factors), [])
    return rows


def _dynamic_grid_ic(
    *,
    h: int,
    factors: tuple[str, ...],
    signs: np.ndarray,
    eval_dates: list[date],
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
    weights: list[np.ndarray],
) -> np.ndarray:
    """선별 팩터의 부호를 높을수록 좋음으로 정규화해 평가일×격자 IC를 산출."""
    out = np.full((len(eval_dates), len(weights)), np.nan, dtype=float)
    wmat = np.vstack(weights)
    pct_cols = [f"pct_{f}" for f in factors]
    for i, t in enumerate(eval_dates):
        sub = _day_panel_with_fwd(
            t=t, h=h, dates=dates, date_to_idx=date_to_idx,
            close_wide=close_wide, panels=panels,
        )
        if sub is None or any(c not in sub.columns for c in pct_cols):
            continue
        x = sub[pct_cols].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)
        y = pd.to_numeric(sub["fwd"], errors="coerce").to_numpy(dtype=float)
        valid = np.isfinite(y) & np.all(np.isfinite(x), axis=1)
        if int(valid.sum()) < MIN_IC_STOCKS:
            continue
        scores = (x[valid] * signs[None, :]) @ wmat.T
        ranks = pd.DataFrame(scores).rank(method="average").to_numpy(dtype=float)
        ranks -= ranks.mean(axis=0, keepdims=True)
        y_rank = pd.Series(y[valid]).rank(method="average").to_numpy(dtype=float)
        y_rank -= y_rank.mean()
        numerator = np.sum(ranks * y_rank[:, None], axis=0)
        denominator = np.sqrt(np.sum(ranks * ranks, axis=0) * np.sum(y_rank * y_rank))
        out[i] = np.divide(
            numerator, denominator, out=np.full(len(weights), np.nan), where=denominator > 0
        )
    return out


def _short_grid_rows(
    h: int,
    factors: tuple[str, ...],
    signs: np.ndarray,
    ic_matrix: np.ndarray,
    eval_dates: list[date],
    weights: list[np.ndarray],
) -> list[dict]:
    """전구간 최적 + expanding 5블록 test 행."""
    valid = np.isfinite(ic_matrix).any(axis=1)
    work = ic_matrix[valid]
    work_dates = [d for d, ok in zip(eval_dates, valid) if ok]
    if work.size == 0:
        return []
    means = np.nanmean(work, axis=0)
    best_j = int(np.nanargmax(means))

    def _row(kind: str, j: int, values: np.ndarray, step: int = 0, note: str = "") -> dict:
        mean_ic, ic_ir, n_obs = ic_stats(values)
        w = weights[j]
        return {
            "h": h, "type": kind, "wf_step": step,
            "selected_factors": "|".join(factors),
            "signs": "|".join(str(int(x)) for x in signs),
            "weights": " / ".join(f"{f}={x:.1f}" for f, x in zip(factors, w)),
            "meanIC": mean_ic, "IC_IR": ic_ir, "n_obs": n_obs, "note": note,
        }

    rows = [_row("best_full", best_j, work[:, best_j], note="전체 표본 선별·최적")]
    if len(work_dates) >= WF_BLOCKS:
        blocks = np.array_split(np.arange(len(work_dates)), WF_BLOCKS)
        for step in range(1, WF_BLOCKS):
            train = np.concatenate(blocks[:step])
            test = blocks[step]
            j = int(np.nanargmax(np.nanmean(work[train], axis=0)))
            rows.append(
                _row(
                    "wf_step", j, work[test, j], step,
                    note=f"train={work_dates[train[0]]}~{work_dates[train[-1]]}; "
                    f"test={work_dates[test[0]]}~{work_dates[test[-1]]}",
                )
            )
    return rows


def run_short_factor_search(ctx: Optional[dict] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1) 모든 후보의 raw/사이즈중립 단일 IC.
    2) |size-neutral meanIC|>=0.02 및 부호 일관 후보(최대 5개)만 h별 격자.
    """
    ctx = ctx or prepare_eval_context()
    close_wide, dates = ctx["close_wide"], ctx["dates"]
    date_to_idx, eval_dates, panels = ctx["date_to_idx"], ctx["eval_dates"], ctx["panels"]
    investor_min = ctx.get("investor_min")

    single_rows: list[dict] = []
    for h in SHORT_HOLDING_DAYS:
        for factor in SHORT_FACTOR_COLS:
            stat = evaluate_size_neutral_ic(
                eval_dates=eval_dates, h=h, factor=factor, dates=dates,
                date_to_idx=date_to_idx, close_wide=close_wide, panels=panels,
            )
            stat["is_investor_factor"] = bool(factor in FLOW_FACTORS)
            stat["sample_note"] = (
                f"investor data from {investor_min}; n_obs differs from full-history factors"
                if factor in FLOW_FACTORS else "full-history factor"
            )
            single_rows.append(stat)
    single_df = pd.DataFrame(single_rows).sort_values(["h", "factor"]).reset_index(drop=True)
    single_df.to_csv(SHORT_IC_CSV, index=False, encoding="utf-8-sig")
    _print_size_neutral_ic_table(single_df)

    grid_rows: list[dict] = []
    for h in SHORT_HOLDING_DAYS:
        part = single_df[single_df["h"] == h].copy()
        part = part[np.isfinite(part["meanIC_size_neutral"])]
        part = part[part["meanIC_size_neutral"].abs() >= SHORT_IC_THRESHOLD]
        # 동일 방향 일관성: raw와 중립 IC 부호가 같아야 하며, 절대 IC 큰 순 최대 5개.
        part = part[
            np.sign(part["meanIC_raw"]) == np.sign(part["meanIC_size_neutral"])
        ].sort_values("meanIC_size_neutral", key=lambda s: s.abs(), ascending=False)
        part = part.head(SHORT_GRID_MAX_FACTORS)
        factors = tuple(part["factor"].tolist())
        if not factors:
            grid_rows.append({
                "h": h, "type": "no_selected_factor", "wf_step": 0,
                "selected_factors": "", "signs": "", "weights": "",
                "meanIC": np.nan, "IC_IR": np.nan, "n_obs": 0,
                "note": f"|size-neutral meanIC| >= {SHORT_IC_THRESHOLD:.2f} 통과 없음",
            })
            continue
        signs = np.sign(part["meanIC_size_neutral"].to_numpy(dtype=float))
        weights = _generate_weight_grid_for(factors)
        matrix = _dynamic_grid_ic(
            h=h, factors=factors, signs=signs, eval_dates=eval_dates, dates=dates,
            date_to_idx=date_to_idx, close_wide=close_wide, panels=panels, weights=weights,
        )
        grid_rows.extend(_short_grid_rows(h, factors, signs, matrix, eval_dates, weights))

    grid_df = pd.DataFrame(grid_rows)
    grid_df.to_csv(SHORT_GRID_CSV, index=False, encoding="utf-8-sig")
    log.info("단기 단일팩터 IC 저장: %s (%d행)", SHORT_IC_CSV, len(single_df))
    log.info("단기 선별 격자 저장: %s (%d행)", SHORT_GRID_CSV, len(grid_df))
    return single_df, grid_df


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Spyder 콘솔에도 보이도록
    logging.getLogger().setLevel(logging.INFO)
    mode = str(RUN_MODE).strip().lower()
    log.info(
        "로컬 picking tv5 광의유니버스 분석 시작 mode=%s "
        "(DB=%s, eval_interval=%d, holdings=%s, WF=%d, mcap>=%s)",
        mode,
        db_connect_kwargs().get("db") or "kor_stock_db",
        EVAL_INTERVAL,
        HOLDING_DAYS,
        WF_BLOCKS,
        f"{MIN_MCAP / 1e8:.0f}억",
    )
    allowed = {"size_neutral", "backtest", "both"}
    if mode not in allowed:
        raise ValueError(f"RUN_MODE 미지원: {RUN_MODE!r} ({'|'.join(sorted(allowed))})")

    ctx = prepare_eval_context()

    # 단기 탐색은 raw·사이즈중립 단일 IC와 선별 격자를 한 실행으로 산출한다.
    # RUN_MODE 호환성은 유지하되 기존 tv5 장기 분석 대신 v4 단기 탐색을 실행한다.
    if mode in {"backtest", "size_neutral", "both"}:
        run_short_factor_search(ctx=ctx)
    log.info("분석 완료 (mode=%s)", mode)


if __name__ == "__main__":
    main()
