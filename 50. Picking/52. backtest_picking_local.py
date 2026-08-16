# -*- coding: utf-8 -*-
"""
picking 결합 가중치 백테스트 (로컬 root DB / kor_stock_db).

naverPub/backtest_picking.py 와 동일 절차·지표 정의.
차이는 로딩 SQL·컬럼명뿐 (krx_ohlcv / krx_ticker / krx_relative_strength).

WEIGHT_SETS 는 수정하지 않고 추천값만 출력한다.
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
from indicators_core import energy_ratio, rs_avg, talent_score  # noqa: E402

log = logging.getLogger("backtest_picking_local")

# ── naverPub content_picking 과 동일 상수 (수정하지 않음) ───────────────────
TOP_N = 30
UNIVERSE_MCAP_MIN = 300_000_000_000  # 3,000억원
METRIC_COLS = ("에너지배율", "RS", "주가위치", "talent")
WEIGHT_SETS: dict[str, dict[str, float]] = {
    "long": {
        "RS": 0.50,
        "주가위치": 0.30,
        "talent": 0.10,
        "에너지배율": 0.10,
    },
    "short": {
        "주가위치": 0.40,
        "RS": 0.20,
        "talent": 0.20,
        "에너지배율": 0.20,
    },
}

MARKETS = ("KOSPI", "KOSDAQ")
PRICE_POS_WINDOW = 120
HOLDING_DAYS = (5, 20, 60)
WEIGHT_STEP = 0.1
IS_FRAC = 0.70
WARMUP_DAYS = max(PRICE_POS_WINDOW + 10, 130)
OUTPUT_CSV = Path(__file__).resolve().parent / "backtest_picking_results_local.csv"
PCT_COLS = tuple(f"pct_{c}" for c in METRIC_COLS)
KIND_KEYS = tuple(WEIGHT_SETS.keys())
FACTOR_PANEL_COLS = [
    "시장",
    "티커",
    "종목명",
    "현재가",
    "시가총액",
    "RS",
    "주가위치",
    "talent",
    "에너지배율",
    "pct_RS",
    "pct_주가위치",
    "pct_talent",
    "pct_에너지배율",
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


def _ensure_tv_mcap(hist: pd.DataFrame) -> pd.DataFrame:
    """trading_value·mcap 정리. TV 없으면 close*volume. mcap은 종목별 ffill/bfill."""
    d = hist.copy()
    for c in ("open", "high", "low", "close", "volume", "mcap", "trading_value"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")
    if "trading_value" not in d.columns:
        d["trading_value"] = np.nan
    tv_missing = d["trading_value"].isna() | (d["trading_value"] <= 0)
    if tv_missing.any() and {"close", "volume"}.issubset(d.columns):
        d.loc[tv_missing, "trading_value"] = (
            d.loc[tv_missing, "close"] * d.loc[tv_missing, "volume"]
        )
    if "mcap" in d.columns:
        d = d.sort_values(["ticker", "date"])
        d["mcap"] = d.groupby("ticker")["mcap"].transform(lambda s: s.ffill().bfill())
    return d


def _load_ohlcv_hist(
    eng,
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    frames = []
    for i in range(0, len(tickers), 400):
        chunk = tickers[i : i + 400]
        ph = ",".join(["%s"] * len(chunk))
        try:
            frames.append(
                pd.read_sql(
                    f"""
                    SELECT date, ticker, open, high, low, close, volume,
                           mcap, trading_value
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
                    SELECT date, ticker, open, high, low, close, volume, mcap
                    FROM krx_ohlcv
                    WHERE date >= %s AND date <= %s
                      AND ticker IN ({ph})
                    """,
                    eng,
                    params=(start, end, *chunk),
                )
            )
    if not frames:
        return pd.DataFrame()
    hist = pd.concat(frames, ignore_index=True)
    if hist.empty:
        return hist
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.date
    hist["ticker"] = hist["ticker"].astype(str).str.strip().str.zfill(6)
    return _ensure_tv_mcap(hist)


def build_factor_panel_local(engine, as_of: Optional[date], market: str) -> pd.DataFrame:
    """
    naverPub build_factor_panel 과 동일 산출 (root 스키마 로딩).

    유니버스: 보통주·시장·전역제외 + as_of 시총 >= 3,000억.
    에너지 분모는 시총 하한 적용 전 해당 시장 전체 보통주.
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

    dates = _trading_dates(engine, as_of, max(PRICE_POS_WINDOW + 10, 130))
    if not dates:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    d0 = dates[-1]
    hist = _load_ohlcv_hist(engine, all_tickers, dates[0], d0)
    if hist.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)

    # 분모 모집단: 해당 시장 전체 보통주(시총 하한 미적용)
    day0_all = hist[hist["date"] == d0].drop_duplicates("ticker", keep="last").copy()
    if day0_all.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    day0_all["name"] = day0_all["ticker"].map(name_map)
    day0_all["market"] = market
    mkt_mcap = day0_all["mcap"].sum(min_count=1)

    energy_days = dates[-3:]
    tv3 = (
        hist[hist["date"].isin(energy_days)]
        .groupby("ticker", as_index=False)["trading_value"]
        .sum(min_count=1)
        .rename(columns={"trading_value": "_tv_3d"})
    )
    mkt_tv3 = hist.loc[hist["date"].isin(energy_days), "trading_value"].sum(min_count=1)
    day0_all = day0_all.merge(tv3, on="ticker", how="left")

    base_d = dates[-4] if len(dates) >= 4 else None
    if base_d is not None:
        base_close = (
            hist[hist["date"] == base_d][["ticker", "close"]]
            .drop_duplicates("ticker", keep="last")
            .rename(columns={"close": "_close_3d_base"})
        )
        day0_all = day0_all.merge(base_close, on="ticker", how="left")
    else:
        day0_all["_close_3d_base"] = np.nan

    base_ok = (
        day0_all["_close_3d_base"].notna()
        & (day0_all["_close_3d_base"] > 0)
        & day0_all["close"].notna()
    )
    ret_3d = np.where(
        base_ok,
        (day0_all["close"] / day0_all["_close_3d_base"] - 1.0) * 100.0,
        np.nan,
    )
    tv_share = (
        day0_all["_tv_3d"] / float(mkt_tv3) * 100.0
        if pd.notna(mkt_tv3) and float(mkt_tv3) > 0
        else pd.Series(np.nan, index=day0_all.index)
    )
    mcap_share = (
        day0_all["mcap"] / float(mkt_mcap) * 100.0
        if pd.notna(mkt_mcap) and float(mkt_mcap) > 0
        else pd.Series(np.nan, index=day0_all.index)
    )
    day0_all["에너지배율"] = energy_ratio(tv_share, mcap_share, ret_3d)

    universe = day0_all[
        day0_all["mcap"].notna() & (day0_all["mcap"] >= UNIVERSE_MCAP_MIN)
    ].copy()
    if universe.empty:
        return pd.DataFrame(columns=FACTOR_PANEL_COLS)
    universe_tickers = set(universe["ticker"])

    # RS: as_of 이하 · market_type=market 종목별 최신
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
        talent_map[tk] = float(talent_score(close)["score"])

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

    for raw_col in ("RS", "주가위치", "talent", "에너지배율"):
        universe[f"pct_{raw_col}"] = _percentile_0_100(universe[raw_col])

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


def ann_sharpe(returns: np.ndarray | list[float], h: int) -> float:
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < 2:
        return float("nan")
    sd = float(r.std(ddof=1))
    if not np.isfinite(sd) or sd <= 0:
        return float("nan")
    return float(r.mean() / sd * np.sqrt(252.0 / float(h)))


def split_is_oos(n: int, is_frac: float = IS_FRAC) -> tuple[int, int]:
    if n <= 0:
        return 0, 0
    if n == 1:
        return 1, 0
    n_is = int(n * float(is_frac))
    n_is = max(1, min(n_is, n - 1))
    return n_is, n - n_is


def load_close_wide(eng=None) -> pd.DataFrame:
    """krx_ohlcv 종가 와이드(date×ticker). 보통주·전역제외 유니버스."""
    eng = eng or make_engine()
    log.info("종가 와이드 적재 시작 (krx_ohlcv)")
    t0 = time.time()
    frames = []
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


def rebalance_dates(dates: list[date], h: int, warmup: int = WARMUP_DAYS) -> list[date]:
    out: list[date] = []
    i = int(warmup)
    n = len(dates)
    while i + h < n:
        out.append(dates[i])
        i += int(h)
    return out


def build_panel_at(eng, as_of: date) -> pd.DataFrame:
    frames = [build_factor_panel_local(eng, as_of, mkt) for mkt in MARKETS]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def precompute_panels(eng, rebals: list[date]) -> dict[date, pd.DataFrame]:
    uniq = sorted(set(rebals))
    out: dict[date, pd.DataFrame] = {}
    for i, d in enumerate(uniq, start=1):
        t0 = time.time()
        panel = build_panel_at(eng, d)
        out[d] = panel
        log.info(
            "panel %d/%d as_of=%s rows=%d (%.1fs)",
            i,
            len(uniq),
            d,
            len(panel),
            time.time() - t0,
        )
    return out


def _forward_mean_return(
    close_wide: pd.DataFrame,
    tickers: list[str],
    d0: date,
    d1: date,
) -> float:
    if not tickers:
        return float("nan")
    cols = [t for t in tickers if t in close_wide.columns]
    if not cols:
        return float("nan")
    c0 = pd.to_numeric(close_wide.loc[d0, cols], errors="coerce")
    c1 = pd.to_numeric(close_wide.loc[d1, cols], errors="coerce")
    ok = c0.notna() & c1.notna() & (c0 > 0) & (c1 > 0)
    if int(ok.sum()) <= 0:
        return float("nan")
    r = (c1[ok] / c0[ok] - 1.0).astype(float)
    return float(r.mean())


def evaluate_weights_on_rebals(
    *,
    rebals: list[date],
    h: int,
    dates: list[date],
    date_to_idx: dict[date, int],
    close_wide: pd.DataFrame,
    panels: dict[date, pd.DataFrame],
    weight_list: list[dict[str, float]],
    top_n: int = TOP_N,
) -> np.ndarray:
    w_mat = weights_to_array(weight_list)
    n_w = len(weight_list)
    n_r = len(rebals)
    out = np.full((n_r, n_w), np.nan, dtype=float)

    for ri, t in enumerate(rebals):
        i0 = date_to_idx.get(t)
        if i0 is None or i0 + h >= len(dates):
            continue
        t_h = dates[i0 + h]
        panel = panels.get(t)
        if panel is None or panel.empty or "티커" not in panel.columns:
            continue

        tickers = panel["티커"].astype(str).tolist()
        eligible = []
        for tk in tickers:
            if tk not in close_wide.columns:
                continue
            v0 = close_wide.at[t, tk] if t in close_wide.index else np.nan
            v1 = close_wide.at[t_h, tk] if t_h in close_wide.index else np.nan
            try:
                v0f = float(v0)
                v1f = float(v1)
            except (TypeError, ValueError):
                continue
            if np.isfinite(v0f) and np.isfinite(v1f) and v0f > 0 and v1f > 0:
                eligible.append(tk)
        if not eligible:
            continue

        sub = panel[panel["티커"].astype(str).isin(eligible)].copy()
        if sub.empty:
            continue
        for c in PCT_COLS:
            if c not in sub.columns:
                sub[c] = 50.0
            else:
                sub[c] = pd.to_numeric(sub[c], errors="coerce").fillna(50.0)

        pct = sub[list(PCT_COLS)].to_numpy(dtype=float)
        tk_arr = sub["티커"].astype(str).to_numpy()
        scores = pct @ w_mat.T
        k = min(int(top_n), len(tk_arr))
        if k <= 0:
            continue

        for j in range(n_w):
            sc = scores[:, j]
            if k >= len(sc):
                pick = tk_arr.tolist()
            else:
                part = np.argpartition(-sc, kth=k - 1)[:k]
                part = part[np.argsort(-sc[part])]
                pick = tk_arr[part].tolist()
            out[ri, j] = _forward_mean_return(close_wide, pick, t, t_h)

        if (ri + 1) % 10 == 0 or ri == n_r - 1:
            log.info("  평가 진행 h=%d rebal %d/%d", h, ri + 1, n_r)

    return out


def summarize_kind_horizon(
    *,
    kind: str,
    h: int,
    ret_mat: np.ndarray,
    weight_list: list[dict[str, float]],
    baseline_w: dict[str, float],
) -> list[dict]:
    n_r, n_w = ret_mat.shape
    n_is, n_oos = split_is_oos(n_r, IS_FRAC)
    is_mat = ret_mat[:n_is, :]
    oos_mat = ret_mat[n_is:, :] if n_oos > 0 else np.empty((0, n_w))

    is_sharpes = np.array([ann_sharpe(is_mat[:, j], h) for j in range(n_w)], dtype=float)
    oos_sharpes = np.array(
        [ann_sharpe(oos_mat[:, j], h) if n_oos > 0 else float("nan") for j in range(n_w)],
        dtype=float,
    )

    base_vec = np.array([float(baseline_w.get(c, 0.0)) for c in METRIC_COLS], dtype=float)
    w_arr = weights_to_array(weight_list)
    dist = np.sum(np.abs(w_arr - base_vec[None, :]), axis=1)
    base_j = int(np.argmin(dist))

    finite = np.isfinite(is_sharpes)
    if not finite.any():
        best_order = list(range(min(5, n_w)))
        best_j = 0
    else:
        order = np.argsort(-np.where(finite, is_sharpes, -np.inf))
        best_order = order.tolist()
        best_j = int(order[0])

    rows: list[dict] = []

    def _row(result_type: str, rank: int, j: int, note: str = "") -> dict:
        w = weight_list[j]
        return {
            "kind": kind,
            "holding": int(h),
            "result_type": result_type,
            "rank": int(rank),
            "w_에너지배율": float(w["에너지배율"]),
            "w_RS": float(w["RS"]),
            "w_주가위치": float(w["주가위치"]),
            "w_talent": float(w["talent"]),
            "weights": format_weights(w),
            "sharpe_is": float(is_sharpes[j]) if np.isfinite(is_sharpes[j]) else np.nan,
            "sharpe_oos": float(oos_sharpes[j]) if np.isfinite(oos_sharpes[j]) else np.nan,
            "n_rebal_is": int(n_is),
            "n_rebal_oos": int(n_oos),
            "n_rebal_total": int(n_r),
            "note": note,
        }

    rows.append(
        _row("best", 1, best_j, "IS 샤프 최대 가중치(추천값). WEIGHT_SETS 미변경.")
    )
    rows.append(
        _row("baseline", 0, base_j, f"현재 WEIGHT_SETS['{kind}'] 대응 격자점")
    )
    for rank, j in enumerate(best_order[:5], start=1):
        rows.append(_row("is_top5", rank, int(j), "IS 샤프 상위5(과최적 판단용)"))
    return rows


def run_backtest(
    close_wide: Optional[pd.DataFrame] = None,
    output_csv: Path | str = OUTPUT_CSV,
    eng=None,
) -> pd.DataFrame:
    eng = eng or make_engine()
    if close_wide is None:
        close_wide = load_close_wide(eng)
    if close_wide is None or close_wide.empty:
        raise RuntimeError("종가 데이터가 비어 있습니다.")

    close_wide = close_wide.copy()
    close_wide.index = pd.to_datetime(close_wide.index, errors="coerce").date
    close_wide = close_wide[~pd.isna(close_wide.index)].sort_index()

    dates = trading_dates_from_wide(close_wide)
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

    weight_list = generate_weight_grid(WEIGHT_STEP)
    log.info("가중치 격자: %d개 (step=%.1f, 합=1)", len(weight_list), WEIGHT_STEP)

    rebals_by_h = {h: rebalance_dates(dates, h, WARMUP_DAYS) for h in HOLDING_DAYS}
    for h, rb in rebals_by_h.items():
        log.info("보유 %d일 리밸일: %d개", h, len(rb))

    all_rebals = sorted({d for rb in rebals_by_h.values() for d in rb})
    log.info("panel 선계산 대상 리밸일(고유): %d", len(all_rebals))
    panels = precompute_panels(eng, all_rebals)

    ret_by_h: dict[int, np.ndarray] = {}
    for h in HOLDING_DAYS:
        log.info("=== 보유기간 h=%d 가중치 평가 시작 ===", h)
        t0 = time.time()
        ret_by_h[h] = evaluate_weights_on_rebals(
            rebals=rebals_by_h[h],
            h=h,
            dates=dates,
            date_to_idx=date_to_idx,
            close_wide=close_wide,
            panels=panels,
            weight_list=weight_list,
            top_n=TOP_N,
        )
        log.info("h=%d 평가 완료 (%.1fs)", h, time.time() - t0)

    rows: list[dict] = []
    for kind in KIND_KEYS:
        baseline = WEIGHT_SETS[kind]
        for h in HOLDING_DAYS:
            part = summarize_kind_horizon(
                kind=kind,
                h=h,
                ret_mat=ret_by_h[h],
                weight_list=weight_list,
                baseline_w=baseline,
            )
            rows.extend(part)

            best = next(r for r in part if r["result_type"] == "best")
            base = next(r for r in part if r["result_type"] == "baseline")
            log.info(
                "[%s h=%d] 추천 %s | IS=%.3f OOS=%.3f || baseline IS=%.3f OOS=%.3f",
                kind,
                h,
                best["weights"],
                best["sharpe_is"],
                best["sharpe_oos"],
                base["sharpe_is"],
                base["sharpe_oos"],
            )
            for r in (x for x in part if x["result_type"] == "is_top5"):
                log.info(
                    "  IS Top%d: %s | IS=%.3f OOS=%.3f",
                    r["rank"],
                    r["weights"],
                    r["sharpe_is"],
                    r["sharpe_oos"],
                )

    result = pd.DataFrame(rows)
    out_path = Path(output_csv)
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info("결과 CSV 저장: %s (%d행)", out_path, len(result))
    return result


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Spyder 콘솔에도 보이도록
    logging.getLogger().setLevel(logging.INFO)
    log.info(
        "로컬 picking 가중치 백테스트 시작 (DB=%s, TopN=%d, holdings=%s)",
        db_connect_kwargs().get("db") or "kor_stock_db",
        TOP_N,
        HOLDING_DAYS,
    )
    run_backtest()
    log.info("완료 (WEIGHT_SETS는 변경하지 않음 — 위 추천값만 참고)")


if __name__ == "__main__":
    main()
