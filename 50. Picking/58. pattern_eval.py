# -*- coding: utf-8 -*-
"""
51 PATTERN_REGISTRY 패턴별 과거 시점 성과 측정.

51 을 import 만 하며 51 동작에는 영향을 주지 않는다.
"""
from __future__ import annotations

import importlib.util
import pickle
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm


def _resolve_here() -> Path:
    """Run Cell(콘솔 실행)에는 __file__ 이 없다. 순서대로 탐색."""
    try:
        return Path(__file__).resolve().parent
    except NameError:
        pass
    import os
    env = os.environ.get("TRADINGKRX_PICKING_DIR")
    if env and Path(env).is_dir():
        return Path(env)
    cur = Path.cwd()
    for p in (cur, *cur.parents):
        if (p / "51. Picking_KRX_v4.0.py").exists():
            return p
        if (p / "50. Picking" / "51. Picking_KRX_v4.0.py").exists():
            return p / "50. Picking"
    raise RuntimeError(
        "51 파일 위치를 찾을 수 없습니다. "
        "TRADINGKRX_PICKING_DIR 환경변수를 설정하거나 F5로 실행하세요."
    )


_HERE = _resolve_here()

# --- 51 인프라 import (52와 동일, 수정 없음) ---
_p51 = _HERE / "51. Picking_KRX_v4.0.py"
_spec = importlib.util.spec_from_file_location("picking51", _p51)
p51 = importlib.util.module_from_spec(_spec)
sys.modules["picking51"] = p51
_spec.loader.exec_module(p51)

from indicators_core import rs_avg  # noqa: E402

# --- 설정 ---
OUTPUT_DIR = _HERE / "results"
EVAL_CACHE = OUTPUT_DIR / "pattern_eval_indicators.pkl"
REBUILD_CACHE = False

EVAL_YEARS = 3
EVAL_STEP = 5  # 5거래일마다 평가 → 3년이면 약 150시점
HORIZONS = (20, 60)
UNMEASURED_CODES = frozenset({"p41", "p42"})  # volume_data=None → 항상 False

INDEX_TICKERS = {"KOSPI": "1001", "KOSDAQ": "2001"}


def _as_ts(x):
    return pd.Timestamp(x).normalize()


def _norm_market(v) -> str:
    s = str(v or "").strip().upper()
    raw = str(v or "").strip()
    if s in ("KOSPI", "KOSDAQ"):
        return s
    if "KOSDAQ" in s or "코스닥" in raw:
        return "KOSDAQ"
    if "KOSPI" in s or "코스피" in raw or "유가" in raw:
        return "KOSPI"
    return s


def _empty_rs() -> pd.DataFrame:
    return pd.DataFrame(columns=["rs10_score", "rs20_score", "rs50_score", "rs_score"])


def _build_rs_snapshot(day_df: pd.DataFrame) -> pd.DataFrame:
    """하루치 krx_relative_strength 행 → 51 과 동일 스키마 rs_df."""
    if day_df is None or day_df.empty:
        return _empty_rs()
    _rs_avg_cols = tuple(f"rs_{p}d" for p in p51.RS_PERIODS if p != 10)
    parts = []
    for _mkt, g in day_df.groupby("market_type"):
        g = g.set_index("ticker").copy()
        g["rs10_score"] = g["rs_10d"] if "rs_10d" in g.columns else np.nan
        g["rs20_score"] = g["rs_20d"] if "rs_20d" in g.columns else np.nan
        g["rs50_score"] = g["rs_50d"] if "rs_50d" in g.columns else np.nan
        g["rs_score"] = rs_avg(frame=g, cols=_rs_avg_cols).round(2)
        parts.append(g[["rs10_score", "rs20_score", "rs50_score", "rs_score"]])
    if not parts:
        return _empty_rs()
    return pd.concat(parts)


def _load_rs_by_date(engine, start_date) -> dict:
    """기간 전체 RS → {Timestamp: rs_df}."""
    _rs_col_sql = ", ".join(f"rs_{p}d" for p in p51.RS_PERIODS)
    q = f"""
        SELECT date, ticker, market_type, {_rs_col_sql}
        FROM krx_relative_strength
        WHERE date >= %s
        ORDER BY date, ticker
    """
    print(f"RS 기간 로드 (date>={start_date}) …")
    t0 = time.time()
    raw = pd.read_sql_query(q, con=engine, params=(start_date,))
    print(f"  RS 원본 {len(raw):,}행 ({time.time() - t0:.1f}s)")
    if raw.empty:
        return {}
    raw["date"] = pd.to_datetime(raw["date"]).dt.normalize()
    out = {}
    for dt, g in tqdm(raw.groupby("date", sort=True), desc="RS 스냅샷"):
        out[_as_ts(dt)] = _build_rs_snapshot(g)
    print(f"  RS 스냅샷 {len(out)}일")
    return out


def _load_mcap_lookup(engine, tickers, start_date) -> dict:
    """ticker → Series(date → mcap)."""
    if not tickers:
        return {}
    print(f"시총(mcap) 기간 로드 (tickers={len(tickers)}, date>={start_date}) …")
    t0 = time.time()
    frames = []
    chunk = 400
    ut = [str(t) for t in tickers]
    for i in range(0, len(ut), chunk):
        part = ut[i : i + chunk]
        ph = ", ".join(["%s"] * len(part))
        q = f"""
            SELECT date, ticker, mcap
            FROM krx_ohlcv
            WHERE date >= %s AND ticker IN ({ph})
        """
        frames.append(
            pd.read_sql_query(q, con=engine, params=(start_date, *part))
        )
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"  mcap 원본 {len(raw):,}행 ({time.time() - t0:.1f}s)")
    if raw.empty:
        return {}
    raw["date"] = pd.to_datetime(raw["date"]).dt.normalize()
    raw["mcap"] = pd.to_numeric(raw["mcap"], errors="coerce")
    out = {}
    for tk, g in raw.groupby("ticker"):
        s = g.dropna(subset=["mcap"]).set_index("date")["mcap"].sort_index()
        s = s[~s.index.duplicated(keep="last")]
        out[str(tk)] = s
    return out


def _load_index_closes(engine, start_date) -> dict:
    """시장 → Series(date → close). krx_ohlcv 우선, 없으면 krx_index_ohlcv."""
    out = {}
    for mkt, tk in INDEX_TICKERS.items():
        q = """
            SELECT date, close FROM krx_ohlcv
            WHERE ticker = %s AND date >= %s
            ORDER BY date
        """
        df = pd.read_sql_query(q, con=engine, params=(tk, start_date))
        if df.empty:
            q2 = """
                SELECT date, close FROM krx_index_ohlcv
                WHERE ticker = %s AND date >= %s
                ORDER BY date
            """
            df = pd.read_sql_query(q2, con=engine, params=(tk, start_date))
            src = "krx_index_ohlcv"
        else:
            src = "krx_ohlcv"
        if df.empty:
            print(f"  ⚠️ 지수 {tk}({mkt}) 없음")
            out[mkt] = pd.Series(dtype=float)
            continue
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        s = df.set_index("date")["close"].astype(float).sort_index()
        s = s[~s.index.duplicated(keep="last")]
        out[mkt] = s
        print(f"  지수 {tk}({mkt}) {len(s)}봉 [{src}]")
    return out


def _load_ticker_market(engine) -> dict:
    """종목코드 → KOSPI/KOSDAQ (krx_ticker 최신 기준일)."""
    q = """
        SELECT 종목코드 AS ticker, 시장구분 AS market
        FROM krx_ticker
        WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
          AND 종목구분 = '보통주'
    """
    df = pd.read_sql_query(q, con=engine)
    return {
        str(r.ticker): _norm_market(r.market)
        for r in df.itertuples(index=False)
    }


def _passes_gates(d: pd.DataFrame, cfg: dict, mcap, audit_list) -> bool:
    """51 screen_all 기본+ATR 필터를 t 시점(d의 마지막 봉) 기준으로 재현."""
    if len(d) < cfg["min_bars"]:
        return False
    r = d.iloc[-1]
    try:
        if not (float(r.open) > 0):
            return False
    except Exception:
        return False
    if mcap is None or not np.isfinite(mcap) or float(mcap) < p51.DISPLAY_MCAP_MIN:
        return False
    _tk = str(r.ticker) if hasattr(r, "ticker") else None
    if _tk is None:
        try:
            _tk = str(d.iloc[-1]["ticker"])
        except Exception:
            return False
    if _tk in audit_list:
        return False
    close = float(r.close)
    if not (close > 0):
        return False
    try:
        if not (
            (float(r.atr14) / close) < cfg["atr14_close_max"]
            and (float(r.mtr7) / close) < cfg["mtr7_close_max"]
            and (float(r.box7) / close) < cfg["box7_close_max"]
        ):
            return False
    except Exception:
        return False
    return True


def _fwd_excess(i, t, h, idx_close: pd.Series):
    """h거래일 후 종가 수익률 − 동일 구간 지수 수익률. 데이터 부족 시 None."""
    if t + h >= len(i):
        return None
    try:
        c0 = float(i.iloc[t].close)
        c1 = float(i.iloc[t + h].close)
    except Exception:
        return None
    if not (c0 > 0 and c1 > 0 and np.isfinite(c0) and np.isfinite(c1)):
        return None
    stock_ret = c1 / c0 - 1.0
    d0 = _as_ts(i.index[t])
    d1 = _as_ts(i.index[t + h])
    if idx_close is None or idx_close.empty:
        return None
    if d0 not in idx_close.index or d1 not in idx_close.index:
        return None
    i0 = float(idx_close.loc[d0])
    i1 = float(idx_close.loc[d1])
    if not (i0 > 0 and i1 > 0 and np.isfinite(i0) and np.isfinite(i1)):
        return None
    return stock_ret - (i1 / i0 - 1.0)


# %% 1) 캐시 준비
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
cfg = p51._resolve_screen_cfg({})
audit_list = list(p51.audit_ticker or [])
engine = create_engine(p51.db_url(), pool_pre_ping=True)

if REBUILD_CACHE or not EVAL_CACHE.exists():
    print("=" * 80)
    print(f"지표 캐시 재구축 → {EVAL_CACHE}")
    print("=" * 80)
    ctx = p51.run_main(do_summary=False, do_charts=False)
    indicators_data = ctx["indicators_data"]
    with open(EVAL_CACHE, "wb") as f:
        pickle.dump(indicators_data, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"캐시 저장: {EVAL_CACHE} ({len(indicators_data)} 종목)")
else:
    print("=" * 80)
    print(f"지표 캐시 로드 ← {EVAL_CACHE}")
    print("=" * 80)
    t0 = time.time()
    with open(EVAL_CACHE, "rb") as f:
        indicators_data = pickle.load(f)
    print(f"캐시 로드: {len(indicators_data)} 종목 ({time.time() - t0:.1f}s)")

# 평가 기간 하한 (대략)
_max_dates = []
for _df in indicators_data.values():
    if _df is not None and len(_df):
        _max_dates.append(pd.Timestamp(_df.index.max()))
_end = max(_max_dates) if _max_dates else pd.Timestamp.today()
_start = (_end - pd.DateOffset(years=EVAL_YEARS + 1)).normalize()  # RS/mcap 여유
_start_sql = _start.strftime("%Y-%m-%d")

rs_by_date = _load_rs_by_date(engine, _start_sql)
mcap_by_ticker = _load_mcap_lookup(engine, list(indicators_data.keys()), _start_sql)
index_closes = _load_index_closes(engine, _start_sql)
ticker_market = _load_ticker_market(engine)

PATTERN_CODES = [c for c in p51.PATTERN_REGISTRY.keys() if c != "p29b"]
print(f"패턴 {len(PATTERN_CODES)}개 (p29b 제외·p29a 통합), min_bars={cfg['min_bars']}, "
      f"EVAL_YEARS={EVAL_YEARS}, EVAL_STEP={EVAL_STEP}")


# %% 2) 패턴 평가
hits = []  # (date, code, ticker)
n_eval_pairs = 0
slot_dates = set()
slot_count_by_date = defaultdict(int)
base_excess = {h: [] for h in HORIZONS}
lookback = EVAL_YEARS * 250

for k, i in tqdm(indicators_data.items(), desc="패턴 평가"):
    if i is None or len(i) < cfg["min_bars"]:
        continue
    _tk = str(k)
    n = len(i)
    start_idx = max(0, n - lookback)
    # 전진수익률 h=60 여유: 평가 상한을 n-1-max(HORIZONS) 로 두면 되지만
    # 히트 수집은 끝까지 하고, 수익률 단계에서 부족분 제외한다.
    for t in range(start_idx, n, EVAL_STEP):
        d = i.iloc[: t + 1]
        if len(d) < cfg["min_bars"]:
            continue
        dt = _as_ts(d.index[-1])
        mcap_s = mcap_by_ticker.get(_tk)
        if mcap_s is None or dt not in mcap_s.index:
            continue
        mcap_v = float(mcap_s.loc[dt])
        if not _passes_gates(d, cfg, mcap_v, audit_list):
            continue

        n_eval_pairs += 1
        slot_dates.add(dt)
        slot_count_by_date[dt] += 1

        # 베이스라인: 게이트 통과 슬롯 전체의 초과수익
        mkt = ticker_market.get(_tk, "")
        idx_s = index_closes.get(mkt)
        for h in HORIZONS:
            ex = _fwd_excess(i, t, h, idx_s)
            if ex is not None and np.isfinite(ex):
                base_excess[h].append(float(ex))

        rs_snap = rs_by_date.get(dt)
        if rs_snap is None:
            rs_snap = _empty_rs()

        for code in PATTERN_CODES:
            try:
                P = p51._pat_params(code)
                P["rs_df"] = rs_snap
                P["volume_data"] = None
                ok = bool(p51.PATTERN_REGISTRY[code]["fn"](d, P))
            except Exception:
                ok = False
            if ok:
                hits.append((dt, code, _tk))

print(f"평가 슬롯(date×ticker 게이트 통과): {n_eval_pairs:,}")
print(f"평가 일자 수: {len(slot_dates):,}")
print(f"히트 수: {len(hits):,}")

_dates = sorted({d for (d, _c, _t) in hits})
_slot_dates = sorted(slot_dates)
print(f"[eval] 슬롯 날짜 {len(_slot_dates)}개: "
      f"{str(_slot_dates[0])[:10] if _slot_dates else '-'} ~ "
      f"{str(_slot_dates[-1])[:10] if _slot_dates else '-'}")
print(f"[eval] 날짜 목록: {[str(d)[:10] for d in _slot_dates]}")
print(f"[eval] 날짜별 슬롯 수: "
      f"{ {str(d)[:10]: c for d, c in sorted(slot_count_by_date.items())} }")
if _dates:
    print(f"[eval] 히트 발생 날짜 {len(_dates)}개: "
          f"{str(_dates[0])[:10]} ~ {str(_dates[-1])[:10]}")

hits_df = pd.DataFrame(hits, columns=["date", "code", "ticker"])
if not hits_df.empty:
    hits_df["date"] = pd.to_datetime(hits_df["date"]).dt.normalize()


# %% 3) 집계·저장
rows = []
n_dates = max(len(slot_dates), 1)

# 베이스라인(대조군): 게이트 통과 슬롯 전체
_base = {
    "code": "__BASE__",
    "group": "",
    "name": "게이트통과 전체",
    "status": "ok",
    "n_hits": n_eval_pairs,
    "avg_hits_per_date": round(n_eval_pairs / n_dates, 4) if n_dates else np.nan,
}
for h in HORIZONS:
    arr = np.asarray(base_excess[h], dtype=float)
    pref = f"h{h}"
    if arr.size == 0:
        _base[f"{pref}_mean_excess"] = np.nan
        _base[f"{pref}_median_excess"] = np.nan
        _base[f"{pref}_winrate"] = np.nan
        _base[f"{pref}_std"] = np.nan
    else:
        _base[f"{pref}_mean_excess"] = float(np.mean(arr))
        _base[f"{pref}_median_excess"] = float(np.median(arr))
        _base[f"{pref}_winrate"] = float(np.mean(arr > 0))
        _base[f"{pref}_std"] = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
rows.append(_base)

for code in sorted(PATTERN_CODES):
    meta = p51.PATTERN_REGISTRY.get(code, {})
    name = meta.get("name", code)
    if code == "p29a":
        name = "125일신고가+정배열+고RS"
    group = meta.get("group", "")
    unmeasured = code in UNMEASURED_CODES

    if unmeasured:
        rows.append(
            {
                "code": code,
                "group": group,
                "name": name,
                "status": "미측정",
                "n_hits": 0,
                "avg_hits_per_date": np.nan,
                "h20_mean_excess": np.nan,
                "h20_median_excess": np.nan,
                "h20_winrate": np.nan,
                "h20_std": np.nan,
                "h60_mean_excess": np.nan,
                "h60_median_excess": np.nan,
                "h60_winrate": np.nan,
                "h60_std": np.nan,
            }
        )
        continue

    sub = hits_df[hits_df["code"] == code] if not hits_df.empty else hits_df
    n_hits = int(len(sub))
    avg_per_date = n_hits / n_dates if n_dates else np.nan

    excess = {h: [] for h in HORIZONS}
    for r in sub.itertuples(index=False):
        idf = indicators_data.get(r.ticker)
        if idf is None:
            idf = indicators_data.get(str(r.ticker).zfill(6))
        if idf is None or idf.empty:
            continue
        # 날짜 → 위치
        try:
            pos = idf.index.get_indexer([r.date], method=None)[0]
            if pos < 0:
                # normalize 불일치 대비
                idx_norm = pd.to_datetime(idf.index).normalize()
                matches = np.where(idx_norm == _as_ts(r.date))[0]
                if len(matches) == 0:
                    continue
                pos = int(matches[-1])
        except Exception:
            continue
        mkt = ticker_market.get(str(r.ticker), "")
        idx_s = index_closes.get(mkt)
        for h in HORIZONS:
            ex = _fwd_excess(idf, pos, h, idx_s)
            if ex is not None and np.isfinite(ex):
                excess[h].append(float(ex))

    rec = {
        "code": code,
        "group": group,
        "name": name,
        "status": "ok",
        "n_hits": n_hits,
        "avg_hits_per_date": round(avg_per_date, 4),
    }
    for h in HORIZONS:
        arr = np.asarray(excess[h], dtype=float)
        pref = f"h{h}"
        if arr.size == 0:
            rec[f"{pref}_mean_excess"] = np.nan
            rec[f"{pref}_median_excess"] = np.nan
            rec[f"{pref}_winrate"] = np.nan
            rec[f"{pref}_std"] = np.nan
        else:
            rec[f"{pref}_mean_excess"] = float(np.mean(arr))
            rec[f"{pref}_median_excess"] = float(np.median(arr))
            rec[f"{pref}_winrate"] = float(np.mean(arr > 0))
            rec[f"{pref}_std"] = float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0
    rows.append(rec)

summary_df = pd.DataFrame(rows)

# 패턴 간 중복률 (Jaccard: |A∩B| / |A∪B|) — __BASE__ 제외
codes_meas = [c for c in sorted(PATTERN_CODES) if c not in UNMEASURED_CODES]
sets = {}
if not hits_df.empty:
    for code in codes_meas:
        s = hits_df.loc[hits_df["code"] == code, ["date", "ticker"]]
        sets[code] = set(zip(s["date"].astype(str), s["ticker"].astype(str)))
else:
    for code in codes_meas:
        sets[code] = set()

overlap = pd.DataFrame(np.nan, index=codes_meas, columns=codes_meas, dtype=float)
for a in codes_meas:
    for b in codes_meas:
        sa, sb = sets[a], sets[b]
        if a == b:
            overlap.loc[a, b] = 1.0 if sa else 0.0
            continue
        union = sa | sb
        if not union:
            overlap.loc[a, b] = 0.0
        else:
            overlap.loc[a, b] = len(sa & sb) / len(union)

summary_path = OUTPUT_DIR / "pattern_eval_summary.csv"
overlap_path = OUTPUT_DIR / "pattern_eval_overlap.csv"
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
overlap.to_csv(overlap_path, encoding="utf-8-sig")

print("=" * 80)
print(f"📊 패턴 평가 요약 (평가일 {len(slot_dates)} / 슬롯 {n_eval_pairs:,} / 히트 {len(hits):,})")
print("=" * 80)
_show = summary_df.copy()
for c in _show.columns:
    if _show[c].dtype.kind == "f":
        _show[c] = _show[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
print(_show.to_string(index=False))
print("=" * 80)
print(f"저장: {summary_path}")
print(f"저장: {overlap_path}")
print("※ p41·p42 는 volume_data=None 이라 미측정 / p29b 는 p29a 로 통합")
print("=" * 80)
