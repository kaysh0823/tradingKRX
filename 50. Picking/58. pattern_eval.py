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
EVAL_VOLUME_PROFILE = False  # True 면 p41·p42 를 시점별 매물대로 평가
                             # 게이트 통과 슬롯마다 gen_tBand 재계산 → 약 40분 추가
EVAL_GATES = True  # 게이트 단계별 코호트 성과 측정
HORIZONS = (20, 60)
UNMEASURED_CODES = (
    frozenset() if EVAL_VOLUME_PROFILE else frozenset({"p41", "p42"})
)
EVAL_SPLITS = [
    ("전체", None, None),
    ("2023~24", "2023-01-01", "2024-12-31"),
    ("2025~26", "2025-01-01", "2026-12-31"),
]

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
    stage, _ = _gate_stage(d, cfg, mcap, audit_list)
    return stage == "S3"


def _gate_stage(d: pd.DataFrame, cfg: dict, mcap, audit_list):
    """
    게이트 도달 단계와 atr14/close.
    S0: min_bars / S1: open+audit / S2: +시총 / S3: +ATR(현 __BASE__)
    min_bars 미달이면 (None, nan).
    """
    if len(d) < cfg["min_bars"]:
        return None, np.nan
    r = d.iloc[-1]
    atr_ratio = np.nan
    try:
        close = float(r.close)
        if close > 0 and hasattr(r, "atr14"):
            atr_ratio = float(r.atr14) / close
    except Exception:
        pass

    stage = "S0"
    try:
        open_ok = float(r.open) > 0
    except Exception:
        open_ok = False
    try:
        _tk = str(r.ticker)
    except Exception:
        try:
            _tk = str(d.iloc[-1]["ticker"])
        except Exception:
            _tk = None
    audit_ok = _tk is not None and _tk not in audit_list
    if not (open_ok and audit_ok):
        return stage, atr_ratio
    stage = "S1"

    mcap_ok = (
        mcap is not None
        and np.isfinite(mcap)
        and float(mcap) >= p51.DISPLAY_MCAP_MIN
    )
    if not mcap_ok:
        return stage, atr_ratio
    stage = "S2"

    try:
        close = float(r.close)
        atr_ok = (
            close > 0
            and (float(r.atr14) / close) < cfg["atr14_close_max"]
            and (float(r.mtr7) / close) < cfg["mtr7_close_max"]
            and (float(r.box7) / close) < cfg["box7_close_max"]
        )
    except Exception:
        atr_ok = False
    if atr_ok:
        stage = "S3"
    return stage, atr_ratio


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
base_records = []  # {date, 20: ex|None, 60: ex|None} — 구간별 __BASE__ 용
gate_records = []  # EVAL_GATES: {date,ticker,stage,atr14_close,mcap,20,60}
lookback = EVAL_YEARS * 250
_STAGE_RANK = {"S0": 0, "S1": 1, "S2": 2, "S3": 3}

if EVAL_VOLUME_PROFILE:
    print("⚠️ EVAL_VOLUME_PROFILE=True — 슬롯별 매물대 재계산으로 40분 이상 추가 소요")
if EVAL_GATES:
    print("⚠️ EVAL_GATES=True — 전체 슬롯 게이트 단계·전진수익 기록 (패턴은 S3만)")

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
        if mcap_s is not None and dt in mcap_s.index:
            mcap_v = float(mcap_s.loc[dt])
        else:
            mcap_v = None

        stage, atr_ratio = _gate_stage(d, cfg, mcap_v, audit_list)
        if stage is None:
            continue

        mkt = ticker_market.get(_tk, "")
        idx_s = index_closes.get(mkt)

        if EVAL_GATES:
            _ge = {
                "date": dt,
                "ticker": _tk,
                "stage": stage,
                "atr14_close": atr_ratio,
                "mcap": float(mcap_v) if mcap_v is not None and np.isfinite(mcap_v) else np.nan,
            }
            for h in HORIZONS:
                ex = _fwd_excess(i, t, h, idx_s)
                _ge[h] = float(ex) if ex is not None and np.isfinite(ex) else None
            gate_records.append(_ge)

        # 패턴·__BASE__ 는 S3(현 게이트 통과)에서만
        if stage != "S3":
            continue

        n_eval_pairs += 1
        slot_dates.add(dt)
        slot_count_by_date[dt] += 1

        # 베이스라인: 게이트 통과 슬롯 전체의 초과수익
        _be = {"date": dt}
        for h in HORIZONS:
            if EVAL_GATES:
                _ex = gate_records[-1].get(h)
            else:
                _ex = _fwd_excess(i, t, h, idx_s)
                _ex = float(_ex) if _ex is not None and np.isfinite(_ex) else None
            if _ex is not None:
                base_excess[h].append(float(_ex))
            _be[h] = _ex
        base_records.append(_be)

        rs_snap = rs_by_date.get(dt)
        if rs_snap is None:
            rs_snap = _empty_rs()

        # 게이트 통과 슬롯에 한해 매물대 재계산 (p41·p42)
        _vol_map = None
        if EVAL_VOLUME_PROFILE:
            try:
                if float(d.iloc[-1]["volume"]) > 0:
                    _tk_v = str(d.iloc[-1]["ticker"])
                    _vol_map = {_tk_v: p51.gen_tBand(d, 50)}
            except Exception:
                _vol_map = None

        for code in PATTERN_CODES:
            try:
                P = p51._pat_params(code)
                P["rs_df"] = rs_snap
                P["volume_data"] = _vol_map
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
def _date_in_split(dt, start, end) -> bool:
    if start is None and end is None:
        return True
    t = _as_ts(dt)
    if start is not None and t < _as_ts(start):
        return False
    if end is not None and t > _as_ts(end):
        return False
    return True


def _excess_stats(vals):
    arr = np.asarray([v for v in vals if v is not None and np.isfinite(v)], dtype=float)
    if arr.size == 0:
        return dict(mean=np.nan, median=np.nan, winrate=np.nan, std=np.nan)
    return dict(
        mean=float(np.mean(arr)),
        median=float(np.median(arr)),
        winrate=float(np.mean(arr > 0)),
        std=float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0,
    )


def _pattern_name(code: str) -> str:
    if code == "p29a":
        return "125일신고가+정배열+고RS"
    meta = p51.PATTERN_REGISTRY.get(code, {})
    return meta.get("name", code)


def _hit_pos(idf, date):
    try:
        pos = idf.index.get_indexer([date], method=None)[0]
        if pos >= 0:
            return int(pos)
        idx_norm = pd.to_datetime(idf.index).normalize()
        matches = np.where(idx_norm == _as_ts(date))[0]
        if len(matches) == 0:
            return None
        return int(matches[-1])
    except Exception:
        return None


# 히트별 초과수익을 한 번만 계산 (구간 필터는 집계에서)
_hit_ex_rows = []
if not hits_df.empty:
    for r in hits_df.itertuples(index=False):
        idf = indicators_data.get(r.ticker)
        if idf is None:
            idf = indicators_data.get(str(r.ticker).zfill(6))
        if idf is None or idf.empty:
            continue
        pos = _hit_pos(idf, r.date)
        if pos is None:
            continue
        mkt = ticker_market.get(str(r.ticker), "")
        idx_s = index_closes.get(mkt)
        row = {"date": _as_ts(r.date), "code": r.code, "ticker": r.ticker}
        for h in HORIZONS:
            ex = _fwd_excess(idf, pos, h, idx_s)
            row[h] = float(ex) if ex is not None and np.isfinite(ex) else None
        _hit_ex_rows.append(row)
hit_ex_df = pd.DataFrame(_hit_ex_rows)

_base_rec_df = pd.DataFrame(base_records)
if not _base_rec_df.empty:
    _base_rec_df["date"] = pd.to_datetime(_base_rec_df["date"]).dt.normalize()


def _summarize_split(label, start, end) -> pd.DataFrame:
    """한 구간의 요약표 (__BASE__ + 패턴)."""
    s_dates = {d for d in slot_dates if _date_in_split(d, start, end)}
    n_dates = max(len(s_dates), 1)
    n_slots = sum(
        c for d, c in slot_count_by_date.items() if _date_in_split(d, start, end)
    )

    if _base_rec_df.empty:
        base_f = _base_rec_df
    else:
        _mask = _base_rec_df["date"].map(lambda d: _date_in_split(d, start, end))
        base_f = _base_rec_df.loc[_mask]

    rows = []
    _base = {
        "split": label,
        "code": "__BASE__",
        "group": "",
        "name": "게이트통과 전체",
        "status": "ok",
        "n_hits": int(n_slots),
        "avg_hits_per_date": round(n_slots / n_dates, 4) if n_dates else np.nan,
    }
    for h in HORIZONS:
        st = _excess_stats(base_f[h].tolist() if h in base_f.columns and len(base_f) else [])
        pref = f"h{h}"
        _base[f"{pref}_mean_excess"] = st["mean"]
        _base[f"{pref}_median_excess"] = st["median"]
        _base[f"{pref}_winrate"] = st["winrate"]
        _base[f"{pref}_std"] = st["std"]
    rows.append(_base)

    if hit_ex_df.empty:
        hit_f = hit_ex_df
    else:
        _hm = hit_ex_df["date"].map(lambda d: _date_in_split(d, start, end))
        hit_f = hit_ex_df.loc[_hm]

    # 히트 수(초과수익 유무와 무관)는 hits_df 기준
    if hits_df.empty:
        hits_f = hits_df
    else:
        _hm2 = hits_df["date"].map(lambda d: _date_in_split(d, start, end))
        hits_f = hits_df.loc[_hm2]

    for code in sorted(PATTERN_CODES):
        meta = p51.PATTERN_REGISTRY.get(code, {})
        name = _pattern_name(code)
        group = meta.get("group", "")
        unmeasured = code in UNMEASURED_CODES
        if unmeasured:
            rows.append(
                {
                    "split": label,
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

        n_hits = int((hits_f["code"] == code).sum()) if not hits_f.empty else 0
        sub_ex = hit_f[hit_f["code"] == code] if not hit_f.empty else hit_f
        rec = {
            "split": label,
            "code": code,
            "group": group,
            "name": name,
            "status": "ok",
            "n_hits": n_hits,
            "avg_hits_per_date": round(n_hits / n_dates, 4) if n_dates else np.nan,
        }
        for h in HORIZONS:
            vals = sub_ex[h].tolist() if (not sub_ex.empty and h in sub_ex.columns) else []
            st = _excess_stats(vals)
            pref = f"h{h}"
            rec[f"{pref}_mean_excess"] = st["mean"]
            rec[f"{pref}_median_excess"] = st["median"]
            rec[f"{pref}_winrate"] = st["winrate"]
            rec[f"{pref}_std"] = st["std"]
        rows.append(rec)

    return pd.DataFrame(rows)


split_summaries = []
for _lab, _st, _en in EVAL_SPLITS:
    _sdf = _summarize_split(_lab, _st, _en)
    split_summaries.append(_sdf)
    print("=" * 80)
    print(f"📊 구간 요약: {_lab}")
    print("=" * 80)
    _show = _sdf.drop(columns=["split"], errors="ignore").copy()
    for c in _show.columns:
        if _show[c].dtype.kind == "f":
            _show[c] = _show[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
    print(_show.to_string(index=False))

summary_by_split = pd.concat(split_summaries, ignore_index=True)
summary_df = split_summaries[0].drop(columns=["split"], errors="ignore").copy()

# 패턴 간 중복률 (전체 기간, Jaccard) — __BASE__ 제외
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

# 일관성 비교표 (2023~24 vs 2025~26, h60Δ vs 구간 __BASE__)
_by = {lab: df.set_index("code") for lab, df in zip(
    [s[0] for s in EVAL_SPLITS], split_summaries
)}
_lab_a, _lab_b = "2023~24", "2025~26"
_dfa = _by.get(_lab_a)
_dfb = _by.get(_lab_b)
_df_all = _by.get("전체")
_cons_rows = []
for code in codes_meas:
    name = _pattern_name(code)
    if _dfa is None or code not in _dfa.index:
        n_a, d_a = 0, np.nan
    else:
        n_a = int(_dfa.loc[code, "n_hits"])
        d_a = float(_dfa.loc[code, "h60_mean_excess"]) - float(
            _dfa.loc["__BASE__", "h60_mean_excess"]
        )
    if _dfb is None or code not in _dfb.index:
        n_b, d_b = 0, np.nan
    else:
        n_b = int(_dfb.loc[code, "n_hits"])
        d_b = float(_dfb.loc[code, "h60_mean_excess"]) - float(
            _dfb.loc["__BASE__", "h60_mean_excess"]
        )
    if n_a < 100 or n_b < 100:
        cons = "-"
    elif not np.isfinite(d_a) or not np.isfinite(d_b):
        cons = "-"
    elif d_a > 0 and d_b > 0:
        cons = "○"
    elif d_a > 0 or d_b > 0:
        cons = "△"
    else:
        cons = "✕"
    # 전체 기준 h60Δ (정렬용)
    if _df_all is not None and code in _df_all.index:
        d_all = float(_df_all.loc[code, "h60_mean_excess"]) - float(
            _df_all.loc["__BASE__", "h60_mean_excess"]
        )
    else:
        d_all = np.nan
    _cons_rows.append(
        {
            "code": code,
            "name": name,
            "n_23_24": n_a,
            "h60Δ_23_24": d_a,
            "n_25_26": n_b,
            "h60Δ_25_26": d_b,
            "일관성": cons,
            "_sort_h60Δ_all": d_all,
        }
    )

consistency_df = pd.DataFrame(_cons_rows)
consistency_df = consistency_df.sort_values(
    "_sort_h60Δ_all", ascending=False, na_position="last"
).drop(columns=["_sort_h60Δ_all"])

print("=" * 80)
print("📊 일관성 비교 (h60Δ = 패턴 − 구간 __BASE__)")
print("=" * 80)
_cshow = consistency_df.copy()
for c in ("h60Δ_23_24", "h60Δ_25_26"):
    _cshow[c] = _cshow[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
print(_cshow.to_string(index=False))

summary_path = OUTPUT_DIR / "pattern_eval_summary.csv"
overlap_path = OUTPUT_DIR / "pattern_eval_overlap.csv"
split_path = OUTPUT_DIR / "pattern_eval_summary_by_split.csv"
cons_path = OUTPUT_DIR / "pattern_eval_consistency.csv"
summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
overlap.to_csv(overlap_path, encoding="utf-8-sig")
summary_by_split.to_csv(split_path, index=False, encoding="utf-8-sig")
consistency_df.to_csv(cons_path, index=False, encoding="utf-8-sig")

print("=" * 80)
print(f"저장: {summary_path}")
print(f"저장: {overlap_path}")
print(f"저장: {split_path}")
print(f"저장: {cons_path}")

# --- 게이트 단계·ATR 분위 성과 (EVAL_GATES) ---
if EVAL_GATES and gate_records:
    gate_df = pd.DataFrame(gate_records)
    gate_df["date"] = pd.to_datetime(gate_df["date"]).dt.normalize()
    gate_df["_rank"] = gate_df["stage"].map(_STAGE_RANK)
    _atr_cut = float(cfg.get("atr14_close_max", 0.1))
    _mcap_cut = float(p51.DISPLAY_MCAP_MIN)

    stage_rows = []
    quin_rows = []
    mcap_quin_rows = []
    for _lab, _st, _en in EVAL_SPLITS:
        _gmask = gate_df["date"].map(lambda d: _date_in_split(d, _st, _en))
        g = gate_df.loc[_gmask].copy()
        print("=" * 80)
        print(f"📊 게이트 단계 성과: {_lab}  (S2→S3 = ATR 필터 순효과)")
        print("=" * 80)
        _stage_tbl = []
        for stg in ("S0", "S1", "S2", "S3"):
            sub = g[g["_rank"] >= _STAGE_RANK[stg]]
            st20 = _excess_stats(sub[20].tolist() if 20 in sub.columns else [])
            st60 = _excess_stats(sub[60].tolist() if 60 in sub.columns else [])
            rec = {
                "split": _lab,
                "stage": stg,
                "n": int(len(sub)),
                "h20_mean": st20["mean"],
                "h20_winrate": st20["winrate"],
                "h60_mean": st60["mean"],
                "h60_winrate": st60["winrate"],
            }
            stage_rows.append(rec)
            _stage_tbl.append(rec)
        _st_df = pd.DataFrame(_stage_tbl)
        _ss = _st_df.drop(columns=["split"]).copy()
        for c in _ss.columns:
            if _ss[c].dtype.kind == "f":
                _ss[c] = _ss[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
        print(_ss.to_string(index=False))

        # S2 통과(S2+S3) 슬롯의 atr14/close 5분위
        s2 = g[g["_rank"] >= _STAGE_RANK["S2"]].copy()
        s2 = s2[np.isfinite(s2["atr14_close"])]
        print("-" * 80)
        print(f"ATR 분위 (S2 통과, atr14/close 컷오프={_atr_cut})")
        if len(s2) < 5:
            print("  (표본 부족 — 분위 생략)")
        else:
            try:
                s2["quintile"] = pd.qcut(
                    s2["atr14_close"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
                )
            except ValueError:
                s2["quintile"] = pd.qcut(
                    s2["atr14_close"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
                )
            _q_tbl = []
            for qv, qg in s2.groupby("quintile", observed=True):
                lo = float(qg["atr14_close"].min())
                hi = float(qg["atr14_close"].max())
                contains_cut = lo <= _atr_cut <= hi
                st60 = _excess_stats(qg[60].tolist() if 60 in qg.columns else [])
                st20 = _excess_stats(qg[20].tolist() if 20 in qg.columns else [])
                rec = {
                    "split": _lab,
                    "quintile": int(qv),
                    "atr14_close_lo": lo,
                    "atr14_close_hi": hi,
                    "atr_range": f"[{lo:.4f}, {hi:.4f}]",
                    "contains_cutoff_0.1": contains_cut,
                    "n": int(len(qg)),
                    "h20_mean": st20["mean"],
                    "h20_winrate": st20["winrate"],
                    "h60_mean": st60["mean"],
                    "h60_winrate": st60["winrate"],
                }
                quin_rows.append(rec)
                _q_tbl.append(rec)
            _qd = pd.DataFrame(_q_tbl)
            _qs = _qd[
                ["quintile", "atr_range", "contains_cutoff_0.1", "n",
                 "h60_mean", "h60_winrate"]
            ].copy()
            for c in ("h60_mean", "h60_winrate"):
                _qs[c] = _qs[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
            print(_qs.to_string(index=False))
            _hit_q = [r["quintile"] for r in _q_tbl if r["contains_cutoff_0.1"]]
            if _hit_q:
                print(f"  → atr14/close < {_atr_cut} 컷오프는 Q{_hit_q} 구간에 위치")

        # S1 통과(S1+) 슬롯의 mcap 5분위
        s1 = g[g["_rank"] >= _STAGE_RANK["S1"]].copy()
        s1 = s1[np.isfinite(s1["mcap"])]
        print("-" * 80)
        print(f"시총 분위 (S1 통과, mcap 컷오프={_mcap_cut:,.0f})")
        if len(s1) < 5:
            print("  (표본 부족 — 분위 생략)")
        else:
            try:
                s1["quintile"] = pd.qcut(
                    s1["mcap"], 5, labels=[1, 2, 3, 4, 5], duplicates="drop"
                )
            except ValueError:
                s1["quintile"] = pd.qcut(
                    s1["mcap"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]
                )
            _mq_tbl = []
            for qv, qg in s1.groupby("quintile", observed=True):
                lo = float(qg["mcap"].min())
                hi = float(qg["mcap"].max())
                contains_cut = lo <= _mcap_cut <= hi
                st60 = _excess_stats(qg[60].tolist() if 60 in qg.columns else [])
                st20 = _excess_stats(qg[20].tolist() if 20 in qg.columns else [])
                rec = {
                    "split": _lab,
                    "quintile": int(qv),
                    "mcap_lo": lo,
                    "mcap_hi": hi,
                    "mcap_range": f"[{lo/1e8:,.0f}억, {hi/1e8:,.0f}억]",
                    "contains_cutoff": contains_cut,
                    "n": int(len(qg)),
                    "h20_mean": st20["mean"],
                    "h20_winrate": st20["winrate"],
                    "h60_mean": st60["mean"],
                    "h60_winrate": st60["winrate"],
                }
                mcap_quin_rows.append(rec)
                _mq_tbl.append(rec)
            _mqd = pd.DataFrame(_mq_tbl)
            _mqs = _mqd[
                ["quintile", "mcap_range", "contains_cutoff", "n",
                 "h60_mean", "h60_winrate"]
            ].copy()
            for c in ("h60_mean", "h60_winrate"):
                _mqs[c] = _mqs[c].map(lambda x: f"{x:.4f}" if pd.notna(x) else "")
            print(_mqs.to_string(index=False))
            _hit_mq = [r["quintile"] for r in _mq_tbl if r["contains_cutoff"]]
            if _hit_mq:
                print(f"  → mcap >= {_mcap_cut:,.0f} 컷오프는 Q{_hit_mq} 구간에 위치")
            print(f"  → S1 중 컷오프 이상 비율: {(s1['mcap'] >= _mcap_cut).mean():.1%}")

    gate_stages_df = pd.DataFrame(stage_rows)
    gate_quin_df = pd.DataFrame(quin_rows)
    gate_mcap_quin_df = pd.DataFrame(mcap_quin_rows)
    gate_stages_path = OUTPUT_DIR / "gate_eval_stages.csv"
    gate_quin_path = OUTPUT_DIR / "gate_eval_atr_quintile.csv"
    gate_mcap_quin_path = OUTPUT_DIR / "gate_eval_mcap_quintile.csv"
    slots_path = OUTPUT_DIR / "gate_eval_slots.csv"
    gate_stages_df.to_csv(gate_stages_path, index=False, encoding="utf-8-sig")
    gate_quin_df.to_csv(gate_quin_path, index=False, encoding="utf-8-sig")
    gate_mcap_quin_df.to_csv(gate_mcap_quin_path, index=False, encoding="utf-8-sig")
    _slot_cols = ["date", "ticker", "stage", "atr14_close", "mcap", 20, 60]
    gate_df[_slot_cols].to_csv(
        slots_path, index=False, encoding="utf-8-sig", float_format="%.6g"
    )
    print("=" * 80)
    print(f"저장: {gate_stages_path}")
    print(f"저장: {gate_quin_path}")
    print(f"저장: {gate_mcap_quin_path}")
    print(f"저장: {slots_path} ({len(gate_df):,}행) — 재분석용 원본 슬롯")

if EVAL_VOLUME_PROFILE:
    print("※ p29b 는 p29a 로 통합 / p41·p42 는 슬롯별 gen_tBand 로 측정")
else:
    print("※ p41·p42 는 volume_data=None 이라 미측정 / p29b 는 p29a 로 통합")
print("※ 일관성: 구간 히트<100 이면 '-' (표본 부족)")
if EVAL_GATES:
    print("※ EVAL_GATES: S0=모집단 → S3=__BASE__(기본+ATR)")
print("=" * 80)
