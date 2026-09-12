# -*- coding: utf-8 -*-
"""
52 주봉 Picking — 지표·스크리닝(w11~w51)을 52가 자체 보유. 51 패턴과 무관.
데이터 로드·매물대·요약 HTML·차트·투자자 OSC 등 인프라만 51을 재사용한다.

Spyder: 셀(# %%) 단위. 0번 셀 먼저 → 1 파이프라인 → 2 요약 → 3 차트.

주의: 52를 수정한 뒤에는 F5(전체 재실행)로 정의를 갱신해야 한다.
      Run Cell 은 이전 정의를 그대로 쓴다.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import time
import traceback
import threading
import datetime
import html as html_module
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objs as go
import talib
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
from tqdm import tqdm

# --- 51 인프라 import (수정 없음) ---
_p51 = Path(__file__).with_name("51. Picking_KRX_v4.0.py")
_spec = importlib.util.spec_from_file_location("picking51", _p51)
p51 = importlib.util.module_from_spec(_spec)
sys.modules["picking51"] = p51
_spec.loader.exec_module(p51)

from indicators_core import atr_wilder, bollinger_band_width_q, investor_net_osc  # noqa: E402

# --- 52 설정 ---
OHLCV_TABLE_WEEK = "krx_ohlcv_week"
OHLCV_TABLE_DAILY = "krx_ohlcv"  # 시총·거래대금비중 조회 전용 (주봉 전환과 무관)
_WEEK_MCAP_CHUNK = 400
OUTPUT_SUFFIX = "_week"
DROP_INCOMPLETE_WEEK = True
CHART_WEEKS = 260  # 차트 tail (5년, 52*5 주봉)
CHART_PLOTLYJS = True  # True(임베드) | "directory"(폴더 공유) | "cdn"(온라인)
CHART_OPEN_MAX = 1  # 자동으로 열 차트 수 (0이면 열지 않음)

MAX_WORKERS_DATA_LOAD = p51.MAX_WORKERS_DATA_LOAD
MAX_WORKERS_INDICATORS = p51.MAX_WORKERS_INDICATORS
MAX_WORKERS_VOLUME = p51.MAX_WORKERS_VOLUME

# 주봉 지표 창 (정본 이름 = 주봉 의미)
W_SMA2, W_SMA4, W_SMA13, W_SMA26, W_SMA52 = 2, 4, 13, 26, 52
W_EMA4, W_EMA13 = 4, 13
W_ATR = 13
W_BAND, W_BAND_LB = 13, 26
W_CSI_LEN = 13
W_MTR_BOX = 7
W_MACD_FAST, W_MACD_SLOW, W_MACD_SIGNAL = 4, 13, 3
W_INVESTOR_OSC_CUM = 4  # 주봉 누적 4주 (일봉 5일의 주봉 대응)

# 스크리닝 (전부 여기서 조정)
W_MIN_BARS = 60      # 52주선 사용 → 최소 60주봉
W_ATR_MAX = 0.15     # atr13/종가 상한
W_SQUEEZE = 0.30     # band26_q 스퀴즈 임계 (진단: w21 a≈58 병목 → 0.25→0.30)
W_BOX_HI = 0.95      # 박스 상단 비율
W_BOX_LO = 0.70
W_VOL_SURGE = 1.3    # 거래량 급증 배수(직전 4주 평균 대비)
W_HI52_RATIO = 0.96  # 52주 고가 근접 (진단: w31 a≈25 병목 → 0.98→0.96)
W_MIN_PATTERNS = 1   # 최소 매칭 패턴 수
W_USE_RS = False
W_RS_MIN = 70        # W_USE_RS=True 일 때 rs_score 하한

WEEKLY_PATTERN_CODES = ("w11", "w21", "w31", "w41", "w51")

WEEKLY_PATTERN_LABELS = {
    "w11": "추세정배열",
    "w21": "스퀴즈확장",
    "w31": "52주신고가",
    "w41": "박스상단",
    "w51": "눌림목지지",
}

# 지표 유효율 진단 (파이프라인 1회)
DIAG_SAMPLE_N = 200
DIAG_COLS = (
    "sma4", "sma13", "sma26", "sma52",
    "max26", "min26", "max52", "min52",
    "band26_q", "atr13", "volume",
)
_INDICATOR_DIAG_DONE = False
_WEEKLY_ROW_DIAG_DONE = False
_WEEKLY_ROW_DIAG_TICKER: str | None = None
_WEEKLY_INDEX_WARN_DONE = False
_WEEKLY_INDICATOR_FAIL_LOCK = threading.Lock()
_WEEKLY_INDICATOR_FAIL_TOTAL = 0
_WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN = False
_WEEK_LOAD_FAIL = {"n": 0}
_WEEK_LOAD_FAIL_LOCK = threading.Lock()
_WEEK_CHART_MISSING_LOGGED: set[str] = set()
_PLOT_TARGET_LOG_DONE = False
_CHART_SAVE_LOG_DONE = False
_CHART_XAXIS_LOG_DONE = False
_WEEK_CHART_FONT = "Malgun Gothic, Segoe UI, sans-serif"
_WEEK_CHART_COL_WIDTHS = (0.86, 0.14)
_WEEK_CHART_ROW_HEIGHTS_7 = (0.14, 0.32, 0.14, 0.11, 0.11, 0.09, 0.09)
_WEEK_CHART_ROW_HEIGHTS_6 = (0.15, 0.35, 0.15, 0.12, 0.12, 0.11)
_WEEK_SNAP_OFFSETS = (57, 43, 29, 15, 1)
_PICKING_KRX_DIR = r"C:\Users\hachi\OneDrive\01. Trading\picking\KRX"

_P51_GLOBAL_KEYS = (
    "engine",
    "ticker_list",
    "volume_data",
    "indicators_data",
    "rs_df",
    "money",
    "risk",
)
_INJECT_P51_LOG_DONE = False


_PATTERN_EXC: dict[str, dict] = {}


def _pattern_exc(code: str, ticker: str, exc: BaseException) -> bool:
    """패턴별 최초 1회 상세 로그 + 이후 건수 누적."""
    st = _PATTERN_EXC.setdefault(code, {"first": None, "n": 0})
    st["n"] += 1
    if st["first"] is None:
        st["first"] = f"{type(exc).__name__}: {exc} ({str(ticker).zfill(6)})"
        print(f"패턴 {code} 예외(최초): {st['first']}")
    return False


def _flush_pattern_exc() -> None:
    for code, st in _PATTERN_EXC.items():
        if st["n"] <= 0:
            continue
        rest = st["n"] - 1
        if rest > 0 and st["first"]:
            print(f"패턴 {code} 예외 … 이후 {rest}건")
    _PATTERN_EXC.clear()


def _fval(row, col) -> float:
    try:
        v = row[col] if col in row.index else getattr(row, col)
        return float(v) if pd.notna(v) else np.nan
    except Exception:
        return np.nan


def _w11_subs(df: pd.DataFrame, last, close: float) -> dict[str, bool]:
    """w11 서브조건 (a~e)."""
    sma4 = _fval(last, "sma4")
    sma13 = _fval(last, "sma13")
    sma26 = _fval(last, "sma26")
    sma52 = _fval(last, "sma52")
    n = len(df)
    d = False
    if n >= 6:
        sma26_5w = _fval(df.iloc[-6], "sma26")
        if np.isfinite(sma26) and np.isfinite(sma26_5w):
            d = sma26 > sma26_5w
    return {
        "a": np.isfinite(sma4) and np.isfinite(sma13) and sma4 > sma13,
        "b": np.isfinite(sma13) and np.isfinite(sma26) and sma13 > sma26,
        "c": np.isfinite(sma26) and np.isfinite(sma52) and sma26 > sma52,
        "d": d,
        "e": np.isfinite(close) and np.isfinite(sma13) and close > sma13,
    }


def _w11(df: pd.DataFrame, last, close: float) -> bool:
    """추세정배열: sma4>13>26>52, 26주선 5주 전 대비 상승, 종가>13주선."""
    try:
        if len(df) < 6:
            return False
        subs = _w11_subs(df, last, close)
        return all(subs.values())
    except Exception as exc:
        tk = _fval(last, "ticker") if last is not None else "?"
        return _pattern_exc("w11", tk, exc)


def _w21_subs(df: pd.DataFrame, last, close: float) -> dict[str, bool]:
    b_now = _fval(last, "band26_q")
    b_prev = np.nan
    if len(df) >= 2:
        b_prev = _fval(df.iloc[-2], "band26_q")
    sma13 = _fval(last, "sma13")
    return {
        "a": np.isfinite(b_now) and b_now < W_SQUEEZE,
        "b": np.isfinite(b_now) and np.isfinite(b_prev) and b_now > b_prev,
        "c": np.isfinite(close) and np.isfinite(sma13) and close > sma13,
    }


def _w21(df: pd.DataFrame, last, close: float) -> bool:
    """스퀴즈확장: band26_q 낮음 + 전주 대비 확장, 종가>13주선."""
    try:
        if len(df) < 2:
            return False
        subs = _w21_subs(df, last, close)
        return all(subs.values())
    except Exception as exc:
        tk = _fval(last, "ticker") if last is not None else "?"
        return _pattern_exc("w21", tk, exc)


def _w31_subs(df: pd.DataFrame, last, close: float) -> dict[str, bool]:
    max52 = _fval(last, "max52")
    vol_last = _fval(last, "volume")
    vol_avg = np.nan
    if len(df) >= 5:
        vol_avg = float(df["volume"].iloc[-5:-1].mean())
    return {
        "a": np.isfinite(close) and np.isfinite(max52) and max52 > 0 and close >= max52 * W_HI52_RATIO,
        "b": (
            np.isfinite(vol_last)
            and np.isfinite(vol_avg)
            and vol_avg > 0
            and vol_last > vol_avg * W_VOL_SURGE
        ),
    }


def _w31(df: pd.DataFrame, last, close: float) -> bool:
    """52주 신고가 근접 + 거래량 급증."""
    try:
        if len(df) < 5:
            return False
        subs = _w31_subs(df, last, close)
        return all(subs.values())
    except Exception as exc:
        tk = _fval(last, "ticker") if last is not None else "?"
        return _pattern_exc("w31", tk, exc)


def _w41(df: pd.DataFrame, last, close: float) -> bool:
    """26주 박스 상단 구간 + 단기>중기 이평."""
    try:
        hi, lo = _fval(last, "max26"), _fval(last, "min26")
        span = hi - lo
        if not (np.isfinite(hi) and np.isfinite(lo) and span > 0):
            return False
        pos = (close - lo) / span
        sma13, sma26 = _fval(last, "sma13"), _fval(last, "sma26")
        return W_BOX_LO < pos < W_BOX_HI and sma13 > sma26
    except Exception as exc:
        tk = _fval(last, "ticker") if last is not None else "?"
        return _pattern_exc("w41", tk, exc)


def _w51(df: pd.DataFrame, last, close: float) -> bool:
    """눌림목: 26주선 위, 13주선 접촉, 양봉 마감."""
    try:
        sma26 = _fval(last, "sma26")
        sma13 = _fval(last, "sma13")
        opn = _fval(last, "open")
        low = _fval(last, "low")
        return close > sma26 and low <= sma13 * 1.02 and close > opn
    except Exception as exc:
        tk = _fval(last, "ticker") if last is not None else "?"
        return _pattern_exc("w51", tk, exc)


_WEEKLY_PATTERN_FNS = {
    "w11": _w11,
    "w21": _w21,
    "w31": _w31,
    "w41": _w41,
    "w51": _w51,
}


def _rs_pass(ticker, rs_df) -> bool:
    if not W_USE_RS:
        return True
    if rs_df is None or len(rs_df) == 0:
        return False
    tk = str(ticker).strip().zfill(6)
    if tk not in rs_df.index:
        return False
    try:
        return float(rs_df.loc[tk].rs_score) >= W_RS_MIN
    except Exception:
        return False


def _inject_p51_globals(log: bool = False, **overrides) -> list[str]:
    """52 전역 → p51 모듈 주입(요약 HTML 등 51 헬퍼용)."""
    global _INJECT_P51_LOG_DONE
    g = globals()
    injected: list[str] = []
    for name in _P51_GLOBAL_KEYS:
        val = overrides[name] if name in overrides else g.get(name)
        if val is None:
            continue
        setattr(p51, name, val)
        injected.append(name)
    p51.USE_RS = bool(overrides.get("use_rs", g.get("W_USE_RS", W_USE_RS)))
    injected.append("USE_RS")
    if log and injected and not _INJECT_P51_LOG_DONE:
        _INJECT_P51_LOG_DONE = True
        print(f"p51 전역 주입: {', '.join(injected)}")
    return injected


def _norm_dt(obj):
    """Series/Index/DatetimeIndex/스칼라 무관하게 날짜를 자정 기준 정규화."""
    conv = pd.to_datetime(obj, errors="coerce")
    if isinstance(conv, pd.Series):
        return conv.dt.normalize()  # Series 만 .dt 필요
    return conv.normalize()  # DatetimeIndex / Timestamp


def _dedupe_weekly_bars(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """주봉 date 인덱스 — NaT 제거, 중복 제거(keep=last), 정렬."""
    if df is None or df.empty:
        return df, 0
    d = df.copy()
    if "date" in d.columns:
        d = d.set_index("date", drop=True)
    elif d.index.name != "date":
        d = d.reset_index()
        if "date" not in d.columns:
            d = d.rename(columns={d.columns[0]: "date"})
        d = d.set_index("date", drop=True)
    d.index = _norm_dt(d.index)
    d = d[d.index.notna()]
    n_before = len(d)
    d = d[~d.index.duplicated(keep="last")].sort_index()
    return d, n_before - len(d)


def _week_chart_output_dir() -> str:
    folder_name = datetime.date.today().strftime("%Y-%m-%d")
    path = os.path.join(_PICKING_KRX_DIR, folder_name)
    os.makedirs(path, exist_ok=True)
    return path


def _week_chart_missing_col(col: str) -> None:
    if col in _WEEK_CHART_MISSING_LOGGED:
        return
    _WEEK_CHART_MISSING_LOGGED.add(col)
    print(f"⚠️ 주봉 차트 컬럼 없음(스킵): {col}")


def _fmt_chart_num(v, ndigits: int = 2) -> str:
    try:
        fv = float(v)
        if not np.isfinite(fv):
            return "-"
        return f"{fv:.{ndigits}f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_mcap_title(mcap_raw) -> str:
    try:
        v = float(mcap_raw)
        if not np.isfinite(v) or v <= 0:
            return "-"
        billion = v / 1_000_000_000
        if billion >= 1000:
            return f"{billion / 1000:.1f}조"
        return f"{billion * 10:.0f}억"
    except (TypeError, ValueError):
        return "-"


def _prepare_week_chart_df(ind_df: pd.DataFrame, weeks: int) -> pd.DataFrame:
    raw_len = len(ind_df)
    d, _ = _dedupe_weekly_bars(ind_df)
    d = d.tail(int(weeks))
    return d, raw_len


def _log_plot_target(raw_len: int, df: pd.DataFrame) -> None:
    global _PLOT_TARGET_LOG_DONE
    if _PLOT_TARGET_LOG_DONE:
        return
    _PLOT_TARGET_LOG_DONE = True
    close_ok = int(df["close"].notna().sum()) if "close" in df.columns else 0
    n_unique = df.index.nunique() if isinstance(df.index, pd.DatetimeIndex) else len(df)
    print(
        f"플롯 대상: {len(df)}봉 (중복제거 전 {raw_len}봉, 고유주 {n_unique}), "
        f"close 유효 {close_ok}"
    )


def _log_chart_save(out_path: str, fig) -> None:
    """저장 직후 파일 크기·트레이스 수 1회 출력."""
    global _CHART_SAVE_LOG_DONE
    if _CHART_SAVE_LOG_DONE:
        return
    _CHART_SAVE_LOG_DONE = True
    try:
        size_kb = os.path.getsize(out_path) / 1024
    except OSError:
        size_kb = 0.0
    n_traces = len(fig.data) if fig is not None else 0
    fname = os.path.basename(out_path)
    print(f"차트 저장: {fname} ({size_kb:.0f}KB, traces={n_traces})")
    if n_traces == 0:
        print("⚠️ traces=0 → 차트 데이터/트레이스 구성 확인")
    elif size_kb < 200 and CHART_PLOTLYJS == "cdn":
        print("⚠️ 파일이 작고 cdn 모드 → plotly.js 미로드(백지) 가능")


def _log_chart_xaxis(plot_df: pd.DataFrame) -> None:
    """저장 직전 plot_df 기준 x축 range·봉수 1회 출력."""
    global _CHART_XAXIS_LOG_DONE
    if _CHART_XAXIS_LOG_DONE or plot_df is None or plot_df.empty:
        return
    _CHART_XAXIS_LOG_DONE = True
    try:
        idx = pd.to_datetime(plot_df.index, errors="coerce")
        idx = idx[idx.notna()]
        if idx.empty:
            return
        print(
            f"x축: {idx[0].date()} ~ {idx[-1].date()} ({len(plot_df)}봉)"
        )
    except Exception as exc:
        print(f"x축 진단 실패: {exc}")


def _week_chart_col(df: pd.DataFrame, *names: str) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    for n in names:
        _week_chart_missing_col(n)
    return None


def _week_pos_series(df: pd.DataFrame, hi_col: str, lo_col: str) -> pd.Series | None:
    if hi_col not in df.columns or lo_col not in df.columns or "close" not in df.columns:
        _week_chart_missing_col(f"{hi_col}/{lo_col}")
        return None
    span = (df[hi_col] - df[lo_col]).replace(0, np.nan)
    return (df["close"] - df[lo_col]) / span


def _week_snap_indices(n: int) -> list[int]:
    if n <= 0:
        return []
    idx = sorted({max(0, n - o) for o in _WEEK_SNAP_OFFSETS if n >= o})
    return idx if idx else [n - 1]


def _week_has_investor_osc(df: pd.DataFrame) -> bool:
    try:
        return p51._has_investor_osc_data(df)
    except Exception:
        for col in ("inst_net_osc", "frgn_net_osc"):
            if col not in df.columns:
                return False
            if df[col].notna().sum() < 5:
                return False
        return True


def _week_ensure_snapshot_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """미니 패널용 MFI·Williams %R — ind_df 컬럼만으로 계산(DB 없음)."""
    d = df
    if "mfi" not in d.columns and all(c in d.columns for c in ("high", "low", "close", "volume")):
        d = d.copy()
        d["mfi"] = talib.MFI(d.high, d.low, d.close, d.volume, 14)
    if "willr" not in d.columns and all(c in d.columns for c in ("high", "low", "close")):
        if d is df:
            d = d.copy()
        d["willr"] = talib.WILLR(d.high, d.low, d.close, 14)
    return d


def _week_subplot_domain_refs(row: int, col: int) -> tuple[str, str]:
    idx = (row - 1) * 2 + col
    xref = f"x{idx} domain" if idx > 1 else "x domain"
    yref = f"y{idx} domain" if idx > 1 else "y domain"
    return xref, yref


def _week_add_panel_label(fig, row: int, col: int, title: str, *, right: bool = False) -> None:
    xref, yref = _week_subplot_domain_refs(row, col)
    fig.add_annotation(
        text=title,
        xref=xref,
        yref=yref,
        x=0.98 if right else 0.02,
        y=0.98 if (right and row == 1) else (0.02 if right else 0.98),
        xanchor="right" if right else "left",
        yanchor="top" if (right and row == 1) else ("bottom" if right else "top"),
        showarrow=False,
        font=dict(size=11, color="rgba(50, 50, 50, 1)", family=_WEEK_CHART_FONT),
        bgcolor="rgba(255, 255, 255, 0.85)",
        bordercolor="rgba(100, 100, 100, 0.5)",
        borderwidth=1,
        borderpad=4,
    )


def _week_macd_hist_colors(series) -> list[str]:
    out = []
    for v in series:
        if pd.isna(v):
            out.append("rgba(128,128,128,0.3)")
        else:
            out.append("#26A69A" if float(v) >= 0 else "#EF5350")
    return out


def _bar_width_ms(idx) -> int:
    """날짜축 Bar 폭(ms) — 인덱스 실제 간격 median×0.7."""
    idx = pd.DatetimeIndex(pd.to_datetime(idx, errors="coerce"))
    idx = idx[idx.notna()]
    if len(idx) < 2:
        return 6 * 24 * 60 * 60 * 1000
    d = np.diff(idx.view("int64") // 10**6)
    med = float(np.median(d))
    return int(med * 0.7)


def _weekly_investor_osc_frame(long_df, groups=None) -> pd.DataFrame:
    """investor_osc_frame 동일 pivot, cum_days=W_INVESTOR_OSC_CUM (주봉 전용)."""
    if groups is None:
        groups = p51._INVESTOR_OSC_GROUPS
    cols = list(groups.keys())
    if long_df is None or getattr(long_df, "empty", True):
        return pd.DataFrame(columns=cols)
    df = long_df.copy()
    if "date" not in df.columns or "invst_tp_cd" not in df.columns or "net_val" not in df.columns:
        return pd.DataFrame(columns=cols)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["invst_tp_cd"] = df["invst_tp_cd"].astype(str).str.strip()
    df["net_val"] = pd.to_numeric(df["net_val"], errors="coerce")
    if df.empty:
        return pd.DataFrame(columns=cols)
    piv = df.pivot_table(
        index="date",
        columns="invst_tp_cd",
        values="net_val",
        aggfunc="sum",
    )
    piv.index = _norm_dt(piv.index)
    piv = piv[piv.index.notna()].sort_index()
    out = pd.DataFrame(index=piv.index)
    for name, codes in groups.items():
        code_list = [str(c).strip() for c in codes]
        net = None
        for c in code_list:
            part = (
                pd.to_numeric(piv[c], errors="coerce")
                if c in piv.columns
                else pd.Series(0.0, index=piv.index)
            )
            net = part if net is None else net.add(part, fill_value=0)
        out[name] = investor_net_osc(
            net if net is not None else pd.Series(0.0, index=piv.index),
            cum_days=W_INVESTOR_OSC_CUM,
        )
    return out[cols]


def plot_week_chart(
    ind_df: pd.DataFrame,
    ticker: str,
    name: str,
    pattern: str,
    out_path: str,
    weeks: int = CHART_WEEKS,
    volume_data: dict | None = None,
    sector_df: pd.DataFrame | None = None,
    open_browser: bool = False,
) -> bool:
    """52 전용 주봉 Plotly 차트 — ind_df(+선택 volume_data/sector_df), DB 재조회 없음."""
    if ind_df is None or not isinstance(ind_df, pd.DataFrame) or ind_df.empty:
        print(f"⚠️ 차트 스킵 {ticker}: 빈 DataFrame")
        return False

    plot_df, raw_len = _prepare_week_chart_df(ind_df, weeks)
    if plot_df.empty:
        print(f"⚠️ 차트 스킵 {ticker}: 유효 봉 없음")
        return False

    plot_df = _week_ensure_snapshot_indicators(plot_df)
    _log_plot_target(raw_len, plot_df)
    x = plot_df.index
    n = len(plot_df)
    idx = plot_df.index
    print(
        f"[chart] x축 타입 점검 ticker={ticker} "
        f"n_bars={len(idx)} idx_dtype={idx.dtype}"
    )
    bar_w = _bar_width_ms(idx)
    try:
        _v = pd.to_numeric(plot_df.get("volume"), errors="coerce")
        print(
            f"[chart] bar_w={bar_w:,}ms "
            f"x_span={int((idx[-1]-idx[0]).total_seconds()*1000):,}ms "
            f"bar_w_ratio={bar_w/max((idx[-1]-idx[0]).total_seconds()*1000,1):.5f}"
        )
        print(
            f"[chart] volume 진단 n={_v.notna().sum()} nunique={_v.nunique()} "
            f"min={_v.min():,.0f} med={_v.median():,.0f} max={_v.max():,.0f}"
        )
        print(f"[chart] volume 최근 6봉={[f'{x:,.0f}' for x in _v.tail(6)]}")
        print(
            f"[chart] vol_sma4 최근 3봉="
            f"{list(pd.to_numeric(plot_df.get('vol_sma4'), errors='coerce').tail(3))}"
        )
    except Exception as e:
        print(f"[chart] 진단 실패: {e}")

    has_inv = _week_has_investor_osc(plot_df)
    _n_rows = 7 if has_inv else 6
    _csi_row = 6
    _inv_row = 7 if has_inv else None
    _box_row = 7 if has_inv else 6
    row_heights = _WEEK_CHART_ROW_HEIGHTS_7 if _n_rows == 7 else _WEEK_CHART_ROW_HEIGHTS_6

    specs = []
    for i in range(_n_rows):
        sec = _inv_row is not None and (i + 1) == _inv_row
        specs.append([{"secondary_y": sec}, {"secondary_y": False}])

    fig = make_subplots(
        rows=_n_rows,
        cols=2,
        row_heights=list(row_heights),
        column_widths=_WEEK_CHART_COL_WIDTHS,
        shared_xaxes=False,
        shared_yaxes=False,
        vertical_spacing=0.02,
        horizontal_spacing=0.01,
        specs=specs,
    )

    # --- Row 1: Sector Performance (sector_df 화이트리스트만, 없으면 행 생략) ---
    sector_norm = None
    if sector_df is not None and not sector_df.empty:
        sn = sector_df.copy()
        if "date" in sn.columns:
            sn = sn.set_index("date")
        sn.index = _norm_dt(sn.index)
        sn = sn[sn.index.notna()].sort_index()
        sec_cols = [c for c in sn.columns if pd.api.types.is_numeric_dtype(sn[c])]
        if sec_cols:
            sn = sn[sec_cols]
            common = plot_df.index.intersection(sn.index)
            if len(common) >= 2:
                sn = sn.loc[common]
                base = sn.iloc[0].replace(0, np.nan)
                sector_norm = (sn / base) * 100

    sector_colors = ["#00BCD4", "#1E88E5", "#42A5F5", "#64B5F6", "#90CAF9"]
    if sector_norm is not None and not sector_norm.empty:
        for i, c in enumerate(sector_norm.columns):
            color = sector_colors[0] if i == 0 else sector_colors[(i - 1) % len(sector_colors) + 1]
            width = 3.5 if i == 0 else 2.0
            fig.add_trace(
                go.Scatter(
                    x=sector_norm.index,
                    y=sector_norm[c],
                    name=str(c),
                    line=dict(color=color, width=width),
                    opacity=1.0 if i == 0 else 0.9,
                ),
                row=1,
                col=1,
            )
        # Momentum (sector_df 있을 때만)
        try:
            mom_rows = []
            for c in sector_norm.columns:
                roc = talib.ROC(sector_norm[c].astype(float), 10)
                v = float(roc.iloc[-1]) if len(roc) and pd.notna(roc.iloc[-1]) else 0.0
                mom_rows.append({"label": f"10주_{c}", "mom": v, "color": sector_colors[0] if c == sector_norm.columns[0] else sector_colors[1]})
            if mom_rows:
                mom_df = pd.DataFrame(mom_rows)
                fig.add_trace(
                    go.Bar(
                        x=mom_df["mom"],
                        y=mom_df["label"],
                        orientation="h",
                        marker=dict(color=mom_df["color"]),
                        showlegend=False,
                        hovertemplate="%{y}: %{x:.2f}%<extra></extra>",
                    ),
                    row=1,
                    col=2,
                )
        except Exception:
            pass

    # --- Row 2: Price & SMA ---
    if all(c in plot_df.columns for c in ("open", "high", "low", "close")):
        fig.add_trace(
            go.Candlestick(
                x=x,
                open=plot_df["open"],
                high=plot_df["high"],
                low=plot_df["low"],
                close=plot_df["close"],
                increasing_line_color="#FF4136",
                decreasing_line_color="#0074D9",
                name="Price",
            ),
            row=2,
            col=1,
        )
    for col, label, color, width in (
        ("sma4", "SMA4", "#FF6B6B", 2.5),
        ("sma13", "SMA13", "#4ECDC4", 2.0),
        ("sma26", "SMA26", "#95E1D3", 1.5),
        ("sma52", "SMA52", "#F38181", 1.5),
    ):
        if col in plot_df.columns:
            fig.add_trace(
                go.Scatter(x=x, y=plot_df[col], name=label, line=dict(color=color, width=width)),
                row=2,
                col=1,
            )
        else:
            _week_chart_missing_col(col)

    # Volume Profile (row 2, col 2)
    if volume_data is None:
        volume_data = globals().get("volume_data")
    try:
        tk_key = str(ticker).strip().zfill(6)
        df_v = None
        if isinstance(volume_data, dict):
            df_v = volume_data.get(tk_key) or volume_data.get(str(ticker).strip())
        if df_v is not None and not df_v.empty and "volume_p" in df_v.columns:
            colors = df_v.volume_p.apply(
                lambda v: f"rgba(255, 107, 53, {min(float(v) / 100, 1)})" if pd.notna(v) and v > 0 else "rgba(128,128,128,0.3)"
            )
            fig.add_trace(
                go.Bar(
                    x=df_v.volume_p,
                    y=df_v.index,
                    orientation="h",
                    marker=dict(color=colors),
                    name="Volume Profile",
                    showlegend=False,
                ),
                row=2,
                col=2,
            )
    except Exception:
        pass

    # --- Row 3: Volume ---
    if "volume" in plot_df.columns:
        fig.add_trace(
            go.Bar(
                x=x,
                y=plot_df["volume"],
                name="Volume",
                width=bar_w,
                marker=dict(color="rgba(128, 128, 128, 0.6)", line=dict(width=0)),
                opacity=0.6,
            ),
            row=3,
            col=1,
        )
    for col, label, color in (("vol_sma4", "Vol SMA4", "#FF6B6B"), ("vol_sma13", "Vol SMA13", "#4ECDC4")):
        if col in plot_df.columns:
            fig.add_trace(
                go.Scatter(x=x, y=plot_df[col], name=label, line=dict(color=color, width=1.5, dash="dot")),
                row=3,
                col=1,
            )
        else:
            _week_chart_missing_col(col)

    # --- Row 4: MACD ---
    macd_col = _week_chart_col(plot_df, "macd")
    sig_col = _week_chart_col(plot_df, "macd_signal", "macdsignal")
    hist_col = _week_chart_col(plot_df, "macd_hist", "macdhist")
    if hist_col:
        fig.add_trace(
            go.Bar(
                x=x,
                y=plot_df[hist_col],
                name="Histogram",
                width=bar_w,
                marker=dict(color=_week_macd_hist_colors(plot_df[hist_col])),
            ),
            row=4,
            col=1,
        )
    if macd_col:
        fig.add_trace(
            go.Scatter(x=x, y=plot_df[macd_col], name="MACD", line=dict(color="#00B4DB", width=2)),
            row=4,
            col=1,
        )
    if sig_col:
        fig.add_trace(
            go.Scatter(x=x, y=plot_df[sig_col], name="Signal", line=dict(color="#FF6B6B", width=2)),
            row=4,
            col=1,
        )

    # --- Row 5: Band Width ---
    if "band26_q" in plot_df.columns:
        fig.add_trace(
            go.Scatter(x=x, y=plot_df["band26_q"], name="Band Squeeze", line=dict(color="#9B59B6", width=1.5)),
            row=5,
            col=1,
        )
        try:
            ax_idx = (5 - 1) * 2 + 1
            fig.add_shape(
                type="rect",
                xref=f"x{ax_idx}" if ax_idx > 1 else "x",
                yref=f"y{ax_idx}" if ax_idx > 1 else "y",
                x0=plot_df.index[0],
                x1=plot_df.index[-1],
                y0=0.2,
                y1=0.8,
                fillcolor="rgba(173, 216, 230, 0.3)",
                layer="below",
                line_width=0,
            )
        except Exception:
            fig.add_hrect(y0=0.2, y1=0.8, row=5, col=1, fillcolor="rgba(173,216,230,0.3)", line_width=0)
    else:
        _week_chart_missing_col("band26_q")

    # --- Row 6: CSI ---
    for col, label, color in (
        ("csi", "CSI", "#0096FF"),
        ("csi_fast", "CSI Fast", "#E74C3C"),
        ("csi_slow", "CSI Slow", "#7E57C2"),
    ):
        if col in plot_df.columns:
            fig.add_trace(
                go.Scatter(x=x, y=plot_df[col], name=label, line=dict(color=color, width=1.5)),
                row=_csi_row,
                col=1,
            )
        else:
            _week_chart_missing_col(col)
    fig.add_hline(
        y=0, row=_csi_row, col=1,
        line=dict(color="rgba(128, 128, 128, 0.5)", width=1, dash="dash"),
    )

    # --- Row 7: Investor OSC (optional) ---
    if has_inv and _inv_row is not None:
        for col, label, color in (
            ("inst_net_osc", "기관(연기금+투신+사모) 순매수금액 OSC(4주)", "#2E86DE"),
            ("frgn_net_osc", "외국인(9000) 순매수금액 OSC(4주)", "#E74C3C"),
        ):
            if col in plot_df.columns:
                fig.add_trace(
                    go.Scatter(x=x, y=plot_df[col], name=label, line=dict(color=color, width=1.8)),
                    row=_inv_row,
                    col=1,
                    secondary_y=False,
                )
        if "frgn_ratio" in plot_df.columns and plot_df["frgn_ratio"].notna().any():
            fig.add_trace(
                go.Bar(
                    x=x,
                    y=plot_df["frgn_ratio"],
                    name="외국인 지분율",
                    width=bar_w,
                    marker=dict(color="#00897B", line=dict(width=0)),
                    opacity=0.55,
                    showlegend=True,
                ),
                row=_inv_row,
                col=1,
                secondary_y=True,
            )

    # --- Right col: snapshot mini panels (MFI, Williams, %B, BOX) ---
    snap_idx = _week_snap_indices(n)
    date_labels = [plot_df.index[i] for i in snap_idx if i < n]

    def _snap_vals(col):
        return [
            float(plot_df.iloc[i][col])
            if col in plot_df.columns and pd.notna(plot_df.iloc[i][col])
            else np.nan
            for i in snap_idx
        ]

    if "mfi" in plot_df.columns and len(date_labels) >= 2:
        vals = _snap_vals("mfi")
        fig.add_trace(
            go.Scatter(
                x=date_labels, y=vals, mode="lines+markers", name="MFI",
                line=dict(color="#FF6B6B", width=2.5),
                marker=dict(size=8, color="#FF6B6B"),
                text=[f"{v:.1f}" if np.isfinite(v) else "-" for v in vals],
                textposition="top center",
                showlegend=False,
            ),
            row=3, col=2,
        )

    if "willr" in plot_df.columns and len(date_labels) >= 2:
        vals = _snap_vals("willr")
        fig.add_trace(
            go.Scatter(
                x=date_labels, y=vals, mode="lines+markers", name="Williams %R",
                line=dict(color="#9B59B6", width=2.5),
                marker=dict(size=8, color="#9B59B6"),
                showlegend=False,
            ),
            row=4, col=2,
        )

    if "pb" in plot_df.columns and len(date_labels) >= 2:
        vals = _snap_vals("pb")
        fig.add_trace(
            go.Scatter(
                x=date_labels, y=vals, mode="lines+markers", name="%B",
                line=dict(color="#00D2FF", width=2.5),
                marker=dict(size=8, color="#00D2FF"),
                showlegend=False,
            ),
            row=5, col=2,
        )

    box_col = "box7" if "box7" in plot_df.columns else None
    if box_col and len(date_labels) >= 2:
        vals = _snap_vals(box_col)
        fig.add_trace(
            go.Bar(
                x=date_labels, y=vals, name="Box7",
                marker=dict(color=vals, colorscale="Viridis", showscale=False),
                text=[f"{v:.1f}" if np.isfinite(v) else "-" for v in vals],
                textposition="outside",
                showlegend=False,
            ),
            row=_box_row, col=2,
        )

    # --- 제목·부제 (paper annotation, 51 스타일) ---
    last = plot_df.iloc[-1]
    mcap_str = _fmt_mcap_title(last.get("market_cap") if "market_cap" in plot_df.columns else np.nan)
    pat_label = WEEKLY_PATTERN_LABELS.get(pattern, "")
    title_pat = f"{pattern} {pat_label}".strip() if pat_label else pattern
    title_line1 = f"{name}({ticker}) · {title_pat} · 시총 {mcap_str}"

    atr_ratio = np.nan
    if "atr13" in plot_df.columns and "close" in plot_df.columns:
        c = float(last["close"]) if pd.notna(last["close"]) else np.nan
        a = float(last["atr13"]) if pd.notna(last["atr13"]) else np.nan
        if np.isfinite(c) and c > 0 and np.isfinite(a):
            atr_ratio = a / c
    pos26 = _week_pos_series(plot_df, "max26", "min26")
    pos52 = _week_pos_series(plot_df, "max52", "min52")
    pos26_last = float(pos26.iloc[-1]) if pos26 is not None and pd.notna(pos26.iloc[-1]) else np.nan
    pos52_last = float(pos52.iloc[-1]) if pos52 is not None and pd.notna(pos52.iloc[-1]) else np.nan
    s4 = float(last["sma4"]) if "sma4" in plot_df.columns and pd.notna(last["sma4"]) else np.nan
    s13 = float(last["sma13"]) if "sma13" in plot_df.columns and pd.notna(last["sma13"]) else np.nan
    s26 = float(last["sma26"]) if "sma26" in plot_df.columns and pd.notna(last["sma26"]) else np.nan
    s52 = float(last["sma52"]) if "sma52" in plot_df.columns and pd.notna(last["sma52"]) else np.nan
    aligned = (
        np.isfinite(s4) and np.isfinite(s13) and np.isfinite(s26) and np.isfinite(s52)
        and s4 > s13 > s26 > s52
    )
    title_line2 = (
        f"주봉 {weeks}주 · atr13/종가 {_fmt_chart_num(atr_ratio, 3)} · "
        f"band26_q {_fmt_chart_num(last.get('band26_q') if 'band26_q' in plot_df.columns else np.nan)} · "
        f"26주위치 {_fmt_chart_num(pos26_last)} · 52주위치 {_fmt_chart_num(pos52_last)} · "
        f"sma4/13/26/52 {'정배열' if aligned else '-'}"
    )

    top_margin = 110
    fig.update_layout(
        title=None,
        autosize=True,
        height=1400,
        margin=dict(l=0, r=140, t=top_margin, b=0, pad=0),
        bargap=0.15,
        barmode="overlay",
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black", size=11, family=_WEEK_CHART_FONT),
        legend=dict(
            orientation="v",
            x=1.02,
            y=1,
            xanchor="left",
            yanchor="top",
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="rgba(128, 128, 128, 0.5)",
            borderwidth=1,
            font=dict(size=10),
        ),
        hovermode="x unified",
        showlegend=True,
        hoverlabel=dict(
            bgcolor="rgba(255, 255, 255, 0.95)",
            bordercolor="rgba(0, 0, 0, 0.3)",
            font=dict(size=13, color="black", family="Consolas, monospace"),
        ),
    )
    fig.add_annotation(
        text=title_line1,
        xref="paper", yref="paper",
        x=0.01, y=1.045,
        xanchor="left", yanchor="bottom",
        showarrow=False,
        font=dict(size=13, color="black", family=_WEEK_CHART_FONT),
    )
    fig.add_annotation(
        text=title_line2,
        xref="paper", yref="paper",
        x=0.01, y=1.015,
        xanchor="left", yanchor="bottom",
        showarrow=False,
        font=dict(size=12, color="#666666", family=_WEEK_CHART_FONT),
    )

    # 패널 라벨 (좌상단 annotation 박스)
    panel_labels = [
        (1, 1, "📉 Sector Performance", False),
        (2, 1, "📈 Price & Moving Averages", False),
        (3, 1, "📊 Volume Analysis", False),
        (4, 1, "📈 MACD", False),
        (5, 1, "📏 Band Width", False),
        (_csi_row, 1, "📦 CSI", False),
        (2, 2, "📊 Volume Profile", True),
        (1, 2, "📊 Momentum", True),
        (3, 2, "MFI", True),
        (4, 2, "Williams R", True),
        (5, 2, "%b", True),
        (_box_row, 2, "📦 BOX", True),
    ]
    if has_inv and _inv_row:
        panel_labels.append((_inv_row, 1, "📊 Investor OSC", False))
    for row, col, lbl, right in panel_labels:
        if row == 1 and col == 1 and sector_norm is None:
            continue
        if row == 1 and col == 2 and sector_norm is None:
            continue
        _week_add_panel_label(fig, row, col, lbl, right=right)

    # 구분선
    dividers = [0.84, 0.68, 0.52, 0.38, 0.25, 0.12] if _n_rows == 7 else [0.82, 0.64, 0.46, 0.31, 0.17]
    for pos in dividers:
        fig.add_shape(
            type="line", xref="paper", yref="paper",
            x0=0, y0=pos, x1=1, y1=pos,
            line=dict(color="rgba(150, 150, 150, 0.4)", width=1.5, dash="dot"),
        )

    # 축 스타일
    fig.update_yaxes(
        gridcolor="rgba(200, 200, 200, 0.8)", gridwidth=0.5,
        zeroline=True, zerolinecolor="rgba(150, 150, 150, 0.8)", zerolinewidth=1,
    )
    fig.update_xaxes(gridcolor="rgba(200, 200, 200, 0.8)", gridwidth=0.5)
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    fig.update_yaxes(showticklabels=False, row=2, col=2)
    fig.update_yaxes(nticks=20, tickfont=dict(size=10, color="black"), row=2, col=1)
    fig.update_xaxes(
        showspikes=True, spikemode="across", spikesnap="cursor",
        spikedash="dot", spikethickness=1, spikecolor="rgba(100,100,100,0.5)",
        row=2, col=1,
    )
    fig.update_yaxes(
        showspikes=True, spikemode="across", spikesnap="cursor",
        spikedash="dot", spikethickness=1, spikecolor="rgba(100,100,100,0.5)",
        row=2, col=1,
    )
    if has_inv and _inv_row:
        fig.update_yaxes(range=[0, 100], row=_inv_row, col=1, secondary_y=False)
        fig.add_hline(y=80, row=_inv_row, col=1, line=dict(color="rgba(255,107,107,0.5)", width=1, dash="dash"))
        fig.add_hline(y=20, row=_inv_row, col=1, line=dict(color="rgba(78,205,196,0.5)", width=1, dash="dash"))
    try:
        fig.update_yaxes(range=[0, 100], row=3, col=2)
        fig.update_yaxes(range=[-100, 0], row=4, col=2)
        fig.add_hline(y=-20, row=4, col=2, line=dict(color="rgba(255,107,107,0.6)", width=1.5, dash="dash"))
        fig.add_hline(y=-80, row=4, col=2, line=dict(color="rgba(78,205,196,0.6)", width=1.5, dash="dash"))
    except Exception:
        pass
    fig.update_yaxes(autorange=True, row=_box_row, col=2, showticklabels=False)
    for row in range(1, _box_row):
        fig.update_xaxes(showticklabels=False, row=row, col=2)
    if date_labels:
        fig.update_xaxes(
            showticklabels=True, tickmode="array", tickvals=date_labels,
            tickformat="%Y-%m-%d", tickangle=-45, row=_box_row, col=2,
        )

    # 좌측 열(col=1): plot_df 기준 x range·3개월 눈금, 마지막 행만 라벨
    x_min, x_max = plot_df.index[0], plot_df.index[-1]
    for row in range(1, _n_rows + 1):
        fig.update_xaxes(
            type="date",
            range=[x_min, x_max],
            tickformat="%y-%m",
            dtick="M3",
            ticks="outside",
            showticklabels=(row == _n_rows),
            row=row,
            col=1,
        )
    fig.update_xaxes(type="date", col=1)
    fig.update_xaxes(rangeslider_visible=False)

    _log_chart_xaxis(plot_df)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    # --- 축 진단: col=1 각 행의 실제 x축 설정 ---
    try:
        print("[axis] col=1 x축 상태")
        for _r in range(1, _n_rows + 1):
            _ax_id = (_r - 1) * 2 + 1
            _key = "xaxis" if _ax_id == 1 else f"xaxis{_ax_id}"
            _ax = fig.layout[_key]
            print(
                f"  row{_r} {_key}: range={_ax.range} matches={_ax.matches} "
                f"type={_ax.type} rslider={_ax.rangeslider.visible} domain={_ax.domain}"
            )
        print("[axis] trace → 축 매핑")
        for _t in fig.data:
            if _t.type == "bar":
                _n = len(_t.x) if _t.x is not None else 0
                print(
                    f"  bar name={_t.name} xaxis={_t.xaxis} yaxis={_t.yaxis} "
                    f"n={_n} width={getattr(_t, 'width', None)} "
                    f"x0={_t.x[0] if _n else None} x_last={_t.x[-1] if _n else None}"
                )
    except Exception as e:
        print(f"[axis] 진단 실패: {e}")

    fig.write_html(out_path, include_plotlyjs=CHART_PLOTLYJS, full_html=True)
    _log_chart_save(out_path, fig)
    if open_browser:
        webbrowser.open("file:///" + os.path.abspath(out_path).replace("\\", "/"))
    return True


def log_indicator_validity_sample(indicators_data: dict, sample_n: int = DIAG_SAMPLE_N) -> None:
    """주요 컬럼 마지막 행 유효값 비율 (파이프라인당 1회)."""
    global _INDICATOR_DIAG_DONE
    if _INDICATOR_DIAG_DONE or not indicators_data:
        return
    _INDICATOR_DIAG_DONE = True

    keys = list(indicators_data.keys())[:sample_n]
    n = len(keys)
    if n == 0:
        return

    print("\n" + "=" * 80)
    print(f"📋 지표 유효율 진단 (샘플 {n}종목, 마지막 행 기준)")
    print("=" * 80)
    sample_df = indicators_data[keys[0]]
    deduped, n_dup = _dedupe_weekly_bars(sample_df)
    print(
        f"  샘플 {keys[0]}: {len(sample_df)}행, 고유주 {len(deduped)}"
        + (f" (중복 {n_dup}행)" if n_dup else "")
    )
    for col in DIAG_COLS:
        valid = 0
        for k in keys:
            df = indicators_data[k]
            if df is None or col not in df.columns:
                continue
            try:
                v = df[col].iloc[-1]
                if pd.notna(v) and np.isfinite(float(v)):
                    valid += 1
            except Exception:
                pass
        pct = valid / n * 100
        flag = " ← 문제" if pct < 80 else ""
        print(f"  {col}: {pct:.1f}%{flag}")
    print("=" * 80)


def _weekly_row_counts(df: pd.DataFrame) -> tuple[int, int]:
    """(전체 행 수, 금요일 행 수)."""
    if df is None or df.empty:
        return 0, 0
    idx = pd.to_datetime(df.index, errors="coerce")
    valid = idx.notna()
    n = int(valid.sum())
    fri = int((idx[valid].dayofweek == 4).sum())
    return n, fri


def _log_weekly_row_diag(stage: str, ticker, df: pd.DataFrame) -> None:
    """샘플 1종목 — load/indi/osc/frgn 단계별 행 수 1회 출력."""
    global _WEEKLY_ROW_DIAG_DONE
    if _WEEKLY_ROW_DIAG_DONE or _WEEKLY_ROW_DIAG_TICKER is None:
        return
    if ticker is None or str(ticker) != str(_WEEKLY_ROW_DIAG_TICKER):
        return
    n, fri = _weekly_row_counts(df)
    t = str(ticker)
    if stage == "load":
        print(f"  주봉 행수 진단 [{t}] load: {n}행 (금요일 {fri})")
    else:
        print(f"  주봉 행수 진단 [{t}] {stage}: {n}행")
    if stage == "frgn":
        _WEEKLY_ROW_DIAG_DONE = True


def _resample_daily_series_w_fri_last(series: pd.Series) -> pd.Series:
    """일봉 시계열 → W-FRI 마지막 값."""
    s = pd.to_numeric(series, errors="coerce")
    s = s.copy()
    s.index = _norm_dt(s.index)
    s = s[s.index.notna()]
    if s.empty:
        return s
    return s.resample("W-FRI").last()


def _attach_weekly_investor_osc(wdf: pd.DataFrame, engine, ticker) -> pd.DataFrame:
    """
    일봉 투자자 OSC·외국인 지분율 → W-FRI 리샘플 후 주봉 인덱스 reindex.
    join/merge로 좌측(주봉) 행을 늘리지 않는다.
    """
    if wdf is None or wdf.empty:
        return wdf
    out = wdf.copy()
    out.index = _norm_dt(out.index)
    out = out[out.index.notna()]
    week_index = out.index
    t = p51._normalize_ticker(ticker, out)
    osc_cols = tuple(p51._INVESTOR_OSC_GROUPS.keys())

    investor_df = p51._load_investor_trading(engine, t)
    for col in osc_cols:
        out[col] = np.nan
    if investor_df is not None and not investor_df.empty:
        osc = _weekly_investor_osc_frame(investor_df, groups=p51._INVESTOR_OSC_GROUPS)
        if osc is not None and not osc.empty:
            osc = osc.copy()
            osc.index = _norm_dt(osc.index)
            osc = osc[~osc.index.isna()]
            osc_w = osc.resample("W-FRI").last()
            for col in osc_cols:
                if col in osc_w.columns:
                    out[col] = osc_w[col].reindex(week_index)
    _log_weekly_row_diag("osc", ticker, out)

    if "frgn_ratio" in out.columns:
        out = out.drop(columns=["frgn_ratio"])
    fh = p51._load_foreign_holding(engine, t)
    if fh is not None and not fh.empty:
        fh = fh.copy()
        fh["date"] = _norm_dt(fh["date"])
        fh = fh.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
        s_w = _resample_daily_series_w_fri_last(fh.set_index("date")["frgn_ratio"])
        out["frgn_ratio"] = s_w.reindex(week_index)
    else:
        out["frgn_ratio"] = np.nan
    _log_weekly_row_diag("frgn", ticker, out)
    return out


def _enforce_weekly_index(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """금요일만 유지 + 중복 제거. 제거 행 수 반환."""
    global _WEEKLY_INDEX_WARN_DONE
    if df is None or df.empty:
        return df, 0
    d = df.copy()
    if "date" in d.columns:
        d = d.set_index("date", drop=True)
    d.index = _norm_dt(d.index)
    d = d[d.index.notna()]
    n_before = len(d)
    d = d[d.index.dayofweek == 4]
    d = d[~d.index.duplicated(keep="last")].sort_index()
    removed = n_before - len(d)
    if removed > 0 and not _WEEKLY_INDEX_WARN_DONE:
        _WEEKLY_INDEX_WARN_DONE = True
        print(f"⚠️ 주봉 인덱스 정리: 비금요일/중복 {removed}행 제거")
    return d, removed


def get_indicators_weekly(d: pd.DataFrame) -> pd.DataFrame:
    """주봉 OHLCV → 주봉 지표 (컬럼명 = 주봉 정본 4/13/26/52주)."""
    d, _ = _dedupe_weekly_bars(d)

    # 이동평균
    d["sma2"] = p51._sma_safe(d.close, W_SMA2)
    d["sma4"] = p51._sma_safe(d.close, W_SMA4)
    d["sma13"] = p51._sma_safe(d.close, W_SMA13)
    d["sma26"] = p51._sma_safe(d.close, W_SMA26)
    d["sma52"] = p51._sma_safe(d.close, W_SMA52)
    d["ema4"] = p51._ema_safe(d.close, W_EMA4)
    d["ema13"] = p51._ema_safe(d.close, W_EMA13)

    # min / max — 26·52주는 rolling 정본 (talib 대체)
    for w in (W_SMA2, W_SMA4, W_SMA13):
        sfx = str(w)
        d[f"max{sfx}"] = p51._max_safe(d.high, w)
        d[f"min{sfx}"] = p51._min_safe(d.low, w)
        d[f"mid{sfx}"] = p51._midprice_safe(d.high, d.low, w)
    d["max26"] = d.high.rolling(W_SMA26, min_periods=W_SMA26).max()
    d["min26"] = d.low.rolling(W_SMA26, min_periods=W_SMA26).min()
    d["mid26"] = p51._midprice_safe(d.high, d.low, W_SMA26)
    d["max52"] = d.high.rolling(W_SMA52, min_periods=W_SMA52).max()
    d["min52"] = d.low.rolling(W_SMA52, min_periods=W_SMA52).min()
    d["mid52"] = p51._midprice_safe(d.high, d.low, W_SMA52)

    d["atr13"] = atr_wilder(d.high, d.low, d.close, W_ATR)

    d["tr"] = talib.TRANGE(d.high, d.low, d.close)
    d["mtr7"] = p51._max_safe(d.tr, W_MTR_BOX)
    d["box7"] = p51._max_safe(d.high, W_MTR_BOX) - p51._min_safe(d.low, W_MTR_BOX)

    d["band26_q"] = bollinger_band_width_q(
        d.close, window=W_BAND, n_sigma=2.0, lookback=W_BAND_LB
    )

    upper, middle, lower = p51._bbands_safe(d.close, W_BAND, nbdevup=2, nbdevdn=2, matype=0)
    d["bol26_up"] = upper
    d["bol26_ma"] = middle
    d["bol26_dn"] = lower
    denom = (d.bol26_up - d.bol26_dn).replace(0, np.nan)
    d["pb"] = (d.close - d.bol26_dn) / denom
    d["pb_max"] = p51._max_safe(d.pb, W_SMA4)
    d["pb_min"] = p51._min_safe(d.pb, W_SMA4)

    # CSI — indicators_core / 51 정의, 창=13주
    _csi_sma = p51._sma_safe(d.close, W_CSI_LEN)
    _csi_atr = atr_wilder(d.high, d.low, d.close, W_CSI_LEN)
    cs = (d.close - _csi_sma) / _csi_atr.replace(0, np.nan)
    d["csi"] = p51._sma_safe(cs, 2)
    d["csi_fast"] = p51._ema_safe(cs, W_SMA2)
    d["csi_slow"] = p51._ema_safe(cs, W_SMA4)

    # 거래량·스크리닝 보조
    d["vol_sma4"] = p51._sma_safe(d.volume, W_SMA4)
    d["vol_sma13"] = p51._sma_safe(d.volume, W_SMA13)
    d["vol_sma26"] = p51._sma_safe(d.volume, W_SMA26)
    d["vol_sum4"] = p51._sum_safe(d.volume, W_SMA4)
    d["vol_sum13"] = p51._sum_safe(d.volume, W_SMA13)
    d["vol_sum4_sma4"] = p51._sma_safe(d.vol_sum4, W_SMA4)
    d["vol_max2"] = p51._max_safe(d.volume, W_SMA2)
    d["vol_max4"] = p51._max_safe(d.volume, W_SMA4)
    d["vol_max13"] = p51._max_safe(d.volume, W_SMA13)
    d["vol_vol13"] = d.volume / d.vol_sma13.replace(0, np.nan)

    box_denom = (d.max4 - d.min4).replace(0, np.nan)
    d["sma_score"] = (d.close - d.min4) / box_denom
    d["pm"] = (d.close - d.min4) / box_denom

    if "trading_value" not in d.columns:
        d["trading_value"] = d.close * d.volume

    d["atr_q"] = np.nan
    d["atr_w"] = d.atr13 / d.close.replace(0, np.nan)
    d["vol_mtr"] = (d.mtr7 / d.close.replace(0, np.nan)) * 100
    d["vol_atr"] = (d.atr13 / d.close.replace(0, np.nan)) * 100

    macd, macdsignal, macdhist = talib.MACD(
        d.close,
        fastperiod=W_MACD_FAST,
        slowperiod=W_MACD_SLOW,
        signalperiod=W_MACD_SIGNAL,
    )
    d["macd"] = macd
    d["macdsignal"] = macdsignal
    d["macdhist"] = macdhist

    return d


def build_indicators_weekly(ohlcv_df, engine=None, ticker=None):
    _log_weekly_row_diag("load", ticker, ohlcv_df)
    out = get_indicators_weekly(ohlcv_df)
    _log_weekly_row_diag("indi", ticker, out)
    out = _attach_weekly_investor_osc(out, engine, ticker)
    out, _ = _enforce_weekly_index(out)
    return out


def this_week_friday_label(asof=None) -> pd.Timestamp:
    d = pd.Timestamp(asof if asof is not None else pd.Timestamp.today()).normalize()
    return d.to_period("W-FRI").to_timestamp(how="end").normalize()


def drop_incomplete_week_bars(ohlcv_data: dict, asof=None) -> dict:
    if not DROP_INCOMPLETE_WEEK or not ohlcv_data:
        return ohlcv_data
    fri = this_week_friday_label(asof)
    out, n_drop = {}, 0
    for ticker, df in ohlcv_data.items():
        if df is None or df.empty:
            out[ticker] = df
            continue
        idx = _norm_dt(df.index)
        if idx.isna().all() and "date" in df.columns:
            work = df.copy()
            work["_d"] = _norm_dt(work["date"])
            before = len(work)
            work = work[work["_d"] != fri].drop(columns=["_d"])
            n_drop += before - len(work)
            out[ticker] = work
            continue
        mask = idx != fri
        n_drop += int((~mask).sum())
        out[ticker] = df.loc[mask]
    if n_drop:
        print(f"미완성 주봉 제외: 이번 주 금요일 라벨 {fri.date()} — {n_drop}행")
    return out


def load_single_ticker_ohlcv_week(ticker, ticker_list, engine):
    try:
        ohlcv = pd.read_sql_query(
            f"""
            SELECT date, open, high, low, close, volume
            FROM `{OHLCV_TABLE_WEEK}`
            WHERE ticker = %s
            ORDER BY date
            """,
            con=engine,
            params=(str(ticker),),
        )
        if ohlcv is None or ohlcv.empty:
            return ticker, None, 0
        for c in ("name", "sector", "market_cap", "ticker"):
            if c in ohlcv.columns:
                ohlcv = ohlcv.drop(columns=[c])
        ohlcv.insert(0, "ticker", str(ticker))
        try:
            ohlcv.insert(1, "name", ticker_list.loc[ticker, "종목명"])
            ohlcv.insert(2, "sector", ticker_list.loc[ticker, "업종명"])
            mc = ticker_list.loc[ticker, "시가총액"] if "시가총액" in ticker_list.columns else None
            ohlcv.insert(3, "market_cap", mc if pd.notna(mc) else None)
        except (KeyError, IndexError):
            ohlcv.insert(1, "name", ticker)
            ohlcv.insert(2, "sector", "")
            ohlcv.insert(3, "market_cap", None)
        ohlcv = ohlcv.set_index("date")
        ohlcv, n_dup = _dedupe_weekly_bars(ohlcv)
        return ticker, ohlcv, n_dup
    except Exception as e:
        with _WEEK_LOAD_FAIL_LOCK:
            _WEEK_LOAD_FAIL["n"] += 1
            n_fail = _WEEK_LOAD_FAIL["n"]
        if n_fail <= 3:
            print(f"티커 {ticker} 주봉 OHLCV 로드 실패 ({n_fail}/3): {e}")
            print(traceback.format_exc())
        return ticker, None, 0


def load_ohlcv_weekly_parallel(ticker_list, engine, max_workers=10):
    global _WEEK_LOAD_FAIL
    _WEEK_LOAD_FAIL["n"] = 0
    ohlcv_data = {}
    dup_rows, dup_tickers = 0, 0
    print(f"병렬 처리로 {len(ticker_list)}개 티커 주봉 OHLCV 로딩 ({OHLCV_TABLE_WEEK})...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {
            executor.submit(load_single_ticker_ohlcv_week, t, ticker_list, engine): t
            for t in ticker_list.index
        }
        for fut in tqdm(as_completed(futs), total=len(futs), desc="주봉 OHLCV"):
            ticker, data, n_dup = fut.result()
            if data is not None:
                if n_dup:
                    dup_rows += n_dup
                    dup_tickers += 1
                ohlcv_data[ticker] = data
    ohlcv_data = drop_incomplete_week_bars(ohlcv_data)
    print(f"성공적으로 로드된 주봉: {len(ohlcv_data)}개")
    if _WEEK_LOAD_FAIL["n"]:
        print(f"주봉 OHLCV 로드 실패 총 {_WEEK_LOAD_FAIL['n']}건")
        _WEEK_LOAD_FAIL["n"] = 0
    if dup_rows:
        print(
            f"주봉 중복 제거(로드): {dup_rows}행 / {dup_tickers}종목 "
            f"(date 인덱스 keep=last)"
        )
    return ohlcv_data


def _reset_weekly_indicator_fail_log() -> None:
    global _WEEKLY_INDICATOR_FAIL_TOTAL, _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN
    with _WEEKLY_INDICATOR_FAIL_LOCK:
        _WEEKLY_INDICATOR_FAIL_TOTAL = 0
        _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN = False


def _log_weekly_indicator_fail(ticker, err) -> None:
    global _WEEKLY_INDICATOR_FAIL_TOTAL, _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN
    with _WEEKLY_INDICATOR_FAIL_LOCK:
        _WEEKLY_INDICATOR_FAIL_TOTAL += 1
        if not _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN:
            _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN = True
            print(f"티커 {ticker} 지표 계산 실패 (최초 1건 traceback): {err}")
            print(traceback.format_exc())


def _flush_weekly_indicator_fail_log() -> None:
    global _WEEKLY_INDICATOR_FAIL_TOTAL, _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN
    with _WEEKLY_INDICATOR_FAIL_LOCK:
        total = _WEEKLY_INDICATOR_FAIL_TOTAL
        if total > 1:
            print(f"… 지표 계산 실패 {total}건 (상세 traceback은 최초 1건만 출력)")
        elif total == 1 and not _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN:
            print(f"… 지표 계산 실패 1건")
        _WEEKLY_INDICATOR_FAIL_TOTAL = 0
        _WEEKLY_INDICATOR_FAIL_TRACEBACK_SHOWN = False


def _calc_single_weekly(ticker_data):
    ticker, data = ticker_data
    try:
        if len(data) >= W_MIN_BARS:
            return ticker, build_indicators_weekly(data, engine=engine, ticker=ticker)
        return ticker, None
    except Exception as e:
        _log_weekly_indicator_fail(ticker, e)
        return ticker, None


def calculate_indicators_weekly_parallel(ohlcv_data, max_workers=8):
    global engine, _INDICATOR_DIAG_DONE, _WEEKLY_ROW_DIAG_DONE, _WEEKLY_ROW_DIAG_TICKER, _WEEKLY_INDEX_WARN_DONE
    _INDICATOR_DIAG_DONE = False
    _WEEKLY_ROW_DIAG_DONE = False
    _WEEKLY_INDEX_WARN_DONE = False
    _WEEKLY_ROW_DIAG_TICKER = sorted(ohlcv_data.keys())[0] if ohlcv_data else None
    indicators_data = {}
    _reset_weekly_indicator_fail_log()
    print("주봉 지표 자체 산출(4/13/26/52주)")
    if _WEEKLY_ROW_DIAG_TICKER:
        print(f"주봉 행수 진단 (샘플 1종목: {_WEEKLY_ROW_DIAG_TICKER})")
    print(f"병렬 처리로 {len(ohlcv_data)}개 종목 주봉 지표 계산...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = {
            executor.submit(_calc_single_weekly, (t, d)): t
            for t, d in ohlcv_data.items()
        }
        for fut in tqdm(as_completed(futs), total=len(futs), desc="주봉 지표"):
            ticker, res = fut.result()
            if res is not None:
                indicators_data[ticker] = res
    _flush_weekly_indicator_fail_log()
    print(f"지표 계산 완료: {len(indicators_data)}개")
    log_indicator_validity_sample(indicators_data)
    return indicators_data


def _load_ticker_list(engine):
    q = """
    select * from krx_ticker
    where 기준일 = (select max(기준일) from krx_ticker) and 종목구분 = '보통주';
    """
    tl = pd.read_sql(q, con=engine)
    if "시가총액" in tl.columns:
        tl = tl[["종목코드", "종목명", "업종명", "시가총액"]]
    else:
        tl["시가총액"] = None
        tl = tl[["종목코드", "종목명", "업종명", "시가총액"]]
    from exclusions import drop_excluded

    tl = drop_excluded(tl, "종목코드")
    return tl.set_index("종목코드")


def _load_rs_empty():
    return pd.DataFrame(columns=["rs10_score", "rs20_score", "rs50_score", "rs_score"])


_WEEKLY_SUB_FN = {
    "w11": _w11_subs,
    "w21": _w21_subs,
    "w31": _w31_subs,
}


def _log_weekly_subconditions(universe: list[tuple]) -> None:
    """w11/w21/w31 서브조건 통과 종목 수 (스크리닝 필터 통과 유니버스)."""
    keys = {
        "w11": ("a", "b", "c", "d", "e"),
        "w21": ("a", "b", "c"),
        "w31": ("a", "b"),
    }
    tallies = {code: {k: 0 for k in keys[code]} for code in keys}
    finals = {code: 0 for code in keys}

    for df, last, close in universe:
        for code, fn in _WEEKLY_SUB_FN.items():
            try:
                subs = fn(df, last, close)
                for k, ok in subs.items():
                    if ok:
                        tallies[code][k] += 1
                if all(subs.values()):
                    finals[code] += 1
            except Exception as exc:
                tk = str(df.iloc[-1].get("ticker", "?"))
                _pattern_exc(code, tk, exc)

    print("\n" + "=" * 80)
    print("📊 w11/w21/w31 서브조건 통과 종목 수 (ATR·RS 필터 후)")
    print("=" * 80)
    w11 = tallies["w11"]
    print(
        f"  w11: a={w11['a']} b={w11['b']} c={w11['c']} d={w11['d']} e={w11['e']}"
        f" → 최종 {finals['w11']}"
    )
    w21 = tallies["w21"]
    print(f"  w21: a={w21['a']} b={w21['b']} c={w21['c']} → 최종 {finals['w21']}")
    w31 = tallies["w31"]
    print(f"  w31: a={w31['a']} b={w31['b']} → 최종 {finals['w31']}")
    print("=" * 80)


def screen_weekly(indicators_data, rs_df=None, **ctx):
    """주봉 자체 패턴 스크리닝 (w11~w51). 51 screen_all 과 무관."""
    ticker_list = ctx.get("ticker_list")
    audit_list = ctx.get("audit_ticker", p51.audit_ticker)
    if rs_df is None:
        rs_df = ctx.get("rs_df") or _load_rs_empty()

    debug_counts = {
        "total_indicators": len(indicators_data),
        "passed_basic_filter": 0,
        "passed_atr_filter": 0,
        "selected_by_pattern": 0,
        **{c: 0 for c in WEEKLY_PATTERN_CODES},
    }
    result = {c: [] for c in WEEKLY_PATTERN_CODES}
    selected_stocks = []
    sub_universe: list[tuple] = []
    _PATTERN_EXC.clear()

    for tk, i in tqdm(indicators_data.items(), desc="주봉 스크리닝"):
        _tk = str(tk)
        try:
            last = i.iloc[-1]
            close = float(last["close"])
        except Exception:
            continue

        if len(i) < W_MIN_BARS:
            continue
        try:
            if float(last["open"]) <= 0:
                continue
        except Exception:
            continue
        if ticker_list is not None:
            try:
                if float(ticker_list.loc[_tk]["시가총액"]) < p51.DISPLAY_MCAP_MIN:
                    continue
            except Exception:
                continue
        if _tk in audit_list:
            continue
        debug_counts["passed_basic_filter"] += 1

        try:
            if float(last["atr13"]) / close >= W_ATR_MAX:
                continue
        except Exception:
            continue
        debug_counts["passed_atr_filter"] += 1

        if not _rs_pass(_tk, rs_df):
            continue

        sub_universe.append((i, last, close))

        matched = [
            code for code in WEEKLY_PATTERN_CODES if _WEEKLY_PATTERN_FNS[code](i, last, close)
        ]
        if len(matched) < W_MIN_PATTERNS:
            continue

        debug_counts["selected_by_pattern"] += 1
        for code in matched:
            result[code].append(i)
            debug_counts[code] += 1
            listed = last[["ticker", "name"]].to_list()
            listed.insert(0, code)
            listed.insert(0, i.index[-1])
            selected_stocks.append(listed)

    _log_weekly_subconditions(sub_universe)
    _flush_pattern_exc()

    if selected_stocks:
        selected_df = pd.DataFrame(
            selected_stocks, columns=["date", "type", "ticker", "company"]
        )
    else:
        selected_df = pd.DataFrame(columns=["date", "type", "ticker", "company"])

    chart_code = "w51" if result["w51"] else next((c for c in WEEKLY_PATTERN_CODES if result[c]), "w51")
    selected_stock_list = [chart_code] + result.get(chart_code, [])

    return {
        "selected_stock_list": selected_stock_list,
        "selected_df": selected_df,
        "selected_stocks": selected_stocks,
        "result": result,
        "debug_counts": debug_counts,
    }


def _week_pct_change(idf, lag_weeks: int):
    """주봉 등락률(%). lag_weeks=1 → 전주 대비, 4 → 4주 전 대비."""
    need = lag_weeks + 1
    if idf is None or len(idf) < need:
        return np.nan
    try:
        c0 = float(idf.iloc[-1]["close"])
        c1 = float(idf.iloc[-1 - lag_weeks]["close"])
        if c1 <= 0:
            return np.nan
        return (c0 / c1 - 1) * 100
    except Exception:
        return np.nan


def _ox_above(close, ma) -> str:
    if np.isfinite(close) and np.isfinite(ma) and close > ma:
        return "O"
    return "X"


def _box_pos(close, lo, hi):
    span = hi - lo
    if not (np.isfinite(close) and np.isfinite(lo) and np.isfinite(hi) and span > 0):
        return np.nan
    return (close - lo) / span


def _norm_tickers_z6(tickers) -> list[str]:
    return sorted({str(t).strip().zfill(6) for t in tickers})


def _week_mcap_tv_maps(engine, tickers):
    """
    시총·거래대금비중 맵 (52 전용, 일봉 krx_ohlcv + krx_ticker 직접 조회).
    반환: (mcap_map, mcap_share_pct, tv_share_pct) — 키 6자리 ticker.
    """
    ut = _norm_tickers_z6(tickers)
    nan_map = {k: np.nan for k in ut}
    empty = (dict(nan_map), dict(nan_map), dict(nan_map))
    if engine is None or not ut:
        return empty
    try:
        ref = pd.read_sql_query(
            f"SELECT MAX(DATE(date)) AS d FROM `{OHLCV_TABLE_DAILY}`", con=engine
        )
        d0 = pd.Timestamp(ref.iloc[0]["d"]).strftime("%Y-%m-%d")

        total_mcap = float(
            pd.read_sql_query(
                """
                SELECT COALESCE(SUM(t.시가총액), 0) AS total_mcap
                FROM krx_ticker t
                WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
                  AND t.종목구분 = '보통주'
                """,
                con=engine,
            ).iloc[0]["total_mcap"]
            or 0
        )
        total_tv = float(
            pd.read_sql_query(
                f"""
                SELECT COALESCE(SUM(o.close * o.volume), 0) AS total_tv
                FROM `{OHLCV_TABLE_DAILY}` o
                INNER JOIN krx_ticker t ON t.종목코드 = o.ticker
                    AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
                WHERE t.종목구분 = '보통주'
                  AND DATE(o.date) = %s
                """,
                con=engine,
                params=(d0,),
            ).iloc[0]["total_tv"]
            or 0
        )

        mcap_raw: dict[str, float] = {}
        for i0 in range(0, len(ut), _WEEK_MCAP_CHUNK):
            chunk = ut[i0 : i0 + _WEEK_MCAP_CHUNK]
            ph = ",".join(["%s"] * len(chunk))
            mrow = pd.read_sql_query(
                f"""
                SELECT 종목코드 AS ticker, 시가총액 AS mcap
                FROM krx_ticker
                WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
                  AND 종목코드 IN ({ph})
                """,
                con=engine,
                params=tuple(chunk),
            )
            for _, r in mrow.iterrows():
                mcap_raw[str(r["ticker"]).zfill(6)] = float(r["mcap"] or 0)

        tv_raw: dict[str, float] = {}
        for i0 in range(0, len(ut), _WEEK_MCAP_CHUNK):
            chunk = ut[i0 : i0 + _WEEK_MCAP_CHUNK]
            ph = ",".join(["%s"] * len(chunk))
            tdf = pd.read_sql_query(
                f"""
                SELECT ticker, SUM(close * volume) AS tv
                FROM `{OHLCV_TABLE_DAILY}`
                WHERE DATE(date) = %s AND ticker IN ({ph})
                GROUP BY ticker
                """,
                con=engine,
                params=tuple([d0] + chunk),
            )
            for _, rr in tdf.iterrows():
                tv_raw[str(rr["ticker"]).zfill(6)] = float(rr["tv"] or 0)

        mcap_map: dict[str, float] = {}
        mcap_share: dict[str, float] = {}
        tv_share: dict[str, float] = {}
        for tk in ut:
            mc = mcap_raw.get(tk)
            mcap_map[tk] = float(mc) if mc is not None and mc > 0 else np.nan
            tv_s = tv_raw.get(tk, 0.0)
            if total_mcap > 0 and mc is not None and mc > 0:
                mcap_share[tk] = mc / total_mcap * 100.0
            else:
                mcap_share[tk] = np.nan
            if total_tv > 0:
                tv_share[tk] = tv_s / total_tv * 100.0
            else:
                tv_share[tk] = np.nan
        return mcap_map, mcap_share, tv_share
    except Exception as e:
        print(f"⚠️ _week_mcap_tv_maps 실패: {type(e).__name__}: {e}")
        return empty


def _week_theme_map(engine, tickers) -> dict[str, str]:
    """테마명 맵 — 실패 시 빈 문자열, 예외 1회 로그."""
    ut = _norm_tickers_z6(tickers)
    fallback = {t: "" for t in ut}
    if engine is None or not ut:
        return fallback
    try:
        return p51._screening_summary_themes(engine, ut)
    except Exception as e:
        print(f"⚠️ _week_theme_map 실패: {type(e).__name__}: {e}")
        return fallback


def export_screening_summary_week_html(
    selected_df,
    indicators_data,
    engine,
    output_path=None,
    open_browser=True,
):
    """주봉 스크리닝 요약 HTML (52 전용). 51 export_screening_summary_html 사용 안 함."""
    today = datetime.date.today()
    folder_name = today.strftime("%Y-%m-%d")
    default_dir = os.path.join(
        "C:\\Users\\hachi\\OneDrive\\01. Trading\\picking\\KRX", folder_name
    )
    os.makedirs(default_dir, exist_ok=True)
    if output_path is None:
        output_path = os.path.join(default_dir, f"screening_summary{OUTPUT_SUFFIX}.html")

    if selected_df is None or len(selected_df) == 0:
        body = "<p>선별된 스크리닝 행이 없습니다.</p>"
    else:
        # 시총·거래대금비중은 최신 일봉(krx_ohlcv) 기준 — 주봉 지표와 분리
        tickers = _norm_tickers_z6(selected_df["ticker"].astype(str))
        theme_map = _week_theme_map(engine, tickers)
        mcap_map, mcap_share_map, tv_share_map = _week_mcap_tv_maps(engine, tickers)

        mcap_hits = sum(1 for k in tickers if np.isfinite(mcap_map.get(k, np.nan)))
        apply_mcap_filter = mcap_hits > 0
        print(
            f"요약표 입력 selected_df {len(selected_df)}행, "
            f"티커 {len(tickers)}종, mcap_map {mcap_hits}건"
        )
        if not apply_mcap_filter and tickers:
            print("시총 조회 실패 → 시총 필터 건너뜀")

        rows = []
        mcap_skip = 0
        for _, r in selected_df.iterrows():
            code = str(r["type"]).strip()
            tk = str(r["ticker"]).strip()
            tkz = tk.zfill(6)
            name = r.get("company", "")
            label = WEEKLY_PATTERN_LABELS.get(code, "")
            scr_name = f"{code} {label}".strip() if label else code
            th = theme_map.get(tkz, theme_map.get(tk, ""))
            mc_raw = mcap_map.get(tkz, mcap_map.get(tk, np.nan))
            if apply_mcap_filter and not (
                np.isfinite(mc_raw) and float(mc_raw) >= p51.DISPLAY_MCAP_MIN
            ):
                mcap_skip += 1
                continue
            vs_raw = tv_share_map.get(tkz, tv_share_map.get(tk, np.nan))
            mcap_disp = f"{mc_raw:,.0f}" if np.isfinite(mc_raw) and mc_raw > 0 else ""
            vshare_disp = f"{vs_raw:.4f}" if np.isfinite(vs_raw) else ""

            idf = p51._resolve_indicators_df(indicators_data, tk)
            if idf is None or len(idf) < 1:
                rows.append(
                    {
                        "스크리닝명": scr_name,
                        "ticker": tk,
                        "종목명": name,
                        "테마명": th,
                        "현재가": "",
                        "주간등락률": "",
                        "4주등락률": "",
                        "sma4위": "",
                        "sma13위": "",
                        "sma26위": "",
                        "sma52위": "",
                        "26주위치": "",
                        "52주위치": "",
                        "band26_q": "",
                        "atr13/종가": "",
                        "거래대금비중": vshare_disp,
                        "시가총액": mcap_disp,
                        "52주신고가": "",
                        "_sort_sn": scr_name,
                        "_sort_tk": tkz,
                        "_sort_nm": str(name) if name else "",
                        "_sort_th": th or "",
                        "_sort_close": None,
                        "_sort_w1": None,
                        "_sort_w4": None,
                        "_sort_s4": "",
                        "_sort_s13": "",
                        "_sort_s26": "",
                        "_sort_s52": "",
                        "_sort_p26": None,
                        "_sort_p52": None,
                        "_sort_bq": None,
                        "_sort_atr": None,
                        "_sort_vshare": float(vs_raw) if np.isfinite(vs_raw) else None,
                        "_sort_mcap": float(mc_raw) if np.isfinite(mc_raw) else None,
                        "_sort_hi52": "",
                    }
                )
                continue

            last = idf.iloc[-1]
            close = float(last["close"]) if pd.notna(last.get("close")) else np.nan
            w1 = _week_pct_change(idf, 1)
            w4 = _week_pct_change(idf, 4)
            sma4 = float(last["sma4"]) if pd.notna(last.get("sma4")) else np.nan
            sma13 = float(last["sma13"]) if pd.notna(last.get("sma13")) else np.nan
            sma26 = float(last["sma26"]) if pd.notna(last.get("sma26")) else np.nan
            sma52 = float(last["sma52"]) if pd.notna(last.get("sma52")) else np.nan
            min26 = float(last["min26"]) if pd.notna(last.get("min26")) else np.nan
            max26 = float(last["max26"]) if pd.notna(last.get("max26")) else np.nan
            min52 = float(last["min52"]) if pd.notna(last.get("min52")) else np.nan
            max52 = float(last["max52"]) if pd.notna(last.get("max52")) else np.nan
            bq = float(last["band26_q"]) if pd.notna(last.get("band26_q")) else np.nan
            atr13 = float(last["atr13"]) if pd.notna(last.get("atr13")) else np.nan
            atr_r = atr13 / close if np.isfinite(atr13) and np.isfinite(close) and close > 0 else np.nan
            p26 = _box_pos(close, min26, max26)
            p52 = _box_pos(close, min52, max52)
            hi52 = "O" if np.isfinite(close) and np.isfinite(max52) and close >= max52 * W_HI52_RATIO else "-"

            rows.append(
                {
                    "스크리닝명": scr_name,
                    "ticker": tk,
                    "종목명": name,
                    "테마명": th,
                    "현재가": f"{close:,.0f}" if np.isfinite(close) else "",
                    "주간등락률": f"{w1:+.2f}" if np.isfinite(w1) else "",
                    "4주등락률": f"{w4:+.2f}" if np.isfinite(w4) else "",
                    "sma4위": _ox_above(close, sma4),
                    "sma13위": _ox_above(close, sma13),
                    "sma26위": _ox_above(close, sma26),
                    "sma52위": _ox_above(close, sma52),
                    "26주위치": f"{p26:.2f}" if np.isfinite(p26) else "",
                    "52주위치": f"{p52:.2f}" if np.isfinite(p52) else "",
                    "band26_q": f"{bq:.2f}" if np.isfinite(bq) else "",
                    "atr13/종가": f"{atr_r:.4f}" if np.isfinite(atr_r) else "",
                    "거래대금비중": vshare_disp,
                    "시가총액": mcap_disp,
                    "52주신고가": hi52,
                    "_sort_sn": scr_name,
                    "_sort_tk": tkz,
                    "_sort_nm": str(name) if name else "",
                    "_sort_th": th or "",
                    "_sort_close": float(close) if np.isfinite(close) else None,
                    "_sort_w1": float(w1) if np.isfinite(w1) else None,
                    "_sort_w4": float(w4) if np.isfinite(w4) else None,
                    "_sort_s4": _ox_above(close, sma4),
                    "_sort_s13": _ox_above(close, sma13),
                    "_sort_s26": _ox_above(close, sma26),
                    "_sort_s52": _ox_above(close, sma52),
                    "_sort_p26": float(p26) if np.isfinite(p26) else None,
                    "_sort_p52": float(p52) if np.isfinite(p52) else None,
                    "_sort_bq": float(bq) if np.isfinite(bq) else None,
                    "_sort_atr": float(atr_r) if np.isfinite(atr_r) else None,
                    "_sort_vshare": float(vs_raw) if np.isfinite(vs_raw) else None,
                    "_sort_mcap": float(mc_raw) if np.isfinite(mc_raw) else None,
                    "_sort_hi52": hi52,
                }
            )

        if mcap_skip:
            print(f"시총<{p51.DISPLAY_MCAP_MIN / 1e8:,.0f}억 제외: {mcap_skip}행")
        if not rows:
            print(f"⚠️ 표시할 행 없음 — mcap_map {mcap_hits}건 확인 (티커 zfill·DB 연결)")

        sum_df = pd.DataFrame(rows)
        n_rows = len(sum_df)
        if apply_mcap_filter:
            mcap_filter_note = (
                f"표시: 시총 {p51.DISPLAY_MCAP_MIN / 1e8:,.0f}억 이상 "
                "(스크리닝 유니버스와 동일)."
            )
        else:
            mcap_filter_note = (
                '<span style="color:#c0392b">시총 조회 실패로 시총 필터 미적용</span>'
            )
        disp_cols = [
            "스크리닝명", "ticker", "종목명", "테마명", "현재가",
            "주간등락률", "4주등락률",
            "sma4위", "sma13위", "sma26위", "sma52위",
            "26주위치", "52주위치", "band26_q", "atr13/종가",
            "거래대금비중", "시가총액", "52주신고가",
        ]
        sort_cols = [
            "_sort_sn", "_sort_tk", "_sort_nm", "_sort_th", "_sort_close",
            "_sort_w1", "_sort_w4",
            "_sort_s4", "_sort_s13", "_sort_s26", "_sort_s52",
            "_sort_p26", "_sort_p52", "_sort_bq", "_sort_atr",
            "_sort_vshare", "_sort_mcap", "_sort_hi52",
        ]
        sort_types = [
            "str", "str", "str", "str", "num",
            "num", "num",
            "str", "str", "str", "str",
            "num", "num", "num", "num",
            "num", "num", "str",
        ]
        th_labels = [
            "스크리닝명", "ticker", "종목명", "테마명", "현재가",
            "주간등락률(%)", "4주등락률(%)",
            "sma4위", "sma13위", "sma26위", "sma52위",
            "26주위치", "52주위치", "band26_q", "atr13/종가",
            "거래대금비중(%)", "시가총액", "52주신고가",
        ]
        th_titles = {
            5: "전주 종가 대비 등락률 (주봉)",
            6: "4주 전 종가 대비 등락률 (주봉)",
            7: "종가 > 4주 SMA (주봉)",
            8: "종가 > 13주 SMA (주봉)",
            9: "종가 > 26주 SMA (주봉)",
            10: "종가 > 52주 SMA (주봉)",
            11: "(종가−min26)/(max26−min26), 26주 박스",
            12: "(종가−min52)/(max52−min52), 52주 박스",
            13: "볼린저 밴드폭 q (window=13주, lookback=26주)",
            14: "atr13/종가 (13주 Wilder ATR)",
            15: "전체 보통주 당일 거래대금 대비 비중(%)",
            16: "시가총액 (원)",
            17: f"종가 ≥ 52주 고가×{W_HI52_RATIO}",
        }

        def _sort_attr(stype, raw):
            if stype == "num":
                if raw is None or (isinstance(raw, float) and not np.isfinite(raw)):
                    return ' data-sort-type="num" data-sort=""'
                return f' data-sort-type="num" data-sort="{float(raw):.12g}"'
            s = "" if raw is None else str(raw)
            esc = html_module.escape(s, quote=True)
            return f' data-sort-type="str" data-sort="{esc}"'

        ths = "".join(
            f'<th class="sortable" data-col="{i}" title="{html_module.escape(th_titles.get(i, "클릭: 정렬"))}">'
            f"{html_module.escape(th_labels[i])}</th>"
            for i in range(len(th_labels))
        )
        trs = []
        for _, rr in sum_df.iterrows():
            tds = []
            for i, dc in enumerate(disp_cols):
                st = sort_types[i]
                sk = rr[sort_cols[i]]
                disp = rr[dc]
                cls = ""
                if dc in ("주간등락률", "4주등락률") and sk is not None and np.isfinite(sk):
                    cls = ' class="up"' if float(sk) > 0 else (' class="dn"' if float(sk) < 0 else "")
                tds.append(
                    f"<td{_sort_attr(st, sk)}{cls}>{html_module.escape(str(disp))}</td>"
                )
            trs.append("<tr>" + "".join(tds) + "</tr>")

        sort_script = r"""
<script>
(function () {
  var table = document.getElementById("summaryTable");
  if (!table) return;
  var tbody = table.querySelector("tbody");
  var headers = table.querySelectorAll("thead th.sortable");
  var sortState = { col: -1, asc: true };
  function cmpCell(ta, tb) {
    var t = ta.getAttribute("data-sort-type") || "str";
    var va = ta.getAttribute("data-sort");
    var vb = tb.getAttribute("data-sort");
    if (t === "num") {
      var na = va === "" || va === null ? NaN : parseFloat(va);
      var nb = vb === "" || vb === null ? NaN : parseFloat(vb);
      if (isNaN(na) && isNaN(nb)) return 0;
      if (isNaN(na)) return 1;
      if (isNaN(nb)) return -1;
      return na < nb ? -1 : na > nb ? 1 : 0;
    }
    var sa = va == null ? "" : String(va);
    var sb = vb == null ? "" : String(vb);
    return sa.localeCompare(sb, "ko");
  }
  headers.forEach(function (th) {
    th.addEventListener("click", function () {
      var col = parseInt(th.getAttribute("data-col"), 10);
      if (sortState.col === col) sortState.asc = !sortState.asc;
      else { sortState.col = col; sortState.asc = true; }
      var rowsArr = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
      rowsArr.sort(function (a, b) {
        var c = cmpCell(a.cells[col], b.cells[col]);
        return sortState.asc ? c : -c;
      });
      rowsArr.forEach(function (tr) { tbody.appendChild(tr); });
      headers.forEach(function (h) {
        h.classList.remove("sort-asc", "sort-desc");
        if (parseInt(h.getAttribute("data-col"), 10) === sortState.col)
          h.classList.add(sortState.asc ? "sort-asc" : "sort-desc");
      });
    });
  });
})();
</script>
"""
        body = f"""
<h2 style="font-family:Segoe UI,Malgun Gothic,sans-serif">주봉 스크리닝 요약 ({html_module.escape(folder_name)})</h2>
<p style="font-family:Segoe UI,Malgun Gothic,sans-serif">행 수: {n_rows}. {mcap_filter_note}</p>
<p style="font-family:Segoe UI,Malgun Gothic,sans-serif;color:#555">
<b>방법:</b> 주봉(krx_ohlcv_week) 기준, 창 4/13/26/52주.
등락률·SMA·박스위치·band26_q·atr13 모두 주봉 봉수 기준입니다.</p>
<table id="summaryTable" class="s">
<thead><tr>{ths}</tr></thead>
<tbody>{"".join(trs)}</tbody>
</table>
{sort_script}
"""
    page = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="utf-8"/>
<title>주봉 스크리닝 요약</title>
<style>
body {{ margin:16px; font-family:Segoe UI,Malgun Gothic,sans-serif; }}
table.s {{ border-collapse:collapse; width:100%; table-layout:fixed; font-size:11px; }}
table.s th, table.s td {{ border:1px solid #ccc; padding:3px 4px; word-break:break-word; overflow-wrap:anywhere; }}
table.s th {{ background:#f0f4f8; text-align:center; white-space:normal; line-height:1.15; }}
table.s thead th {{ position:sticky; top:0; z-index:2; box-shadow:inset 0 -1px 0 #ccc; }}
table.s th.sortable {{ cursor:pointer; user-select:none; }}
table.s th.sortable:hover {{ background:#dde8f2; }}
table.s th.sort-asc::after {{ content:" \\25B2"; font-size:0.65em; opacity:0.85; }}
table.s th.sort-desc::after {{ content:" \\25BC"; font-size:0.65em; opacity:0.85; }}
table.s td:nth-child(5), table.s td:nth-child(6), table.s td:nth-child(7),
table.s td:nth-child(12), table.s td:nth-child(13), table.s td:nth-child(14),
table.s td:nth-child(15), table.s td:nth-child(16), table.s td:nth-child(17) {{ text-align:right; }}
table.s td:nth-child(8), table.s td:nth-child(9), table.s td:nth-child(10),
table.s td:nth-child(11), table.s td:nth-child(18) {{ text-align:center; }}
table.s td.up {{ color:#c0392b; font-weight:600; }}
table.s td.dn {{ color:#2471a3; font-weight:600; }}
table.s tbody tr:nth-child(even) {{ background:#fafafa; }}
</style></head><body>
{body}
</body></html>"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(page)
    abs_path = os.path.abspath(output_path).replace("\\", "/")
    print(f"주봉 스크리닝 요약 HTML 저장: {output_path}")
    if open_browser:
        webbrowser.open(f"file:///{abs_path}")
    return output_path


def run_main_weekly(do_summary=True, do_charts=False):
    """주봉 자체 파이프라인: OHLCV(week) → 지표(52) → 스크리닝(52) → (선택) 요약/차트(51)."""
    global engine, ticker_list, ohlcv_data, indicators_data, volume_data
    global rs_df, selected_df, selected_stocks, selected_stock_list, result, debug_counts
    global money, risk

    money = p51.money
    risk = p51.risk

    engine = create_engine(p51.db_url())
    ticker_list = _load_ticker_list(engine)
    _inject_p51_globals(engine=engine, ticker_list=ticker_list)

    print("=" * 80)
    print("📅 KRX 주봉 Picking (52 자체 지표·스크리닝)")
    print("=" * 80)

    t0 = time.time()
    ohlcv_data = load_ohlcv_weekly_parallel(
        ticker_list, engine, max_workers=MAX_WORKERS_DATA_LOAD
    )
    ohlcv_data = p51.filter_ohlcv_zero_latest(ohlcv_data)

    p51._INVESTOR_CACHE = None
    p51._FOREIGN_CACHE = None
    bulk = list(ohlcv_data.keys())
    p51._INVESTOR_CACHE = p51._bulk_load_investor_trading(engine, bulk)
    p51._FOREIGN_CACHE = p51._bulk_load_foreign_holding(engine, bulk)

    indicators_data = calculate_indicators_weekly_parallel(
        ohlcv_data, max_workers=MAX_WORKERS_INDICATORS
    )
    volume_data = p51.calculate_volume_band_parallel(
        ohlcv_data, max_workers=MAX_WORKERS_VOLUME
    )

    rs_df = _load_rs_empty()
    if W_USE_RS:
        print("RS 조건 활성 (W_USE_RS=True)")
    else:
        print("RS 스킵 (W_USE_RS=False)")

    print("\n" + "=" * 80)
    print("🔍 주봉 스크리닝")
    print("=" * 80)
    print("스크리너: screen_weekly(52 자체 패턴 w11~w51)")

    scr = screen_weekly(
        indicators_data,
        rs_df=rs_df,
        volume_data=volume_data,
        ticker_list=ticker_list,
        audit_ticker=p51.audit_ticker,
    )
    result = scr["result"]
    debug_counts = scr["debug_counts"]
    selected_stocks = scr["selected_stocks"]
    selected_df = scr["selected_df"]
    selected_stock_list = scr["selected_stock_list"]

    print("\n" + "=" * 80)
    print("📊 스크리닝 디버깅 정보")
    print("=" * 80)
    print(f"📈 총 지표 데이터: {debug_counts['total_indicators']}개")
    print(f"✅ 기본 필터 통과: {debug_counts['passed_basic_filter']}개")
    print(f"✅ ATR 필터 통과: {debug_counts['passed_atr_filter']}개")
    print(f"✅ 패턴 매칭 종목: {debug_counts.get('selected_by_pattern', 0)}개")
    print(f"✅ 선정 행 수: {len(selected_stocks)}개")
    for code in WEEKLY_PATTERN_CODES:
        print(f"   · {code}: {debug_counts.get(code, 0)}건")
    print("=" * 80)

    if len(selected_df) == 0:
        print("⚠️ 선별된 종목이 없습니다.")

    if do_summary and len(selected_df) > 0:
        export_screening_summary_week_html(selected_df, indicators_data, engine)
    if do_charts and selected_stock_list:
        global _PLOT_TARGET_LOG_DONE, _WEEK_CHART_MISSING_LOGGED, _CHART_SAVE_LOG_DONE, _CHART_XAXIS_LOG_DONE
        _PLOT_TARGET_LOG_DONE = False
        _CHART_SAVE_LOG_DONE = False
        _CHART_XAXIS_LOG_DONE = False
        _WEEK_CHART_MISSING_LOGGED.clear()
        out_dir = _week_chart_output_dir()
        code = (
            selected_stock_list[0]
            if selected_stock_list and not isinstance(selected_stock_list[0], pd.DataFrame)
            else "w51"
        )
        n_chart = 0
        for stock in selected_stock_list:
            if not isinstance(stock, pd.DataFrame):
                code = str(stock)
                continue
            tk = _chart_ticker(stock)
            if not tk:
                continue
            name = _chart_name(stock, tk)
            out_path = os.path.join(out_dir, f"{tk}_week_{code}.html")
            if plot_week_chart(
                stock, tk, name, code, out_path,
                weeks=CHART_WEEKS, open_browser=False,
            ):
                n_chart += 1
        print(f"차트 {code}: {n_chart}종 생성 ({CHART_WEEKS}주)")

    elapsed = time.time() - t0
    print(f"⏱️ 주봉 파이프라인 완료: {elapsed:.1f}s")

    ctx = {
        "engine": engine,
        "ticker_list": ticker_list,
        "ohlcv_data": ohlcv_data,
        "indicators_data": indicators_data,
        "volume_data": volume_data,
        "selected_df": selected_df,
        "selected_stocks": selected_stocks,
        "selected_stock_list": selected_stock_list,
        "result": result,
        "debug_counts": debug_counts,
        "rs_df": rs_df,
        "money": money,
        "risk": risk,
    }
    return ctx


def _require_vars(*names: str) -> bool:
    g = globals()
    missing = [n for n in names if n not in g or g[n] is None]
    if missing:
        print(f"⚠️ 필수 변수 없음: {missing}\n   → 0번 셀 후 1번 셀을 먼저 실행하세요.")
        return False
    return True


def _publish_ctx(ctx: dict) -> dict:
    globals().update(ctx)
    return ctx


def cell1_pipeline():
    ctx = run_main_weekly(do_summary=False, do_charts=False)
    _publish_ctx(ctx)
    n = len(ctx["selected_df"]) if ctx.get("selected_df") is not None else 0
    print(f"✅ 파이프라인 완료 — selected_df {n}행. 요약=셀2, 차트=셀3.")
    return ctx


def cell2_summary():
    if not _require_vars("selected_df", "indicators_data", "engine"):
        return None
    _inject_p51_globals()
    return export_screening_summary_week_html(selected_df, indicators_data, engine)


def _chart_ticker(stock_df) -> str | None:
    try:
        return str(stock_df.iloc[-1]["ticker"]).strip().zfill(6)
    except Exception:
        return None


def _chart_name(stock_df, ticker: str) -> str:
    try:
        return str(stock_df.iloc[-1]["name"]).strip()
    except Exception:
        return ticker


def _normalize_chart_patterns(pattern) -> list[str] | None:
    """pattern: None | str | list → 패턴 코드 리스트. 잘못된 코드면 None."""
    avail = ", ".join(WEEKLY_PATTERN_CODES)
    if pattern is None:
        return list(WEEKLY_PATTERN_CODES)
    if isinstance(pattern, str):
        codes = [pattern.strip()]
    else:
        codes = [str(p).strip() for p in pattern]
    invalid = [c for c in codes if c not in WEEKLY_PATTERN_CODES]
    if invalid:
        print(f"⚠️ 패턴 {invalid[0]} 없음 — 사용 가능: {avail}")
        return None
    return codes


def cell3_charts(pattern=None, max_charts=None, dedupe=False, open_browser=True):
    global _PLOT_TARGET_LOG_DONE, _WEEK_CHART_MISSING_LOGGED, _CHART_SAVE_LOG_DONE, _CHART_XAXIS_LOG_DONE
    if not _require_vars("result"):
        return None
    _PLOT_TARGET_LOG_DONE = False
    _CHART_SAVE_LOG_DONE = False
    _CHART_XAXIS_LOG_DONE = False
    _WEEK_CHART_MISSING_LOGGED.clear()

    codes = _normalize_chart_patterns(pattern)
    if codes is None:
        return None

    if pattern is None:
        codes = [c for c in codes if result.get(c)]

    if not codes:
        print("⚠️ 차트할 패턴/종목 없음")
        return None

    out_dir = _week_chart_output_dir()
    seen: set[str] = set()
    total = 0
    opened = 0

    for code in codes:
        stocks = list(result.get(code, []))
        if not stocks:
            print(f"차트 {code}: 0종 (스킵)")
            continue

        n_code = 0
        for stock in stocks:
            tk = _chart_ticker(stock)
            if not tk:
                continue
            if dedupe and tk in seen:
                continue
            if max_charts is not None and n_code >= max_charts:
                break

            name = _chart_name(stock, tk)
            out_path = os.path.join(out_dir, f"{tk}_week_{code}.html")
            do_open = (
                open_browser
                and CHART_OPEN_MAX > 0
                and opened < CHART_OPEN_MAX
            )
            if plot_week_chart(
                stock, tk, name, code, out_path,
                weeks=CHART_WEEKS, open_browser=do_open,
            ):
                if do_open:
                    opened += 1
                seen.add(tk)
                n_code += 1
                total += 1

        if n_code:
            print(f"차트 {code}: {n_code}종 생성 ({CHART_WEEKS}주)")
        else:
            print(f"차트 {code}: 0종 생성 (dedupe 또는 max_charts)")

    print(f"합계 {total}장")
    print(f"차트 저장 폴더: {os.path.abspath(out_dir)}")
    return total


def run_main(do_summary=True, do_charts=False):
    ctx = run_main_weekly(do_summary=do_summary, do_charts=do_charts)
    _publish_ctx(ctx)
    return ctx


# %% 0) 51 로드 + 주봉 모듈 초기화
print("=" * 80)
print("📅 52 주봉 Picking — 지표·스크리닝 자체 산출")
print(f"  OHLCV_TABLE = {OHLCV_TABLE_WEEK}")
print(f"  OUTPUT_SUFFIX = {OUTPUT_SUFFIX!r}")
print(f"  DROP_INCOMPLETE_WEEK = {DROP_INCOMPLETE_WEEK}")
print("=" * 80)


# %% 1) 데이터 로드 → 주봉 지표 → 스크리닝
if __name__ == "__main__":
    cell1_pipeline()


# %% 2) 스크리닝 요약 HTML
if __name__ == "__main__":
    cell2_summary()
    print("F5 완료 (0→1→2). 차트는 셀 3 Run Cell.")
    raise SystemExit(0)


# %% 3) 패턴별 차트 — pattern 인자로 원하는 패턴만
# 사용 예:
#   cell3_charts()                  → result 에 있는 전체 패턴
#   cell3_charts("w31")             → w31 만
#   cell3_charts(["w11", "w41"])    → 두 패턴만
#   cell3_charts("w11", max_charts=10)
#   cell3_charts("w11", max_charts=5, dedupe=True)
if __name__ == "__main__":
    cell3_charts("w11", max_charts=2)


# %% 4) (선택) 단일 종목 차트
# ticker = "005930"
# idf = indicators_data[ticker]
# out = os.path.join(_week_chart_output_dir(), f"{ticker}_week_w11.html")
# plot_week_chart(idf, ticker, "삼성전자", "w11", out)
