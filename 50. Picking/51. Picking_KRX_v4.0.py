# -*- coding: utf-8 -*-
"""
Created on Tue Apr 30 14:00:16 2024
KRX 종목 스크리닝·차트 (완전 단일 파일)

@author: hachi
"""


import os
import sys
from pathlib import Path

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
from env_config import load_project_env, require_env, db_url, db_connect_kwargs
load_project_env()
from exclusions import drop_excluded
from indicators_core import (
    atr_wilder,
    bollinger_band_width,
    bollinger_band_width_q,
    energy_ratio,
    investor_osc_frame,
    rs_avg,
    talent_up_share,
    INVESTOR_FOREIGN_CODES,
    INVESTOR_INST_CODES,
    INVESTOR_OSC_CUM_DAYS,
)


import os
import pymysql
import pandas as pd
import numpy as np
import talib
from tqdm import tqdm
from sqlalchemy import create_engine
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import webbrowser
import time
import datetime
import html as html_module
import traceback
from collections import defaultdict

# 병렬 처리를 위한 추가 import
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


##########################################################################################################################################
money = 100000000
risk = 0.005

# 스크리닝·요약표 표시 유니버스 시총 하한 (naverPub / 21 리포트 표시와 동일)
DISPLAY_MCAP_MIN = 300_000_000_000  # 3,000억
SECTOR_CONCENTRATION_WARN = 0.15  # 대분류 1위 비중 경고 임계

# 성능 최적화 설정
MAX_WORKERS_DATA_LOAD = 10  # 데이터 로딩용 워커 수
MAX_WORKERS_INDICATORS = 8  # 지표 계산용 워커 수
MAX_WORKERS_VOLUME = 6  # 매물대 계산용 워커 수

# 차트 생성 설정
SAVE_JPEG = False  # True로 설정하면 JPEG 파일도 생성 (기본값: False, HTML만 생성)

# --- 지표·투자자 OSC (tradingKIS_test.py와 동일, 단일 파일 내장) ---
CHART_PERIOD_DAYS = 252

# --- 일봉/주봉 공용 설정 (주봉 래퍼가 덮어씀) ---
OHLCV_TABLE = "krx_ohlcv"
USE_ADJ_PRICE = True   # 일봉(krx_ohlcv) 조회 시 *_adj 컬럼 사용


def _ohlcv_cols_sql() -> str:
    """일봉 테이블일 때만 수정주가 별칭. 주봉(krx_ohlcv_week)은 이미 조정가라 원본 그대로."""
    if USE_ADJ_PRICE and OHLCV_TABLE == "krx_ohlcv":
        return (
            "date, "
            "COALESCE(open_adj, open)     AS open, "
            "COALESCE(high_adj, high)     AS high, "
            "COALESCE(low_adj, low)       AS low, "
            "COALESCE(close_adj, close)   AS close, "
            "COALESCE(volume_adj, volume) AS volume"
        )
    return "date, open, high, low, close, volume"


# 이름=주봉 의미(1·2·4·13·26·52주), 값=일봉 기본 거래일
WIN_1, WIN_2, WIN_4, WIN_13, WIN_26, WIN_52 = 5, 10, 20, 50, 120, 200
RS_PERIODS = (10, 20, 50, 120, 200)
USE_RS = True          # 주봉판에서 False 로 끄기 위한 플래그
OUTPUT_SUFFIX = ""     # 주봉판이 "_week" 로 덮어씀
SCREEN_FN = None       # 주봉 래퍼가 screen_weekly 등으로 주입

# run_main() 중간 산출물 (대화식·래퍼에서 p51.CONTEXT["selected_df"] 등으로 접근)
CONTEXT = {}

# CSI (Pine Script: close loc length)
_CSI_LENGTH = WIN_4

# --- 투자자 OSC (krx_investor_trade_krx net_val, indicators_core 정본, 누적 INVESTOR_OSC_CUM_DAYS일) ---
_INVESTOR_OSC_COLS = ("inst_net_osc", "frgn_net_osc")
_INVESTOR_OSC_REQUIRED = _INVESTOR_OSC_COLS
# 기관OSC = 연기금+투신+사모 (7050 합계 아님). 외국인OSC = 9000.
_INVESTOR_OSC_GROUPS = {
    "inst_net_osc": INVESTOR_INST_CODES,
    "frgn_net_osc": INVESTOR_FOREIGN_CODES,
    "pension_net_osc": ("6000",),
    "trust_net_osc": ("3000",),
    "private_net_osc": ("3100",),
}

# 지표 계산용 벌크 캐시 (None=미로드 → 단건 쿼리 폴백; dict=읽기 전용)
_INVESTOR_CACHE = None
_FOREIGN_CACHE = None
_BULK_TICKER_CHUNK = 500


def _csi_grade(last):
    for col in ("csi", "csi_fast", "csi_slow"):
        if col not in last.index:
            return "-"
    try:
        csi = float(last["csi"])
        fast = float(last["csi_fast"])
        slow = float(last["csi_slow"])
        if any(np.isnan(v) for v in (csi, fast, slow)):
            return "-"
        if csi > fast and csi > slow:
            return "◎"
        if csi < fast and csi < slow:
            return "●"
        return "○"
    except (TypeError, ValueError):
        return "-"


def _osc_two_day_rise(series):
    """연속 두 구간 상승=매집, 연속 두 구간 하락=분산."""
    if series is None or len(series) < 3:
        return "-"
    try:
        v0 = float(series.iloc[-1])
        v1 = float(series.iloc[-2])
        v2 = float(series.iloc[-3])
        if any(np.isnan(v) for v in (v0, v1, v2)):
            return "-"
        if v0 > v1 and v1 > v2:
            return "매집"
        if v0 < v1 and v1 < v2:
            return "분산"
        return "-"
    except Exception:
        return "-"


def _normalize_ticker(ticker=None, ohlcv_df=None):
    if ticker is not None and str(ticker).strip():
        return str(ticker).strip().zfill(6)
    if ohlcv_df is not None and "ticker" in ohlcv_df.columns:
        v = ohlcv_df["ticker"].dropna()
        if not v.empty:
            return str(v.iloc[-1]).strip().zfill(6)
    return None


def _unique_tickers_zfill(tickers):
    out = []
    seen = set()
    for t in tickers or []:
        if t is None:
            continue
        s = str(t).strip().zfill(6)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _bulk_load_investor_trading(engine, tickers):
    """전 종목 투자자 매매를 청크 조회 → dict[ticker, DataFrame(date, invst_tp_cd, net_val)]."""
    result = {}
    ut = _unique_tickers_zfill(tickers)
    if engine is None or not ut:
        return result
    frames = []
    try:
        for i0 in range(0, len(ut), _BULK_TICKER_CHUNK):
            chunk = ut[i0 : i0 + _BULK_TICKER_CHUNK]
            ph = ",".join(["%s"] * len(chunk))
            q = f"""
                SELECT `date`, ticker, invst_tp_cd, net_val
                FROM krx_investor_trade_krx
                WHERE ticker IN ({ph})
                  AND invst_tp_cd IN ('6000','3000','3100','9000')
                ORDER BY ticker, `date`
            """
            part = pd.read_sql(q, engine, params=tuple(chunk))
            if part is not None and not part.empty:
                frames.append(part)
    except Exception:
        return result
    if not frames:
        return result
    df = pd.concat(frames, ignore_index=True)
    df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["invst_tp_cd"] = df["invst_tp_cd"].astype(str).str.strip()
    df["net_val"] = pd.to_numeric(df["net_val"], errors="coerce")
    df = df.dropna(subset=["date"])
    for tk, g in df.groupby("ticker", sort=False):
        result[str(tk)] = g[["date", "invst_tp_cd", "net_val"]].reset_index(drop=True)
    return result


def _bulk_load_foreign_holding(engine, tickers):
    """전 종목 외국인 지분율을 청크 조회 → dict[ticker, DataFrame(date, frgn_ratio)]."""
    result = {}
    ut = _unique_tickers_zfill(tickers)
    if engine is None or not ut:
        return result
    frames = []
    try:
        for i0 in range(0, len(ut), _BULK_TICKER_CHUNK):
            chunk = ut[i0 : i0 + _BULK_TICKER_CHUNK]
            ph = ",".join(["%s"] * len(chunk))
            q = f"""
                SELECT `date`, ticker, frgn_ratio
                FROM krx_foreign_holding
                WHERE ticker IN ({ph})
                ORDER BY ticker, `date`
            """
            part = pd.read_sql(q, engine, params=tuple(chunk))
            if part is not None and not part.empty:
                frames.append(part)
    except Exception:
        return result
    if not frames:
        return result
    df = pd.concat(frames, ignore_index=True)
    df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["frgn_ratio"] = pd.to_numeric(df["frgn_ratio"], errors="coerce")
    df = df.dropna(subset=["date"])
    for tk, g in df.groupby("ticker", sort=False):
        result[str(tk)] = g[["date", "frgn_ratio"]].reset_index(drop=True)
    return result


def _load_investor_trading(engine, ticker):
    """krx_investor_trade_krx 롱 테이블에서 금액(net_val) 시계열을 로드."""
    if engine is None or ticker is None:
        return pd.DataFrame()
    t = str(ticker).strip().zfill(6)
    if _INVESTOR_CACHE is not None:
        df = _INVESTOR_CACHE.get(t)
        return df.copy() if df is not None and not df.empty else pd.DataFrame()
    query = """
        SELECT `date`, invst_tp_cd, net_val
        FROM krx_investor_trade_krx
        WHERE ticker = %(ticker)s AND invst_tp_cd IN ('6000','3000','3100','9000')
        ORDER BY `date`
    """
    try:
        df = pd.read_sql(query, engine, params={"ticker": t})
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["invst_tp_cd"] = df["invst_tp_cd"].astype(str).str.strip()
    df["net_val"] = pd.to_numeric(df["net_val"], errors="coerce")
    return df.dropna(subset=["date"])


def _load_foreign_holding(engine, ticker):
    """krx_foreign_holding 에서 date, frgn_ratio(%) 로드."""
    if engine is None or ticker is None:
        return pd.DataFrame()
    t = str(ticker).strip().zfill(6)
    if _FOREIGN_CACHE is not None:
        df = _FOREIGN_CACHE.get(t)
        return df.copy() if df is not None and not df.empty else pd.DataFrame()
    query = """
        SELECT `date`, frgn_ratio
        FROM krx_foreign_holding
        WHERE ticker = %(ticker)s
        ORDER BY `date`
    """
    try:
        df = pd.read_sql(query, engine, params={"ticker": t})
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["frgn_ratio"] = pd.to_numeric(df["frgn_ratio"], errors="coerce")
    return df.dropna(subset=["date"])


def _merge_frgn_ratio_on_date(left_df, fh_df):
    """left_df(date 컬럼)에 frgn_ratio left join. 실패 시 NaN 컬럼만 추가."""
    out = left_df.copy()
    if "frgn_ratio" in out.columns:
        out = out.drop(columns=["frgn_ratio"])
    if fh_df is None or fh_df.empty:
        out["frgn_ratio"] = np.nan
        return out
    fh = fh_df.copy()
    fh["date"] = pd.to_datetime(fh["date"], errors="coerce").dt.normalize()
    fh = fh.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    return out.merge(fh[["date", "frgn_ratio"]], on="date", how="left")


def _frgn_ratio_axis_range(series):
    """min/max ± 5% 여유. 유효값 없으면 None."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    lo, hi = float(s.min()), float(s.max())
    span = hi - lo
    pad = span * 0.05 if span > 0 else max(abs(hi) * 0.05, 0.5)
    return [lo - pad, hi + pad]


def _attach_investor_osc(ohlcv_df, engine, ticker=None, investor_df=None):
    """investor_osc_frame 결과(inst_net_osc, frgn_net_osc 등)를 날짜 기준 join."""
    if ohlcv_df is None or ohlcv_df.empty:
        return ohlcv_df
    t = _normalize_ticker(ticker, ohlcv_df)
    if investor_df is None:
        investor_df = _load_investor_trading(engine, t)
    d = ohlcv_df.copy()
    if "date" in d.columns:
        d = d.set_index("date")
    d.index = pd.to_datetime(d.index, errors="coerce").normalize()
    d = d[~d.index.isna()]
    osc_cols = tuple(_INVESTOR_OSC_GROUPS.keys())
    fh = _load_foreign_holding(engine, t)
    if investor_df is None or investor_df.empty:
        for col in osc_cols:
            d[col] = np.nan
        left = d.reset_index()
        if left.columns[0] != "date":
            left = left.rename(columns={left.columns[0]: "date"})
        return _merge_frgn_ratio_on_date(left, fh).set_index("date")
    osc = investor_osc_frame(investor_df, groups=_INVESTOR_OSC_GROUPS)
    if osc is None or osc.empty:
        for col in osc_cols:
            d[col] = np.nan
        left = d.reset_index()
        if left.columns[0] != "date":
            left = left.rename(columns={left.columns[0]: "date"})
        return _merge_frgn_ratio_on_date(left, fh).set_index("date")
    osc = osc.copy()
    osc.index = pd.to_datetime(osc.index, errors="coerce").normalize()
    osc = osc[~osc.index.isna()]
    left = d.reset_index()
    if left.columns[0] != "date":
        left = left.rename(columns={left.columns[0]: "date"})
    left["date"] = pd.to_datetime(left["date"], errors="coerce").dt.normalize()
    osc_reset = osc.reset_index()
    if osc_reset.columns[0] != "date":
        osc_reset = osc_reset.rename(columns={osc_reset.columns[0]: "date"})
    osc_reset["date"] = pd.to_datetime(osc_reset["date"], errors="coerce").dt.normalize()
    for col in osc_cols:
        if col in left.columns:
            left = left.drop(columns=[col])
    merged = left.merge(osc_reset, on="date", how="left")
    merged = _merge_frgn_ratio_on_date(merged, fh)
    return merged.set_index("date")


def _has_investor_osc_data(df, min_valid=5):
    if df is None or df.empty:
        return False
    for col in _INVESTOR_OSC_REQUIRED:
        if col not in df.columns:
            return False
        if pd.Series(df[col]).notna().sum() < min_valid:
            return False
    return True


def _investor_osc_summary(indicators_data, min_valid=5):
    rows = []
    for t, df in indicators_data.items():
        n = int(df["inst_net_osc"].notna().sum()) if "inst_net_osc" in df.columns else 0
        rows.append({"ticker": t, "ok": _has_investor_osc_data(df, min_valid), "inst_net_osc_days": n})
    return pd.DataFrame(rows)


# --- TA-Lib 기간 가드 (timeperiod < 2 는 TA-Lib 오류) ---
def _as_float_array(x):
    return np.asarray(x, dtype=float)


def _nan_like(x):
    arr = _as_float_array(x)
    return np.full(len(arr), np.nan, dtype=float)


def _like(src, arr):
    """입력 src 가 pandas Series 면 그 인덱스를 가진 Series 로 되돌린다."""
    if isinstance(src, pd.Series):
        return pd.Series(np.asarray(arr), index=src.index, dtype="float64")
    return arr


def _sma_safe(x, n):
    n = int(n)
    arr = _as_float_array(x)
    if n < 2:
        return _like(x, arr.copy())
    return _like(x, talib.SMA(arr, n))


def _ema_safe(x, n):
    n = int(n)
    arr = _as_float_array(x)
    if n < 2:
        return _like(x, _nan_like(x))
    return _like(x, talib.EMA(arr, n))


def _max_safe(x, n):
    n = int(n)
    arr = _as_float_array(x)
    if n < 2:
        return _like(x, _nan_like(x))
    return _like(x, talib.MAX(arr, n))


def _min_safe(x, n):
    n = int(n)
    arr = _as_float_array(x)
    if n < 2:
        return _like(x, _nan_like(x))
    return _like(x, talib.MIN(arr, n))


def _midprice_safe(high, low, n):
    n = int(n)
    h = _as_float_array(high)
    l = _as_float_array(low)
    if n < 2:
        return _like(high, _nan_like(h))
    return _like(high, talib.MIDPRICE(h, l, n))


def _sum_safe(x, n):
    n = int(n)
    arr = _as_float_array(x)
    if n < 2:
        return _like(x, _nan_like(x))
    return _like(x, talib.SUM(arr, n))


def _bbands_safe(x, n, nbdevup=2, nbdevdn=2, matype=0):
    n = int(n)
    arr = _as_float_array(x)
    nan = _nan_like(x)
    if n < 2:
        return _like(x, nan), _like(x, nan), _like(x, nan)
    upper, middle, lower = talib.BBANDS(
        arr, n, nbdevup=nbdevup, nbdevdn=nbdevdn, matype=matype
    )
    return _like(x, upper), _like(x, middle), _like(x, lower)


def _minmax_safe(x, n):
    n = int(n)
    arr = _as_float_array(x)
    nan = _nan_like(x)
    if n < 2:
        return _like(x, nan), _like(x, nan)
    mn, mx = talib.MINMAX(arr, n)
    return _like(x, mn), _like(x, mx)


def _atr_wilder_safe(high, low, close, n):
    n = int(n)
    if n < 2:
        return _like(close, _nan_like(close))
    return atr_wilder(high, low, close, n)


_INDICATOR_FAIL_LOCK = threading.Lock()
_INDICATOR_FAIL_SHOWN = 0
_INDICATOR_FAIL_TOTAL = 0
_INDICATOR_FAIL_MAX_DETAIL = 5


def _reset_indicator_fail_log():
    global _INDICATOR_FAIL_SHOWN, _INDICATOR_FAIL_TOTAL
    with _INDICATOR_FAIL_LOCK:
        _INDICATOR_FAIL_SHOWN = 0
        _INDICATOR_FAIL_TOTAL = 0


def _log_indicator_fail(ticker, err):
    global _INDICATOR_FAIL_SHOWN, _INDICATOR_FAIL_TOTAL
    with _INDICATOR_FAIL_LOCK:
        _INDICATOR_FAIL_TOTAL += 1
        if _INDICATOR_FAIL_SHOWN < _INDICATOR_FAIL_MAX_DETAIL:
            print(f"티커 {ticker} 지표 계산 실패: {err}")
            _INDICATOR_FAIL_SHOWN += 1


def _flush_indicator_fail_log():
    global _INDICATOR_FAIL_SHOWN, _INDICATOR_FAIL_TOTAL
    with _INDICATOR_FAIL_LOCK:
        extra = _INDICATOR_FAIL_TOTAL - _INDICATOR_FAIL_SHOWN
        if extra > 0:
            print(f"… 지표 계산 실패 외 {extra}건")
        _INDICATOR_FAIL_SHOWN = 0
        _INDICATOR_FAIL_TOTAL = 0


# --- 기술적 지표 ---

def _concat_cols(d, df, tag):
    if not df.index.equals(d.index):
        raise ValueError(
            f"[get_indicators] {tag} 인덱스 불일치: "
            f"d={type(d.index).__name__}({len(d)}) vs {tag}={type(df.index).__name__}({len(df)})"
        )
    return pd.concat([d, df], axis=1)


def get_indicators(d, tradeHist=None):
        
        d = d.reset_index()
        d = d.set_index('date')
    
        d['typical'] = talib.TYPPRICE(d.high, d.low, d.close)
        
        # d.loc[d['open'] >= d['close'], 'upper'] = d['open']
        # d.loc[d['open'] <= d['close'], 'lower'] = d['open']
        # d.loc[d['open'] <= d['close'], 'upper'] = d['close']
        # d.loc[d['open'] >= d['close'], 'lower'] = d['close']

        sma2 = _sma_safe(d.close,2)            
        sma5 = _sma_safe(d.close,WIN_1)    
        sma10 = _sma_safe(d.close,WIN_2)    
        sma20 = _sma_safe(d.close,WIN_4)
        sma30 = _sma_safe(d.close,30)
        sma50 = _sma_safe(d.close,WIN_13)
        sma60 = _sma_safe(d.close,60)
        sma120 =  _sma_safe(d.close,WIN_26)
        sma150 =  _sma_safe(d.close,150)
        sma200 =  _sma_safe(d.close,WIN_52)
        sma250 =  _sma_safe(d.close,250)
        
        sma_df = pd.DataFrame({'sma2': sma2, 'sma5': sma5, 'sma10': sma10, 'sma20': sma20, 'sma30': sma30, 'sma50': sma50, 'sma60': sma60,
                               'sma120': sma120, 'sma150': sma150, 'sma200': sma200, 'sma250': sma250})
        
        d = _concat_cols(d, sma_df, "sma_df")
        
        d['ema20'] = _ema_safe(d.close,WIN_4)    
        
        max5 =  _max_safe(d.high,WIN_1)
        max10 =  _max_safe(d.high,WIN_2)
        max20 =  _max_safe(d.high,WIN_4)
        max50 =  _max_safe(d.high,WIN_13)
        max125 =  _max_safe(d.high,125)
        max250 =  _max_safe(d.high,250)
        
        min5 =  _min_safe(d.low,WIN_1)
        min10 =  _min_safe(d.low,WIN_2)
        min20 =  _min_safe(d.low,WIN_4)
        min50 =  _min_safe(d.low,WIN_13)
        min125 =  _min_safe(d.low,125)
        min250 =  _min_safe(d.low,250)

        mid5 = _midprice_safe(d.high, d.low,WIN_1)          
        mid10 = _midprice_safe(d.high, d.low,WIN_2)        
        mid20 = _midprice_safe(d.high, d.low,WIN_4)
        mid50 = _midprice_safe(d.high, d.low,WIN_13)
        mid125 = _midprice_safe(d.high, d.low,125)
        mid250 = _midprice_safe(d.high, d.low,250)
        
        minmax_df = pd.DataFrame({'max5': max5, 'max10': max10, 'max20': max20, 'max50': max50, 'max125': max125, 'max250': max250,
                                  'min5': min5, 'min10': min10, 'min20': min20, 'min50': min50, 'min125': min125, 'min250': min250,
                                  'mid5': mid5, 'mid10': mid10, 'mid20': mid20, 'mid50': mid50, 'mid125': mid125, 'mid250': mid250})
        
        d = _concat_cols(d, minmax_df, "minmax_df")
        
        atr4 = atr_wilder(d.high, d.low, d.close, 4)
        atr14 = atr_wilder(d.high, d.low, d.close, 14)
        atr10 = _atr_wilder_safe(d.high, d.low, d.close, WIN_2)
        atr20 = _atr_wilder_safe(d.high, d.low, d.close, WIN_4)
        atr30 = atr_wilder(d.high, d.low, d.close, 30)
        # d['atr120'] = talib.ATR(d.high, d.low, d.close,WIN_26)
        # d['atr56'] = talib.ATR(d.high, d.low, d.close,56)
        
        atr_df = pd.DataFrame({'atr4': atr4, 'atr10': atr10, 'atr20': atr20, 'atr30': atr30, 
                               'atr14': atr14})
        
        d = _concat_cols(d, atr_df, "atr_df")
        
        d['tr'] = talib.TRANGE(d.high, d.low, d.close)
        # d['mtr4'] = _max_safe(d.tr,4)
        d['mtr7'] = _max_safe(d.tr,7)
        d['mtr14'] = _max_safe(d.tr,14)
        d['mtr20'] = _max_safe(d.tr,WIN_4)

        
        d['maxP_index'] = talib.MAXINDEX(d.close,60)
        d['minP_index'] = talib.MININDEX(d.close,60)
    
        d['maxV_index'] = talib.MAXINDEX(d.volume,60)
        
        
        d['slope20'] = talib.LINEARREG_ANGLE(d.typical,WIN_4)
        d['slope30'] = talib.LINEARREG_ANGLE(d.typical,30)
        d['slope40'] = talib.LINEARREG_ANGLE(d.typical,40)
        
        
        upperband, middleband, lowerband = _bbands_safe(d.close,WIN_4, nbdevup=2, nbdevdn=2, matype=0)
        
        d['bol20_up'] = upperband
        d['bol20_ma'] = middleband
        d['bol20_dn'] = lowerband
        
        # 정본: indicators_core (루트·naverPub 동일). band20_q = 정규화 0~1
        d['band20_w'] = bollinger_band_width(d.close, window=WIN_4, n_sigma=2.0)
        d['band20_w_min'] = d['band20_w'].rolling(125, min_periods=125).min()
        d['band20_w_max'] = d['band20_w'].rolling(125, min_periods=125).max()
        d['band20_q'] = bollinger_band_width_q(d.close, window=WIN_4, n_sigma=2.0, lookback=125)

        d['atr_w'] = d.atr14 / d.close.replace(0, np.nan)
        d['atr_w_min'], d['atr_w_max'] = _minmax_safe(d.atr_w,125)
        d['atr_q'] = (d.atr_w - d.atr_w_min) / (d.atr_w_max - d.atr_w_min).replace(0, np.nan)

        upperband, middleband, lowerband = _bbands_safe(d.close,WIN_13, nbdevup=2, nbdevdn=2, matype=0)
        
        d['bol50_up'] = upperband
        d['bol50_ma'] = middleband
        d['bol50_dn'] = lowerband
        
        d['band50_w'] = (d.bol50_up - d.bol50_dn)/d.bol50_ma     
        
        upperband, middleband, lowerband = _bbands_safe(d.close,WIN_26, nbdevup=2, nbdevdn=2, matype=0)
        
        d['bol120_up'] = upperband
        d['bol120_ma'] = middleband
        d['bol120_dn'] = lowerband
        
        d['band120_w'] = (d.bol120_up - d.bol120_dn)/d.bol120_ma      
        
        
        
        dsprt20 = d['sma2'] - d['sma20']
        dsprt50 = d['sma2'] - d['sma50']
        dsprt120 = d['sma2'] - d['sma120']
        
        
        dsprt_df = pd.DataFrame({'dsprt20': dsprt20, 'dsprt50': dsprt50, 'dsprt120': dsprt120
                                 })
        
        d = _concat_cols(d, dsprt_df, "dsprt_df")

        # CSI (screening_loop.py — indicators.py 동일)
        _atr10 = d.atr10.replace(0, np.nan)
        _atr20 = d.atr20.replace(0, np.nan)
        _atr30 = d.atr30.replace(0, np.nan)
        d['csi20'] = ((d.high - d.sma20) / _atr20 + (d.low - d.sma20) / _atr20) / 2
        d['csi10'] = ((d.high - d.sma10) / _atr10 + (d.low - d.sma10) / _atr10) / 2
        d['csi30'] = ((d.high - d.sma30) / _atr30 + (d.low - d.sma30) / _atr30) / 2
        d['csi20_max'] = _max_safe(d.csi20,WIN_4)

        # CSI — Pine: cs=(close-sma(close,L))/atr(L); csi=sma(cs,2); fast=ema(cs,10); slow=ema(cs,20)
        _L = _CSI_LENGTH
        _csi_sma = _sma_safe(d.close,_L)
        _csi_atr = _atr_wilder_safe(d.high, d.low, d.close, _L)
        cs = (d.close - _csi_sma) / _csi_atr.replace(0, np.nan)
        d["csi"] = _sma_safe(cs,2)
        d["csi_fast"] = _ema_safe(cs,WIN_2)
        d["csi_slow"] = _ema_safe(cs,WIN_4)

        ## momentum
        
        d['di_P'] = talib.PLUS_DI(d.high, d.low, d.close,14)
        d['di_M'] = talib.MINUS_DI(d.high, d.low, d.close,14)
        d['adx'] = talib.ADX(d.high, d.low, d.close,14)
    
        
        macd, macdsignal, macdhist = talib.MACD(d.close, fastperiod=12, slowperiod=26, signalperiod=9)
        
        d['macd'] = macd
        d['macdsignal'] = macdsignal
        d['macdhist'] = macdhist
        

        d['ch_up1'] = talib.ADD(d.sma20, d.atr14*1)
        d['ch_dn1'] = talib.SUB(d.sma20, d.atr14*1)        
        d['ch_up2'] = talib.ADD(d.ema20, d.atr14*2)
        d['ch_dn2'] = talib.SUB(d.ema20, d.atr14*2)
        d['ch_up3'] = talib.ADD(d.ema20, d.atr14*3)
        d['ch_dn3'] = talib.SUB(d.ema20, d.atr14*3)
        d['ch_up4'] = talib.ADD(d.ema20, d.atr14*4)
        d['ch_dn4'] = talib.SUB(d.ema20, d.atr14*4)
        
        d['ch_w'] = (d.ch_up2 - d.ch_dn2)/d.ema20
        
            
        d['minmax_w'] = (d.max20 - d.min20)/d.mid20
        
        d['minmax5'] = (d.max5 - d.min5)
        
        # d['minmax_slope'] = 
        
        d['vol_mtr'] = (d.mtr14/d.close)*100
        d['vol_atr'] = (d.atr14/d.close)*100
        
       
    ### 거래량
           
        d['vol_sma5'] = _sma_safe(d.volume,WIN_1)
        d['vol_sma20'] = _sma_safe(d.volume,WIN_4)
        d['vol_sma50'] = _sma_safe(d.volume,WIN_13)
        
        d['vol_sum5'] = _sum_safe(d.volume,WIN_1)
        d['vol_sum20'] = _sum_safe(d.volume,WIN_4)
        d['vol_sum50'] = _sum_safe(d.volume,WIN_13)
            
        d['vol_sum5_sma5'] = _sma_safe(d.vol_sum5,WIN_1)
        d['vol_sum20_sma20'] = _sma_safe(d.vol_sum20,WIN_4)
                
        d['vol_max5'] =  _max_safe(d.volume,WIN_1)
        d['vol_max20'] =  _max_safe(d.volume,WIN_4)
        d['vol_max50'] =  _max_safe(d.volume,WIN_13)
       
        d.loc[(d['volume'] == 0) & (d['close'] != 0), ['open', 'high', 'low']] = d['close']
        
        d['obv'] = talib.OBV(d.close, d.volume)
        
        # d['obv_slope'] = talib.LINEARREG_ANGLE(d.obv,WIN_4)
        
        d['vol_vol50'] = d.volume/d.vol_sma50
        
        d['max_vol50'] = _max_safe(d.vol_vol50,WIN_13)
        
        # d['volume_id50'] = d['vol_max50']/d['vol_sma50']
        # d['volume_id20'] = d['vol_max20']/d['vol_sma20']
            
        ### BOX
        
        
        box7 = _max_safe(d.high,7) - _min_safe(d.low,7)
        box14 = _max_safe(d.high,14) - _min_safe(d.low,14)
        box21 = _max_safe(d.high,21) - _min_safe(d.low,21)
        box30 = _max_safe(d.high,30) - _min_safe(d.low,30)
        box40 = _max_safe(d.high,40) - _min_safe(d.low,40)
        box50 = _max_safe(d.high,WIN_13) - _min_safe(d.low,WIN_13)
        
        box_df = pd.DataFrame({'box7': box7, 'box14': box14, 'box21': box21, 'box30': box30, 'box40': box40, 'box50': box50})
        
        d = _concat_cols(d, box_df, "box_df")

        
        d['pb'] = (d.close - d.bol20_dn)/(d.bol20_up - d.bol20_dn)
        d['pm'] = (d.close - d.min20)/(d.max20 - d.min20)
        d['sma_score'] = d['pm']  # screening_loop p14 등
        d['pc'] = (d.close - d.ch_dn2)/(d.ch_up2 - d.ch_dn2)
        
        d['pb_max'] = _max_safe(d.pb,WIN_4)
        d['pb_min'] = _min_safe(d.pb,WIN_4)
        
        d['mfi'] = talib.MFI(d.high, d.low, d.close, d.volume,14)
        d['willr'] = talib.WILLR(d.high, d.low, d.close,14)

        if tradeHist is not None:
            if isinstance(tradeHist, str):
                buyDay = datetime.strptime(tradeHist.replace("-", "")[:8], "%Y%m%d").date()
            elif isinstance(tradeHist, datetime):
                buyDay = tradeHist.date()
            else:
                buyDay = tradeHist
            
            try:
                risk = d.loc[buyDay].atr14
            except:
                risk = 0
            
    
            d['stop_loss'] = d.iloc[-1].ent_p - risk*1.5
                
            d['stop_atr'] = min(d.iloc[-1].close, d.iloc[-1].open) - d.iloc[-1].atr14*1.5
            d['stop_mtr'] = min(d.iloc[-1].close, d.iloc[-1].open) - d.iloc[-1].mtr14*1 
            
            # maxIndex = talib.MAXINDEX(d.high,WIN_4)
    
            # d['stop_h'] = min(d.iloc[talib.MAXINDEX(d.high,WIN_4).iloc[-1]].close, 
            #                   d.iloc[talib.MAXINDEX(d.high,WIN_4).iloc[-1]].open) - _max_safe(d.atr14,WIN_4).iloc[-1]*1.5
            
            d['stop_h'] = d.iloc[-1].max20 - d.iloc[-1].mtr20*1.5

            d['take_profit'] = d.iloc[-1].ent_p + risk*4.5

            try:
                loc = int(d.index.get_indexer([buyDay], method="pad")[0])
            except Exception:
                loc = len(d) - 1
            if loc < 0:
                loc = len(d) - 1
            base = d.iloc[max(0, loc - 59): loc + 1]
            d["top"] = float(base["high"].max())
            d["bottom"] = float(base["low"].min())
            d["take_profit2"] = d.iloc[-1].ent_p + risk * 3.0

        return d


def build_indicators(ohlcv_df, ord_dt, engine=None, ticker=None):
    out = get_indicators(ohlcv_df, ord_dt)
    return _attach_investor_osc(out, engine, ticker)


def _ensure_investor_osc_on_df(df, ticker):
    if _has_investor_osc_data(df):
        return df, True
    tcode = _normalize_ticker(ticker, df)
    enriched = _attach_investor_osc(df.copy(), engine, tcode)
    return enriched, _has_investor_osc_data(enriched)

##########################################################################################################################################

_TRADING_DAYS_CACHE = {}


def _load_trading_days(engine, start, end):
    """krx_ohlcv 실제 거래일 집합. (start, end) 캐시."""
    key = (str(start)[:10], str(end)[:10])
    if key in _TRADING_DAYS_CACHE:
        return _TRADING_DAYS_CACHE[key]
    # 휴장일은 '일봉' 달력이므로 주봉 테이블로 바뀌어도 krx_ohlcv 고정
    q = "SELECT DISTINCT date FROM krx_ohlcv WHERE date BETWEEN %s AND %s"
    df = pd.read_sql_query(q, con=engine, params=(key[0], key[1]))
    out = set(pd.DatetimeIndex(pd.to_datetime(df["date"])).normalize())
    _TRADING_DAYS_CACHE[key] = out
    return out


def _nontrading_weekdays(engine, start, end):
    """구간 내 평일 중 거래일이 아닌 날 = 휴장일 (YYYY-MM-DD 문자열)."""
    try:
        td = _load_trading_days(engine, start, end)
        return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(start, end) if d not in td]
    except Exception as e:
        print(f"⚠️ 휴장일 자동 도출 실패: {e}")
        return []


audit_ticker = ['006620', '001705', '005440', '272210', '298040', '322000', '329180', '375500', '383220', '456010', '460930',
                '139480', '122870', '084010', '082740', '069460', '069410', '068930', '067730', '037270', '035890', '034310',
                '033780', '033500', '031820', '030530', '024720', '017800', '014830', '012450', '008060', '006120', '004800',
                '000150', '000155', '002960', '003690', '005680', '005720', '005725', '006260', '011760', '015760', '024800',
                '030190', '034020', '041920', '053300', '069960', '124500', '37550K', '37550L', '402340', '000720', '000725',
                '000880', '003540', '003545', '003547', '005810', '029530', '030610', '036000', '000157', '060280', '089150',
                '111770', '114090', '227840', '267250', '004710', '007660', '009300', '010420', '047810', '049630', '161890',
                '042660', '105330', '003520', '034220', '089590', '010620', '086670', '009160', '00088K', '003530', '009540',
                '016880', '060370', '263700', '010120', '032940', '052690', '136490', '217820', '336260', '042670', '051600',
                '064350', '277260', '100090', '006040', '004020', '014790', '020000', '021320', '023410', '024880', '027410',
                '054920', '057050', '285130', '28513K', '347740', '377740', '000660', '001740', '003090', '005090', '006125',
                '008930', '016740', '030000', '036670', '079980', '085620', '088350', '361610', '213500', '000070', '357780',
                '053610', '001430', '005387', '022100', '489790', '009180', '009830', '267270', '002790', '00279K', '000075',
                '009970', '03473K', '001120', '001515', '019440', '161000', '124500', '093380', '195940', '353200', '104830',
                '071970', '099320', '002350', '002355', '005850', '161390', '267260', 
                ]

audit_ticker = []


# --- 스크리닝 기본 임계값 (일봉). ctx["screen_cfg"] 로 덮어씀 ---
def _default_screen_cfg():
    return {
        "min_bars": WIN_26,
        "atr14_close_max": 0.1,
        "mtr7_close_max": 0.2,
        "box7_close_max": 0.3,
        "band_squeeze_q": 0.1,
        "band_squeeze_q2": 0.2,
        "use_rs": USE_RS,
    }


def _resolve_screen_cfg(ctx):
    cfg = _default_screen_cfg()
    if ctx:
        cfg.update(ctx.get("screen_cfg") or {})
    return cfg


### 서버 접속·티커는 run_main()에서 초기화 (import 시 실행 안 함)
engine = None
conn_str = None
ticker_list = None


# 최적화된 데이터 로딩 함수들
def load_single_ticker_ohlcv(ticker, ticker_list, engine):
    """단일 티커 OHLCV 데이터 로드 (병렬 처리용)"""
    try:
        # KRX 통일 후 name/market/mcap 등 추가 → select * + insert(name) 충돌 방지
        ohlcv = pd.read_sql_query(
            f"""
            SELECT {_ohlcv_cols_sql()}
            FROM `{OHLCV_TABLE}`
            WHERE ticker = %s
            ORDER BY date
            """,
            con=engine,
            params=(str(ticker),),
        )

        if ohlcv is not None and not ohlcv.empty:
            for c in ("name", "sector", "market_cap", "ticker"):
                if c in ohlcv.columns:
                    ohlcv = ohlcv.drop(columns=[c])
            ohlcv.insert(0, "ticker", str(ticker))
            try:
                ohlcv.insert(1, 'name', ticker_list.loc[ticker, '종목명'])
                ohlcv.insert(2, 'sector', ticker_list.loc[ticker, '업종명'])
                # 시가총액 추가 (억원 단위로 변환하지 않고 원본 값 유지)
                try:
                    if '시가총액' in ticker_list.columns and ticker in ticker_list.index:
                        market_cap = ticker_list.loc[ticker, '시가총액']
                        if pd.notna(market_cap):
                            ohlcv.insert(3, 'market_cap', market_cap)
                        else:
                            ohlcv.insert(3, 'market_cap', None)
                    else:
                        ohlcv.insert(3, 'market_cap', None)
                except (KeyError, IndexError):
                    ohlcv.insert(3, 'market_cap', None)
            except (KeyError, IndexError):
                # ticker가 ticker_list에 없는 경우
                ohlcv.insert(1, 'name', ticker)
                ohlcv.insert(2, 'sector', '')
                ohlcv.insert(3, 'market_cap', None)
            ohlcv = ohlcv.set_index('date')    
            
            return ticker, ohlcv
        return ticker, None
        
    except Exception as e:
        print(f"티커 {ticker} OHLCV 데이터 로드 실패: {e}")
        return ticker, None


def load_ohlcv_parallel(ticker_list, engine, max_workers=10):
    """병렬 처리로 OHLCV 데이터 로드"""
    ohlcv_data = {}
    
    print(f"병렬 처리로 {len(ticker_list)}개 티커 OHLCV 데이터 로딩 시작...")
    print(f"사용할 워커 수: {max_workers}")
    
    # 병렬 처리 실행
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 각 티커에 대해 작업 제출
        future_to_ticker = {
            executor.submit(load_single_ticker_ohlcv, ticker, ticker_list, engine): ticker 
            for ticker in ticker_list.index
        }
        
        # 완료된 작업들 처리
        for future in tqdm(as_completed(future_to_ticker), total=len(ticker_list), desc="OHLCV 로딩"):
            ticker, data = future.result()
            if data is not None:
                ohlcv_data[ticker] = data
    
    print(f"성공적으로 로드된 데이터: {len(ohlcv_data)}개")
    return ohlcv_data


def _latest_bar_ohlcv_valid(df: pd.DataFrame) -> bool:
    """최신 일자 봉의 시고저종·거래량이 모두 유한하고 0보다 큰지 검사 (스크리닝 제외용)."""
    if df is None or df.empty:
        return False
    colmap = {str(c).lower(): c for c in df.columns}
    aliases = [
        ("open", "o"),
        ("high", "h"),
        ("low", "l"),
        ("close", "c"),
        ("volume", "v"),
    ]
    last = df.iloc[-1]
    for long_name, short_name in aliases:
        col = colmap.get(long_name) or colmap.get(short_name)
        if col is None:
            return False
        v = pd.to_numeric(last[col], errors="coerce")
        if not np.isfinite(v) or v <= 0:
            return False
    return True


def filter_ohlcv_zero_latest(ohlcv_data: dict) -> dict:
    """최신 봉 OHLCV 중 하나라도 0 이하/비유한이면 제외 (지표·스크리닝 대상에서 제거)."""
    bad = [t for t, d in ohlcv_data.items() if not _latest_bar_ohlcv_valid(d)]
    for t in bad:
        del ohlcv_data[t]
    if bad:
        print(f"최신 봉 OHLCV 비정상(0 이하 등)으로 제외: {len(bad)}개")
    return ohlcv_data


def calculate_single_indicators(ticker_data):
    """단일 종목 지표 계산 (병렬 처리용)"""
    ticker, data = ticker_data
    try:
        if len(data) >= WIN_26:
            indicators_result = build_indicators(data, None, engine=engine, ticker=ticker)
            return ticker, indicators_result
        return ticker, None
    except Exception as e:
        _log_indicator_fail(ticker, e)
        return ticker, None


def calculate_indicators_parallel(ohlcv_data, max_workers=8):
    """병렬 처리로 지표 계산"""
    indicators_data = {}
    _reset_indicator_fail_log()

    print(f"병렬 처리로 {len(ohlcv_data)}개 종목 지표 계산 시작...")
    print(f"사용할 워커 수: {max_workers}")
    
    # 병렬 처리 실행
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 각 종목에 대해 작업 제출
        future_to_ticker = {
            executor.submit(calculate_single_indicators, (ticker, data)): ticker 
            for ticker, data in ohlcv_data.items()
        }
        
        # 완료된 작업들 처리
        for future in tqdm(as_completed(future_to_ticker), total=len(ohlcv_data), desc="지표 계산"):
            ticker, indicators_result = future.result()
            if indicators_result is not None:
                indicators_data[ticker] = indicators_result

    _flush_indicator_fail_log()
    print(f"지표 계산 완료: {len(indicators_data)}개")
    return indicators_data




def gen_tBand(df, period):
    """매물대 (tradingKIS_test.py gen_tBand 동일)"""
    df = df.reset_index()
    df = df.set_index('date')
    df = df.tail(period)
    df.loc[:, '3q'] = np.round(df.loc[:, 'high'] - (df.loc[:, 'high'] - df.loc[:, 'low']) * 0.25, -2).astype(int).copy()
    df.loc[:, '1q'] = np.round(df.loc[:, 'low'] + (df.loc[:, 'high'] - df.loc[:, 'low']) * 0.25, -2).astype(int).copy()
    df.loc[:, 'open_v'] = df.loc[:, 'volume'] * 0.2
    df.loc[:, 'high_v'] = df.loc[:, 'volume'] * 0.1
    df.loc[:, 'low_v'] = df.loc[:, 'volume'] * 0.1
    df.loc[:, 'close_v'] = df.loc[:, 'volume'] * 0.2
    df.loc[:, '3q_v'] = df.loc[:, 'volume'] * 0.2
    df.loc[:, '1q_v'] = df.loc[:, 'volume'] * 0.2
    volume_df = None
    for col in ['open', 'high', 'low', 'close', '3q', '1q']:
        tmp = df[[col, col + '_v']]
        tmp.columns = ['price', 'volume']
        volume_df = pd.concat([volume_df, tmp], axis=0)
    price_term = np.int64((volume_df['price'].max() - volume_df['price'].min()) / 10)
    term_list = np.arange(volume_df['price'].min(), volume_df['price'].max() + int(price_term / 3), price_term)
    volume_df.loc[:, 'cut'] = 0
    for i, v in enumerate(term_list[1:]):
        if i == 0:
            volume_df.loc[volume_df['price'] <= term_list[1], 'cut'] = int((term_list[0] + term_list[1]) / 2)
        elif i == len(term_list) - 2:
            volume_df.loc[volume_df['price'] > term_list[i], 'cut'] = int((term_list[i] + term_list[i + 1]) / 2)
        else:
            volume_df.loc[(volume_df['price'] > term_list[i]) & (volume_df['price'] <= term_list[i + 1]), 'cut'] = int(
                (term_list[i] + term_list[i + 1]) / 2
            )
    volume_chart = volume_df.groupby(['cut']).sum()[['volume']]
    volume_chart.loc[:, 'volume_p'] = volume_chart['volume'] / volume_chart['volume'].sum() * 100
    volume_chart.loc[:, 'ticker'] = df.iloc[0].ticker
    volume_chart['volume_p_cum'] = volume_chart['volume_p'].cumsum()
    df = df.reset_index()
    multi_factor = df.shape[0] / volume_chart['volume_p'].max()
    for i in volume_chart.index:
        tmp = volume_chart[volume_chart.index == i]
        df[i] = None
        df.loc[0:int(tmp.iloc[0]['volume_p'] * multi_factor), i] = tmp.index.values[0]
    df = df.set_index('date')
    return volume_chart

def calculate_single_volume_band(ticker_data):
    """단일 종목 매물대 계산 (병렬 처리용)"""
    ticker, data = ticker_data
    try:
        if len(data) > 0 and data.iloc[-1]['volume'] > 0:
            volume_result = gen_tBand(data, 50)
            return ticker, volume_result
        return ticker, None
    except Exception as e:
        print(f"티커 {ticker} 매물대 계산 실패: {e}")
        return ticker, None


def calculate_volume_band_parallel(ohlcv_data, max_workers=6):
    """병렬 처리로 매물대 계산"""
    volume_data = {}
    
    print(f"병렬 처리로 {len(ohlcv_data)}개 종목 매물대 계산 시작...")
    print(f"사용할 워커 수: {max_workers}")
    
    # 병렬 처리 실행
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 각 종목에 대해 작업 제출
        future_to_ticker = {
            executor.submit(calculate_single_volume_band, (ticker, data)): ticker 
            for ticker, data in ohlcv_data.items()
        }
        
        # 완료된 작업들 처리
        for future in tqdm(as_completed(future_to_ticker), total=len(ohlcv_data), desc="매물대 계산"):
            ticker, volume_result = future.result()
            if volume_result is not None:
                volume_data[ticker] = volume_result
    
    print(f"매물대 계산 완료: {len(volume_data)}개")
    return volume_data


# ---------------------------------------------------------------------------
# [1] 패턴 레지스트리 골격
# ---------------------------------------------------------------------------
# 측정 기준: 2026-09-12 · 통합풀 RS(지수 포함) + 수정주가(close_adj)
#   이전 측정(시장별 RS · 미조정 가격)과 값이 다르다.

PATTERN_REGISTRY = {}   # code -> {"fn","group","name","params"}

# 폐기 — h20·h60 모두 베이스라인(__BASE__) 이하이거나 죽은 코드
DISABLED_PATTERNS = {
    "p13", "p14", "p17", "p27", "p34", "p43", "p61",
    "p25", "p26",   # p24 와 중복(Jaccard 0.72/0.36), 성과 동일 → p24 로 통합
    "p41",          # 58 재측정(68히트): h60Δ −0.0715, 양구간 음수(−0.052/−0.100),
                    #   승률 0.299 → 폐기 유지
    "p42",          # 58 매물대 첫 측정(EVAL_VOLUME_PROFILE, 1,658히트):
                    #   h60Δ +0.0009(사실상 0) · 승률 0.371(base 0.386) · 일관성 △
                    #   베이스와 구별 불가 → 폐기
    "p53",          # p53: 58 구간분할(2023~24 / 2025~26) h60Δ = -0.062 / +0.095.
                    #      한 구간 의존, 표본 150/305, std 0.52~0.59 → 신뢰 불가
}

# 강등 — 성과가 베이스라인과 동일하고 히트가 과다(전체의 55%).
#        선정 목록에는 넣지 않고 참고용으로만 계산한다.
REFERENCE_PATTERNS = {"p71", "p81", "p93"}

# 58 구간분할 검증 (2026-09-12 · 통합풀 RS + 수정주가 기준)
# A: 두 구간 모두 h60Δ ≥ +0.02 / B: 두 구간 양수이나 약함 / C: 한 구간 의존(△)
PATTERN_GRADE = {
    "p11": "A", "p15": "A", "p16": "A", "p28": "A", "p32": "A",
    "p51": "A", "p52": "A",
    "p12": "B", "p21": "B", "p23": "B", "p24": "B", "p29a": "B",
    "p31": "B", "p33": "B", "p36": "B", "p91": "B", "p92": "B",
    "p22": "C", "p35": "C", "p54": "C", "p55": "C",
}
# 주: p33 은 2023~24 가 +0.0199 로 A 기준(0.02)에 0.0001 미달이다. 경계값.

# 58 섹터중립 (2026-09-12 전체기간). 잔존율 = h60Δ_섹터 ÷ h60Δ_시장
#   종목 ≥0.7 / 혼합 0.3~0.7 / 업종 <0.3 / 미판정 = 시장Δ ≤ 0
# 주의: p81(32.1)·p42(7.05)·p61(3.85)·p32(3.88) 등은 분모가 0에 가까워
#       비율이 과장돼 있다. 부호와 대략적 크기만 참고할 것.
PATTERN_SECTOR_TYPE = {
    # 종목형
    "p51": "종목", "p91": "종목", "p52": "종목", "p36": "종목", "p15": "종목",
    "p31": "종목", "p16": "종목", "p26": "종목", "p92": "종목", "p32": "종목",
    "p55": "종목", "p54": "종목", "p28": "종목", "p12": "종목", "p35": "종목",
    "p29a": "종목", "p93": "종목", "p21": "종목", "p61": "종목", "p42": "종목",
    "p81": "종목",
    # 혼합
    "p24": "혼합", "p25": "혼합", "p33": "혼합",
    # 업종형
    "p53": "업종", "p23": "업종", "p11": "업종", "p22": "업종",
    "p71": "업종", "p13": "업종",
    # 미판정 (시장Δ ≤ 0 이라 비율 무의미)
    "p34": "미판정", "p14": "미판정", "p17": "미판정",
    "p43": "미판정", "p41": "미판정", "p27": "미판정",
}

def _sector_type(code: str) -> str:
    """p29b 는 58에서 p29a 로 통합 측정되므로 같은 태그를 쓴다."""
    c = "p29a" if str(code) in ("p29", "p29b") else str(code)
    return PATTERN_SECTOR_TYPE.get(c, "")

def pattern(code, group, name, **params):
    def deco(fn):
        PATTERN_REGISTRY[code] = {
            "fn": fn, "group": group, "name": name, "params": dict(params)
        }
        return fn
    return deco

def _pat_params(code, ctx=None):
    P = dict(PATTERN_REGISTRY[code]["params"])
    if ctx:
        P.update((ctx.get("pattern_params") or {}).get(code, {}))
        if "rs_df" not in P and ctx.get("rs_df") is not None:
            P["rs_df"] = ctx["rs_df"]
        if "volume_data" not in P and ctx.get("volume_data") is not None:
            P["volume_data"] = ctx["volume_data"]
    return P

_PAT_EXC_SHOWN = set()

def _pat_exc(code):
    if code not in _PAT_EXC_SHOWN:
        _PAT_EXC_SHOWN.add(code)
        print(f"⚠️ 패턴 {code} 예외 (최초 1회):\n{traceback.format_exc()}")


# ---------------------------------------------------------------------------
# [2] 공통 술어 (p11~p14 등에서 반복)
# ---------------------------------------------------------------------------

def ma_aligned(r):
    return r.sma20 > r.sma50 and r.sma50 > r.sma120 and r.sma120 > r.sma200

def ma_rising(d, n50=6, n120=13, n200=21):
    last = d.iloc[-1]
    return (last.sma50 > d.iloc[-n50].sma50
            and last.sma120 > d.iloc[-n120].sma120
            and last.sma200 > d.iloc[-n200].sma200)

def box_pos(r, hi_col, lo_col):
    rng = r[hi_col] - r[lo_col]
    if not (rng > 0):
        return np.nan          # NaN 비교는 False → 원본의 >0 가드와 동일
    return (r["close"] - r[lo_col]) / rng


# ---------------------------------------------------------------------------
# [3] 추출된 패턴 함수 (p11 ~ p17)
# ---------------------------------------------------------------------------

@pattern("p11", group="이동평균", name="정배열+20일박스상단",
         box20_lo=0.75, box20_hi=0.90, box125_lo=0.75)
def _p11(d, P):
    r = d.iloc[-1]
    return bool(ma_aligned(r) and ma_rising(d)
                and box_pos(r, "max20", "min20") < P["box20_hi"]
                and box_pos(r, "max20", "min20") > P["box20_lo"]
                and box_pos(r, "max125", "min125") > P["box125_lo"])


@pattern("p12", group="이동평균", name="정배열+CSI음수",
         box125_lo=0.75, csi_max=0)
def _p12(d, P):
    r = d.iloc[-1]
    return bool(ma_aligned(r) and ma_rising(d)
                and box_pos(r, "max125", "min125") > P["box125_lo"]
                and (r.csi10 < P["csi_max"] or r.csi20 < P["csi_max"] or r.csi30 < P["csi_max"]))


@pattern("p13", group="이동평균", name="정배열+20일박스+거래량축소",
         box20_lo=0.75, box20_hi=0.90, box125_lo=0.75, vol_surge_mult=2)
def _p13(d, P):
    r = d.iloc[-1]
    return bool(ma_aligned(r) and ma_rising(d)
                and box_pos(r, "max20", "min20") < P["box20_hi"]
                and box_pos(r, "max20", "min20") > P["box20_lo"]
                and box_pos(r, "max125", "min125") > P["box125_lo"]
                and (d.iloc[-21].vol_sum20 > d.iloc[-41].vol_sum20 * P["vol_surge_mult"]
                     or d.iloc[-21].vol_sum50 > d.iloc[-71].vol_sum50 * P["vol_surge_mult"])
                and r.vol_sum20 < d.iloc[-21].vol_sum20)


@pattern("p14", group="이동평균", name="정배열+CSI음수+거래량축소",
         box125_lo=0.75, csi_max=0, vol_surge_mult=2)
def _p14(d, P):
    r = d.iloc[-1]
    return bool(ma_aligned(r) and ma_rising(d, n50=6, n120=6, n200=6)
                and box_pos(r, "max125", "min125") > P["box125_lo"]
                and (r.csi10 < P["csi_max"] or r.csi20 < P["csi_max"] or r.csi30 < P["csi_max"])
                and (d.iloc[-21].vol_sum20 > d.iloc[-41].vol_sum20 * P["vol_surge_mult"]
                     or d.iloc[-21].vol_sum50 > d.iloc[-71].vol_sum50 * P["vol_surge_mult"])
                and r.vol_sum20 < d.iloc[-21].vol_sum20)


@pattern("p15", group="이동평균", name="정배열+이평점수+ATR근접",
         sma_score_lo=0.75, atr_mult=1.0)
def _p15(d, P):
    r = d.iloc[-1]
    return bool(ma_aligned(r)
                and r.sma_score > P["sma_score_lo"]
                and r.close < r.sma20 + r.atr14 * P["atr_mult"])


@pattern("p16", group="이동평균", name="정배열+20선밀착+저점상승",
         atr_band=0.5, min_ratio_lo=0.5)
def _p16(d, P):
    r = d.iloc[-1]
    rng50 = r.max50 - r.min50
    return bool(ma_aligned(r)
                and (r.close < r.sma20 + r.atr14 * P["atr_band"] or r.open < r.sma20 + r.atr14 * P["atr_band"])
                and (r.close > r.sma20 - r.atr14 * P["atr_band"] or r.open > r.sma20 - r.atr14 * P["atr_band"])
                and ma_rising(d)
                and (rng50 > 0 and (r.min20 - r.min50) / rng50 > P["min_ratio_lo"]))


_P17_NO_RS_WARNED = False


@pattern("p17", group="거래량", name="거래량급증상승_축소횡보_재상승",
         min_bars=100, surge_gain_min=15, vol_surge_ratio_min=1.3,
         consol_range_max=20, vol_contraction_max=0.70, price_from_high_max=15,
         price_pos_consol_min=0.4, ma50_support_mult=0.98, rs_min=70,
         consol_breakout_mult=0.90)
def _p17(d, P):
    if len(d) < P["min_bars"]:
        return False
    try:
        idc = d.iloc
        # === 1단계: 거래량 급증 상승 구간 확인 (30~50일 전) ===
        surge_start_price = idc[-50].close if len(d) >= 50 else idc[-40].close
        surge_high_price = idc[-35:-25].high.max() if len(d) >= 35 else idc[-30:-20].high.max()

        if surge_start_price > 0:
            surge_gain = (surge_high_price - surge_start_price) / surge_start_price * 100
        else:
            surge_gain = 0

        vol_surge_period = idc[-50:-30].volume.mean() if len(d) >= 50 else idc[-40:-25].volume.mean()
        vol_before_surge = idc[-80:-50].volume.mean() if len(d) >= 80 else idc[-60:-40].volume.mean()

        if vol_before_surge > 0:
            vol_surge_ratio = vol_surge_period / vol_before_surge
        else:
            vol_surge_ratio = 0

        # === 2단계: 횡보/조정 구간 확인 (최근 10~25일) ===
        consol_high = idc[-25:-1].high.max()
        consol_low = idc[-25:-1].low.min()

        if consol_high > 0:
            consol_range = (consol_high - consol_low) / consol_high * 100
        else:
            consol_range = 100

        vol_consol = idc[-25:-1].volume.mean()

        if vol_surge_period > 0:
            vol_contraction = vol_consol / vol_surge_period
        else:
            vol_contraction = 1

        # === 3단계: 현재 위치 확인 ===
        current_price = idc[-1].close

        if surge_high_price > 0:
            price_from_high = (surge_high_price - current_price) / surge_high_price * 100
        else:
            price_from_high = 100

        if (consol_high - consol_low) > 0:
            price_position_in_consol = (current_price - consol_low) / (consol_high - consol_low)
        else:
            price_position_in_consol = 0

        # === 4단계: 이동평균선 확인 ===
        above_ma20 = current_price > idc[-1].sma20
        ma50_support = idc[-1].sma50 >= idc[-10].sma50 * P["ma50_support_mult"]

        # === 5단계: RS 확인 ===
        global _P17_NO_RS_WARNED
        _tk = str(idc[-1].ticker)
        _rs_df = P.get("rs_df")
        if _rs_df is None:
            if not _P17_NO_RS_WARNED:
                _P17_NO_RS_WARNED = True
                print("⚠️ p17: rs_df 없음 — RS 조건 항상 False")
            has_rs = False
        else:
            has_rs = _tk in _rs_df.index and _rs_df.loc[_tk].rs_score >= P["rs_min"]

        # === 6단계: 최종 조건 ===
        return bool(surge_gain >= P["surge_gain_min"]
                    and vol_surge_ratio >= P["vol_surge_ratio_min"]
                    and consol_range <= P["consol_range_max"]
                    and vol_contraction <= P["vol_contraction_max"]
                    and price_from_high <= P["price_from_high_max"]
                    and price_position_in_consol >= P["price_pos_consol_min"]
                    and above_ma20
                    and ma50_support
                    and has_rs
                    and current_price > consol_high * P["consol_breakout_mult"])
    except Exception:
        return False


def _extract_rs(d, P):
    """지표 DF와 파라미터 P(rs_df 포함)에서 종목 RS 점수 추출."""
    _tk = str(d.iloc[-1].ticker)
    rs_df = P.get("rs_df")
    if rs_df is not None and len(rs_df) > 0 and _tk in rs_df.index:
        try:
            row = rs_df.loc[_tk]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            return float(row.rs20_score), float(row.rs50_score), float(row.rs_score), True
        except Exception:
            pass
    return 0.0, 0.0, 0.0, False


@pattern("p21", group="신고가", name="50일신고가+음봉조정",
         n50=6, atr_mult=1.0)
def _p21(d, P):
    r = d.iloc[-1]
    return bool(r.max5 > d.iloc[-P["n50"]].max50
                and r.close > r.max5 - r.atr14 * P["atr_mult"]
                and r.close < r.open)


@pattern("p22", group="신고가", name="하이앤타이트플래그",
         surge_ratio=1.9)
def _p22(d, P):
    r = d.iloc[-1]
    max_min_diff_50 = 0
    max_min_diff_5 = 0
    if r.max10 > 0 and r.min50 > 0:
        max_min_diff_50 = r.max50 - r.min50
        max_min_diff_5 = r.max5 - r.min5
    if max_min_diff_50 > 0 and max_min_diff_5 > 0 and r.min50 > 0:
        return bool(r.max10 / r.min50 > P["surge_ratio"]
                    and r.high < r.max10)
    return False


@pattern("p23", group="신고가", name="52주신고가+눌림목+고RS",
         n250=21, rs20_min=80, rs50_min=80, band20_q_max=1, pb_max=1)
def _p23(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r = d.iloc[-1]
    return bool(r.max20 > d.iloc[-P["n250"]].max250
                and r.close > r.sma20 and r.close < r.sma10
                and (rs20S > P["rs20_min"] and rs50S > P["rs50_min"])
                and (r.band20_q < P["band20_q_max"] or r.pb < P["pb_max"]))


@pattern("p24", group="신고가", name="52주신고가+20선지지+고RS",
         n250=21, atr_mult=1.0, rs20_min=85, rs50_min=85)
def _p24(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r = d.iloc[-1]
    return bool(r.max20 > d.iloc[-P["n250"]].max250
                and r.close < r.sma20 + r.atr14 * P["atr_mult"]
                and r.close > r.sma20 - r.atr14 * P["atr_mult"]
                and r.close > r.sma50
                and (rs20S > P["rs20_min"] or rs50S > P["rs50_min"]))


@pattern("p25", group="신고가", name="50일신고가(250대비)+20선지지+고RS",
         n250=51, atr_mult=1.0, rs20_min=85, rs50_min=85)
def _p25(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r = d.iloc[-1]
    return bool(r.max50 > d.iloc[-P["n250"]].max250
                and r.close < r.sma20 + r.atr14 * P["atr_mult"]
                and r.close > r.sma20 - r.atr14 * P["atr_mult"]
                and r.close > r.sma50
                and (rs20S > P["rs20_min"] or rs50S > P["rs50_min"]))


@pattern("p26", group="신고가", name="10일신고가(50대비)+20선지지+고RS",
         n50=11, atr_mult=1.0, rs20_min=85, rs50_min=85)
def _p26(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r = d.iloc[-1]
    return bool(r.max10 > d.iloc[-P["n50"]].max50
                and r.close < r.sma20 + r.atr14 * P["atr_mult"]
                and r.close > r.sma20 - r.atr14 * P["atr_mult"]
                and r.close > r.sma50
                and (rs20S > P["rs20_min"] or rs50S > P["rs50_min"]))


@pattern("p27", group="신고가", name="10일신고가(125대비)+20선지지+고RS",
         n125=11, atr_mult=1.0, rs20_min=85, rs50_min=85, dead=True)
def _p27(d, P):
    # 원본 가드: (rs_df 없음 or 티커가 rs_df에 없음) and p26 미선정
    # → 그 경우 rs20S/rs50S 가 0 이므로 내부 rs>85 가 항상 False.
    # 즉 원본에서 p27 은 한 번도 발동하지 않는 죽은 패턴이다.
    # 측정 단계에서 로직을 바꾸지 않기 위해 원본 동작을 그대로 보존한다.
    # (2단계 통합 때 폐기 또는 의도대로 수정할지 결정)
    return False


@pattern("p28", group="신고가", name="신고가돌파봉+고RS",
         rs20_min=80, rs50_min=80)
def _p28(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r1 = d.iloc[-1]
    r2 = d.iloc[-2]
    return bool(((r1.close > r2.max50 and r2.close < r2.max50)
                 or (r1.close > r2.max125 and r2.close < r2.max125)
                 or (r1.close > r2.max250 and r2.close < r2.max250))
                and (rs20S > P["rs20_min"] or rs50S > P["rs50_min"]))


@pattern("p29a", group="신고가", name="125일신고가+정배열+고RS(50이내)",
         n125=6, rs20_min=80, rs50_min=80)
def _p29a(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r = d.iloc[-1]
    return bool(r.max5 > d.iloc[-P["n125"]].max125
                and r.close > d.iloc[-P["n125"]].max125
                and (rs20S > P["rs20_min"] or rs50S > P["rs50_min"])
                and ma_aligned(r))


@pattern("p29b", group="신고가", name="125일신고가+정배열+고RS(50초과)",
         n125=6, rs20_min=80, rs50_min=80)
def _p29b(d, P):
    return _p29a(d, P)


@pattern("p31", group="볼린저", name="밴드수축+정배열+박스상단",
         band_squeeze_q=0.1, n50=6, box125_lo=0.5)
def _p31(d, P):
    r = d.iloc[-1]
    return bool(r.band20_q < P["band_squeeze_q"]
                and ma_aligned(r)
                and r.sma50 > d.iloc[-P["n50"]].sma50
                and box_pos(r, "max125", "min125") > P["box125_lo"])


@pattern("p32", group="볼린저", name="밴드하단근접+정배열+박스상단",
         pb_max=0.3, n50=6, box125_lo=0.75)
def _p32(d, P):
    r = d.iloc[-1]
    return bool(r.pb < P["pb_max"]
                and ma_aligned(r)
                and r.sma50 > d.iloc[-P["n50"]].sma50
                and box_pos(r, "max125", "min125") > P["box125_lo"])


@pattern("p33", group="볼린저", name="밴드상단돌파후회귀+50선배열",
         n50=6, pb_hi=1.0, pb_lo=0.8)
def _p33(d, P):
    r = d.iloc[-1]
    return bool(r.max5 > r.bol20_up
                and r.max5 > d.iloc[-P["n50"]].max50
                and r.pb < P["pb_hi"]
                and r.pb > P["pb_lo"]
                and r.sma50 > r.sma120
                and r.sma120 > r.sma200)


@pattern("p34", group="볼린저", name="밴드돌파+50일신고가+밴드수축",
         n50=6, band_squeeze_q2=0.2, pb_hi=1.2, pb_lo=0.8)
def _p34(d, P):
    r = d.iloc[-1]
    return bool(r.max5 > d.iloc[-P["n50"]].bol20_up
                and r.max5 > d.iloc[-P["n50"]].max50
                and r.band20_q < P["band_squeeze_q2"]
                and r.pb < P["pb_hi"]
                and r.pb > P["pb_lo"])


@pattern("p35", group="볼린저", name="밴드근접+박스상단",
         pb_max=1.0, box125_lo=0.75)
def _p35(d, P):
    r = d.iloc[-1]
    return bool(r.pb_max < P["pb_max"]
                and box_pos(r, "max125", "min125") > P["box125_lo"])


@pattern("p36", group="볼린저", name="밴드스퀴즈반등+고RS",
         band_squeeze_q2=0.2, rs_min=80)
def _p36(d, P):
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r1 = d.iloc[-1]
    r2 = d.iloc[-2]
    return bool(r2.band20_q < P["band_squeeze_q2"]
                and r1.band20_q > r2.band20_q
                and rsS > P["rs_min"])


_P41_NO_VOL_WARNED = False

@pattern("p41", group="매물대", name="최대매물대돌파+매물집중",
         n_prev=6, vol_p_max=40)
def _p41(d, P):
    global _P41_NO_VOL_WARNED
    vol_data = P.get("volume_data")
    if not vol_data:
        if not _P41_NO_VOL_WARNED:
            _P41_NO_VOL_WARNED = True
            print("⚠️ p41: volume_data 없음 — 조건 항상 False")
        return False
    _tk = str(d.iloc[-1].ticker)
    vd = vol_data.get(_tk)
    if vd is None and hasattr(d.iloc[-1], 'ticker'):
        vd = vol_data.get(d.iloc[-1].ticker)
    if vd is None:
        return False
    try:
        r = d.iloc[-1]
        max_p = vd['volume_p'].idxmax()
        return bool(r.close > max_p
                    and d.iloc[-P["n_prev"]].close < max_p
                    and vd['volume_p'].max() > P["vol_p_max"])
    except Exception:
        return False


_P42_NO_VOL_WARNED = False

@pattern("p42", group="매물대", name="최대매물대돌파+누적매물80",
         n_prev=6, vol_cum_min=80)
def _p42(d, P):
    global _P42_NO_VOL_WARNED
    vol_data = P.get("volume_data")
    if not vol_data:
        if not _P42_NO_VOL_WARNED:
            _P42_NO_VOL_WARNED = True
            print("⚠️ p42: volume_data 없음 — 조건 항상 False")
        return False
    _tk = str(d.iloc[-1].ticker)
    vd = vol_data.get(_tk)
    if vd is None and hasattr(d.iloc[-1], 'ticker'):
        vd = vol_data.get(d.iloc[-1].ticker)
    if vd is None:
        return False
    try:
        r = d.iloc[-1]
        max_p = vd['volume_p'].idxmax()
        cum_row = vd.loc[max_p]
        cum_val = cum_row.volume_p_cum
        if hasattr(cum_val, 'iloc'):
            cum_val = cum_val.iloc[-1]
        return bool(r.close > max_p
                    and d.iloc[-P["n_prev"]].close < max_p
                    and float(cum_val) > P["vol_cum_min"])
    except Exception:
        return False


@pattern("p43", group="매물대", name="변동성수축+박스압축+이평정렬",
         min20_ratio_lo=0.75, box20_hi=0.9)
def _p43(d, P):
    r = d.iloc[-1]
    rng125 = r.max125 - r.min125
    rng20 = r.max20 - r.min20
    rng50 = r.max50 - r.min50
    if not (rng125 > 0 and rng20 > 0):
        return False
    return bool(rng20 < rng50
                and rng50 < rng125
                and (r.min20 - r.min125) / rng125 > P["min20_ratio_lo"]
                and box_pos(r, "max20", "min20") < P["box20_hi"]
                and r.sma50 > r.sma120 and r.sma120 > r.sma200)


@pattern("p51", group="차트패턴", name="Cup with Handle(단순)",
         min_bars=22, n200=21, max125_mult=0.75, min125_mult=1.25,
         n20=2, max20_mult=0.9, n50=6, max50_mult=1.1, rs_min=90)
def _p51(d, P):
    if len(d) < P["min_bars"]:
        return False
    rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
    if not has_rs:
        return False
    r = d.iloc[-1]
    return bool(r.sma50 > r.sma150 and r.sma150 > r.sma200
                and r.sma200 > d.iloc[-P["n200"]].sma200
                and r.min5 > r.max125 * P["max125_mult"]
                and r.min5 > r.min125 * P["min125_mult"]
                and r.close > d.iloc[-P["n20"]].max20 * P["max20_mult"]
                and r.max5 < d.iloc[-P["n50"]].max50 * P["max50_mult"]
                and rsS > P["rs_min"])


@pattern("p52", group="차트패턴", name="미너비니 CWH",
         min_bars=120, cup_depth_min=5, cup_depth_max=70,
         handle_depth_min=5, handle_depth_max=25, handle_pos_min=40,
         vol_drying_1_min=0.05, vol_drying_2_min=0.02,
         tt_conditions_min=5, tt6_mult=1.20, tt7_mult=0.65, n200=21,
         rs_min=65, pivot_ratio_min=0.80, pivot_ratio_max=1.10)
def _p52(d, P):
    if len(d) < P["min_bars"]:
        return False
    try:
        idc = d.iloc
        rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)

        # === 1단계: 컵(Cup) 패턴 찾기 ===
        cup_left_window_start = min(150, len(d)-1)
        cup_left_window_end = min(70, len(d)-1)
        cup_left_high = idc[-cup_left_window_start:-cup_left_window_end].high.max()

        cup_bottom = idc[-70:-15].low.min()
        cup_right_high = idc[-15:].high.max()

        if not (cup_left_high > 0):
            return False

        cup_depth_pct = (cup_left_high - cup_bottom) / cup_left_high * 100

        # === 2단계: 핸들(Handle) 패턴 찾기 ===
        handle_high = idc[-15:-1].high.max()
        handle_low = idc[-15:-1].low.min()

        handle_depth_pct = (handle_high - handle_low) / cup_left_high * 100

        cup_height = cup_left_high - cup_bottom
        if cup_height > 0:
            handle_position_pct = (handle_low - cup_bottom) / cup_height * 100
        else:
            handle_position_pct = 0

        # === 3단계: 거래량 패턴 분석 ===
        vol_left = idc[-100:-70].volume.mean() if len(d) >= 100 else idc[-70:-50].volume.mean()
        vol_bottom = idc[-50:-20].volume.mean()
        vol_handle = idc[-15:-1].volume.mean()
        vol_recent = idc[-1].volume

        vol_drying_1 = (vol_left - vol_bottom) / vol_left if vol_left > 0 else 0
        vol_drying_2 = (vol_bottom - vol_handle) / vol_bottom if vol_bottom > 0 else 0
        vol_surge = vol_recent / vol_handle if vol_handle > 0 else 0

        # === 4단계: Trend Template 검증 (8개 조건) ===
        tt_conditions = 0

        if idc[-1].close > idc[-1].sma150 and idc[-1].close > idc[-1].sma200:
            tt_conditions += 1

        if idc[-1].sma150 > idc[-1].sma200:
            tt_conditions += 1

        if idc[-1].sma200 > idc[-P["n200"]].sma200:
            tt_conditions += 1

        if idc[-1].sma50 > idc[-1].sma150 and idc[-1].sma50 > idc[-1].sma200:
            tt_conditions += 1

        if idc[-1].close > idc[-1].sma50:
            tt_conditions += 1

        week52_low = idc[-200:].low.min() if len(d) >= 200 else idc[-120:].low.min()
        if idc[-1].close > week52_low * P["tt6_mult"]:
            tt_conditions += 1

        week52_high = idc[-200:].high.max() if len(d) >= 200 else idc[-120:].high.max()
        if week52_high > 0 and idc[-1].close >= week52_high * P["tt7_mult"]:
            tt_conditions += 1

        if has_rs and rsS >= P["rs_min"]:
            tt_conditions += 1

        # === 5단계: 피벗 포인트 (매수 시점) 계산 ===
        pivot_point = handle_high
        buy_point = pivot_point * 1.001
        current_vs_pivot = idc[-1].close / pivot_point

        # === 6단계: 최종 조건 검증 ===
        return bool(P["cup_depth_min"] <= cup_depth_pct <= P["cup_depth_max"]
                    and P["handle_depth_min"] <= handle_depth_pct <= P["handle_depth_max"]
                    and handle_position_pct >= P["handle_pos_min"]
                    and tt_conditions >= P["tt_conditions_min"]
                    and has_rs
                    and rsS >= P["rs_min"]
                    and vol_drying_1 >= P["vol_drying_1_min"]
                    and vol_drying_2 >= P["vol_drying_2_min"]
                    and P["pivot_ratio_min"] <= current_vs_pivot <= P["pivot_ratio_max"]
                    and idc[-1].close > idc[-1].sma50)
    except Exception:
        return False


@pattern("p53", group="차트패턴", name="High Tight Flag",
         min_bars=80, surge_pct_min=60, flag_depth_min=8, flag_depth_max=30,
         price_from_high_max=20, vol_decrease_min=0.20, rs_min=75,
         surge_high_mult=0.85)
def _p53(d, P):
    if len(d) < P["min_bars"]:
        return False
    try:
        idc = d.iloc
        rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)

        # === 1단계: 급등 확인 (최근 25~50일) ===
        surge_start_price = idc[-50:-40].close.min() if len(d) >= 50 else idc[-40:-30].close.min()
        surge_high = idc[-25:-8].high.max()

        if not (surge_start_price > 0):
            return False

        surge_pct = (surge_high - surge_start_price) / surge_start_price * 100

        # === 2단계: 타이트한 조정 확인 (최근 8~20일) ===
        flag_high = idc[-20:-1].high.max()
        flag_low = idc[-20:-1].low.min()

        if not (surge_high > 0):
            return False

        flag_depth_pct = (flag_high - flag_low) / surge_high * 100
        price_from_high = (surge_high - idc[-1].close) / surge_high * 100

        # === 3단계: 거래량 패턴 ===
        vol_surge = idc[-40:-15].volume.mean()
        vol_flag = idc[-15:-1].volume.mean()
        vol_decrease = (vol_surge - vol_flag) / vol_surge if vol_surge > 0 else 0

        # === 4단계: 이동평균선 체크 ===
        ma_aligned = idc[-1].sma20 > idc[-1].sma50

        # === 5단계: 최종 조건 (완화) ===
        return bool(surge_pct >= P["surge_pct_min"]
                    and P["flag_depth_min"] <= flag_depth_pct <= P["flag_depth_max"]
                    and price_from_high <= P["price_from_high_max"]
                    and vol_decrease >= P["vol_decrease_min"]
                    and ma_aligned
                    and idc[-1].close > idc[-1].sma20
                    and has_rs
                    and rsS >= P["rs_min"]
                    and idc[-1].close > surge_high * P["surge_high_mult"])
    except Exception:
        return False


@pattern("p54", group="차트패턴", name="VCP",
         min_bars=120, range_contract_mult=0.9, vol_contract_mult=0.9,
         is_tight_max=12, base_depth_max=50, ma50_mult=0.98,
         rs_min=70, base_breakout_mult=0.80)
def _p54(d, P):
    if len(d) < P["min_bars"]:
        return False
    try:
        idc = d.iloc
        rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)

        # === 1단계: 세 번의 수축 구간 정의 ===

        # T1: 첫 번째 수축 (50~35일 전)
        t1_high = idc[-50:-35].high.max()
        t1_low = idc[-50:-35].low.min()
        t1_range = (t1_high - t1_low) / t1_high * 100 if t1_high > 0 else 0
        t1_vol = idc[-50:-35].volume.mean()

        # T2: 두 번째 수축 (35~18일 전)
        t2_high = idc[-35:-18].high.max()
        t2_low = idc[-35:-18].low.min()
        t2_range = (t2_high - t2_low) / t2_high * 100 if t2_high > 0 else 0
        t2_vol = idc[-35:-18].volume.mean()

        # T3: 세 번째 수축 (최근 18일)
        t3_high = idc[-18:-1].high.max()
        t3_low = idc[-18:-1].low.min()
        t3_range = (t3_high - t3_low) / t3_high * 100 if t3_high > 0 else 0
        t3_vol = idc[-18:-1].volume.mean()

        # === 2단계: 변동성 축소 확인 (완화) ===

        # 각 구간의 변동성이 감소 경향인지 (완벽하지 않아도 OK)
        volatility_contracting = (t1_range > t2_range * P["range_contract_mult"] and t2_range > t3_range * P["range_contract_mult"])

        # 거래량도 감소 경향인지
        volume_contracting = (t1_vol > t2_vol * P["vol_contract_mult"] and t2_vol > t3_vol * P["vol_contract_mult"])

        # 마지막 수축이 충분히 타이트한지 (완화)
        is_tight = t3_range < P["is_tight_max"]

        # === 3단계: 베이스 높이 확인 ===

        # 전체 베이스 깊이
        base_high = idc[-50:].high.max()
        base_low = idc[-50:].low.min()
        base_depth = (base_high - base_low) / base_high * 100 if base_high > 0 else 0

        # === 4단계: 이동평균선 지지 ===

        # 50일선 상승 중 또는 평탄
        ma50_rising = idc[-1].sma50 >= idc[-10].sma50 * P["ma50_mult"]

        # 현재가가 주요 이평선 위
        above_ma = idc[-1].close > idc[-1].sma20

        # === 5단계: 최종 조건 (완화) ===

        return bool(volatility_contracting
                    and volume_contracting
                    and is_tight
                    and base_depth <= P["base_depth_max"]
                    and ma50_rising
                    and above_ma
                    and has_rs
                    and rsS >= P["rs_min"]
                    and idc[-1].close > base_high * P["base_breakout_mult"])
    except Exception:
        return False


@pattern("p55", group="차트패턴", name="Flat Base",
         min_bars=100, pre_base_gain_min=10, base_range_pct_max=25,
         vol_dried_mult=0.85, vol_increasing_mult=1.05, ma_setup_mult=0.95,
         base_above_ma20_mult=0.90, near_breakout_mult=0.85, rs_min=70,
         ma50_support_mult=0.95)
def _p55(d, P):
    if len(d) < P["min_bars"]:
        return False
    try:
        idc = d.iloc
        rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)

        # === 1단계: 이전 상승 추세 확인 ===

        # 베이스 전 가격 (70일 전)
        pre_base_price = idc[-70].close if len(d) >= 70 else idc[-50].close

        # 베이스 시작점 (35일 전)
        base_start_price = idc[-35].close

        # 베이스 전 상승률
        if pre_base_price > 0:
            pre_base_gain = (base_start_price - pre_base_price) / pre_base_price * 100
        else:
            pre_base_gain = 0

        # === 2단계: Flat Base 확인 (최근 15~35일) ===

        # 베이스 구간 고점/저점
        base_high = idc[-35:-1].high.max()
        base_low = idc[-35:-1].low.min()

        # 베이스 변동폭
        if base_high > 0:
            base_range_pct = (base_high - base_low) / base_high * 100
        else:
            base_range_pct = 100

        # 현재가 위치
        current_position = (idc[-1].close - base_low) / (base_high - base_low) if (base_high - base_low) > 0 else 0

        # === 3단계: 거래량 패턴 ===

        # 베이스 전 거래량 (70~35일 전)
        vol_pre_base = idc[-70:-35].volume.mean() if len(d) >= 70 else idc[-50:-25].volume.mean()

        # 베이스 중 거래량 (35~8일 전)
        vol_during_base = idc[-35:-8].volume.mean()

        # 최근 거래량 (최근 8일)
        vol_recent = idc[-8:].volume.mean()

        # 거래량 감소 후 증가 (더욱 완화)
        vol_dried = vol_during_base < vol_pre_base * P["vol_dried_mult"]
        vol_increasing = vol_recent > vol_during_base * P["vol_increasing_mult"]

        # === 4단계: 이동평균선 배열 ===

        # 주요 이평선 정배열 또는 지지 (더욱 완화)
        ma_setup = idc[-1].close > idc[-1].sma50 * P["ma_setup_mult"]

        # 베이스가 20일선 근처에서 형성 (더욱 완화)
        base_above_ma20 = base_low > idc[-25].sma20 * P["base_above_ma20_mult"]

        # === 5단계: 돌파 확인 ===

        # 최근 고점 테스트 중 (더욱 완화)
        near_breakout = idc[-1].close > base_high * P["near_breakout_mult"]

        # 피벗 포인트
        pivot = base_high

        # === 6단계: 최종 조건 (더욱 완화) ===

        return bool(pre_base_gain >= P["pre_base_gain_min"]
                    and base_range_pct <= P["base_range_pct_max"]
                    and vol_dried
                    and ma_setup
                    and base_above_ma20
                    and has_rs
                    and rsS >= P["rs_min"]
                    and near_breakout
                    and idc[-1].sma50 >= idc[-10].sma50 * P["ma50_support_mult"])
    except Exception:
        return False


@pattern("p61", group="준비단계", name="Pre-Rally Setup",
         min_bars=120, band20_q_max=0.20, pos_lo=0.10, pos_hi=0.50,
         dmi_diff_min=3, adx_min=20, ma20_dist_min=-10, ma20_dist_max=10,
         mfi_lo=40, mfi_hi=90, willr_lo=-80, willr_hi=-20,
         vol_avg_mult=0.95, support_min=2)
def _p61(d, P):
    if len(d) < P["min_bars"]:
        return False
    try:
        idc = d.iloc
        current_price = idc[-1].close

        # === 핵심 조건 6개 (모두 AND - 필수) ===

        # 1. Band Squeeze: < 0.20 (완화)
        if not ('band20_q' in d.columns and idc[-1].band20_q < P["band20_q_max"]):
            return False

        # 2. 가격 위치: 0.10-0.50 (완화)
        if not all(col in d.columns for col in ['close', 'max20', 'min20']):
            return False
        if (idc[-1].max20 - idc[-1].min20) <= 0:
            return False
        position = (current_price - idc[-1].min20) / (idc[-1].max20 - idc[-1].min20)
        if not (P["pos_lo"] <= position <= P["pos_hi"]):
            return False

        # 3. DMI 우위: DI+ - DI- > 3 (완화)
        if not all(col in d.columns for col in ['di_P', 'di_M']):
            return False
        if not (idc[-1].di_P - idc[-1].di_M > P["dmi_diff_min"]):
            return False

        # 4. ADX: > 20 (완화)
        if not ('adx' in d.columns and idc[-1].adx > P["adx_min"]):
            return False

        # 5. MA20 근처: ±10% (완화)
        if not ('sma20' in d.columns and idc[-1].sma20 > 0):
            return False
        ma20_dist_pct = (current_price - idc[-1].sma20) / idc[-1].sma20 * 100
        if not (P["ma20_dist_min"] <= ma20_dist_pct <= P["ma20_dist_max"]):
            return False

        # 6. MFI: 40-90 (완화)
        if not ('mfi' in d.columns and P["mfi_lo"] <= idc[-1].mfi <= P["mfi_hi"]):
            return False

        # === 보조 조건 (3개 중 2개 이상 만족) ===

        support_count = 0

        # 7. Williams %R: -80 ~ -20 (선택)
        if 'willr' in d.columns and P["willr_lo"] <= idc[-1].willr <= P["willr_hi"]:
            support_count += 1

        # 8. 이평선 정배열: 20>50>120 (선택)
        if all(col in d.columns for col in ['sma20', 'sma50', 'sma120']):
            if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120:
                support_count += 1

        # 9. 거래량 증가: 5일 > 20일 (선택, 완화)
        if all(col in d.columns for col in ['vol_sum5', 'vol_sum20']):
            vol_avg5 = idc[-1].vol_sum5 / 5
            vol_avg20 = idc[-1].vol_sum20 / 20
            if vol_avg5 > vol_avg20 * P["vol_avg_mult"]:  # 거의 동등 이상이면 OK
                support_count += 1

        # 보조 조건 2개 이상 만족 시 선정
        if support_count >= P["support_min"]:
            return True
        return False
    except Exception:
        return False


@pattern("p71", group="변동성", name="mtr7 급확대 + 정배열",
         n8=8, mtr7_mult=1.5)
def _p71(d, P):
    if len(d) < P["n8"]:
        return False
    try:
        r = d.iloc[-1]
        return bool(r.mtr7 > d.iloc[-P["n8"]].atr14 * P["mtr7_mult"]
                    and r.close < r.max5
                    and ma_aligned(r))
    except Exception:
        return False


@pattern("p81", group="이격", name="밴드·CSI 동반 확대",
         n2=2)
def _p81(d, P):
    if len(d) < P["n2"]:
        return False
    try:
        r = d.iloc[-1]
        prev = d.iloc[-P["n2"]]
        return bool(r.band20_q > prev.band20_q and r.csi > prev.csi)
    except Exception:
        return False


@pattern("p91", group="RS", name="rs50 상위 + rs20 중위",
         rs50_min=95, rs20_hi=95, rs20_lo=50, rs_min=90)
def _p91(d, P):
    try:
        rs20S, rs50S, rsS, has_rs = _extract_rs(d, P)
        if not has_rs:
            return False
        return bool(rs50S > P["rs50_min"]
                    and rs20S < P["rs20_hi"]
                    and rs20S > P["rs20_lo"]
                    and rsS > P["rs_min"])
    except Exception:
        return False


@pattern("p92", group="RS", name="밴드스퀴즈 + 신고가",
         band_squeeze_q2=0.2, atr_q_max=0.2, n11=11, n21=21, n51=51)
def _p92(d, P):
    if len(d) < P["n51"]:
        return False
    try:
        r = d.iloc[-1]
        cond_break = (r.max10 > d.iloc[-P["n11"]].max50 or r.max10 > d.iloc[-P["n11"]].max125
                      or r.max20 > d.iloc[-P["n21"]].max50 or r.max20 > d.iloc[-P["n21"]].max125
                      or r.max50 > d.iloc[-P["n51"]].max50 or r.max50 > d.iloc[-P["n51"]].max125)
        return bool(r.band20_q < P["band_squeeze_q2"]
                    and r.atr_q < P["atr_q_max"]
                    and cond_break)
    except Exception:
        return False


@pattern("p93", group="RS", name="밴드 확장 초입",
         band20_q_max=0.8, n6=6)
def _p93(d, P):
    if len(d) < P["n6"]:
        return False
    try:
        r = d.iloc[-1]
        return bool(r.band20_q < P["band20_q_max"] and r.band20_q > d.iloc[-P["n6"]].band20_q)
    except Exception:
        return False


def screen_all(indicators_data, rs_df=None, **ctx):
    """종목 스크리닝 (screening_loop.py 내장). SCREEN_FN 미설정 시 일봉 기본."""
    cfg = _resolve_screen_cfg(ctx)
    volume_data = ctx.get("volume_data") or {}
    ticker_list = ctx.get("ticker_list")
    audit_list = ctx.get("audit_ticker", audit_ticker)
    if rs_df is None:
        rs_df = ctx.get("rs_df")
    if rs_df is None:
        rs_df = pd.DataFrame(columns=["rs10_score", "rs20_score", "rs50_score", "rs_score"])
    ctx["rs_df"] = rs_df
    debug_counts = {"total_indicators": len(indicators_data), "passed_basic_filter": 0, "passed_atr_filter": 0, "selected_by_pattern": 0}
    selected_stocks = []
    _buckets = defaultdict(list)

    try:
        _mc = pd.to_numeric(ticker_list['시가총액'], errors='coerce')
        print(f"[filter] DISPLAY_MCAP_MIN={DISPLAY_MCAP_MIN:,}")
        print(f"[filter] 시가총액 n={_mc.notna().sum()} min={_mc.min():,.0f} "
              f"med={_mc.median():,.0f} max={_mc.max():,.0f}")
        print(f"[filter] 기준 통과 종목수={(_mc >= DISPLAY_MCAP_MIN).sum()}")
        print(f"[filter] min_bars={cfg['min_bars']} audit_list={len(audit_list)}건")
    except Exception as e:
        print(f"[filter] 진단 실패: {e}")

    _basic_sub = {"bars": 0, "open": 0, "mcap": 0, "audit": 0}

    for k, i in tqdm(indicators_data.items(), desc="스크리닝"):
        idc = i.iloc
        rs20S, rs50S, rsS = 0, 0, 0
        _tk = str(k)
        if _tk not in rs_df.index:
            pass
        else:
            try:
                _rt = str(i.iloc[-1].ticker)
                if _rt in rs_df.index:
                    rs20S = rs_df.loc[_rt].rs20_score
                    rs50S = rs_df.loc[_rt].rs50_score
                    rsS = rs_df.loc[_rt].rs_score
            except Exception:
                pass

        _c_bars = len(i) >= cfg["min_bars"]
        _c_open = i.iloc[-1].open > 0
        try:
            _c_mcap = ticker_list.loc[_tk]['시가총액'] >= DISPLAY_MCAP_MIN
        except Exception:
            _c_mcap = False
        _c_audit = _tk not in audit_list
        _basic_sub["bars"] += int(_c_bars)
        _basic_sub["open"] += int(_c_open)
        _basic_sub["mcap"] += int(_c_mcap)
        _basic_sub["audit"] += int(_c_audit)

        if _c_bars and _c_open and _c_mcap and _c_audit:
                debug_counts['passed_basic_filter'] += 1
                ticker = i.iloc[0].ticker
                close = i.iloc[-1].close

                if (idc[-1].atr14/close) < cfg["atr14_close_max"] and idc[-1].mtr7/close < cfg["mtr7_close_max"] and idc[-1].box7/close < cfg["box7_close_max"]:
                    debug_counts['passed_atr_filter'] += 1

                    _p29_matched = False
                    for _code in (
                        "p11", "p12", "p13", "p14", "p15", "p16", "p17",
                        "p21", "p22", "p23", "p24", "p25", "p26", "p27", "p28", "p29a", "p29b",
                        "p31", "p32", "p33", "p34", "p35", "p36",
                        "p41", "p42", "p43",
                        "p51", "p52", "p53",
                        "p54", "p55",
                        "p61", "p71", "p81", "p91", "p92", "p93",
                    ):
                        if _code not in PATTERN_REGISTRY:
                            continue
                        if _code in DISABLED_PATTERNS:
                            continue
                        if _code == "p29a" and len(_buckets["p29a"]) >= 49:
                            continue
                        if _code == "p29b" and (len(_buckets["p29a"]) < 49 or _p29_matched):
                            continue
                        try:
                            _P = _pat_params(_code, ctx)
                            _P.setdefault("rs_df", rs_df)      # screen_all 이 확정한 로컬 rs_df 우선
                            _P.setdefault("volume_data", volume_data)
                            _ok = PATTERN_REGISTRY[_code]["fn"](i, _P)
                        except Exception:
                            _pat_exc(_code)
                            _ok = False
                        if _ok:
                            if _code in ("p29a", "p29b"):
                                _p29_matched = True
                            _buckets[_code].append(i)
                            if _code in REFERENCE_PATTERNS:
                                continue
                            listed = i.iloc[-1][['ticker', 'name']].to_list()
                            listed.insert(0, 'p29' if _code in ('p29a', 'p29b') else _code)
                            listed.insert(0, i.index[-1])
                            selected_stocks.append(listed)

    print(f"[filter] 서브조건 통과 {_basic_sub}")

    result = {"p11": [x for x in _buckets["p11"] if hasattr(x,"iloc")], "p12": [x for x in _buckets["p12"] if hasattr(x,"iloc")], "p13": [x for x in _buckets["p13"] if hasattr(x,"iloc")], "p14": [x for x in _buckets["p14"] if hasattr(x,"iloc")], "p15": [x for x in _buckets["p15"] if hasattr(x,"iloc")], "p16": [x for x in _buckets["p16"] if hasattr(x,"iloc")], "p17": [x for x in _buckets["p17"] if hasattr(x,"iloc")], "p21": [x for x in _buckets["p21"] if hasattr(x,"iloc")], "p22": [x for x in _buckets["p22"] if hasattr(x,"iloc")], "p23": [x for x in _buckets["p23"] if hasattr(x,"iloc")], "p24": [x for x in _buckets["p24"] if hasattr(x,"iloc")], "p25": [x for x in _buckets["p25"] if hasattr(x,"iloc")], "p26": [x for x in _buckets["p26"] if hasattr(x,"iloc")], "p27": [x for x in _buckets["p27"] if hasattr(x,"iloc")], "p28": [x for x in _buckets["p28"] if hasattr(x,"iloc")], "p29a": [x for x in _buckets["p29a"] if hasattr(x,"iloc")], "p29b": [x for x in _buckets["p29b"] if hasattr(x,"iloc")], "p31": [x for x in _buckets["p31"] if hasattr(x,"iloc")], "p32": [x for x in _buckets["p32"] if hasattr(x,"iloc")], "p33": [x for x in _buckets["p33"] if hasattr(x,"iloc")], "p34": [x for x in _buckets["p34"] if hasattr(x,"iloc")], "p35": [x for x in _buckets["p35"] if hasattr(x,"iloc")], "p36": [x for x in _buckets["p36"] if hasattr(x,"iloc")], "p41": [x for x in _buckets["p41"] if hasattr(x,"iloc")], "p42": [x for x in _buckets["p42"] if hasattr(x,"iloc")], "p43": [x for x in _buckets["p43"] if hasattr(x,"iloc")], "p51": [x for x in _buckets["p51"] if hasattr(x,"iloc")], "p52": [x for x in _buckets["p52"] if hasattr(x,"iloc")], "p53": [x for x in _buckets["p53"] if hasattr(x,"iloc")], "p54": [x for x in _buckets["p54"] if hasattr(x,"iloc")], "p55": [x for x in _buckets["p55"] if hasattr(x,"iloc")], "p61": [x for x in _buckets["p61"] if hasattr(x,"iloc")], "p71": [x for x in _buckets["p71"] if hasattr(x,"iloc")], "p81": [x for x in _buckets["p81"] if hasattr(x,"iloc")], "p91": [x for x in _buckets["p91"] if hasattr(x,"iloc")], "p92": [x for x in _buckets["p92"] if hasattr(x,"iloc")], "p93": [x for x in _buckets["p93"] if hasattr(x,"iloc")]}

    debug_counts["selected_by_pattern"] = len(selected_stocks)

    if len(selected_stocks) > 0:
        selected_df = pd.DataFrame(
            selected_stocks, columns=["date", "type", "ticker", "company"]
        )
    else:
        selected_df = pd.DataFrame(columns=["date", "type", "ticker", "company"])

    selected_stock_list = ["p35"] + result.get("p35", [])

    _cnt = {k: len(v) for k, v in result.items()}
    _hit = {k: v for k, v in _cnt.items() if v}
    print("=" * 80)
    print(f"📊 패턴별 선정 건수 (합계 {sum(_cnt.values())} / 발동 {len(_hit)}개 패턴)")
    for _k in sorted(_cnt):
        _st = _sector_type(_k)
        _suffix = f"·{_st}" if _st and _st != "미판정" else ""
        if _k in DISABLED_PATTERNS:
            print(f"   · {_k}: - [폐기{_suffix}]")
        elif _k in REFERENCE_PATTERNS:
            print(f"   · {_k}: {_cnt[_k]}건 [참고{_suffix}]")
        else:
            _g = PATTERN_GRADE.get(_k)
            if _g:
                print(f"   · {_k}: {_cnt[_k]}건 [{_g}{_suffix}]")
            elif _st and _st != "미판정":
                print(f"   · {_k}: {_cnt[_k]}건 [{_st}]")
            else:
                print(f"   · {_k}: {_cnt[_k]}건")
    _sec_n = {"종목": 0, "혼합": 0, "업종": 0, "미분류": 0}
    for _k, _n in _cnt.items():
        if _k in DISABLED_PATTERNS or _k in REFERENCE_PATTERNS:
            continue
        _st = _sector_type(_k)
        if _st in ("종목", "혼합", "업종"):
            _sec_n[_st] += _n
        else:
            _sec_n["미분류"] += _n
    if sum(_sec_n.values()) > 0:
        print(
            f"   ▶ 섹터성격 구성: 종목 {_sec_n['종목']}건 / 혼합 {_sec_n['혼합']}건"
            f" / 업종 {_sec_n['업종']}건 / 미분류 {_sec_n['미분류']}건"
        )
    print("=" * 80)

    return {
        "selected_stock_list": selected_stock_list,
        "selected_df": selected_df,
        "selected_stocks": selected_stocks,
        "result": result,
        "debug_counts": debug_counts,
    }



# (파이프라인 실행부는 run_main()으로 이동)


def gen_chart(df, typeP, sector_df, rs_df, period, money, risk, save_jpeg=False, trade_data=None):
    ticker = str(df.iloc[0].ticker)
    # print(f"[INFO] 차트 생성 시작: {ticker}")  # tqdm 진행 상태 바와 충돌 방지
    
    # df 인덱스 처리: 날짜 형식으로 통일
    # 먼저 현재 상태 확인
    original_index_is_date = isinstance(df.index, pd.DatetimeIndex) or (df.index.name == 'date')
    
    if 'date' in df.columns:
        # 'date' 컬럼이 있으면 이를 인덱스로 설정
        df = df.set_index('date')
    elif not original_index_is_date:
        # 인덱스가 날짜가 아니면 날짜로 변환 시도
        try:
            df.index = pd.to_datetime(df.index)
        except:
            # 실패하면 reset_index 후 date 컬럼 찾기
            df = df.reset_index()
            if 'date' in df.columns:
                df = df.set_index('date')
            else:
                raise ValueError(f"날짜 컬럼을 찾을 수 없습니다. 컬럼: {df.columns.tolist()}")
    
    # 날짜 형식을 DatetimeIndex로 확실히 변환
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors='coerce')
        # 변환 실패한 날짜 제거
        df = df[df.index.notna()]
    
    df = df.tail(period)
    df, _has_investor_osc = _ensure_investor_osc_on_df(df, ticker)

    # 데이터 유효성 검사
    if df.empty:
        print(f"[ERROR] 티커 {ticker}: df가 비어있습니다.")
        return
    
    # 필요한 컬럼 확인
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"[ERROR] 티커 {ticker}: 필수 컬럼이 없습니다: {missing_columns}")
        return
    
    # 디버깅 정보 출력 (주석 처리 - tqdm 진행 상태 바와 충돌 방지)
    # print(f"[DEBUG] 티커 {ticker}: df.shape={df.shape}, 인덱스 타입={type(df.index)}, 인덱스 범위={df.index.min()} ~ {df.index.max()}")
    
    # sector_df 인덱스 처리: 날짜 형식으로 통일
    original_sector_index_is_date = isinstance(sector_df.index, pd.DatetimeIndex) or (sector_df.index.name == 'date')
    
    if 'date' in sector_df.columns:
        sector_df = sector_df.set_index('date')
    elif not original_sector_index_is_date:
        try:
            sector_df.index = pd.to_datetime(sector_df.index)
        except:
            sector_df = sector_df.reset_index()
            if 'date' in sector_df.columns:
                sector_df = sector_df.set_index('date')
            else:
                sector_df.index.name = 'date'
                sector_df.index = pd.to_datetime(sector_df.index, errors='coerce')
                sector_df = sector_df[sector_df.index.notna()]
    
    # 날짜 형식을 DatetimeIndex로 확실히 변환
    if not isinstance(sector_df.index, pd.DatetimeIndex):
        sector_df.index = pd.to_datetime(sector_df.index, errors='coerce')
        sector_df = sector_df[sector_df.index.notna()]
    
    sector_df = sector_df.tail(period)
    
    # sector_df 유효성 검사
    if sector_df.empty:
        print(f"[WARN] 티커 {ticker}: sector_df가 비어있습니다.")
    
    # 인덱스 정렬 (날짜 순서대로)
    df = df.sort_index()
    sector_df = sector_df.sort_index()
    
    # 인덱스 교집합 확인
    common_dates = df.index.intersection(sector_df.index)
    if len(common_dates) == 0:
        print(f"[WARN] 티커 {ticker}: df와 sector_df의 날짜가 겹치지 않습니다.")
        # print(f"[DEBUG] df 날짜 범위: {df.index.min()} ~ {df.index.max()}")
        # print(f"[DEBUG] sector_df 날짜 범위: {sector_df.index.min()} ~ {sector_df.index.max()}")
    
    # RS 점수 가져오기 (에러 처리 추가)
    if not USE_RS:
        rs20 = rs50 = rs = "-"
    else:
        try:
            rs20 = float(rs_df.loc[ticker].rs20_score)
            rs50 = float(rs_df.loc[ticker].rs50_score)
            rs = float(rs_df.loc[ticker].rs_score)
        except (KeyError, AttributeError):
            rs20 = rs50 = rs = 0.0
    
    try:
        if 'market_cap' in df.columns and pd.notna(df.iloc[0]['market_cap']):
            market_cap_value = df.iloc[0]['market_cap']
            if isinstance(market_cap_value, (int, float)) and market_cap_value > 0:
                market_cap_billion = market_cap_value / 1000000000
                if market_cap_billion >= 1000:
                    market_cap_str = f"{market_cap_billion/1000:.1f}조"
                else:
                    market_cap_str = f"{market_cap_billion*10:.0f}억"
                market_cap_display = f" | 시총: {market_cap_str}"
            else:
                market_cap_display = ""
        else:
            market_cap_display = ""
    except (KeyError, AttributeError, IndexError):
        market_cap_display = ""

    sector_name = df.iloc[0].get('sector', '') if 'sector' in df.columns else ''
    cpN = df.iloc[0]['name'] + '(' + ticker + ')' + market_cap_display + ' ' + typeP
    if sector_name:
        cpN += ' | ' + str(sector_name)

    fileN = ticker + OUTPUT_SUFFIX + '.html'
    fileN_jpeg = ticker + OUTPUT_SUFFIX + '.jpeg' if save_jpeg else None
    
    # 테마 정보 조회
    theme_title_segment = ''
    try:
        theme_query = """
            SELECT 
                ts.theme_code,
                COALESCE(ts.theme_name, t.theme_name) AS theme_name,
                t.change_rate,
                t.recent_3days_change_rate
            FROM krx_theme_stock ts
            LEFT JOIN krx_theme t ON ts.theme_code = t.theme_code
            WHERE ts.ticker = %(ticker)s
        """
        theme_df = pd.read_sql_query(theme_query, con=engine, params={'ticker': ticker})

        if not theme_df.empty:
            def fmt_rate(value):
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    return '-'
                try:
                    return f"{float(value):.2f}"
                except (TypeError, ValueError):
                    return '-'

            theme_entries = []
            for _, row in theme_df.iterrows():
                theme_name = row.get('theme_name') or row.get('theme_code') or ''
                if not theme_name:
                    continue
                one_day = fmt_rate(row.get('change_rate'))
                three_day = fmt_rate(row.get('recent_3days_change_rate'))
                theme_entries.append(f"{theme_name}({one_day}%/{three_day}%)")

            if theme_entries:
                theme_title_segment = " " + " · ".join(theme_entries[:3])
    except Exception as e:
        print(f"[WARN] 테마 정보 조회 실패 ({ticker}): {e}")
        theme_title_segment = ''
    
    # 업종 정보 조회
    industry_title_segment = ''
    try:
        industry_query = """
            SELECT 
                i.industry_code,
                COALESCE(is_stock.industry_name, i.industry_name) AS industry_name,
                i.change_rate,
                i.recent_3days_change_rate,
                i.up_count,
                i.same_count,
                i.down_count,
                i.leading_stock1_name,
                i.leading_stock2_name
            FROM krx_industry_stock is_stock
            LEFT JOIN krx_industry i ON is_stock.industry_code = i.industry_code
            WHERE is_stock.ticker = %(ticker)s
            ORDER BY i.update_date DESC
            LIMIT 1
        """
        industry_df = pd.read_sql_query(industry_query, con=engine, params={'ticker': ticker})

        if not industry_df.empty:
            def fmt_rate(value):
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    return '-'
                try:
                    return f"{float(value):+.2f}"
                except (TypeError, ValueError):
                    return '-'
            
            def fmt_count(value):
                if value is None or (isinstance(value, float) and np.isnan(value)):
                    return '0'
                try:
                    return f"{int(value)}"
                except (TypeError, ValueError):
                    return '0'

            row = industry_df.iloc[0]
            industry_name = row.get('industry_name') or row.get('industry_code') or ''
            
            if industry_name:
                one_day = fmt_rate(row.get('change_rate'))
                three_day = fmt_rate(row.get('recent_3days_change_rate'))
                up_count = fmt_count(row.get('up_count'))
                same_count = fmt_count(row.get('same_count'))
                down_count = fmt_count(row.get('down_count'))
                leading1 = row.get('leading_stock1_name') or ''
                leading2 = row.get('leading_stock2_name') or ''
                
                # 업종명(등락률) 형식으로 구성
                industry_info = f"{industry_name}({one_day}%)"
                
                # 상승/보합/하락 종목수 추가
                if up_count != '0' or same_count != '0' or down_count != '0':
                    industry_info += f" [상:{up_count} 보:{same_count} 하:{down_count}]"
                
                # 주도주 정보 추가 (있는 경우)
                leading_stocks = []
                if leading1:
                    leading_stocks.append(leading1)
                if leading2:
                    leading_stocks.append(leading2)
                if leading_stocks:
                    industry_info += f" 주도주:{'/'.join(leading_stocks)}"
                
                industry_title_segment = " " + industry_info
    except Exception as e:
        print(f"[WARN] 업종 정보 조회 실패 ({ticker}): {e}")
        import traceback
        traceback.print_exc()
        industry_title_segment = ''

    def has_nonzero(series):
        """시리즈에 0이 아닌 값이 하나라도 있는지 확인"""
        if series is None:
            return False
        return pd.Series(series).dropna().ne(0).any()

    def _line_trace(col, name, **line_kw):
        if col not in df.columns:
            return None
        return go.Scatter(x=df.index, y=df[col], name=name, line=line_kw)

    def _title_level(col):
        if col not in df.columns:
            return "-"
        v = df.iloc[-1][col]
        return "-" if pd.isna(v) else str(v)
    
    # 캔들스틱 (한국식 색상: 상승=빨강, 하락=파랑)
    candle = go.Candlestick(x=df.index,
                            open=df.open,
                            high=df.high,
                            low=df.low,
                            close=df.close,
                            increasing_line_color='#FF4136', 
                            decreasing_line_color='#0074D9',
                            name='Price')
    
    # candle_w = go.Candlestick(x=df_w.index,
    #                         open=df_w.open,
    #                         high=df_w.high,
    #                         low=df_w.low,
    #                         close=df_w.close,
    #                         increasing_line_color= 'red', decreasing_line_color= 'blue')
    
    
    
    # 거래량 (회색 계열, 투명도 적용)
    volume = go.Bar(x=df.index, y=df.volume, name="Volume", 
                    marker=dict(color='rgba(128, 128, 128, 0.3)'))
    vol_sma20 = go.Scatter(x=df.index, y=df.vol_sma20, name="Vol SMA20",
                           line=dict(color='#FF6B6B', width=1.5, dash='dot'))
    vol_sma50 = go.Scatter(x=df.index, y=df.vol_sma50, name="Vol SMA50",
                           line=dict(color='#4ECDC4', width=1.5, dash='dot'))
    
    # 이동평균선 (중요도별 색상 및 두께 차별화)
    
    sma10 = go.Scatter(x=df.index, y=df.sma10, name='SMA10',
                       line=dict(color='#F38181', width=1.5))
    sma20 = go.Scatter(x=df.index, y=df.sma20, name='SMA20',
                       line=dict(color='#FF6B6B', width=2.5))
    sma50 = go.Scatter(x=df.index, y=df.sma50, name='SMA50',
                       line=dict(color='#4ECDC4', width=2))
    sma150 = go.Scatter(x=df.index, y=df.sma150, name='SMA150',
                        line=dict(color='#95E1D3', width=1.5))
    sma200 = go.Scatter(x=df.index, y=df.sma200, name='SMA200',
                        line=dict(color='#F38181', width=1.5))
    
    ent_p = _line_trace('ent_p', '매입가',
                        color='#FFA500', width=2, dash='dashdot')
    
    ch_up1 = go.Scatter(x=df.index, y=df.ch_up1, name='CH UP1',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    ch_dn1 = go.Scatter(x=df.index, y=df.ch_dn1, name='CH DN1',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))    
    
    ch_up2 = go.Scatter(x=df.index, y=df.ch_up2, name='CH UP2',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    ch_dn2 = go.Scatter(x=df.index, y=df.ch_dn2, name='CH DN2',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    
    ch_up3 = go.Scatter(x=df.index, y=df.ch_up3, name='CH UP3',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    ch_dn3 = go.Scatter(x=df.index, y=df.ch_dn3, name='CH DN3',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    
    ch_up4 = go.Scatter(x=df.index, y=df.ch_up4, name='CH UP4',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    ch_dn4 = go.Scatter(x=df.index, y=df.ch_dn4, name='CH DN4',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    
    # 볼린저 밴드 (보라색 계열, 반투명)
    bol_up = go.Scatter(x=df.index, y=df.bol20_up, name='BB Up',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    bol_dn = go.Scatter(x=df.index, y=df.bol20_dn, name='BB Down',
                        line=dict(color='rgba(147, 51, 234, 0.6)', width=1, dash='dash'))
    
    
    atr14 = go.Scatter(x=df.index, y=df.atr14, name='ATR14',
                       line=dict(color='#16A085', width=2))
    atr4 = go.Scatter(x=df.index, y=df.atr4, name='ATR4',
                      line=dict(color='#3498DB', width=1.5))    
    mtr14 = go.Scatter(x=df.index, y=df.mtr14, name='MTR14',
                       line=dict(color='#9B59B6', width=1.5))
    box14 = go.Scatter(x=df.index, y=df.box14, name='BOX14',
                       line=dict(color='#FF6B35', width=2))

    # atrL = go.Scatter(x=df.index, y=df.atr56, name=' ATR56')
        

    _stop_line = dict(color='rgba(239, 83, 80, 0.7)', width=2, dash='dashdot')
    _take_line = dict(color='rgba(38, 166, 154, 0.7)', width=2, dash='dashdot')
    stop_loss = _line_trace('stop_loss', 'stop loss', **_stop_line)
    stop_atr = _line_trace('stop_atr', 'stop atr', **_stop_line)
    stop_h = _line_trace('stop_h', 'stop Helicopter', **_stop_line)
    take_p = _line_trace('take_profit', 'take profit', **_take_line)
    top = _line_trace('top', 'top',
                      color='rgba(255, 107, 107, 0.6)', width=1.5, dash='dash')
    bottom = _line_trace('bottom', 'bottom',
                         color='rgba(78, 205, 196, 0.6)', width=1.5, dash='dash')
    take_p2 = _line_trace('take_profit2', 'take profit2', **_take_line)
    

    band20_w = go.Scatter(x=df.index, y=df.band20_w, name='Band20',
                          line=dict(color='#16A085', width=2))
    band50_w = go.Scatter(x=df.index, y=df.band50_w, name='Band50',
                          line=dict(color='#3498DB', width=1.5))
    band120_w = go.Scatter(x=df.index, y=df.band120_w, name='Band120',
                           line=dict(color='#9B59B6', width=1.5))
    
    dsprt200 = _line_trace('dsprt200_up1', 'dsprt 200', color='#0096FF', width=1.5)
    dsprt50 = _line_trace('dsprt50_up1', 'dsprt 50', color='#9B59B6', width=1.5)
    dsprt120 = _line_trace('dsprt120_up1', 'dsprt 120', color='#A29BFE', width=1.5)
    
    # DMI/ADX (강렬한 색상으로 명확하게)
    di_P = go.Scatter(x=df.index, y=df.di_P, name='DI+',
                      line=dict(color='#2ECC71', width=2))
    di_M = go.Scatter(x=df.index, y=df.di_M, name='DI-',
                      line=dict(color='#E74C3C', width=2))
    
    adx = go.Scatter(x=df.index, y=df.adx, name='ADX',
                     line=dict(color='#F39C12', width=2.5))
    
    # MACD (대비되는 색상)
    macd = go.Scatter(x=df.index, y=df.macd, name='MACD',
                      line=dict(color='#00B4DB', width=2))
    macdS = go.Scatter(x=df.index, y=df.macdsignal, name='Signal',
                       line=dict(color='#FF6B6B', width=2))
    macdH = go.Bar(x=df.index, y=df.macdhist, name='Histogram',
                   marker=dict(color=df.macdhist.apply(lambda x: '#26A69A' if x > 0 else '#EF5350')))
    
    
    # boxR_ul = go.Scatter(x=df.index, y=df.boxR_ul, name='BOXR upper lower')
    
    ## 지수 그래프 - 단일 색상 계열 (파란색 그라데이션)
    # 종목: 청록색(Cyan), 섹터: 파란색 명도 그라데이션
    sector_colors_gradient = ['#1E88E5', '#42A5F5', '#64B5F6', '#90CAF9']  # 진한 파랑 -> 연한 파랑
    
    # 정규화 (기준일 대비 100)
    for c in sector_df.columns:
        sector_df[c] = (sector_df[c]/sector_df.iloc[0][c])*100
    
    # 10일, 20일, 50일, 120일 모멘텀(ROC) 산출
    mom_periods = [10, 20, 50, 120]
    mom_df = pd.DataFrame()
    for c in sector_df.columns:
        mom_df[c+'_mom'] = talib.ROC(sector_df[c],WIN_2)
    mom_df_10 = mom_df.tail(1).copy()
    
    mom_by_period = {}  # period -> DataFrame (columns = sector, one row)
    for period in mom_periods:
        m = pd.DataFrame()
        for c in sector_df.columns:
            m[c+'_mom'] = talib.ROC(sector_df[c],period)
        mom_by_period[period] = m.tail(1)
    
    # 기존 로직: 10일 모멘텀으로 상위 섹터 선택 (현재는 종목+코스피/코스닥 2개만 있음)
    mom_dff = mom_df_10.transpose()
    mom_dff = mom_dff.sort_values(by=mom_dff.columns[0], ascending=True)
    mom_dff.columns = ['mom']
    
    stock_name = sector_df.columns[0]
    stock_mom_name = stock_name + '_mom'
    top4_sectors_mom = mom_dff[mom_dff.index != stock_mom_name].nlargest(4, 'mom').index.tolist()
    top4_sectors = [s.replace('_mom', '') for s in top4_sectors_mom]
    selected_columns = [stock_name] + top4_sectors
    sector_df = sector_df[[c for c in selected_columns if c in sector_df.columns]]
    
    mom_order = [c + '_mom' for c in sector_df.columns]
    mom_dff = mom_dff.loc[mom_order] if all(x in mom_dff.index for x in mom_order) else mom_dff
    mom_dff = mom_dff.sort_values(by='mom', ascending=True)
    
    # 10·20·50·120일 모멘텀 통합 (오른쪽 Momentum 영역용, 120일→50일→20일→10일 순)
    mom_combined = []
    for period in reversed(mom_periods):
        mp = mom_by_period[period]
        for c in sector_df.columns:
            col_mom = c + '_mom'
            if col_mom in mp.columns:
                val = mp[col_mom].iloc[-1] if len(mp) > 0 else np.nan
                mom_combined.append({'label': f'{period}일_{c}', 'mom': float(val) if pd.notna(val) else 0.0})
    mom_dff_all = pd.DataFrame(mom_combined).set_index('label')
    
    # 색상 매핑 딕셔너리 생성 (1x1과 1x2에서 동일하게 사용)
    color_mapping = {}
    sector_graphs = []
    for idx, c in enumerate(sector_df.columns):
        if idx == 0:  # 첫 번째 항목(종목)은 눈에 띄게 - 청록색
            color = '#00BCD4'  # Material Design Cyan
            width = 3.5
            opacity = 1.0
        else:  # 나머지 섹터들 - 파란색 그라데이션
            color = sector_colors_gradient[(idx - 1) % len(sector_colors_gradient)]
            width = 2.0
            opacity = 0.9
        
        color_mapping[c] = color  # 색상 저장
        
        globals()[c] = go.Scatter(
            x=sector_df.index, 
            y=sector_df[c], 
            name=c,
            line=dict(color=color, width=width),
            opacity=opacity
        )
        sector_graphs.append(globals()[c])
    
    
    
    # Price & Moving Averages 그래프 (기존 그대로 유지)
    price_graphs1 = [candle, sma10, sma20, sma50]
    if ent_p is not None and has_nonzero(df['ent_p']):
        price_graphs1.append(ent_p)
    # price_graphs_w = [candle_w]
    
    # ch_graphs = [ch_up1, ch_dn1, ch_up2, ch_up3, ch_up4]
    stop_graphs = []
    if stop_loss is not None and has_nonzero(df['stop_loss']):
        stop_graphs.append(stop_loss)
    if stop_h is not None and has_nonzero(df['stop_h']):
        stop_graphs.append(stop_h)
    if top is not None and has_nonzero(df['top']):
        stop_graphs.append(top)
    if bottom is not None and has_nonzero(df['bottom']):
        stop_graphs.append(bottom)
    
    # Band Width (band20_q만 사용)
    try:
        band20_q = go.Scatter(x=df.index, y=df.band20_q, name='Band Squeeze',
                              line=dict(color='#9B59B6', width=1.5))
        width_graphs = [band20_q]
    except (KeyError, AttributeError):
        width_graphs = []
    
    # CSI (Disparity) — 투자자 OSC와 별도 행에 표시
    csi_graphs = []
    try:
        for col, label, color in (
            ("csi", "CSI", "#0096FF"),
            ("csi_fast", "CSI Fast", "#E74C3C"),
            ("csi_slow", "CSI Slow", "#7E57C2"),
        ):
            if col in df.columns:
                csi_graphs.append(
                    go.Scatter(x=df.index, y=df[col], name=label,
                               line=dict(color=color, width=1.5))
                )
    except (KeyError, AttributeError, TypeError):
        csi_graphs = []

    _n_rows = 7 if _has_investor_osc else 6
    _csi_row = 6
    _inv_row = 7 if _has_investor_osc else None
    _box_row = 7 if _has_investor_osc else 6
    
    volume_graphs = [volume, vol_sma20, vol_sma50]
    
    macd_graphs = [macdH]

    # 제목 구성 (두 줄로 표시)
    # 첫 줄: 종목명, 테마, 업종 정보
    title_line1 = [cpN]
    if theme_title_segment:
        title_line1.append(theme_title_segment.strip())  # 앞뒤 공백 제거
    if industry_title_segment:
        title_line1.append(industry_title_segment.strip())  # 앞뒤 공백 제거
    
    # 둘째 줄: 기술적 지표 정보만 (들여쓰기)
    try:
        amount = int((money * risk) / float(df.iloc[-1].atr14 * 1.5))
    except (ValueError, ZeroDivisionError, TypeError):
        amount = 0
    try:
        vol_m = round(float(df.iloc[-1].vol_mtr), 2)
    except (ValueError, KeyError, TypeError):
        vol_m = 0.0
    try:
        vol_a = round(float(df.iloc[-1].vol_atr), 2)
    except (ValueError, KeyError, TypeError):
        vol_a = 0.0
    try:
        vol_b = round(float(df.iloc[-1].box7 / df.iloc[-1].close * 100), 2)
    except (ValueError, KeyError, TypeError, ZeroDivisionError):
        vol_b = 0.0

    title_line2 = [
        'Bottom ' + _title_level('bottom') + ' | Top ' + _title_level('top'),
        f'# of trade {amount} | 변동성 (%) mtr {vol_m} , atr {vol_a}, box {vol_b}',
        'SMA ' + str(df.iloc[-1].sma20) + ' | ATR ' + str(df.iloc[-1].atr14),
        'RS20 ' + str(rs20) + ', RS50 ' + str(rs50) + ', RS ' + str(rs)
    ]
    
    # 테마명 첫 글자까지의 들여쓰기 계산
    # "종목명 : 티커 | " 까지의 길이를 계산
    first_part = cpN + ' | '
    indent_length = len(first_part)
    
    # 두 줄을 <br> 태그로 연결, 둘째 줄에 들여쓰기 추가 (테마명 첫 글자 위치까지)
    indent_spaces = "&nbsp;" * indent_length
    title_text = " | ".join(title_line1) + "<br>" + indent_spaces + " | ".join(title_line2)
    
    layout = go.Layout(
        title=title_text,
        autosize=True,
        height=1000 if _n_rows == 7 else 900,
        margin=dict(
            l=0,
            r=0,
            t=80,  # 두 줄 제목을 위해 상단 여백 증가
            b=0,
            pad=0
        )
    )
                       # xaxis = dict(type="category", 
                                    # categoryorder='category ascending'))

    layout2 = go.Layout(xaxis = dict(type="category", 
                                     categoryorder='category ascending'))


    _row_heights = (
        (0.14, 0.32, 0.14, 0.11, 0.11, 0.09, 0.09)
        if _n_rows == 7
        else (0.15, 0.35, 0.15, 0.12, 0.12, 0.11)
    )
    specs = []
    for i in range(_n_rows):
        sec = _inv_row is not None and (i + 1) == _inv_row
        specs.append([{"secondary_y": sec}, {"secondary_y": False}])
    fig = make_subplots(
        rows=_n_rows, cols=2,
        row_heights=_row_heights,
        column_widths=(0.9, 0.1),
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.02,
        horizontal_spacing=0.01,
        specs=specs,
    )
    
    # Row 1: Sector Performance
    for g in sector_graphs:
        fig.add_trace(g, 1, 1)
    
    # Row 2: Price & Moving Averages (기존 그대로)
    for g in price_graphs1:
        fig.add_trace(g, 2, 1)

    #for g in ch_graphs:
    #    fig.add_trace(g, 2, 1)
        
    for g in stop_graphs:
        fig.add_trace(g, 2, 1)
    
    # 매수/매도 마커 추가
    if trade_data is not None and not trade_data.empty:
        # 날짜 형식 변환 및 필터링
        try:
            # trade_data의 컬럼명 확인 및 디버깅 (주석 처리 - tqdm 진행 상태 바와 충돌 방지)
            # print(f"[DEBUG] 티커 {ticker}: trade_data 컬럼: {trade_data.columns.tolist()}")
            # print(f"[DEBUG] 티커 {ticker}: trade_data 행 수: {len(trade_data)}")
            
            # 날짜 필드명 확인 (ord_dt 또는 ord_dt_str 등)
            date_col = None
            for col in ['ord_dt', 'ord_dt_str', 'ordr_dt', 'ordr_dt_str']:
                if col in trade_data.columns:
                    date_col = col
                    break
            
            if date_col is None:
                # print(f"[DEBUG] 티커 {ticker}: 날짜 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {trade_data.columns.tolist()}")
                pass
            else:
                # print(f"[DEBUG] 티커 {ticker}: 날짜 컬럼 발견: {date_col}")
                # print(f"[DEBUG] 티커 {ticker}: 날짜 컬럼 샘플: {trade_data[date_col].head(3).tolist()}")
                pass
                
                # 날짜 형식 변환
                if trade_data[date_col].dtype == 'object':
                    # 문자열인 경우 (YYYYMMDD 형식)
                    trade_data[date_col] = pd.to_datetime(trade_data[date_col], format='%Y%m%d', errors='coerce')
                else:
                    # 이미 날짜 형식인 경우
                    trade_data[date_col] = pd.to_datetime(trade_data[date_col], errors='coerce')
                
                # NaT 제거
                trade_data = trade_data[trade_data[date_col].notna()].copy()
                
                if trade_data.empty:
                    # print(f"[DEBUG] 티커 {ticker}: 날짜 변환 후 데이터 없음")
                    pass
                
                if not trade_data.empty:
                    # df.index도 날짜 형식으로 변환
                    df_index_dates = pd.to_datetime(df.index)
                    # print(f"[DEBUG] 티커 {ticker}: df.index 날짜 범위: {df_index_dates.min()} ~ {df_index_dates.max()}")
                    # print(f"[DEBUG] 티커 {ticker}: trade_data 날짜 범위: {trade_data[date_col].min()} ~ {trade_data[date_col].max()}")
                    
                    # 날짜 매칭 (날짜만 비교) - 문자열로 변환해서 비교
                    df_date_strs = set([d.strftime('%Y-%m-%d') for d in df_index_dates])
                    trade_data['date_str'] = trade_data[date_col].dt.strftime('%Y-%m-%d')
                    trade_data = trade_data[trade_data['date_str'].isin(df_date_strs)].copy()
                    
                    # print(f"[DEBUG] 티커 {ticker}: 필터링 후 trade_data 행 수: {len(trade_data)}")
                    
                    if not trade_data.empty:
                        # 매수/매도 구분 필드 확인
                        buy_sell_col = None
                        for col in ['sll_buy_dvsn_cd', 'sll_buy_dvsn_cd_nm', 'buy_sell_dvsn_cd']:
                            if col in trade_data.columns:
                                buy_sell_col = col
                                break
                        
                        # print(f"[DEBUG] 티커 {ticker}: 매수/매도 컬럼: {buy_sell_col}")
                        
                        if buy_sell_col:
                            # 매수 데이터 (02: 매수, 1: 매수)
                            buy_data = trade_data[(trade_data[buy_sell_col] == '02') | (trade_data[buy_sell_col] == '1') | 
                                                 (trade_data[buy_sell_col] == 1) | (trade_data[buy_sell_col] == '매수')].copy()
                            # print(f"[DEBUG] 티커 {ticker}: 매수 데이터 행 수: {len(buy_data)}")
                            
                            if not buy_data.empty:
                                buy_dates_list = []
                                buy_prices_list = []
                                buy_hover_list = []
                                
                                for _, row in buy_data.iterrows():
                                    try:
                                        # date_str을 사용해서 매칭
                                        date_str = row['date_str']
                                        matched_date = None
                                        
                                        # df.index에서 매칭
                                        for df_date in df.index:
                                            if pd.to_datetime(df_date).strftime('%Y-%m-%d') == date_str:
                                                matched_date = df_date
                                                break
                                        
                                        if matched_date is not None:
                                            price = df.loc[matched_date, 'close']
                                            buy_dates_list.append(matched_date)
                                            buy_prices_list.append(price)
                                            
                                            # 수량 및 금액 필드 확인
                                            qty = 0
                                            amt = 0
                                            for qty_col in ['tot_ccld_qty', 'ord_qty', 'ccld_qty', 'qty']:
                                                if qty_col in row and pd.notna(row[qty_col]):
                                                    qty = int(float(row[qty_col]))
                                                    break
                                            
                                            for amt_col in ['tot_ccld_amt', 'ord_amt', 'ccld_amt', 'amt']:
                                                if amt_col in row and pd.notna(row[amt_col]):
                                                    amt = int(float(row[amt_col]))
                                                    break
                                            
                                            price_per_share = int(amt / qty) if qty > 0 else int(price)
                                            hover = f"수량: {qty:,}주"
                                            if qty > 0:
                                                hover += f"<br>단가: {price_per_share:,}원"
                                            buy_hover_list.append(hover)
                                    except Exception as e:
                                        # print(f"[DEBUG] 매수 데이터 처리 오류: {e}")
                                        continue
                                
                                if buy_dates_list and buy_prices_list:
                                    fig.add_trace(go.Scatter(
                                        x=buy_dates_list,
                                        y=buy_prices_list,
                                        mode='markers',
                                        marker=dict(
                                            symbol='triangle-up',
                                            size=9,
                                            color='#2ECC71',
                                            line=dict(width=1.5, color='#27AE60')
                                        ),
                                        name='매수',
                                        hovertext=buy_hover_list,
                                        hoverinfo='text+x',
                                        showlegend=True
                                    ), row=2, col=1)
                                    # print(f"[DEBUG] 티커 {ticker}: 매수 마커 {len(buy_dates_list)}개 추가")
                            
                            # 매도 데이터 (01: 매도, 2: 매도)
                            sell_data = trade_data[(trade_data[buy_sell_col] == '01') | (trade_data[buy_sell_col] == '2') | 
                                                   (trade_data[buy_sell_col] == 2) | (trade_data[buy_sell_col] == '매도')].copy()
                            # print(f"[DEBUG] 티커 {ticker}: 매도 데이터 행 수: {len(sell_data)}")
                            
                            if not sell_data.empty:
                                sell_dates_list = []
                                sell_prices_list = []
                                sell_hover_list = []
                                
                                for _, row in sell_data.iterrows():
                                    try:
                                        # date_str을 사용해서 매칭
                                        date_str = row['date_str']
                                        matched_date = None
                                        
                                        # df.index에서 매칭
                                        for df_date in df.index:
                                            if pd.to_datetime(df_date).strftime('%Y-%m-%d') == date_str:
                                                matched_date = df_date
                                                break
                                        
                                        if matched_date is not None:
                                            price = df.loc[matched_date, 'close']
                                            sell_dates_list.append(matched_date)
                                            sell_prices_list.append(price)
                                            
                                            # 수량 및 금액 필드 확인
                                            qty = 0
                                            amt = 0
                                            for qty_col in ['tot_ccld_qty', 'ord_qty', 'ccld_qty', 'qty']:
                                                if qty_col in row and pd.notna(row[qty_col]):
                                                    qty = int(float(row[qty_col]))
                                                    break
                                            
                                            for amt_col in ['tot_ccld_amt', 'ord_amt', 'ccld_amt', 'amt']:
                                                if amt_col in row and pd.notna(row[amt_col]):
                                                    amt = int(float(row[amt_col]))
                                                    break
                                            
                                            price_per_share = int(amt / qty) if qty > 0 else int(price)
                                            hover = f"수량: {qty:,}주"
                                            if qty > 0:
                                                hover += f"<br>단가: {price_per_share:,}원"
                                            sell_hover_list.append(hover)
                                    except Exception as e:
                                        # print(f"[DEBUG] 매도 데이터 처리 오류: {e}")
                                        continue
                                
                                if sell_dates_list and sell_prices_list:
                                    fig.add_trace(go.Scatter(
                                        x=sell_dates_list,
                                        y=sell_prices_list,
                                        mode='markers',
                                        marker=dict(
                                            symbol='triangle-down',
                                            size=9,
                                            color='#E74C3C',
                                            line=dict(width=1.5, color='#C0392B')
                                        ),
                                        name='매도',
                                        hovertext=sell_hover_list,
                                        hoverinfo='text+x',
                                        showlegend=True
                                    ), row=2, col=1)
                                    # print(f"[DEBUG] 티커 {ticker}: 매도 마커 {len(sell_dates_list)}개 추가")
                    else:
                        # print(f"[DEBUG] 티커 {ticker}: 매수/매도 구분 컬럼을 찾을 수 없습니다.")
                        pass
        except Exception as e:
            print(f"매수/매도 마커 표시 오류 ({ticker}): {e}")
            import traceback
            traceback.print_exc()

    # Row 3: Volume Analysis
    for g in volume_graphs:
        fig.add_trace(g, 3, 1)
        
    # Row 4: MACD
    for g in macd_graphs:
        fig.add_trace(g, 4, 1)
        
    # Row 5: Band Width
    if width_graphs:
        for g in width_graphs:
            fig.add_trace(g, 5, 1)

    # Row 6: CSI / Row 7: 투자자 OSC (기관·외국인 순매수만)
    row6_panel = "empty"
    row7_panel = "empty"

    if csi_graphs:
        for g in csi_graphs:
            fig.add_trace(g, _csi_row, 1)
        row6_panel = "csi"

    investor_graphs = []
    if _has_investor_osc and _inv_row is not None:
        try:
            investor_graphs = [
                go.Scatter(
                    x=df.index, y=df["inst_net_osc"], name="기관(연기금+투신+사모) 순매수금액 OSC",
                    line=dict(color="#2E86DE", width=1.8),
                ),
                go.Scatter(
                    x=df.index, y=df["frgn_net_osc"], name="외국인(9000) 순매수금액 OSC",
                    line=dict(color="#E74C3C", width=1.8),
                ),
            ]
            row7_panel = "investor"
        except (KeyError, TypeError):
            investor_graphs = []

    if investor_graphs and _inv_row is not None:
        if "frgn_ratio" in df.columns and pd.Series(df["frgn_ratio"]).notna().any():
            fig.add_trace(
                go.Bar(
                    x=df.index,
                    y=df["frgn_ratio"],
                    name="외국인 지분율",
                    marker=dict(color="#00897B", line=dict(width=0)),
                    opacity=0.55,
                    showlegend=True,
                ),
                row=_inv_row,
                col=1,
                secondary_y=True,
            )
        for g in investor_graphs:
            fig.add_trace(g, _inv_row, 1, secondary_y=False)


    # Row 1, Col 2: y축 레이블 숨기기
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    
    # Row 2, Col 2: y축 레이블 숨기기
    fig.update_yaxes(showticklabels=False, row=2, col=2)
    
    # Row 6, Col 1: CSI (-10~10)
    if row6_panel == "csi":
        fig.update_yaxes(autorange=True, row=_csi_row, col=1, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
        fig.add_hline(y=0, row=_csi_row, col=1, line=dict(color='rgba(128, 128, 128, 0.5)', width=1, dash='dash'))

    # Row 7, Col 1: 투자자 OSC (좌 0~100) + 외국인 지분율(우 %)
    if row7_panel == "investor" and _inv_row is not None:
        fig.update_yaxes(
            range=[0, 100], row=_inv_row, col=1, secondary_y=False,
            showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)',
        )
        fig.add_hline(y=80, row=_inv_row, col=1, line=dict(color='rgba(255, 107, 107, 0.5)', width=1, dash='dash'))
        fig.add_hline(y=20, row=_inv_row, col=1, line=dict(color='rgba(78, 205, 196, 0.5)', width=1, dash='dash'))
        yr = _frgn_ratio_axis_range(df["frgn_ratio"]) if "frgn_ratio" in df.columns else None
        if yr is not None:
            fig.update_yaxes(
                range=yr,
                row=_inv_row,
                col=1,
                secondary_y=True,
                showgrid=False,
            )
    elif row6_panel != "csi" and _inv_row is None:
        fig.update_yaxes(range=[0, 100], row=_csi_row, col=1, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')

    # 마지막 행 Col 2 (BOX): y축 자동
    fig.update_yaxes(autorange=True, row=_box_row, col=2, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)', showticklabels=False)
    
    # Row 3, Col 2: MFI y축 범위 설정
    try:
        fig.update_yaxes(range=[0, 100], row=3, col=2, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
    except:
        pass
    
    # Row 4, Col 2: Williams %R y축 범위 및 기준선 설정
    try:
        fig.update_yaxes(range=[-100, 0], row=4, col=2, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
        # Williams %R 과매수/과매도 기준선 (-20, -50, -80)
        fig.add_hline(y=-20, row=4, col=2, line=dict(color='rgba(255, 107, 107, 0.6)', width=1.5, dash='dash'), annotation_text='과매수')
        fig.add_hline(y=-50, row=4, col=2, line=dict(color='rgba(128, 128, 128, 0.5)', width=1, dash='dot'))
        fig.add_hline(y=-80, row=4, col=2, line=dict(color='rgba(78, 205, 196, 0.6)', width=1.5, dash='dash'), annotation_text='과매도')
    except:
        pass
    
    # Row 5, Col 2: PB (%B) y축 범위 및 기준선 설정 (값에 따라 자동 조정)
    try:
        # pb 값의 최소/최대값에 여백을 두어 y축 범위 계산
        if 'pb' in df.columns:
            pb_min = df['pb'].min()
            pb_max = df['pb'].max()
            pb_range = pb_max - pb_min
            # 10% 여백 추가
            y_min = pb_min - pb_range * 0.1
            y_max = pb_max + pb_range * 0.1
            fig.update_yaxes(range=[y_min, y_max], row=5, col=2, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
        else:
            # pb 컬럼이 없으면 자동 조정
            fig.update_yaxes(autorange=True, row=5, col=2, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
        # %B 과매수/과매도 기준선 (데이터 범위 내에 있는 경우에만 표시)
        if 'pb' in df.columns:
            pb_max_val = df['pb'].max()
            pb_min_val = df['pb'].min()
            if pb_max_val >= 0.8:
                fig.add_hline(y=0.8, row=5, col=2, line=dict(color='rgba(255, 107, 107, 0.6)', width=1.5, dash='dash'), annotation_text='과매수')
            if 0.5 >= pb_min_val and 0.5 <= pb_max_val:
                fig.add_hline(y=0.5, row=5, col=2, line=dict(color='rgba(128, 128, 128, 0.5)', width=1, dash='dot'))
            if pb_min_val <= 0.2:
                fig.add_hline(y=0.2, row=5, col=2, line=dict(color='rgba(78, 205, 196, 0.6)', width=1.5, dash='dash'), annotation_text='과매도')
    except:
        pass
    
    # 공통 날짜 라벨 (모든 차트에서 동일한 x축 사용)
    try:
        date_labels = [
            df.index[-57],
            df.index[-43],
            df.index[-29],
            df.index[-15],
            df.index[-1]
        ]
    except (IndexError, KeyError):
        date_labels = []
    
    # Row 3, Col 2: MFI Chart (5개 시점 - 선 차트)
    try:
        MFI_values = [
            df.iloc[-57].mfi,
            df.iloc[-43].mfi,
            df.iloc[-29].mfi,
            df.iloc[-15].mfi,
            df.iloc[-1].mfi
        ]
        
        mfi_line = go.Scatter(
            x=date_labels,
            y=MFI_values,
            name='MFI',
            mode='lines+markers',
            line=dict(color='#FF6B6B', width=2.5),
            marker=dict(size=8, color='#FF6B6B', symbol='circle'),
            text=[f'{v:.1f}' for v in MFI_values],
            textposition='top center'
        )
        fig.add_trace(mfi_line, 3, 2)
    except (IndexError, KeyError) as e:
        pass  # 데이터가 부족할 경우 skip

    # Row 4, Col 2: Williams %R Chart (5개 시점 - 선 차트)
    try:
        willr_values = [
            df.iloc[-57].willr,
            df.iloc[-43].willr,
            df.iloc[-29].willr,
            df.iloc[-15].willr,
            df.iloc[-1].willr
        ]
        
        willr_line = go.Scatter(
            x=date_labels,
            y=willr_values,
            name='Williams %R',
            mode='lines+markers',
            line=dict(color='#9B59B6', width=2.5),
            marker=dict(size=8, color='#9B59B6', symbol='circle'),
            text=[f'{v:.1f}' for v in willr_values],
            textposition='top center'
        )
        fig.add_trace(willr_line, 4, 2)
    except (IndexError, KeyError) as e:
        pass  # 데이터가 부족할 경우 skip
    
    # Row 5, Col 2: PB (%B) Chart (5개 시점 - 선 차트)
    try:
        pb_values = [
            df.iloc[-57].pb,
            df.iloc[-43].pb,
            df.iloc[-29].pb,
            df.iloc[-15].pb,
            df.iloc[-1].pb
        ]
        
        pb_line = go.Scatter(
            x=date_labels,
            y=pb_values,
            name='%B',
            mode='lines+markers',
            line=dict(color='#00D2FF', width=2.5),
            marker=dict(size=8, color='#00D2FF', symbol='circle'),
            text=[f'{v:.2f}' for v in pb_values],
            textposition='top center'
        )
        fig.add_trace(pb_line, 5, 2)
    except (IndexError, KeyError) as e:
        pass  # 데이터가 부족할 경우 skip
        
    # 마지막 행 Col 2: box14 Bar Chart (5개 시점)
    try:
        box14_values = [
            df.iloc[-57].box14,
            df.iloc[-43].box14,
            df.iloc[-29].box14,
            df.iloc[-15].box14,
            df.iloc[-1].box14
        ]
        
        box14_bar = go.Bar(
            x=date_labels,
            y=box14_values,
            name='Box14',
            marker=dict(
                color=box14_values,
                colorscale='Viridis',
                showscale=False
            ),
            text=[f'{v:.1f}' for v in box14_values],
            textposition='outside'
        )
        fig.add_trace(box14_bar, _box_row, 2)
    except (IndexError, KeyError):
        pass  # 데이터가 부족할 경우 skip
            
    # volume_data 접근 시 예외 처리
    try:
        ticker_key = df.iloc[0]['ticker']
        if ticker_key in volume_data:
            df_v = volume_data[ticker_key]
            
            # 매물대 바 차트 (그라데이션 효과) - row 2 (Price와 같은 행)
            if not df_v.empty and 'volume_p' in df_v.columns:
                volume_bar_colors = df_v.volume_p.apply(
                    lambda x: f'rgba(255, 107, 53, {min(x/100, 1)})' if x > 0 else 'rgba(128, 128, 128, 0.3)'
                )
                fig.add_trace(go.Bar(x=df_v.volume_p, y=df_v.index, orientation='h',
                                     marker=dict(color=volume_bar_colors),
                                     name='Volume Profile'), 2, 2)
        else:
            print(f"[WARN] volume_data에 티커 {ticker_key}가 없습니다.")
    except (KeyError, AttributeError, IndexError) as e:
        print(f"[WARN] 매물대 데이터 처리 실패 ({ticker}): {e}")
    
    # 모멘텀 바 차트 - 10일/20일/50일/120일 모두 출력 (1x1과 동일한 색상: 종목=청록, 지수=파랑)
    momentum_bar_colors = []
    period_colors = {'10일': '#1E88E5', '20일': '#42A5F5', '50일': '#64B5F6', '120일': '#90CAF9'}
    for idx_name in mom_dff_all.index:
        # '10일_종목명' 형태에서 종목/지수 구분
        parts = str(idx_name).split('_', 1)
        period_key = parts[0] if len(parts) >= 2 else '10일'
        sector_name = parts[1] if len(parts) >= 2 else idx_name
        if sector_name in color_mapping:
            momentum_bar_colors.append(color_mapping[sector_name])
        else:
            momentum_bar_colors.append(period_colors.get(period_key, '#95a5a6'))
    
    fig.add_trace(go.Bar(x=mom_dff_all['mom'], y=mom_dff_all.index, orientation='h',
                         marker=dict(color=momentum_bar_colors),
                         name='',
                         hovertemplate='%{y}: %{x:.2f}%<extra></extra>'), 1, 2)
    
    fig.update_xaxes(rangeslider_thickness = 0)
    

    fig.update_xaxes(nticks=13)
    fig.update_xaxes(ticks="outside")
    
    # 레이아웃 업데이트 (라이트 테마)
    fig.update_layout(
        layout,
        xaxis_rangeslider_visible=False,
        plot_bgcolor='white',  # 차트 배경 (흰색)
        paper_bgcolor='white',  # 전체 배경 (흰색)
        font=dict(color='black', size=11),  # 폰트 색상 (검은색)
        margin=dict(l=0, r=0, t=80, b=0, pad=0),  # 두 줄 제목을 위해 상단 여백 증가
        legend=dict(
            bgcolor='rgba(255, 255, 255, 0.9)',
            bordercolor='rgba(128, 128, 128, 0.5)',
            borderwidth=1,
            font=dict(size=10)
        ),
        hovermode='x unified',
        showlegend=True,
        hoverdistance=100,
        # 호버 레이블 스타일 개선 (가격 정보 더 명확하게)
        hoverlabel=dict(
            bgcolor='rgba(255, 255, 255, 0.95)',
            bordercolor='rgba(0, 0, 0, 0.3)',
            font=dict(size=13, color='black', family='Consolas, monospace')
        )
    )
    
    # 각 서브플롯에 제목 추가 (그래프 안쪽)
    subplot_titles_data = [
        (1, 1, '📉 Sector Performance', 0.02, 0.98, 'left', 'top'),
        (2, 1, '📈 Price & Moving Averages', 0.02, 0.98, 'left', 'top'),
        (3, 1, '📊 Volume Analysis', 0.02, 0.98, 'left', 'top'),
        (4, 1, '📈 MACD', 0.02, 0.98, 'left', 'top'),
        (5, 1, '📏 Band Width', 0.02, 0.98, 'left', 'top'),
        (_csi_row, 1, '📦 CSI', 0.02, 0.98, 'left', 'top'),
        (2, 2, '📊 Volume Profile', 0.98, 0.02, 'right', 'bottom'),  # 오른쪽 하단
        (1, 2, '📊 Momentum', 0.98, 0.98, 'right', 'top'),  # 모멘텀 차트 제목: 우측 상단
        (3, 2, 'MFI', 0.98, 0.02, 'right', 'bottom'),  # Row 3, Col 2
        (4, 2, 'Williams R', 0.98, 0.02, 'right', 'bottom'),  # Row 4, Col 2
        (5, 2, '%b', 0.98, 0.02, 'right', 'bottom'),  # Row 5, Col 2
        (_box_row, 2, '📦 BOX', 0.98, 0.02, 'right', 'bottom')  # 오른쪽 하단
    ]
    if row7_panel == "investor" and _inv_row is not None:
        subplot_titles_data.append(
            (_inv_row, 1, '📊 Investor OSC', 0.02, 0.98, 'left', 'top')
        )
    elif _inv_row is None and row6_panel != "csi":
        subplot_titles_data[5] = (
            _csi_row, 1, '📊 Investor OSC (DB 데이터 없음)', 0.02, 0.98, 'left', 'top'
        )
    
    for row, col, title, x_pos, y_pos, x_anchor, y_anchor in subplot_titles_data:
        # subplot 인덱스 계산 (col=1이면 홀수, col=2이면 짝수)
        subplot_idx = (row - 1) * 2 + col
        xref = f'x{subplot_idx} domain' if subplot_idx > 1 else 'x domain'
        yref = f'y{subplot_idx} domain' if subplot_idx > 1 else 'y domain'
        
        fig.add_annotation(
            text=title,
            xref=xref,
            yref=yref,
            x=x_pos,
            y=y_pos,
            xanchor=x_anchor,
            yanchor=y_anchor,
            showarrow=False,
            font=dict(size=11, color='rgba(50, 50, 50, 1)', family='Arial Black'),
            bgcolor='rgba(255, 255, 255, 0.85)',
            bordercolor='rgba(100, 100, 100, 0.5)',  # 테두리 다시 추가
            borderwidth=1,  # 테두리 다시 추가
            borderpad=4
        )
    
    # 각 차트 사이에 구분선 추가
    divider_positions = (
        [0.84, 0.68, 0.52, 0.38, 0.25, 0.12]
        if _n_rows == 7
        else [0.82, 0.64, 0.46, 0.31, 0.17]
    )
    for pos in divider_positions:
        fig.add_shape(
            type="line",
            xref="paper", yref="paper",
            x0=0, y0=pos, x1=1, y1=pos,
            line=dict(
                color="rgba(150, 150, 150, 0.4)",
                width=1.5,
                dash="dot"
            )
        )
    
    # y축 그리드 색상 설정 (모든 서브플롯)
    fig.update_yaxes(
        gridcolor='rgba(200, 200, 200, 0.8)',
        gridwidth=0.5,
        zeroline=True,
        zerolinecolor='rgba(150, 150, 150, 0.8)',
        zerolinewidth=1
    )
    
    # Band Width 차트 0.2~0.8 구간 음영 처리 (row 5)
    if width_graphs:
        try:
            fig.add_shape(
                type="rect",
                xref="x9", yref="y9",  # x9와 y9는 row 5, col 1의 축
                x0=df.index.min(), x1=df.index.max(),  # 날짜 범위
                y0=0.2, y1=0.8,
                fillcolor="rgba(173, 216, 230, 0.3)",  # 연한 하늘색 반투명
                layer="below",
                line_width=0
            )
        except:
            pass
    
    # 가격 차트(row 2)의 Y축 눈금을 더 세밀하게 표시
    fig.update_yaxes(
        nticks=20,  # Y축 눈금 개수 증가 (더 많은 가격 값 표시)
        tickfont=dict(size=10, color='black'),  # 눈금 폰트 크기
        showticklabels=True,  # Y축 레이블 표시
        showspikes=True,  # 호버 시 Y축 스파이크 표시
        spikemode='across',  # 가로선으로 표시
        spikesnap='cursor',  # 커서에 스냅
        spikedash='dot',  # 점선
        spikethickness=1,  # 선 두께
        spikecolor='rgba(100, 100, 100, 0.5)',  # 회색 반투명
        row=2, col=1
    )
    
    # 가격 차트(row 2)의 X축 세로선도 추가
    fig.update_xaxes(
        showspikes=True,  # 호버 시 X축 스파이크 표시
        spikemode='across',  # 세로선으로 표시
        spikesnap='cursor',  # 커서에 스냅
        spikedash='dot',  # 점선
        spikethickness=1,  # 선 두께
        spikecolor='rgba(100, 100, 100, 0.5)',  # 회색 반투명
        row=2, col=1
    )
    
    # x축 그리드 색상 설정
    fig.update_xaxes(
        gridcolor='rgba(200, 200, 200, 0.8)',
        gridwidth=0.5
    )
    
    _auto_holidays = _nontrading_weekdays(engine, df.index.min(), df.index.max())
    fig.update_xaxes(rangebreaks=[
        dict(bounds=["sat", "mon"]),
        dict(values=_auto_holidays),
    ])
    
    # Col 2 x축: 마지막 행만 날짜 레이블 표시
    for row in range(3, _box_row):
        fig.update_xaxes(showticklabels=False, row=row, col=2)

    if len(date_labels) > 0:
        fig.update_xaxes(
            showticklabels=True,
            tickmode='array',
            tickvals=date_labels,
            tickformat='%Y-%m-%d',
            tickangle=-45,
            row=_box_row, col=2
        )
    else:
        fig.update_xaxes(
            showticklabels=True,
            tickformat='%Y-%m-%d',
            tickangle=-45,
            row=_box_row, col=2
        )
    
    try:
        html_output = fig.to_html(
            full_html=True,
            include_plotlyjs='cdn',
            config={'responsive': True}
        ).replace('<body>', '<body style="margin:0;">')

        with open(fileN, 'w', encoding='utf-8') as f:
            f.write(html_output)

        abs_path = os.path.abspath(fileN).replace('\\', '/')
        webbrowser.open(f'file:///{abs_path}')
    except Exception as e:
        print(f"[ERROR] 차트 생성/저장 실패 ({ticker}): {e}")
        import traceback
        traceback.print_exc()
        raise

    if save_jpeg and fileN_jpeg:
        try:
            fig.write_image(fileN_jpeg, format='jpeg', width=1920, height=1080, scale=2)
        except Exception as e:
            print(f"[WARN] JPEG 생성 실패 ({ticker}): {e}")
    




# 스크리닝 코드 → 한글 표시명 (요약 HTML용)
SCREENING_SUMMARY_LABELS = {
    "p11": "이평 정배열·20일·125일 박스",
    "p12": "이평 정배열·CSI 약세",
    "p13": "이평 정배열·거래량 급증 후 축소",
    "p14": "이평 정배열·CSI·거래량 패턴",
    "p15": "이평 정배열·sma_score>0.75·20선 근접",
    "p16": "이평 정배열·20선 박스·저점박스",
    "p17": "거래량 급증→횡보→재상승",
    "p21": "50일고 신고가·음봉",
    "p22": "High and Tight Flag",
    "p23": "52주 신고가(20일250고)·RS",
    "p24": "52주 신고가 변형",
    "p25": "50일고 신고가 변형",
    "p26": "10일고·50일고 신고가",
    "p27": "10일고·125일고 신고가",
    "p28": "전일 대비 구간고점 돌파(50/125/250)",
    "p29": "125일고 돌파·이평정배열",
    "p31": "볼린저 스퀴즈",
    "p32": "이평정배열·BB하단",
    "p33": "BB 패턴",
    "p34": "BB 패턴",
    "p35": "BB 패턴",
    "p36": "BB 패턴",
    "p41": "패턴 p41",
    "p42": "패턴 p42",
    "p43": "패턴 p43",
    "p51": "패턴 p51",
    "p52": "패턴 p52",
    "p53": "패턴 p53",
    "p54": "패턴 p54",
    "p55": "패턴 p55",
    "p61": "패턴 p61",
    "p71": "패턴 p71",
    "p81": "패턴 p81",
    "p91": "패턴 p91",
    "p92": "패턴 p92",
    "p93": "패턴 p93",
}


def _resolve_indicators_df(indicators_data, ticker):
    """indicators_data 키(앞자리 0 유무) 불일치 보정."""
    raw = str(ticker).strip()
    for c in (raw, raw.zfill(6), raw.lstrip("0") or raw):
        if c in indicators_data:
            return indicators_data[c]
    return None


def _energy_ratio_tradingkis_style(engine, tickers):
    """
    정본 에너지배율: (tv비중/mcap비중)×(1+tanh(당일등락률%/15)).
    최신 krx_ohlcv 일자 기준.
    """
    out = {str(t).zfill(6): np.nan for t in tickers}
    if engine is None or not tickers:
        return out
    ut = sorted({str(t).zfill(6) for t in tickers})
    try:
        ref = pd.read_sql_query(f"SELECT MAX(date) AS d FROM `{OHLCV_TABLE}`", con=engine)
        d0 = pd.Timestamp(ref.iloc[0]["d"]).strftime("%Y-%m-%d")
        q_mkt_tv = f"""
            SELECT ts.sector_cd, SUM(o.close * o.volume) AS total_tv
            FROM `{OHLCV_TABLE}` o
            INNER JOIN krx_ticker t ON t.종목코드 = o.ticker
                AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
            INNER JOIN krx_ticker_sector ts ON ts.ticker = o.ticker
            WHERE t.종목구분 = '보통주'
              AND ts.sector_cd IN ('1001', '2001')
              AND DATE(o.date) = %s
            GROUP BY ts.sector_cd
        """
        mtv = pd.read_sql_query(q_mkt_tv, con=engine, params=(d0,))
        tv_by_sec = {str(r["sector_cd"]): float(r["total_tv"] or 0) for _, r in mtv.iterrows()}
        q_mkt_m = """
            SELECT ts.sector_cd, SUM(t.시가총액) AS total_mcap
            FROM krx_ticker t
            INNER JOIN krx_ticker_sector ts ON t.종목코드 = ts.ticker
            WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
              AND t.종목구분 = '보통주'
              AND ts.sector_cd IN ('1001', '2001')
            GROUP BY ts.sector_cd
        """
        mm = pd.read_sql_query(q_mkt_m, con=engine)
        mcap_by_sec = {str(r["sector_cd"]): float(r["total_mcap"] or 0) for _, r in mm.iterrows()}
        _ph = ",".join(["%s"] * len(ut))
        q_sec = f"""
            SELECT ticker, sector_cd FROM krx_ticker_sector
            WHERE sector_cd IN ('1001', '2001') AND ticker IN ({_ph})
        """
        sec_df = pd.read_sql_query(q_sec, con=engine, params=tuple(ut))
        ticker_sec = {}
        for _, r in sec_df.iterrows():
            tk = str(r["ticker"]).zfill(6)
            if tk not in ticker_sec:
                ticker_sec[tk] = str(r["sector_cd"])
        q_mcap_t = f"""
            SELECT 종목코드 AS ticker, 시가총액 AS mcap
            FROM krx_ticker
            WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
              AND 종목구분 = '보통주'
              AND 종목코드 IN ({_ph})
        """
        mrow = pd.read_sql_query(q_mcap_t, con=engine, params=tuple(ut))
        mcap_t = {str(r["ticker"]).zfill(6): float(r["mcap"] or 0) for _, r in mrow.iterrows()}
        _chunk = 400
        tv_t = {}
        for i0 in range(0, len(ut), _chunk):
            chunk = ut[i0 : i0 + _chunk]
            _pc = ",".join(["%s"] * len(chunk))
            q_tv = f"""
                SELECT o.ticker, SUM(o.close * o.volume) AS tv
                FROM `{OHLCV_TABLE}` o
                WHERE DATE(o.date) = %s AND o.ticker IN ({_pc})
                GROUP BY o.ticker
            """
            tdf = pd.read_sql_query(q_tv, con=engine, params=tuple([d0] + chunk))
            for _, rr in tdf.iterrows():
                tv_t[str(rr["ticker"]).zfill(6)] = float(rr["tv"] or 0)
        chg_map = {}
        try:
            drows = pd.read_sql_query(
                f"SELECT DISTINCT date FROM `{OHLCV_TABLE}` ORDER BY date DESC LIMIT 2",
                con=engine,
            )
            dl = pd.to_datetime(drows["date"], errors="coerce").dropna().sort_values(ascending=False).tolist()
            if len(dl) >= 2:
                d0s, d1s = dl[0].strftime("%Y-%m-%d"), dl[1].strftime("%Y-%m-%d")
                for i0 in range(0, len(ut), _chunk):
                    chunk = ut[i0 : i0 + _chunk]
                    _pc = ",".join(["%s"] * len(chunk))
                    q_ch = f"""
                        SELECT ticker, date,
                               {"COALESCE(close_adj, close) AS close" if USE_ADJ_PRICE and OHLCV_TABLE == "krx_ohlcv" else "close"}
                        FROM `{OHLCV_TABLE}`
                        WHERE date IN (%s, %s) AND ticker IN ({_pc})
                    """
                    cdf = pd.read_sql_query(q_ch, con=engine, params=tuple([d0s, d1s] + chunk))
                    if cdf.empty:
                        continue
                    cdf["ticker"] = cdf["ticker"].astype(str).str.zfill(6)
                    cdf["date"] = pd.to_datetime(cdf["date"], errors="coerce")
                    cdf["close"] = pd.to_numeric(cdf["close"], errors="coerce")
                    for tk0, g0 in cdf.groupby("ticker"):
                        g0 = g0.sort_values("date")
                        if len(g0) < 2:
                            continue
                        c0 = float(g0["close"].iloc[-1])
                        c1 = float(g0["close"].iloc[-2])
                        if np.isfinite(c0) and np.isfinite(c1) and c1 != 0:
                            chg_map[str(tk0)] = (c0 - c1) / c1 * 100.0
        except Exception:
            chg_map = {}
        for tk in ut:
            sec = ticker_sec.get(tk)
            tv_s = tv_t.get(tk)
            mc = mcap_t.get(tk)
            if not sec or tv_s is None or mc is None or mc <= 0:
                continue
            total_tv = tv_by_sec.get(sec, 0) or 0
            total_mcap = mcap_by_sec.get(sec, 0) or 0
            if total_tv <= 0 or total_mcap <= 0:
                continue
            tv_pct = tv_s / total_tv * 100.0
            mcap_pct = mc / total_mcap * 100.0
            if mcap_pct <= 0:
                continue
            ret = chg_map.get(tk)
            er = energy_ratio(tv_pct, mcap_pct, ret)
            out[tk] = float(er) if np.isfinite(er) else np.nan
    except Exception:
        pass
    return out


def _screening_summary_themes(engine, tickers):
    """krx_theme_stock 기준 테마명 (여러 개는 ' · ' 구분)."""
    out = {str(t).zfill(6): "" for t in tickers}
    if engine is None or not tickers:
        return out
    ut = sorted({str(t).zfill(6) for t in tickers})
    try:
        _ph = ",".join(["%s"] * len(ut))
        q = f"""
            SELECT ticker,
                   GROUP_CONCAT(DISTINCT theme_name ORDER BY theme_name SEPARATOR ' · ') AS theme_str
            FROM krx_theme_stock
            WHERE ticker IN ({_ph})
            GROUP BY ticker
        """
        df = pd.read_sql_query(q, con=engine, params=tuple(ut))
        for _, r in df.iterrows():
            out[str(r["ticker"]).zfill(6)] = str(r["theme_str"] or "")
    except Exception as e:
        print(f"⚠️ _screening_summary_themes 실패: {type(e).__name__}: {e}")
    return out


def _screening_summary_sectors(engine, tickers):
    """v_ticker_sector_wide 기준 sector1/sector2. 키는 6자리 ticker."""
    out = {str(t).zfill(6): ("", "") for t in tickers}
    if engine is None or not tickers:
        return out
    ut = sorted({str(t).zfill(6) for t in tickers})
    try:
        _ph = ",".join(["%s"] * len(ut))
        q = f"""
            SELECT ticker, sector1, sector2
            FROM v_ticker_sector_wide
            WHERE ticker IN ({_ph})
        """
        df = pd.read_sql_query(q, con=engine, params=tuple(ut))
        for _, r in df.iterrows():
            out[str(r["ticker"]).zfill(6)] = (
                str(r["sector1"] or ""),
                str(r["sector2"] or ""),
            )
    except Exception as e:
        print(f"⚠️ _screening_summary_sectors 실패: {type(e).__name__}: {e}")
    return out


def _sector1_major_label(sector1: str) -> str:
    """sector1 → 대분류. '_' 첫 번째 앞부분. 빈 값이면 '(미분류)'."""
    s1 = str(sector1 or "").strip()
    if not s1:
        return "(미분류)"
    return s1.split("_", 1)[0]


def _sector_tilt_counts(sector_map, tickers, *, level: str = "detail"):
    """선정 종목 섹터 쏠림 집계.

    level:
      - 'detail': sector1 전체 키
      - 'major': sector1 의 '_' 첫 토큰(대분류)
    반환: (total, [(label, n, pct), ...]) 내림차순, '(미분류)' 마지막.
    pct 는 0~100.
    """
    from collections import Counter

    ut = sorted({str(t).zfill(6) for t in tickers})
    total = len(ut)
    cnt = Counter()
    for tk in ut:
        s1, _ = sector_map.get(tk, ("", ""))
        if level == "major":
            lab = _sector1_major_label(s1)
        else:
            s1 = str(s1 or "").strip()
            lab = s1 if s1 else "(미분류)"
        cnt[lab] += 1
    known = [(k, n) for k, n in cnt.items() if k != "(미분류)"]
    known.sort(key=lambda x: (-x[1], x[0]))
    rows = [
        (lab, n, (100.0 * n / total) if total else 0.0) for lab, n in known
    ]
    if "(미분류)" in cnt:
        n = cnt["(미분류)"]
        rows.append(("(미분류)", n, (100.0 * n / total) if total else 0.0))
    return total, rows


def _sector_tilt_line(lab: str, n: int, pct: float, *, warn: bool = False) -> str:
    """콘솔/HTML 공통 한 줄. warn 이면 ' ⚠️ 쏠림' 접미."""
    suffix = "  ⚠️ 쏠림" if warn else ""
    return f"{lab}: {n}건 ({pct:.1f}%){suffix}"


def _sector_tilt_should_warn(rows, idx: int) -> bool:
    """대분류 1위(미분류 제외 첫 행) 비중이 임계 초과 시 True."""
    if idx != 0 or not rows:
        return False
    lab, _n, pct = rows[0]
    if lab == "(미분류)":
        return False
    return (pct / 100.0) > SECTOR_CONCENTRATION_WARN


def _screening_summary_mcap_tv_shares(engine, tickers):
    """
    보통주(최신 기준일) 전체 합 대비 시가총액·당일(krx_ohlcv 최신일) 거래대금 비중(%).
    반환: (mcap_by_ticker, mcap_share_pct, tv_share_pct) — 키는 6자리 ticker.
    """
    zf = lambda t: str(t).zfill(6)
    ut = sorted({zf(t) for t in tickers})
    empty = ({k: np.nan for k in ut}, {k: np.nan for k in ut}, {k: np.nan for k in ut})
    if engine is None or not ut:
        return empty
    try:
        ref = pd.read_sql_query(f"SELECT MAX(date) AS d FROM `{OHLCV_TABLE}`", con=engine)
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
                FROM `{OHLCV_TABLE}` o
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
        _ph = ",".join(["%s"] * len(ut))
        mrow = pd.read_sql_query(
            f"""
            SELECT 종목코드 AS ticker, 시가총액 AS mcap
            FROM krx_ticker
            WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
              AND 종목구분 = '보통주'
              AND 종목코드 IN ({_ph})
            """,
            con=engine,
            params=tuple(ut),
        )
        mcap_map = {zf(r["ticker"]): float(r["mcap"] or 0) for _, r in mrow.iterrows()}
        tv_map = {}
        _chunk = 400
        for i0 in range(0, len(ut), _chunk):
            chunk = ut[i0 : i0 + _chunk]
            _pc = ",".join(["%s"] * len(chunk))
            tdf = pd.read_sql_query(
                f"""
                SELECT o.ticker, SUM(o.close * o.volume) AS tv
                FROM `{OHLCV_TABLE}` o
                WHERE DATE(o.date) = %s AND o.ticker IN ({_pc})
                GROUP BY o.ticker
                """,
                con=engine,
                params=tuple([d0] + chunk),
            )
            for _, rr in tdf.iterrows():
                tv_map[zf(rr["ticker"])] = float(rr["tv"] or 0)
        mcap_share = {}
        tv_share = {}
        full_mcap = {}
        for tk in ut:
            mc = mcap_map.get(tk)
            full_mcap[tk] = float(mc) if mc is not None and mc > 0 else np.nan
            tv_s = tv_map.get(tk, 0.0)
            if total_mcap > 0 and mc is not None and mc > 0:
                mcap_share[tk] = mc / total_mcap * 100.0
            else:
                mcap_share[tk] = np.nan
            if total_tv > 0:
                tv_share[tk] = tv_s / total_tv * 100.0
            else:
                tv_share[tk] = np.nan
        return full_mcap, mcap_share, tv_share
    except Exception as e:
        print(f"⚠️ _screening_summary_mcap_tv_shares 실패: {e}")
        return empty


def _flag_nd_close_high(df, n):
    """최근 n거래일(당일 포함) 중 종가가 구간 최고 종가이면 O. 데이터가 n일 미만이면 가용 구간만 사용."""
    if df is None or len(df) < 1 or n < 1:
        return ""
    tail = df.tail(n)
    lc = float(tail.iloc[-1]["close"])
    mx = float(pd.to_numeric(tail["close"], errors="coerce").max())
    if not np.isfinite(lc) or not np.isfinite(mx):
        return ""
    return "O" if lc + 1e-9 >= mx else "X"


def _flag_120d_close_high(df):
    """최근 120거래일(당일 포함) 중 종가가 구간 최고 종가이면 O."""
    return _flag_nd_close_high(df, WIN_26)


def _talent_pct(df, window=WIN_26, thr=0.10):
    """
    최근 window거래일 중 전일종가 대비 등락률 ≥ thr 인 날 비중(%).
    indicators_core.talent_up_share × 100.
    """
    if df is None or len(df) < 1 or "close" not in df.columns:
        return np.nan
    share = talent_up_share(df["close"], window, thr=thr)
    return float(share * 100.0) if np.isfinite(share) else np.nan


def export_screening_summary_html(
    selected_df,
    indicators_data,
    engine,
    output_path=None,
    open_browser=True,
):
    """
    스크리닝 적중 종목(selected_df) 요약표를 HTML로 저장 후 브라우저에서 연다.
    선행 실행 후 이 함수만 다시 호출해도 동작하려면 selected_df·indicators_data·engine 이 메모리에 있어야 한다.
    """
    today = datetime.date.today()
    folder_name = today.strftime("%Y-%m-%d")
    default_dir = os.path.join("C:\\Users\\hachi\\OneDrive\\01. Trading\\picking\\KRX", folder_name)
    os.makedirs(default_dir, exist_ok=True)
    if output_path is None:
        output_path = os.path.join(default_dir, f"screening_summary{OUTPUT_SUFFIX}.html")

    if selected_df is None or len(selected_df) == 0:
        body = "<p>선별된 스크리닝 행이 없습니다.</p>"
    else:
        tickers = selected_df["ticker"].astype(str).unique().tolist()
        energy_map = _energy_ratio_tradingkis_style(engine, tickers)
        theme_map = _screening_summary_themes(engine, tickers)
        sector_map = _screening_summary_sectors(engine, tickers)
        mcap_map, mcap_share_map, tv_share_map = _screening_summary_mcap_tv_shares(engine, tickers)
        rows = []
        for _, r in selected_df.iterrows():
            code = str(r["type"]).strip()
            tk = str(r["ticker"]).strip()
            tkz = tk.zfill(6)
            name = r.get("company", "")
            th = theme_map.get(tkz, "")
            s1, s2 = sector_map.get(tkz, ("", ""))
            if s1 and s2:
                sec_disp = f"{s1} · {s2}"
            else:
                sec_disp = s1 or s2 or ""
            mc_raw = mcap_map.get(tkz, np.nan)
            # 표시: 시총 ≥ DISPLAY_MCAP_MIN (스크리닝 유니버스와 동일)
            if not (np.isfinite(mc_raw) and float(mc_raw) >= DISPLAY_MCAP_MIN):
                continue
            ms_raw = mcap_share_map.get(tkz, np.nan)
            vs_raw = tv_share_map.get(tkz, np.nan)
            mcap_disp = f"{mc_raw:,.0f}" if np.isfinite(mc_raw) and mc_raw > 0 else ""
            mshare_disp = f"{ms_raw:.4f}" if np.isfinite(ms_raw) else ""
            vshare_disp = f"{vs_raw:.4f}" if np.isfinite(vs_raw) else ""
            idf = _resolve_indicators_df(indicators_data, tk)
            if idf is None or len(idf) < 1:
                scr = f"{code} {SCREENING_SUMMARY_LABELS.get(code, '')}".strip()
                er = energy_map.get(tkz, np.nan)
                erdisp = f"{er:.2f}" if np.isfinite(er) else ""
                _st_disp = _sector_type(code)
                if _st_disp == "미판정":
                    _st_disp = ""
                rows.append(
                    {
                        "스크리닝명": scr,
                        "섹터성격": _st_disp,
                        "ticker": tk,
                        "종목명": name,
                        "섹터": sec_disp,
                        "테마명": th,
                        "현재가": "",
                        "sma5위": "",
                        "sma10위": "",
                        "sma20위": "",
                        "b%": "",
                        "에너지배율": erdisp,
                        "Talent비율": "",
                        "시가총액": mcap_disp,
                        "시총비중": mshare_disp,
                        "거래대금비중": vshare_disp,
                        "50일신고가": "",
                        "120일신고가": "",
                        "250일신고가": "",
                        "밴드스퀴즈": "",
                        "CSI": "",
                        "기관OSC": "",
                        "국민연금등OSC": "",
                        "투신OSC": "",
                        "사모OSC": "",
                        "외국인OSC": "",
                        "_sort_sn": scr or "",
                        "_sort_st": _st_disp or "",
                        "_sort_tk": tkz,
                        "_sort_nm": str(name) if name is not None else "",
                        "_sort_sec": sec_disp or "",
                        "_sort_th": th or "",
                        "_sort_close": None,
                        "_sort_s5": "",
                        "_sort_s10": "",
                        "_sort_s20": "",
                        "_sort_pb": None,
                        "_sort_er": float(er) if np.isfinite(er) else None,
                        "_sort_talent": None,
                        "_sort_mcap": float(mc_raw) if np.isfinite(mc_raw) else None,
                        "_sort_mshare": float(ms_raw) if np.isfinite(ms_raw) else None,
                        "_sort_vshare": float(vs_raw) if np.isfinite(vs_raw) else None,
                        "_sort_50": "",
                        "_sort_120": "",
                        "_sort_250": "",
                        "_sort_bsq": None,
                        "_sort_csi": "",
                        "_sort_inst": "",
                        "_sort_pension": "",
                        "_sort_trust": "",
                        "_sort_private": "",
                        "_sort_frgn": "",
                    }
                )
                continue
            last = idf.iloc[-1]
            close = float(last["close"]) if pd.notna(last.get("close")) else np.nan
            sma5 = float(last["sma5"]) if "sma5" in last.index and pd.notna(last.get("sma5")) else np.nan
            sma10 = float(last["sma10"]) if "sma10" in last.index and pd.notna(last.get("sma10")) else np.nan
            sma20 = float(last["sma20"]) if "sma20" in last.index and pd.notna(last.get("sma20")) else np.nan
            pb = float(last["pb"]) if "pb" in last.index and pd.notna(last.get("pb")) else np.nan
            label = SCREENING_SUMMARY_LABELS.get(code, "")
            scr_name = f"{code} {label}".strip() if label else code
            _st_disp = _sector_type(code)
            if _st_disp == "미판정":
                _st_disp = ""
            er = energy_map.get(tkz, np.nan)
            s5 = "O" if np.isfinite(close) and np.isfinite(sma5) and close > sma5 else "X"
            s10 = "O" if np.isfinite(close) and np.isfinite(sma10) and close > sma10 else "X"
            s20 = "O" if np.isfinite(close) and np.isfinite(sma20) and close > sma20 else "X"
            bdisp = f"{pb * 100:.2f}" if np.isfinite(pb) else ""
            erdisp = f"{er:.2f}" if np.isfinite(er) else ""
            tal = _talent_pct(idf, window=WIN_26, thr=0.10)
            taldisp = f"{tal:.2f}" if np.isfinite(tal) else ""
            hi50 = _flag_nd_close_high(idf, WIN_13)
            hi120 = _flag_nd_close_high(idf, WIN_26)
            hi250 = _flag_nd_close_high(idf, 250)
            idf2, _ = _ensure_investor_osc_on_df(idf, tk)
            bsq_v = (
                float(last["band20_q"])
                if "band20_q" in last.index and pd.notna(last.get("band20_q"))
                else np.nan
            )
            bsq_disp = f"{bsq_v:.2f}" if np.isfinite(bsq_v) else ""
            csi_disp = _csi_grade(last)
            inst_osc = (
                _osc_two_day_rise(idf2["inst_net_osc"])
                if "inst_net_osc" in idf2.columns
                else "-"
            )
            pension_osc = (
                _osc_two_day_rise(idf2["pension_net_osc"])
                if "pension_net_osc" in idf2.columns
                else "-"
            )
            trust_osc = (
                _osc_two_day_rise(idf2["trust_net_osc"])
                if "trust_net_osc" in idf2.columns
                else "-"
            )
            private_osc = (
                _osc_two_day_rise(idf2["private_net_osc"])
                if "private_net_osc" in idf2.columns
                else "-"
            )
            frgn_osc = (
                _osc_two_day_rise(idf2["frgn_net_osc"])
                if "frgn_net_osc" in idf2.columns
                else "-"
            )
            rows.append(
                {
                    "스크리닝명": scr_name,
                    "섹터성격": _st_disp,
                    "ticker": tk,
                    "종목명": name,
                    "섹터": sec_disp,
                    "테마명": th,
                    "현재가": f"{close:,.0f}" if np.isfinite(close) else "",
                    "sma5위": s5,
                    "sma10위": s10,
                    "sma20위": s20,
                    "b%": bdisp,
                    "에너지배율": erdisp,
                    "Talent비율": taldisp,
                    "시가총액": mcap_disp,
                    "시총비중": mshare_disp,
                    "거래대금비중": vshare_disp,
                    "50일신고가": hi50,
                    "120일신고가": hi120,
                    "250일신고가": hi250,
                    "밴드스퀴즈": bsq_disp,
                    "CSI": csi_disp,
                    "기관OSC": inst_osc,
                    "국민연금등OSC": pension_osc,
                    "투신OSC": trust_osc,
                    "사모OSC": private_osc,
                    "외국인OSC": frgn_osc,
                    "_sort_sn": scr_name or "",
                    "_sort_st": _st_disp or "",
                    "_sort_tk": tkz,
                    "_sort_nm": str(name) if name is not None else "",
                    "_sort_sec": sec_disp or "",
                    "_sort_th": th or "",
                    "_sort_close": float(close) if np.isfinite(close) else None,
                    "_sort_s5": s5,
                    "_sort_s10": s10,
                    "_sort_s20": s20,
                    "_sort_pb": float(pb * 100) if np.isfinite(pb) else None,
                    "_sort_er": float(er) if np.isfinite(er) else None,
                    "_sort_talent": float(tal) if np.isfinite(tal) else None,
                    "_sort_mcap": float(mc_raw) if np.isfinite(mc_raw) else None,
                    "_sort_mshare": float(ms_raw) if np.isfinite(ms_raw) else None,
                    "_sort_vshare": float(vs_raw) if np.isfinite(vs_raw) else None,
                    "_sort_50": hi50,
                    "_sort_120": hi120,
                    "_sort_250": hi250,
                    "_sort_bsq": float(bsq_v) if np.isfinite(bsq_v) else None,
                    "_sort_csi": csi_disp,
                    "_sort_inst": inst_osc,
                    "_sort_pension": pension_osc,
                    "_sort_trust": trust_osc,
                    "_sort_private": private_osc,
                    "_sort_frgn": frgn_osc,
                }
            )
        sum_df = pd.DataFrame(rows)
        n_rows = len(sum_df)
        disp_cols = [
            "스크리닝명",
            "섹터성격",
            "ticker",
            "종목명",
            "섹터",
            "테마명",
            "현재가",
            "sma5위",
            "sma10위",
            "sma20위",
            "b%",
            "에너지배율",
            "Talent비율",
            "시가총액",
            "시총비중",
            "거래대금비중",
            "50일신고가",
            "120일신고가",
            "250일신고가",
            "밴드스퀴즈",
            "CSI",
            "기관OSC",
            "국민연금등OSC",
            "투신OSC",
            "사모OSC",
            "외국인OSC",
        ]
        sort_cols = [
            "_sort_sn",
            "_sort_st",
            "_sort_tk",
            "_sort_nm",
            "_sort_sec",
            "_sort_th",
            "_sort_close",
            "_sort_s5",
            "_sort_s10",
            "_sort_s20",
            "_sort_pb",
            "_sort_er",
            "_sort_talent",
            "_sort_mcap",
            "_sort_mshare",
            "_sort_vshare",
            "_sort_50",
            "_sort_120",
            "_sort_250",
            "_sort_bsq",
            "_sort_csi",
            "_sort_inst",
            "_sort_pension",
            "_sort_trust",
            "_sort_private",
            "_sort_frgn",
        ]
        sort_types = [
            "str",
            "str",
            "str",
            "str",
            "str",
            "str",
            "num",
            "str",
            "str",
            "str",
            "num",
            "num",
            "num",
            "num",
            "num",
            "num",
            "str",
            "str",
            "str",
            "num",
            "str",
            "str",
            "str",
            "str",
            "str",
            "str",
        ]
        th_labels = [
            "스크리닝명",
            "섹터성격",
            "ticker",
            "종목명",
            "섹터",
            "테마명",
            "현재가",
            "sma5위 여부",
            "sma10위 여부",
            "sma20위 여부",
            "b%",
            "에너지배율",
            "Talent 비율(%)",
            "시가총액",
            "시총비중(%)",
            "거래대금비중(%)",
            "50일 신고가 여부",
            "120일 신고가여부",
            "250일 신고가여부",
            "밴드스퀴즈",
            "CSI",
            "기관OSC",
            "국민연금등OSC",
            "투신OSC",
            "사모OSC",
            "외국인OSC",
        ]

        def _sort_attr(stype, raw):
            if stype == "num":
                if raw is None or (isinstance(raw, float) and not np.isfinite(raw)):
                    return ' data-sort-type="num" data-sort=""'
                return f' data-sort-type="num" data-sort="{float(raw):.12g}"'
            s = "" if raw is None else str(raw)
            esc = html_module.escape(s, quote=True)
            return f' data-sort-type="str" data-sort="{esc}"'

        th_titles = {
            12: "최근 120거래일 중 종가가 시가 대비 +10% 이상 상승한 날의 비중(%)",
            14: "전체 보통주 시가총액 합 대비 해당 종목 시가총액 비중(%)",
            15: "전체 보통주 당일 거래대금 합 대비 해당 종목 거래대금 비중(%)",
            21: "기관(연기금+투신+사모) 순매수금액 OSC. 기관합계(7050)가 아님.",
            22: "국민연금등(6000) 순매수금액 OSC",
            23: "투신(3000) 순매수금액 OSC",
            24: "사모(3100) 순매수금액 OSC",
            25: "외국인(9000) 순매수금액 OSC",
        }
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
                tds.append(
                    f"<td{_sort_attr(st, sk)}>{html_module.escape(str(disp))}</td>"
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
        _tilt_total, _tilt_major = _sector_tilt_counts(
            sector_map, tickers, level="major"
        )
        _tilt_total, _tilt_detail = _sector_tilt_counts(
            sector_map, tickers, level="detail"
        )

        def _tilt_ul(rows, top_n: int, *, warn_top1: bool) -> str:
            show = [r for r in rows if r[0] != "(미분류)"][:top_n]
            show += [r for r in rows if r[0] == "(미분류)"]
            items = []
            for i, (lab, n, pct) in enumerate(show):
                warn = warn_top1 and _sector_tilt_should_warn(rows, i)
                line = _sector_tilt_line(lab, n, pct, warn=warn)
                items.append(f"<li>{html_module.escape(line)}</li>")
            return "".join(items)

        _major_ul = _tilt_ul(_tilt_major, 15, warn_top1=True)
        _detail_ul = _tilt_ul(_tilt_detail, 15, warn_top1=False)
        sector_tilt_html = f"""
<h3 style="font-family:Segoe UI,Malgun Gothic,sans-serif;margin:12px 0 6px">섹터 쏠림 — 대분류</h3>
<p style="font-family:Segoe UI,Malgun Gothic,sans-serif;font-size:12px;color:#555;margin:0 0 8px">
선정 종목 {_tilt_total}개 기준, sector1 대분류(첫 '_' 앞) 상위 15개</p>
<ul style="font-family:Segoe UI,Malgun Gothic,sans-serif;font-size:13px;line-height:1.55;margin:0 0 16px;padding-left:1.2rem">
{_major_ul}
</ul>
<h3 style="font-family:Segoe UI,Malgun Gothic,sans-serif;margin:12px 0 6px">섹터 쏠림 — 세부</h3>
<p style="font-family:Segoe UI,Malgun Gothic,sans-serif;font-size:12px;color:#555;margin:0 0 8px">
선정 종목 {_tilt_total}개 기준, sector1 상위 15개</p>
<ul style="font-family:Segoe UI,Malgun Gothic,sans-serif;font-size:13px;line-height:1.55;margin:0 0 16px;padding-left:1.2rem">
{_detail_ul}
</ul>
"""
        body = f"""
<h2 style="font-family:Segoe UI,Malgun Gothic,sans-serif">스크리닝 요약 ({html_module.escape(folder_name)})</h2>
<p style="font-family:Segoe UI,Malgun Gothic,sans-serif">행 수: {n_rows}.
표시: 시총 3,000억 이상 (스크리닝 유니버스와 동일).
기관OSC는 연기금+투신+사모 순매수금액 OSC(누적 {INVESTOR_OSC_CUM_DAYS}일)이며 기관합계(7050)가 아닙니다.
외국인OSC는 9000 순매수금액 OSC입니다.
외국인 지분율(%) = KRX 12023 외국인보유량 기준.
섹터성격: 58 섹터중립 측정(전체 기간) 기준. 종목=업종을 중립화해도 초과수익이 남는 패턴,
업종=초과수익이 대부분 업종 베타인 패턴, 혼합=중간. 선정 여부에는 영향을 주지 않는다.</p>
{sector_tilt_html}
<table id="summaryTable" class="s">
<thead><tr>{ths}</tr></thead>
<tbody>{"".join(trs)}</tbody>
</table>
{sort_script}
"""
    page = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="utf-8"/>
<title>스크리닝 요약</title>
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
table.s td:nth-child(7),
table.s td:nth-child(11),
table.s td:nth-child(12),
table.s td:nth-child(13),
table.s td:nth-child(14),
table.s td:nth-child(15),
table.s td:nth-child(16),
table.s td:nth-child(20) {{ text-align:right; }}
table.s td:nth-child(2),
table.s td:nth-child(8), table.s td:nth-child(9), table.s td:nth-child(10),
table.s td:nth-child(17), table.s td:nth-child(18), table.s td:nth-child(19),
table.s td:nth-child(21), table.s td:nth-child(22), table.s td:nth-child(23),
table.s td:nth-child(24), table.s td:nth-child(25), table.s td:nth-child(26) {{ text-align:center; }}
table.s tbody tr:nth-child(even) {{ background:#fafafa; }}
</style></head><body>
{body}
</body></html>"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(page)
    abs_path = os.path.abspath(output_path).replace("\\", "/")
    print(f"스크리닝 요약 HTML 저장: {output_path}")
    if open_browser:
        webbrowser.open(f"file:///{abs_path}")
    return output_path


def create_charts_for_selected_stocks(selected_stock_list, rs_df, money, risk, engine):
    """선별된 종목들의 차트 생성"""
    
    print("\n" + "=" * 80)
    print("📊 차트 생성 시작")
    print("=" * 80)
    
    today = datetime.date.today()
    folder_name = today.strftime('%Y-%m-%d')

    # 폴더 경로 설정 (현재 디렉토리 내에 폴더 생성)
    folder_path = os.path.join('C:\\Users\\hachi\\OneDrive\\01. Trading\\picking\\KRX', folder_name)

    os.makedirs(folder_path, exist_ok=True)
    os.chdir(folder_path)

    print(f"차트 저장 경로: {folder_path}")
    print(f"생성할 차트 수: {len(selected_stock_list)-1}개")
        
    chart_start_time = time.time()

    for stock in tqdm(selected_stock_list, desc="차트 생성"):
        
        if isinstance(stock, pd.DataFrame) == False:
            typeP = stock
        else:
            try:
                # 지수 가져오기
                query = """select sector_cd, sector_nm from krx_ticker_sector where ticker = '{}';"""
                query = query.format(stock.iloc[-1].ticker)                
                sector = pd.read_sql_query(query, con=engine)
                sector = sector.set_index('sector_cd')
                
                sector_df = pd.DataFrame(stock.close)
                sector_df.columns = [stock.iloc[0]['name']]  # close를 종목명으로 변경
                
                for s in sector.index:
                    q = """select date, close from krx_index_ohlcv where ticker = '{}';""".format(s)
                    close = pd.read_sql_query(q, con=engine)
                    close = close.set_index('date')
                    close.columns = [sector.loc[s].sector_nm]
                    
                    sector_df = pd.merge(sector_df, close, left_index=True, right_index=True, how='left')
                
                # Sector: 해당 종목(close) + 코스피 또는 코스닥 메인 지수만 (tradingKIS_test.py와 동일)
                try:
                    k = stock.iloc[-1].ticker
                    market = 'KOSPI' if k in rs_kospi_df.index else 'KOSDAQ'
                    first_col = sector_df.columns[0]
                    market_index_col = None
                    for c in sector_df.columns[1:]:
                        cs = str(c).strip()
                        cu = cs.upper()
                        if market == 'KOSPI' and (cs == '코스피' or cu == 'KOSPI'):
                            market_index_col = c
                            break
                        if market == 'KOSDAQ' and (cs == '코스닥' or cu == 'KOSDAQ'):
                            market_index_col = c
                            break
                    if market_index_col is not None:
                        sector_df = sector_df[[first_col, market_index_col]].copy()
                    else:
                        sector_df = sector_df[[first_col]].copy()
                except Exception:
                    pass
                
                gen_chart(stock, typeP, sector_df, rs_df, CHART_PERIOD_DAYS, money, risk, save_jpeg=SAVE_JPEG)
                time.sleep(1)
                
            except Exception as e:
                print(f"차트 생성 중 오류 발생: {e}")
                continue

    chart_time = time.time() - chart_start_time
    
    print("\n" + "=" * 80)
    print("📊 차트 생성 완료")
    print("=" * 80)
    print(f"⏱️ 차트 생성 시간: {chart_time:.2f}초")
    print(f"📊 생성된 차트 수: {len(selected_stock_list)-1}개")
    print("=" * 80)

def run_main(do_summary=True, do_charts=False):
    """티커 로드 → 지표 → 스크리닝 → (선택) 요약/차트. import 만으로는 실행되지 않음.

    Parameters
    ----------
    do_summary : bool
        export_screening_summary_html 실행 여부 (기본 True).
    do_charts : bool
        create_charts_for_selected_stocks 실행 여부 (기본 False — 기존과 동일).

    Returns
    -------
    dict
        engine, ticker_list, ohlcv_data, indicators_data, volume_data,
        selected_df, selected_stock_list(=selected_stock35), rs_df, money, risk 등.
        동일 내용이 모듈 전역 CONTEXT 에도 보관된다.
    """
    global engine, conn_str, ticker_list
    global ohlcv_data, indicators_data, volume_data
    global rs_df, rs_kospi_df, rs_kosdaq_df
    global selected_df, selected_stocks, result, debug_counts
    global selected_stock11, selected_stock12, selected_stock13, selected_stock14, selected_stock15
    global selected_stock16, selected_stock17, selected_stock18
    global selected_stock21, selected_stock22, selected_stock23, selected_stock24, selected_stock25
    global selected_stock26, selected_stock27, selected_stock28, selected_stock29
    global selected_stock29a, selected_stock29b
    global selected_stock31, selected_stock32, selected_stock33, selected_stock34, selected_stock35, selected_stock36
    global selected_stock41, selected_stock42, selected_stock43
    global selected_stock51, selected_stock52, selected_stock53, selected_stock54, selected_stock55
    global selected_stock61, selected_stock71, selected_stock81
    global selected_stock91, selected_stock92, selected_stock93
    global CONTEXT

    ### 서버 접속
    engine = create_engine(db_url())
    conn_str = db_url().replace('mysql+pymysql://', 'mysql://', 1)

    ### 티커를 가져옴
    query = """
    select * from krx_ticker
    where 기준일 = (select max(기준일) from krx_ticker) and 종목구분 = '보통주';
    """

    ticker_list = pd.read_sql(query, con=engine)
    # 시가총액 컬럼이 있으면 가져오고, 없으면 None으로 설정
    if '시가총액' in ticker_list.columns:
        ticker_list = ticker_list[['종목코드', '종목명', '업종명', '시가총액']]
    else:
        ticker_list['시가총액'] = None
        ticker_list = ticker_list[['종목코드', '종목명', '업종명', '시가총액']]
    ticker_list = drop_excluded(ticker_list, "종목코드")
    ticker_list = ticker_list.set_index('종목코드')


    # 성능 모니터링 시작
    print("=" * 80)
    print("🚀 최적화된 KRX 주식 선별 시스템 시작")
    print(f"· USE_ADJ_PRICE={USE_ADJ_PRICE} / OHLCV_TABLE={OHLCV_TABLE}")
    print("=" * 80)

    start_time = time.time()

    ### OHLCV 데이터 로딩 (병렬 처리)
    print("\n" + "=" * 80)
    print("📊 OHLCV 데이터 로딩")
    print("=" * 80)

    data_load_start = time.time()
    ohlcv_data = load_ohlcv_parallel(ticker_list, engine, max_workers=MAX_WORKERS_DATA_LOAD)
    ohlcv_data = filter_ohlcv_zero_latest(ohlcv_data)
    data_load_time = time.time() - data_load_start
    print(f"⏱️ OHLCV 데이터 로딩 완료: {data_load_time:.2f}초")
    print(f"📊 성공률: {len(ohlcv_data)}/{len(ticker_list)} ({len(ohlcv_data)/len(ticker_list)*100:.1f}%)")

    ### 투자자/외국인 벌크 로드 (지표 ThreadPool 진입 전, 이후 캐시 읽기 전용)
    global _INVESTOR_CACHE, _FOREIGN_CACHE
    _INVESTOR_CACHE = None
    _FOREIGN_CACHE = None
    bulk_tickers = list(ohlcv_data.keys())
    bulk_start = time.time()
    _INVESTOR_CACHE = _bulk_load_investor_trading(engine, bulk_tickers)
    _FOREIGN_CACHE = _bulk_load_foreign_holding(engine, bulk_tickers)
    bulk_rows = sum(len(v) for v in _INVESTOR_CACHE.values()) + sum(
        len(v) for v in _FOREIGN_CACHE.values()
    )
    bulk_time = time.time() - bulk_start
    print(
        f"투자자/외국인 벌크 로드: 티커 {len(bulk_tickers)}종, 행 {bulk_rows} ({bulk_time:.1f}s)"
    )

    ### 지표 계산 (병렬 처리)
    print("\n" + "=" * 80)
    print("📈 지표 계산 시작")
    print("=" * 80)

    indicators_start_time = time.time()
    indicators_data = calculate_indicators_parallel(ohlcv_data, max_workers=MAX_WORKERS_INDICATORS)
    indicators_time = time.time() - indicators_start_time
    print(f"⏱️ 지표 계산 완료: {indicators_time:.2f}초")

    ### 매물대 계산 (병렬 처리)
    print("\n" + "=" * 80)
    print("📊 매물대 지표 생성")
    print("=" * 80)

    volume_start_time = time.time()
    volume_data = calculate_volume_band_parallel(ohlcv_data, max_workers=MAX_WORKERS_VOLUME)
    volume_time = time.time() - volume_start_time
    print(f"⏱️ 매물대 계산 완료: {volume_time:.2f}초")

    ### RS 데이터 DB에서 가져오기
    rs_start_time = time.time()
    _rs_empty = pd.DataFrame(columns=["rs10_score", "rs20_score", "rs50_score", "rs_score"])
    if not USE_RS:
        print("\n" + "=" * 80)
        print("📊 RS 스킵 (USE_RS=False)")
        print("=" * 80)
        rs_kospi_df = _rs_empty.copy()
        rs_kosdaq_df = _rs_empty.copy()
        rs_df = _rs_empty.copy()
        rs_time = time.time() - rs_start_time
        print(f"⏱️ RS 스킵 완료: {rs_time:.2f}초")
    else:
        print("\n" + "=" * 80)
        print("📊 RS 데이터 로드 시작")
        print("=" * 80)
        _rs_col_sql = ", ".join(f"rs_{p}d" for p in RS_PERIODS)
        _rs_avg_cols = tuple(f"rs_{p}d" for p in RS_PERIODS if p != 10)

        # DB에서 최신 날짜의 RS 데이터 가져오기
        query_max_date = """
            SELECT MAX(date) as max_date
            FROM krx_relative_strength;
        """
        max_rs_date = pd.read_sql_query(query_max_date, con=engine)

        if len(max_rs_date) > 0 and max_rs_date.iloc[0]['max_date'] is not None:
            latest_date = max_rs_date.iloc[0]['max_date']
            print(f"최신 RS 데이터 날짜: {latest_date}")

            # 최신 날짜의 RS 데이터 가져오기
            query_rs = f"""
                SELECT ticker, market_type, {_rs_col_sql}
                FROM krx_relative_strength
                WHERE date = %s;
            """
            rs_data = pd.read_sql_query(query_rs, con=engine, params=(latest_date,))

            if len(rs_data) == 0:
                print("⚠️ 경고: RS 데이터가 없습니다. 최신 날짜로 다시 시도합니다.")
                # 최신 날짜로 다시 조회
                query_rs_all = f"""
                    SELECT ticker, market_type, {_rs_col_sql}, date
                    FROM krx_relative_strength
                    ORDER BY date DESC
                    LIMIT 10000;
                """
                rs_data_all = pd.read_sql_query(query_rs_all, con=engine)
                if len(rs_data_all) > 0:
                    latest_date = rs_data_all.iloc[0]['date']
                    print(f"실제 최신 날짜: {latest_date}")
                    rs_data = rs_data_all[rs_data_all['date'] == latest_date].copy()
                    rs_data = rs_data[['ticker', 'market_type'] + [f"rs_{p}d" for p in RS_PERIODS]]

            if len(rs_data) > 0:
                print(f"RS 데이터 로드 완료: {len(rs_data)}개 종목")

                # 코스피/코스닥별로 분리
                rs_kospi_df = rs_data[rs_data['market_type'] == 'KOSPI'].copy()
                rs_kosdaq_df = rs_data[rs_data['market_type'] == 'KOSDAQ'].copy()

                # ticker를 인덱스로 설정
                if len(rs_kospi_df) > 0:
                    rs_kospi_df = rs_kospi_df.set_index('ticker')
                    rs_kospi_df['rs10_score'] = rs_kospi_df['rs_10d'] if 'rs_10d' in rs_kospi_df.columns else np.nan
                    rs_kospi_df['rs20_score'] = rs_kospi_df['rs_20d'] if 'rs_20d' in rs_kospi_df.columns else np.nan
                    rs_kospi_df['rs50_score'] = rs_kospi_df['rs_50d'] if 'rs_50d' in rs_kospi_df.columns else np.nan
                    # rs_score = 가중평균(rs_20/50/120/200) — 정본 indicators_core.rs_avg
                    rs_kospi_df['rs_score'] = rs_avg(
                        frame=rs_kospi_df, cols=_rs_avg_cols
                    ).round(2)
                    rs_kospi_df = rs_kospi_df[['rs10_score', 'rs20_score', 'rs50_score', 'rs_score']]
                    print(f"코스피 RS 데이터: {len(rs_kospi_df)}개 종목")
                else:
                    rs_kospi_df = _rs_empty.copy()
                    print("⚠️ 코스피 RS 데이터가 없습니다.")

                if len(rs_kosdaq_df) > 0:
                    rs_kosdaq_df = rs_kosdaq_df.set_index('ticker')
                    rs_kosdaq_df['rs10_score'] = rs_kosdaq_df['rs_10d'] if 'rs_10d' in rs_kosdaq_df.columns else np.nan
                    rs_kosdaq_df['rs20_score'] = rs_kosdaq_df['rs_20d'] if 'rs_20d' in rs_kosdaq_df.columns else np.nan
                    rs_kosdaq_df['rs50_score'] = rs_kosdaq_df['rs_50d'] if 'rs_50d' in rs_kosdaq_df.columns else np.nan
                    rs_kosdaq_df['rs_score'] = rs_avg(
                        frame=rs_kosdaq_df, cols=_rs_avg_cols
                    ).round(2)
                    rs_kosdaq_df = rs_kosdaq_df[['rs10_score', 'rs20_score', 'rs50_score', 'rs_score']]
                    print(f"코스닥 RS 데이터: {len(rs_kosdaq_df)}개 종목")
                else:
                    rs_kosdaq_df = _rs_empty.copy()
                    print("⚠️ 코스닥 RS 데이터가 없습니다.")

                # 두 데이터프레임 합치기
                if len(rs_kospi_df) > 0 and len(rs_kosdaq_df) > 0:
                    rs_df = pd.concat([rs_kospi_df, rs_kosdaq_df])
                elif len(rs_kospi_df) > 0:
                    rs_df = rs_kospi_df
                elif len(rs_kosdaq_df) > 0:
                    rs_df = rs_kosdaq_df
                else:
                    rs_df = _rs_empty.copy()
                    print("⚠️ 경고: RS 데이터프레임이 비어있습니다.")

            else:
                print("⚠️ 경고: RS 데이터를 가져올 수 없습니다. 빈 데이터프레임을 생성합니다.")
                rs_kospi_df = _rs_empty.copy()
                rs_kosdaq_df = _rs_empty.copy()
                rs_df = _rs_empty.copy()
        else:
            print("⚠️ 경고: RS 데이터가 없습니다. 빈 데이터프레임을 생성합니다.")
            rs_kospi_df = _rs_empty.copy()
            rs_kosdaq_df = _rs_empty.copy()
            rs_df = _rs_empty.copy()

        rs_time = time.time() - rs_start_time
        print(f"⏱️ RS 데이터 로드 완료: {rs_time:.2f}초")





    print("\n" + "=" * 80)
    print("🔍 종목 스크리닝 시작")
    print("=" * 80)

    screening_start_time = time.time()
    _screener = SCREEN_FN or screen_all
    _screener_label = (
        f"{getattr(_screener, '__name__', repr(_screener))}(주봉 전용)"
        if SCREEN_FN is not None
        else "screen_all(일봉 기본)"
    )
    print(f"스크리너: {_screener_label}")

    screening_result = _screener(
        indicators_data,
        rs_df=rs_df,
        volume_data=volume_data,
        ticker_list=ticker_list,
        audit_ticker=audit_ticker,
    )

    result = screening_result["result"]
    debug_counts = screening_result["debug_counts"]
    selected_stocks = screening_result["selected_stocks"]
    selected_df = screening_result["selected_df"]
    selected_stock_list = screening_result["selected_stock_list"]

    selected_stock11 = ['p11'] + result.get('p11', [])
    selected_stock12 = ['p12'] + result.get('p12', [])
    selected_stock13 = ['p13'] + result.get('p13', [])
    selected_stock14 = ['p14'] + result.get('p14', [])
    selected_stock15 = ['p15'] + result.get('p15', [])
    selected_stock16 = ['p16'] + result.get('p16', [])
    selected_stock17 = ['p17'] + result.get('p17', [])
    selected_stock18 = ['p18'] + result.get('p18', [])
    selected_stock21 = ['p21'] + result.get('p21', [])
    selected_stock22 = ['p22'] + result.get('p22', [])
    selected_stock23 = ['p23'] + result.get('p23', [])
    selected_stock24 = ['p24'] + result.get('p24', [])
    selected_stock25 = ['p25'] + result.get('p25', [])
    selected_stock26 = ['p26'] + result.get('p26', [])
    selected_stock27 = ['p27'] + result.get('p27', [])
    selected_stock28 = ['p28'] + result.get('p28', [])
    selected_stock29 = ['p29'] + result.get('p29', [])
    selected_stock29a = ['p29'] + result.get('p29a', [])
    selected_stock29b = ['p29'] + result.get('p29b', [])
    selected_stock31 = ['p31'] + result.get('p31', [])
    selected_stock32 = ['p32'] + result.get('p32', [])
    selected_stock33 = ['p33'] + result.get('p33', [])
    selected_stock34 = ['p34'] + result.get('p34', [])
    selected_stock35 = ['p35'] + result.get('p35', [])
    selected_stock36 = ['p36'] + result.get('p36', [])
    selected_stock41 = ['p41'] + result.get('p41', [])
    selected_stock42 = ['p42'] + result.get('p42', [])
    selected_stock43 = ['p43'] + result.get('p43', [])
    selected_stock51 = ['p51'] + result.get('p51', [])
    selected_stock52 = ['p52'] + result.get('p52', [])
    selected_stock53 = ['p53'] + result.get('p53', [])
    selected_stock54 = ['p54'] + result.get('p54', [])
    selected_stock55 = ['p55'] + result.get('p55', [])
    selected_stock61 = ['p61'] + result.get('p61', [])
    selected_stock71 = ['p71'] + result.get('p71', [])
    selected_stock81 = ['p81'] + result.get('p81', [])
    selected_stock91 = ['p91'] + result.get('p91', [])
    selected_stock92 = ['p92'] + result.get('p92', [])
    selected_stock93 = ['p93'] + result.get('p93', [])

    screening_time = time.time() - screening_start_time
    print(f"⏱️ 스크리닝 완료: {screening_time:.2f}초")

    if selected_df is not None and len(selected_df) > 0:
        _sec_map = _screening_summary_sectors(
            engine, selected_df["ticker"].astype(str).tolist()
        )
        _tk_list = selected_df["ticker"].astype(str).tolist()
        _maj_n, _maj_rows = _sector_tilt_counts(_sec_map, _tk_list, level="major")
        _det_n, _det_rows = _sector_tilt_counts(_sec_map, _tk_list, level="detail")
        print("📊 섹터 쏠림 — 대분류 Top 10")
        for _i, (_lab, _n, _pct) in enumerate(_maj_rows[:10]):
            _warn = _sector_tilt_should_warn(_maj_rows, _i)
            print(f"   · {_sector_tilt_line(_lab, _n, _pct, warn=_warn)}")
        print("📊 섹터 쏠림 — 세부 Top 10")
        for _lab, _n, _pct in _det_rows[:10]:
            print(f"   · {_sector_tilt_line(_lab, _n, _pct)}")

    # 디버깅 정보 출력
    print("\n" + "=" * 80)
    print("📊 스크리닝 디버깅 정보")
    print("=" * 80)
    print(f"📈 총 지표 데이터: {debug_counts['total_indicators']}개")
    print(f"✅ 기본 필터 통과: {debug_counts['passed_basic_filter']}개")
    print(f"✅ ATR 필터 통과: {debug_counts['passed_atr_filter']}개")
    print(f"✅ 패턴 매칭 선택: {debug_counts.get('selected_by_pattern', len(selected_stocks))}개")

    if debug_counts['passed_basic_filter'] == 0:
        print("\n⚠️ 기본 필터를 통과한 종목이 없습니다.")
        print("   체크 사항:")
        print(f"   - 지표 데이터: {len(indicators_data)}개")
        print(f"   - 매물대 데이터: {len(volume_data)}개")

    if debug_counts['passed_basic_filter'] > 0 and debug_counts['passed_atr_filter'] == 0:
        print("\n⚠️ ATR 필터를 통과한 종목이 없습니다.")
        print(f"   기본 필터 통과 종목 중 (ATR14/close)*1.5 < 0.1 조건을 만족하는 종목이 없습니다.")

    if debug_counts['passed_atr_filter'] > 0 and len(selected_stocks) == 0:
        print("\n⚠️ 패턴 매칭 조건을 만족하는 종목이 없습니다.")
        print("   각 패턴의 조건이 너무 까다로울 수 있습니다.")
    print("=" * 80)


    if len(selected_df) == 0:
        print("⚠️ 선별된 종목이 없습니다.")


    # 데이터베이스에 저장
    if len(selected_df) > 0:
        print("\n" + "=" * 80)
        print("💾 데이터베이스에 저장 중...")
        print("=" * 80)

        con = pymysql.connect(user=require_env('DB_USER'),
        passwd=require_env('DB_PASSWORD'),
        host='127.0.0.1',
        db='kor_stock_db',
        charset='utf8')

        mycursor = con.cursor()

        query = """
            insert into krx_selected_stock (date, type, ticker, company)
            values (%s, %s, %s, %s) as new
            on duplicate key update
            date=new.date, type=new.type, ticker=new.ticker, company=new.company;
        """

        args = selected_df.values.tolist()
        mycursor.executemany(query, args)
        con.commit()            

        con.close()

        print(f"✅ {len(selected_df)}개 종목 데이터베이스 저장 완료")
    else:
        print("\n" + "=" * 80)
        print("⚠️ 저장할 종목이 없습니다. 데이터베이스 저장을 건너뜁니다.")
        print("=" * 80)

    # 1) 스크리닝 결과 요약 HTML
    if do_summary:
        export_screening_summary_html(selected_df, indicators_data, engine)

    # 2) 선별 종목 차트 생성 (기본 꺼짐 — 기존과 동일)
    if do_charts:
        create_charts_for_selected_stocks(selected_stock_list, rs_df, money, risk, engine)

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
    CONTEXT.clear()
    CONTEXT.update(ctx)
    return ctx


def _require_vars(*names: str) -> bool:
    g = globals()
    missing = [n for n in names if n not in g or g[n] is None]
    if missing:
        print(f"⚠️ 필수 변수 없음: {missing}\n   → 셀1을 먼저 실행하세요.")
        return False
    return True


def cell1_pipeline(do_summary=True):
    """데이터 로드 → 지표 → 스크리닝. 차트는 셀2."""
    ctx = run_main(do_summary=do_summary, do_charts=False)
    globals().update({
        "engine": ctx["engine"],
        "ticker_list": ctx["ticker_list"],
        "ohlcv_data": ctx["ohlcv_data"],
        "indicators_data": ctx["indicators_data"],
        "volume_data": ctx["volume_data"],
        "selected_df": ctx["selected_df"],
        "selected_stocks": ctx["selected_stocks"],
        "selected_stock_list": ctx["selected_stock_list"],
        "result": ctx["result"],
        "debug_counts": ctx["debug_counts"],
        "rs_df": ctx["rs_df"],
        "money": ctx["money"],
        "risk": ctx["risk"],
    })
    n = len(ctx["selected_df"]) if ctx.get("selected_df") is not None else 0
    print(f"✅ 파이프라인 완료 — selected_df {n}행. 차트=셀2 Run Cell.")
    return ctx


def cell2_charts(pattern=None, max_charts=None, dedupe=False):
    """
    pattern : "p35" / ["p11","p35"] / None(선정된 전 패턴)
    max_charts : 패턴별 최대 장수
    dedupe : True 면 여러 패턴에 중복 등장하는 종목을 1회만
    """
    if not _require_vars("result", "rs_df", "money", "risk", "engine"):
        return None
    if pattern is None:
        codes = [c for c in result.keys() if result.get(c)]
    elif isinstance(pattern, str):
        codes = [pattern]
    else:
        codes = list(pattern)

    seen = set()
    chart_list = []
    total = 0
    for code in codes:
        items = result.get(code) or []
        if not items:
            print(f"  {code}: 0건 — 건너뜀")
            continue
        picked = []
        for it in items:
            tk = None
            try:
                tk = str(it.iloc[-1]["ticker"])
            except Exception:
                pass
            if dedupe and tk and tk in seen:
                continue
            if tk:
                seen.add(tk)
            picked.append(it)
            if max_charts and len(picked) >= max_charts:
                break
        if not picked:
            continue
        chart_list.append(code)
        chart_list.extend(picked)
        total += len(picked)
        print(f"  {code}: {len(picked)}장")

    if total == 0:
        print("생성할 차트가 없습니다.")
        return None
    print(f"차트 대상 합계 {total}장 (패턴 {len([c for c in chart_list if isinstance(c, str)])}종)")
    create_charts_for_selected_stocks(chart_list, rs_df, money, risk, engine)
    return chart_list


# %% 1) 데이터 로드 → 지표 → 스크리닝 (오래 걸림)
if __name__ == "__main__":
    cell1_pipeline()
    print("F5 완료. 차트는 셀2 Run Cell.")
    raise SystemExit(0)


# %% 2) 차트 생성 — 패턴 선택 (셀1 실행 후 Run Cell)
if __name__ == "__main__":
    cell2_charts(pattern="p22")

# 새 셀을 51 파일 맨 끝에 추가한다 (셀2 아래).

# %% 3) 원본 표현식 대조 (셀1 실행 후 Run Cell)
def cell3_verify_original():
    """
    HEAD 커밋의 인라인 표현식을 그대로 옮겨 세고,
    레지스트리 결과와 비교한다. 리팩터 검증 전용.
    """
    ref = {c: 0 for c in ("p93", "p81", "p92", "p71", "p22")}
    n_atr = 0
    for k, i in indicators_data.items():
        idc = i.iloc
        _tk = str(k)
        try:
            if not (len(i) >= 120 and i.iloc[-1].open > 0
                    and ticker_list.loc[_tk]['시가총액'] >= DISPLAY_MCAP_MIN
                    and _tk not in audit_ticker):
                continue
            close = i.iloc[-1].close
            if not ((idc[-1].atr14 / close) < 0.1
                    and idc[-1].mtr7 / close < 0.2
                    and idc[-1].box7 / close < 0.3):
                continue
        except Exception:
            continue
        n_atr += 1

        if idc[-1].band20_q < 0.8 and idc[-1].band20_q > idc[-6].band20_q:
            ref["p93"] += 1
        if idc[-1].band20_q > idc[-2].band20_q and idc[-1].csi > idc[-2].csi:
            ref["p81"] += 1
        if idc[-1].band20_q < 0.2 and idc[-1].atr_q < 0.2 \
            and (idc[-1].max10 > idc[-11].max50 or idc[-1].max10 > idc[-11].max125
                 or idc[-1].max20 > idc[-21].max50 or idc[-1].max20 > idc[-21].max125
                 or idc[-1].max50 > idc[-51].max50 or idc[-1].max50 > idc[-51].max125):
            ref["p92"] += 1
        if idc[-1].mtr7 > idc[-8].atr14 * 1.5 and idc[-1].close < idc[-1].max5 \
            and idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 \
            and idc[-1].sma120 > idc[-1].sma200:
            ref["p71"] += 1
        if idc[-1].max10 > 0 and idc[-1].min50 > 0:
            _d50 = idc[-1].max50 - idc[-1].min50
            _d5 = idc[-1].max5 - idc[-1].min5
            if _d50 > 0 and _d5 > 0 and idc[-1].min50 > 0:
                if idc[-1].max10 / idc[-1].min50 > 1.9 and idc[-1].high < idc[-1].max10:
                    ref["p22"] += 1

    print(f"[verify] ATR 통과 {n_atr}종목")
    for c in ("p93", "p81", "p92", "p71", "p22"):
        print(f"   · {c}: 원본식 {ref[c]}건")

if __name__ == "__main__":
    cell3_verify_original()

# py_compile 만 확인하고 실행하지 않는다.