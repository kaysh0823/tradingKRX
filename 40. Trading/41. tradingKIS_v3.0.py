# -*- coding: utf-8 -*-
"""
tradingKIS_test.py — KIS 잔고 차트 (단일 파일)

Spyder: F5(전체 실행) 권장. 셀만 실행할 때는 작업 디렉터리를 본 파일 폴더로 맞출 것.
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

from exclusions import drop_excluded, filter_tickers
from indicators_core import atr_wilder, energy_ratio, rs_avg


import requests
import json
import pandas as pd
import numpy as np
import talib
from tqdm import tqdm
from sqlalchemy import create_engine
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import webbrowser
import os
import time
import gc
import inspect


def _script_dir():
    """Spyder F5 / 셀 실행 / IPython 모두에서 프로젝트 경로 반환."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        for fi in inspect.stack():
            p = getattr(fi, "filename", "") or ""
            if p.endswith("tradingKIS_test.py"):
                return os.path.dirname(os.path.abspath(p))
        wd = os.getcwd()
        if os.path.isfile(os.path.join(wd, "tradingKIS_test.py")):
            return wd
        return wd


_SCRIPT_DIR = _script_dir()

# 차트 표시 기간 (거래일 기준 약 1년)
CHART_PERIOD_DAYS = 252

# 지표 계산용 OHLCV 최대 행 수 (SMA250 등에 여유; 비정상 대용량·OOM·커널 종료 완화)
_MAX_OHLCV_ROWS_FOR_INDICATORS = 6000

# CSI (Pine Script: close loc length)
_CSI_LENGTH = 20

# --- 투자자 OSC (krx_investor_trading) ---
_INVESTOR_OSC_PERIOD = 20
_INVESTOR_CUM_DAYS = 10
_INVESTOR_OSC_SMOOTH = 2
_INVESTOR_OSC_COLS = ("inst_net_osc", "frgn_net_osc", "frgn_hold_osc")


def _normalize_ticker(ticker=None, ohlcv_df=None):
    if ticker is not None and str(ticker).strip():
        return str(ticker).strip().zfill(6)
    if ohlcv_df is not None and "ticker" in ohlcv_df.columns:
        v = ohlcv_df["ticker"].dropna()
        if not v.empty:
            return str(v.iloc[-1]).strip().zfill(6)
    return None


def _load_investor_trading(engine, ticker):
    if engine is None or ticker is None:
        return pd.DataFrame()
    t = str(ticker).strip().zfill(6)
    query = """
        SELECT date, `기관_순매매량`, `외국인_순매매량`, `외국인_보유율`
        FROM krx_investor_trading
        WHERE ticker = %(ticker)s
        ORDER BY date
    """
    try:
        inv = pd.read_sql(query, engine, params={"ticker": t})
    except Exception:
        return pd.DataFrame()
    if inv.empty:
        return inv
    inv["date"] = pd.to_datetime(inv["date"], errors="coerce")
    return inv.dropna(subset=["date"]).set_index("date")


def _stochastic_osc(series, period=_INVESTOR_OSC_PERIOD, smooth=_INVESTOR_OSC_SMOOTH):
    s = pd.to_numeric(series, errors="coerce")
    lo = s.rolling(period, min_periods=period).min()
    hi = s.rolling(period, min_periods=period).max()
    raw = 100 * (s - lo) / (hi - lo).replace(0, np.nan)
    return raw.ewm(span=smooth, adjust=False).mean().clip(0, 100)


def _compute_investor_oscillators(investor_df):
    if investor_df is None or investor_df.empty:
        return pd.DataFrame()
    inv = investor_df.copy()
    inv.index = pd.to_datetime(inv.index, errors="coerce").normalize()
    inv = inv[~inv.index.isna()].sort_index()
    inst = pd.to_numeric(inv.get("기관_순매매량"), errors="coerce").fillna(0)
    frgn = pd.to_numeric(inv.get("외국인_순매매량"), errors="coerce").fillna(0)
    hold = pd.to_numeric(inv.get("외국인_보유율"), errors="coerce")
    out = inv.copy()
    out["inst_net_osc"] = _stochastic_osc(inst.rolling(_INVESTOR_CUM_DAYS, min_periods=1).sum())
    out["frgn_net_osc"] = _stochastic_osc(frgn.rolling(_INVESTOR_CUM_DAYS, min_periods=1).sum())
    out["frgn_hold_osc"] = _stochastic_osc(hold)
    return out


def _attach_investor_osc(ohlcv_df, engine, ticker=None, investor_df=None):
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
    inv_cols = (
        "기관_순매매량", "외국인_순매매량", "외국인_보유율",
        "inst_net_osc", "frgn_net_osc", "frgn_hold_osc",
    )
    if investor_df is None or investor_df.empty:
        for col in inv_cols:
            d[col] = np.nan
        return d
    inv = _compute_investor_oscillators(investor_df)
    merged = d.join(inv, how="left")
    for col in inv_cols:
        if col in merged.columns:
            d[col] = merged[col]
    return d


def _has_investor_osc_data(df, min_valid=5):
    if df is None or df.empty:
        return False
    for col in _INVESTOR_OSC_COLS:
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


# --- 기술적 지표 ---

def get_indicators(d, tradeHist=None):
        
        d = d.reset_index()
        d = d.set_index('date')
        d.index = pd.to_datetime(d.index, errors="coerce")
        d = d.loc[d.index.notna()]
        d = d[~d.index.duplicated(keep="last")].sort_index()
        if d.empty:
            return d

        d['typical'] = talib.TYPPRICE(d.high, d.low, d.close)
        
        # d.loc[d['open'] >= d['close'], 'upper'] = d['open']
        # d.loc[d['open'] <= d['close'], 'lower'] = d['open']
        # d.loc[d['open'] <= d['close'], 'upper'] = d['close']
        # d.loc[d['open'] >= d['close'], 'lower'] = d['close']

        sma2 = talib.SMA(d.close, timeperiod=2)            
        sma5 = talib.SMA(d.close, timeperiod=5)    
        sma10 = talib.SMA(d.close, timeperiod=10)    
        sma20 = talib.SMA(d.close, timeperiod=20)
        sma30 = talib.SMA(d.close, timeperiod=30)
        sma50 = talib.SMA(d.close, timeperiod=50)
        sma60 = talib.SMA(d.close, timeperiod=60)
        sma120 =  talib.SMA(d.close, timeperiod=120)
        sma150 =  talib.SMA(d.close, timeperiod=150)
        sma200 =  talib.SMA(d.close, timeperiod=200)
        sma250 =  talib.SMA(d.close, timeperiod=250)
        
        sma_df = pd.DataFrame({'sma2': sma2, 'sma5': sma5, 'sma10': sma10, 'sma20': sma20, 'sma30': sma30, 'sma50': sma50, 'sma60': sma60,
                               'sma120': sma120, 'sma150': sma150, 'sma200': sma200, 'sma250': sma250})
        
        d = pd.concat([d, sma_df], axis=1)
        
        d['ema20'] = talib.EMA(d.close, timeperiod=20)    
        
        max5 =  talib.MAX(d.high, timeperiod=5)
        max10 =  talib.MAX(d.high, timeperiod=10)
        max20 =  talib.MAX(d.high, timeperiod=20)
        max50 =  talib.MAX(d.high, timeperiod=50)
        max125 =  talib.MAX(d.high, timeperiod=125)
        max250 =  talib.MAX(d.high, timeperiod=250)
        
        min5 =  talib.MIN(d.low, timeperiod=5)
        min10 =  talib.MIN(d.low, timeperiod=10)
        min20 =  talib.MIN(d.low, timeperiod=20)
        min50 =  talib.MIN(d.low, timeperiod=50)
        min125 =  talib.MIN(d.low, timeperiod=125)
        min250 =  talib.MIN(d.low, timeperiod=250)

        mid5 = talib.MIDPRICE(d.high, d.low, timeperiod=5)          
        mid10 = talib.MIDPRICE(d.high, d.low, timeperiod=10)        
        mid20 = talib.MIDPRICE(d.high, d.low, timeperiod=20)
        mid50 = talib.MIDPRICE(d.high, d.low, timeperiod=50)
        mid125 = talib.MIDPRICE(d.high, d.low, timeperiod=125)
        mid250 = talib.MIDPRICE(d.high, d.low, timeperiod=250)
        
        minmax_df = pd.DataFrame({'max5': max5, 'max10': max10, 'max20': max20, 'max50': max50, 'max125': max125, 'max250': max250,
                                  'min5': min5, 'min10': min10, 'min20': min20, 'min50': min50, 'min125': min125, 'min250': min250,
                                  'mid5': mid5, 'mid10': mid10, 'mid20': mid20, 'mid50': mid50, 'mid125': mid125, 'mid250': mid250})
        
        d = pd.concat([d, minmax_df], axis=1)
        
        atr4 = atr_wilder(d.high, d.low, d.close, 4)
        atr14 = atr_wilder(d.high, d.low, d.close, 14)
        atr10 = atr_wilder(d.high, d.low, d.close, 10)
        atr20 = atr_wilder(d.high, d.low, d.close, 20)
        atr30 = atr_wilder(d.high, d.low, d.close, 30)
        # d['atr120'] = atr_wilder(d.high, d.low, d.close, 120)
        # d['atr56'] = atr_wilder(d.high, d.low, d.close, 56)
        
        atr_df = pd.DataFrame({'atr4': atr4, 'atr10': atr10, 'atr20': atr20, 'atr30': atr30, 
                               'atr14': atr14})
        
        d = pd.concat([d, atr_df], axis=1)
        
        d['tr'] = talib.TRANGE(d.high, d.low, d.close)
        # d['mtr4'] = talib.MAX(d.tr, timeperiod=4)
        d['mtr14'] = talib.MAX(d.tr, timeperiod=14)
        d['mtr20'] = talib.MAX(d.tr, timeperiod=20)

        
        d['maxP_index'] = talib.MAXINDEX(d.close, timeperiod=60)
        d['minP_index'] = talib.MININDEX(d.close, timeperiod=60)
    
        d['maxV_index'] = talib.MAXINDEX(d.volume, timeperiod=60)
        
        
        d['slope20'] = talib.LINEARREG_ANGLE(d.typical, timeperiod=20)
        d['slope30'] = talib.LINEARREG_ANGLE(d.typical, timeperiod=30)
        d['slope40'] = talib.LINEARREG_ANGLE(d.typical, timeperiod=40)
        
        
        upperband, middleband, lowerband = talib.BBANDS(d.close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        
        d['bol20_up'] = upperband
        d['bol20_ma'] = middleband
        d['bol20_dn'] = lowerband
        
        d['band20_w'] = (d.bol20_up - d.bol20_dn)/d.bol20_ma
        
        d['band20_w_min'], d['band20_w_max'] = talib.MINMAX(d.band20_w, timeperiod=125)
        
        d['band20_q'] = (d.band20_w - d.band20_w_min)/(d.band20_w_max - d.band20_w_min)

        upperband, middleband, lowerband = talib.BBANDS(d.close, timeperiod=50, nbdevup=2, nbdevdn=2, matype=0)
        
        d['bol50_up'] = upperband
        d['bol50_ma'] = middleband
        d['bol50_dn'] = lowerband
        
        d['band50_w'] = (d.bol50_up - d.bol50_dn)/d.bol50_ma     
        
        upperband, middleband, lowerband = talib.BBANDS(d.close, timeperiod=120, nbdevup=2, nbdevdn=2, matype=0)
        
        d['bol120_up'] = upperband
        d['bol120_ma'] = middleband
        d['bol120_dn'] = lowerband
        
        d['band120_w'] = (d.bol120_up - d.bol120_dn)/d.bol120_ma      
        
        
        
        dsprt20 = d['sma2'] - d['sma20']
        dsprt50 = d['sma2'] - d['sma50']
        dsprt120 = d['sma2'] - d['sma120']
        
        
        dsprt_df = pd.DataFrame({'dsprt20': dsprt20, 'dsprt50': dsprt50, 'dsprt120': dsprt120
                                 })
        
        d = pd.concat([d, dsprt_df], axis=1)
        
        
        # CSI — Pine: cs=(close-sma(close,L))/atr(L); csi=sma(cs,2); fast=ema(cs,10); slow=ema(cs,20)
        _L = _CSI_LENGTH
        _csi_sma = talib.SMA(d.close, timeperiod=_L)
        _csi_atr = atr_wilder(d.high, d.low, d.close, _L)
        cs = (d.close - _csi_sma) / _csi_atr.replace(0, np.nan)
        d["csi"] = talib.SMA(cs, timeperiod=2)
        d["csi_fast"] = talib.EMA(cs, timeperiod=10)
        d["csi_slow"] = talib.EMA(cs, timeperiod=20)

        ## momentum
        
        d['di_P'] = talib.PLUS_DI(d.high, d.low, d.close, timeperiod=14)
        d['di_M'] = talib.MINUS_DI(d.high, d.low, d.close, timeperiod=14)
        d['adx'] = talib.ADX(d.high, d.low, d.close, timeperiod=14)
    
        
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
           
        d['vol_sma5'] = talib.SMA(d.volume, timeperiod=5)
        d['vol_sma20'] = talib.SMA(d.volume, timeperiod=20)
        d['vol_sma50'] = talib.SMA(d.volume, timeperiod=50)
        
        d['vol_sum5'] = talib.SUM(d.volume, timeperiod=5)
        d['vol_sum20'] = talib.SUM(d.volume, timeperiod=20)
        d['vol_sum50'] = talib.SUM(d.volume, timeperiod=50)
            
        d['vol_sum5_sma5'] = talib.SMA(d.vol_sum5, timeperiod=5)
        d['vol_sum20_sma20'] = talib.SMA(d.vol_sum20, timeperiod=20)
                
        d['vol_max5'] =  talib.MAX(d.volume, timeperiod=5)
        d['vol_max20'] =  talib.MAX(d.volume, timeperiod=20)
        d['vol_max50'] =  talib.MAX(d.volume, timeperiod=50)
       
        d.loc[(d['volume'] == 0) & (d['close'] != 0), ['open', 'high', 'low']] = d['close']
        
        d['obv'] = talib.OBV(d.close, d.volume)
        
        # d['obv_slope'] = talib.LINEARREG_ANGLE(d.obv, timeperiod=20)
        
        d['vol_vol50'] = d.volume/d.vol_sma50
        
        d['max_vol50'] = talib.MAX(d.vol_vol50, timeperiod=50)
        
        # d['volume_id50'] = d['vol_max50']/d['vol_sma50']
        # d['volume_id20'] = d['vol_max20']/d['vol_sma20']
            
        ### BOX
        
        
        box7 = talib.MAX(d.high, timeperiod=7) - talib.MIN(d.low, timeperiod=7)
        box14 = talib.MAX(d.high, timeperiod=14) - talib.MIN(d.low, timeperiod=14)
        box21 = talib.MAX(d.high, timeperiod=21) - talib.MIN(d.low, timeperiod=21)
        box30 = talib.MAX(d.high, timeperiod=30) - talib.MIN(d.low, timeperiod=30)
        box40 = talib.MAX(d.high, timeperiod=40) - talib.MIN(d.low, timeperiod=40)
        box50 = talib.MAX(d.high, timeperiod=50) - talib.MIN(d.low, timeperiod=50)
        
        box_df = pd.DataFrame({'box7': box7, 'box14': box14, 'box21': box21, 'box30': box30, 'box40': box40, 'box50': box50})
        
        d = pd.concat([d, box_df], axis=1)

        
        d['pb'] = (d.close - d.bol20_dn)/(d.bol20_up - d.bol20_dn)
        d['pm'] = (d.close - d.min20)/(d.max20 - d.min20)
        d['pc'] = (d.close - d.ch_dn2)/(d.ch_up2 - d.ch_dn2)
        
        d['pb_max'] = talib.MAX(d.pb, timeperiod=20)
        d['pb_min'] = talib.MIN(d.pb, timeperiod=20)
        
        d['mfi'] = talib.MFI(d.high, d.low, d.close, d.volume, timeperiod=14)
        d['willr'] = talib.WILLR(d.high, d.low, d.close, timeperiod=14)

        if tradeHist is not None:
            if isinstance(tradeHist, str):
                buyDay = datetime.strptime(tradeHist.replace("-", "")[:8], "%Y%m%d").date()
            elif isinstance(tradeHist, datetime):
                buyDay = tradeHist.date()
            else:
                buyDay = tradeHist
            
            try:
                loc_rows = d.loc[buyDay]
                if isinstance(loc_rows, pd.DataFrame):
                    risk = float(pd.to_numeric(loc_rows["atr14"], errors="coerce").iloc[-1])
                else:
                    risk = float(pd.to_numeric(loc_rows["atr14"], errors="coerce"))
                if not np.isfinite(risk):
                    risk = 0.0
            except Exception:
                risk = 0.0

            d['stop_loss'] = d.iloc[-1].ent_p - risk * 1.5
                
            d['stop_atr'] = min(d.iloc[-1].close, d.iloc[-1].open) - d.iloc[-1].atr14*1.5
            d['stop_mtr'] = min(d.iloc[-1].close, d.iloc[-1].open) - d.iloc[-1].mtr14*1 
            
            # maxIndex = talib.MAXINDEX(d.high, timeperiod=20)
    
            # d['stop_h'] = min(d.iloc[talib.MAXINDEX(d.high, timeperiod=20).iloc[-1]].close, 
            #                   d.iloc[talib.MAXINDEX(d.high, timeperiod=20).iloc[-1]].open) - talib.MAX(d.atr14, timeperiod=20).iloc[-1]*1.5
            
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
    if ohlcv_df is None or ohlcv_df.empty:
        return pd.DataFrame()
    work = ohlcv_df.copy()
    if len(work) > _MAX_OHLCV_ROWS_FOR_INDICATORS:
        work = work.iloc[-_MAX_OHLCV_ROWS_FOR_INDICATORS:].copy()
    try:
        out = get_indicators(work, ord_dt)
        if out is None or out.empty:
            return pd.DataFrame()
        return _attach_investor_osc(out, engine, ticker)
    except Exception as e:
        t = str(ticker).strip() if ticker is not None else "?"
        print(f"[WARN] 지표 생성 실패 (티커 {t}): {e}")
        return pd.DataFrame()


def _ensure_investor_osc_on_df(df, ticker):
    if _has_investor_osc_data(df):
        return df, True
    tcode = _normalize_ticker(ticker, df)
    enriched = _attach_investor_osc(df.copy(), engine, tcode)
    return enriched, _has_investor_osc_data(enriched)


### 비즈데이는 기준일을 정의 
biz_day = date.today().strftime('%Y%m%d')

# biz_day = '20250310'

### 휴일을 입력
holidays = ['2023-08-15', '2023-09-28', '2023-09-29', '2023-10-02', '2023-10-03', '2023-10-09', "2023-12-25", '2023-12-29',
            "2024-01-01", '2024-02-09', '2024-02-12', '2024-03-01', '2024-04-10', "2024-05-06", '2024-05-01', '2024-05-15', "2024-06-06",
            '2024-08-15', '2024-09-16', '2024-09-17', '2024-09-18', '2024-10-01', '2024-10-03', '2024-10-09', '2024-12-25', '2024-12-31',
            '2025-01-01', '2025-01-27', '2025-01-28', '2025-01-29', '2025-01-30', '2025-03-03', '2025-05-01', '2025-05-05', '2025-05-06',
            '2025-06-03', '2025-06-06', '2025-08-15', '2025-10-03', '2025-10-06', '2025-10-07', '2025-10-08', '2025-10-09',
            '2025-12-25', '2025-12-31', '2026-01-01', '2026-02-16', '2026-02-17', '2026-02-18', '2026-03-02', '2026-05-01',
            '2026-05-05', '2026-05-25']



### 비즈데이를 준으로 90일을 시작이로 정함 == 한투가 90일 기준임
fr = (datetime.strptime(biz_day, '%Y%m%d') + relativedelta(days=-90)).strftime("%Y%m%d")
to = datetime.strptime(biz_day, '%Y%m%d').strftime("%Y%m%d")

# 주식일별주문체결조회: TTTC8001R(3개월 이내) / CTSC9115R(3개월 이전) 분기일
_biz_dt = datetime.strptime(biz_day, '%Y%m%d')
_ccld_cutoff_dt = _biz_dt + relativedelta(months=-3)
CCLD_CUTOFF_DT = _ccld_cutoff_dt.strftime("%Y%m%d")
CCLD_OLD_END_DT = (_ccld_cutoff_dt + timedelta(days=-1)).strftime("%Y%m%d")



###################################
## 키 

app_key = require_env('KIS_APP_KEY')
app_secret = require_env('KIS_APP_SECRET')
account_no = "72627877"

url_base = "https://openapi.koreainvestment.com:9443" # 실전투자 도메인

#### 접근 토큰 발급 받기

# information

headers = {"content-type" : "application/json"}
path = "oauth2/tokenP"
body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret":  app_secret
        }

url = f"{url_base}/{path}"

print(url)


_TOKEN_CACHE_PATH = os.path.join(_SCRIPT_DIR, ".kis_token_cache.json")


def fetch_access_token(url_token, app_key, app_secret, timeout=20, use_cache=True):
    """KIS OAuth2 접근토큰 발급 (실패 시 응답 본문 포함 예외)."""
    cache_key = f"{url_token}|{app_key}"
    if use_cache and os.path.isfile(_TOKEN_CACHE_PATH):
        try:
            with open(_TOKEN_CACHE_PATH, "r", encoding="utf-8") as f:
                cache = json.load(f)
            if cache.get("key") == cache_key:
                exp = cache.get("expires_at", 0)
                if exp > time.time() + 60 and cache.get("access_token"):
                    return cache["access_token"]
        except (OSError, json.JSONDecodeError, TypeError):
            pass

    req_headers = {"content-type": "application/json"}
    req_body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "appsecret": app_secret,
    }
    res = requests.post(
        url_token, headers=req_headers, data=json.dumps(req_body), timeout=timeout
    )
    try:
        data = res.json()
    except ValueError:
        raise RuntimeError(
            f"토큰 발급 실패 (HTTP {res.status_code}): JSON 아님\n{res.text[:500]}"
        ) from None

    token = data.get("access_token")
    if token:
        expires_in = int(data.get("expires_in", 86400))
        if use_cache:
            try:
                with open(_TOKEN_CACHE_PATH, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "key": cache_key,
                            "access_token": token,
                            "expires_at": time.time() + max(expires_in - 300, 3600),
                        },
                        f,
                    )
            except OSError:
                pass
        return token

    err_parts = [
        f"토큰 발급 실패 (HTTP {res.status_code})",
        f"URL: {url_token}",
    ]
    for key in ("error_code", "error_description", "msg_cd", "msg1", "message", "detail"):
        if data.get(key):
            err_parts.append(f"{key}: {data[key]}")
    if len(err_parts) == 2:
        err_parts.append(f"응답: {data}")
    hint = (
        "\n힌트: 1분 이내 재발급 제한(EGW00133)이면 60초 후 재시도하거나 "
        "모의/실전 도메인·appkey가 일치하는지 확인하세요."
    )
    raise RuntimeError("\n".join(err_parts) + hint)


access_token = fetch_access_token(url, app_key, app_secret)
print("접근 토큰 발급 완료")

##### 해시키 발급 - 조회 외 주문, 정정, 취소 등 위해 필요
def hashkey(datas):
    path = 'uapi/hashkey'
    url = f"{url_base}/{path}"
    
    headers = {
        "content-type" : "application/json",
        "appkey": app_key,
        "appsecret":  app_secret
        }
    res = requests.post(url, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    
    return hashkey    
############################################



##### 주식 잔고 조회
def get_balance():
    path = "/uapi/domestic-stock/v1/trading/inquire-balance"   
    url = f"{url_base}/{path}"
    
    
    headers = {
            "content-type" : "application/json",
            "authorization" : f"Bearer {access_token}",
            "appkey": app_key,
            "appsecret":  app_secret,
            # "tr_id" : "TTTC8434R",
            "tr_id" : "TTTC8434R"
            # "hashkey" : hashkey(datas)
        }
    
    
    params = {
        "CANO" : account_no,  # 계좌번호 앞 8자리
        "ACNT_PRDT_CD" : "01", # 계좌번호 귀 2자리
        "AFHR_FLPR_YN" : "N",  # 시간외 단일가 여부
        "OFL_YN" : "",         # 공란
        "INQR_DVSN" : "01",    # 조회구분 01 대출일별, 02 종목별
        "UNPR_DVSN" : "01",    # 단가구분 01 기본값
        "FUND_STTL_ICLD_YN" : "N", # 펀드 결제분 포함여부
        "FNCG_AMT_AUTO_RDPT_YN" : "N", # 융자금액 자동상환여부
        "PRCS_DVSN" : "01", # 전일매매포함
        "CTX_AREA_FK100" : "",
        "CTX_AREA_NK100" : ""
        }
    
    res = requests.get(url, headers=headers, params=params)
    
    res.json()['output1']
    
    balance = pd.DataFrame.from_records(res.json()['output1'])
    
    balance = balance.drop(['bfdy_buy_qty', 'bfdy_sll_qty', 'thdt_buyqty', 'thdt_sll_qty',
                              'loan_dt', 'loan_amt', 'stln_slng_chgs', 'expd_dt',
                              'item_mgna_rt_name', 'grta_rt_name', 'sbst_pric', 'stck_loan_unpr'], axis=1)
    
    return balance


##### 주식일별주문체결조회 (페이징·기간 분기)
_CCLD_PATH = "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
_trade_hist_cache = {}
_CCLD_TIMEOUT = 20
_CCLD_MAX_PAGES = 15


def _ccld_headers(tr_id):
    return {
        "content-type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": app_key,
        "appsecret": app_secret,
        "tr_id": tr_id,
    }


def _filter_ticker_ccld(df, ticker):
    if df is None or df.empty or not ticker:
        return df
    t = str(ticker).zfill(6)
    if "pdno" in df.columns:
        mask = df["pdno"].astype(str).str.zfill(6) == t
        return df.loc[mask].copy()
    return df


def _fetch_daily_ccld_page(ticker, tr_id, inqr_strt_dt, inqr_end_dt, sll_buy_dvsn_cd="00",
                            pdno=None, ctx_fk="", ctx_nk=""):
    """주식일별주문체결조회 1회 호출 (연속조회 키 지원)."""
    url = f"{url_base}{_CCLD_PATH}"
    params = {
        "CANO": account_no,
        "ACNT_PRDT_CD": "01",
        "INQR_STRT_DT": inqr_strt_dt,
        "INQR_END_DT": inqr_end_dt,
        "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd,
        "INQR_DVSN": "00",
        "PDNO": pdno if pdno is not None else (ticker or ""),
        "CCLD_DVSN": "01",
        "ORD_GNO_BRNO": "",
        "ODNO": "",
        "INQR_DVSN_3": "00",
        "INQR_DVSN_1": "",
        "CTX_AREA_FK100": ctx_fk,
        "CTX_AREA_NK100": ctx_nk,
    }
    res = requests.get(
        url, headers=_ccld_headers(tr_id), params=params, timeout=_CCLD_TIMEOUT
    )
    res_json = res.json()
    if res_json.get("rt_cd") != "0":
        msg = res_json.get("msg1", "N/A")
        raise RuntimeError(f"rt_cd={res_json.get('rt_cd')} msg={msg}")
    rows = res_json.get("output1") or []
    df = pd.DataFrame.from_records(rows) if rows else pd.DataFrame()
    if ticker and pdno is None and not df.empty:
        df = _filter_ticker_ccld(df, ticker)
    tr_cont = (res.headers.get("tr_cont") or res_json.get("tr_cont") or "").strip()
    next_fk = res_json.get("ctx_area_fk100", "") or ""
    next_nk = res_json.get("ctx_area_nk100", "") or ""
    return df, tr_cont, next_fk, next_nk


def _fetch_daily_ccld(ticker, tr_id, inqr_strt_dt, inqr_end_dt, sll_buy_dvsn_cd="00",
                      pdno=None, max_pages=_CCLD_MAX_PAGES):
    """기간 내 체결 내역 (연속조회, 페이지 상한)."""
    if inqr_strt_dt > inqr_end_dt:
        return pd.DataFrame()
    parts = []
    ctx_fk, ctx_nk = "", ""
    prev_ctx = None
    for _ in range(max_pages):
        chunk, tr_cont, ctx_fk, ctx_nk = _fetch_daily_ccld_page(
            ticker, tr_id, inqr_strt_dt, inqr_end_dt, sll_buy_dvsn_cd, pdno, ctx_fk, ctx_nk
        )
        if not chunk.empty:
            parts.append(chunk)
        if tr_cont not in ("F", "M"):
            break
        cur_ctx = (ctx_fk, ctx_nk)
        if cur_ctx == prev_ctx:
            break
        prev_ctx = cur_ctx
        time.sleep(0.05)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).drop_duplicates()


def _ccld_old_windows(max_days_back=365, chunk_days=90):
    """CTSC9115R용 구간: 종료일은 반드시 3개월 분기일 이전."""
    end_dt = datetime.strptime(CCLD_OLD_END_DT, "%Y%m%d")
    start_limit = _biz_dt + relativedelta(days=-max_days_back)
    windows = []
    while end_dt >= start_limit:
        start_dt = max(end_dt + relativedelta(days=-(chunk_days - 1)), start_limit)
        windows.append((start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")))
        end_dt = start_dt + relativedelta(days=-1)
    return windows


def _ord_dt_from_ccld(df):
    if df is None or df.empty or "ord_dt" not in df.columns:
        return None
    if "sll_buy_dvsn_cd" in df.columns:
        buys = df[df["sll_buy_dvsn_cd"].astype(str).isin(["02", "2"])]
        target = buys if not buys.empty else df
    else:
        target = df
    return str(target.iloc[0]["ord_dt"])


def _pchs_dt_from_position(position_row):
    if position_row is None:
        return None
    for col in ("pchs_dt", "ord_dt"):
        val = position_row.get(col) if hasattr(position_row, "get") else getattr(position_row, col, None)
        if val and str(val).strip() not in ("", "0", "00000000"):
            return str(val).strip()
    return None


def resolve_ord_dt(ticker, position_row=None):
    """지표용 기준일 (API 최소 호출: 잔고 매입일 우선, 없으면 1~2회 조회)."""
    pchs = _pchs_dt_from_position(position_row)
    if pchs:
        return pchs

    inqr_start_recent = max(fr, CCLD_CUTOFF_DT)
    try:
        recent, _, _, _ = _fetch_daily_ccld_page(
            ticker, "TTTC8001R", inqr_start_recent, to, "00", pdno=ticker
        )
        ord_dt = _ord_dt_from_ccld(recent)
        if ord_dt:
            return ord_dt
    except (RuntimeError, requests.RequestException):
        pass

    old_windows = _ccld_old_windows()
    if old_windows:
        start_dt, end_dt = old_windows[0]
        try:
            old, _, _, _ = _fetch_daily_ccld_page(
                ticker, "CTSC9115R", start_dt, end_dt, "00", pdno=ticker
            )
            ord_dt = _ord_dt_from_ccld(old)
            if ord_dt:
                return ord_dt
        except (RuntimeError, requests.RequestException):
            pass
    return None


def get_ticker_trade_history(ticker, sll_buy_dvsn_cd="00", use_cache=True, verbose=False):
    """차트용 체결 내역 (종목 지정 조회만, 기간별 최대 2구간)."""
    cache_key = (str(ticker).zfill(6), sll_buy_dvsn_cd)
    if use_cache and cache_key in _trade_hist_cache:
        return _trade_hist_cache[cache_key].copy()

    parts = []
    inqr_start_recent = max(fr, CCLD_CUTOFF_DT)
    try:
        recent = _fetch_daily_ccld(
            ticker, "TTTC8001R", inqr_start_recent, to, sll_buy_dvsn_cd, pdno=ticker
        )
        if not recent.empty:
            parts.append(recent)
    except (RuntimeError, requests.RequestException) as e:
        if verbose:
            print(f"[WARN] 티커 {ticker} 최근 체결 조회: {e}")

    if not parts:
        for start_dt, end_dt in _ccld_old_windows()[:2]:
            try:
                old = _fetch_daily_ccld(
                    ticker, "CTSC9115R", start_dt, end_dt, sll_buy_dvsn_cd, pdno=ticker
                )
                if not old.empty:
                    parts.append(old)
                    break
            except (RuntimeError, requests.RequestException):
                continue

    if parts:
        out = pd.concat(parts, ignore_index=True).drop_duplicates()
    else:
        out = pd.DataFrame()

    if use_cache:
        _trade_hist_cache[cache_key] = out.copy()
    if verbose and not out.empty:
        print(f"[DEBUG] 티커 {ticker}: 체결 {len(out)}건 (sll_buy={sll_buy_dvsn_cd})")
    return out


def get_tradeHist(ticker, tr_id, inqr_strt_dt):
    """하위 호환: 단일 TR/기간 조회."""
    inqr_end_dt = to if tr_id == "TTTC8001R" else min(to, CCLD_OLD_END_DT)
    if tr_id == "CTSC9115R":
        inqr_end_dt = CCLD_OLD_END_DT
        if inqr_strt_dt > inqr_end_dt:
            return pd.DataFrame()
    try:
        return _fetch_daily_ccld(ticker, tr_id, inqr_strt_dt, inqr_end_dt, "02", pdno=ticker)
    except RuntimeError:
        return pd.DataFrame()


def get_all_tradeHist(ticker, tr_id=None, inqr_strt_dt=None):
    """차트용 매수/매도 체결 (캐시·기간 분기 적용)."""
    return get_ticker_trade_history(ticker, sll_buy_dvsn_cd="00", use_cache=True, verbose=False)




#### 포지션 잔고를 가져옴
position = get_balance()


#### 포지션 잔고의 OHLCV 가져오기

## 서버 접속
engine = create_engine(db_url())


ohlcv_data = {}
print("서버에서 데이터를 가젹오고 있습니다.")
_pos_meta = {}
for idx, row in position.iterrows():
    _t = str(row["pdno"]).zfill(6)
    _pos_meta[_t] = (row["prdt_name"], float(row["pchs_avg_pric"]))

if _pos_meta:
    _tickers = list(_pos_meta.keys())
    _ph = ",".join(["%s"] * len(_tickers))
    # sma250 등 지표용으로 최근 ~3년 (기존 전량 로드와 실사용 구간 동일)
    _q = f"""
        SELECT ticker, date, open, high, low, close, volume
        FROM krx_ohlcv
        WHERE ticker IN ({_ph})
          AND date >= DATE_SUB((SELECT MAX(date) FROM krx_ohlcv), INTERVAL 1100 DAY)
        ORDER BY ticker, date
    """
    _ohlcv_all = pd.read_sql_query(_q, con=engine, params=tuple(_tickers))
    if len(_ohlcv_all) > 0:
        _ohlcv_all["ticker"] = _ohlcv_all["ticker"].astype(str).str.zfill(6)
    _by_ticker = {
        str(t): g.copy()
        for t, g in _ohlcv_all.groupby("ticker", sort=False)
    } if len(_ohlcv_all) > 0 else {}

    for _t, (_name, _ent_p) in tqdm(_pos_meta.items()):
        if _t in _by_ticker:
            data = _by_ticker[_t].copy()
        else:
            data = pd.DataFrame(
                columns=["ticker", "date", "open", "high", "low", "close", "volume"]
            )
        data.insert(1, "name", _name)
        data.insert(1, "ent_p", _ent_p)
        data = data.set_index("date")
        ohlcv_data[_t] = data



#### 매물대 생성하기
def gen_tBand(df, period):
    empty_chart = pd.DataFrame(columns=['volume', 'volume_p', 'ticker'])

    df = df.reset_index()
    df = df.set_index('date')
    df = df.tail(period)
    if df.empty:
        return empty_chart
    
    
    df.loc[:, '3q'] = np.round(df.loc[:, 'high'] - (df.loc[:, 'high'] - df.loc[:, 'low'])*0.25, -2).astype(int).copy()
    df.loc[:, '1q'] = np.round(df.loc[:, 'low'] + (df.loc[:, 'high'] - df.loc[:, 'low'])*0.25, -2).astype(int).copy()
    
    # 시가, 고가, 저가, 종가, 3분위가, 1분위가에 각각 거래량을 배분
    
    df.loc[:, 'open_v'] = df.loc[:, 'volume']*0.2
    df.loc[:, 'high_v'] = df.loc[:, 'volume']*0.1
    df.loc[:, 'low_v'] = df.loc[:, 'volume']*0.1
    df.loc[:, 'close_v'] = df.loc[:, 'volume']*0.2
    df.loc[:, '3q_v'] = df.loc[:, 'volume']*0.2
    df.loc[:, '1q_v'] = df.loc[:, 'volume']*0.2
    
    
    
    # 매물대 데이퍼프레임 생성해서 죽 이어 붙이기
    volume_df = None
    
    price_list = ['open', 'high', 'low', 'close', '3q', '1q']
    
    for col in price_list:
        tmp = df[[col, col+'_v']]
        tmp.columns = ['price', 'volume']
        volume_df = pd.concat([volume_df, tmp], axis=0)

    volume_df = volume_df.dropna(subset=['price', 'volume'])
    pmin, pmax = volume_df['price'].min(), volume_df['price'].max()
    if volume_df.empty or not np.isfinite(pmin) or not np.isfinite(pmax):
        return empty_chart

    # 가격 구간화 (NaN·단일가·구간폭 0 대비)
    volume_df = volume_df.copy()
    volume_df.loc[:, 'cut'] = 0
    if pmax <= pmin:
        volume_df.loc[:, 'cut'] = int(round(float(pmin)))
    else:
        price_term = max(int((pmax - pmin) / 10), 1)
        term_list = np.arange(
            pmin,
            pmax + max(1, price_term // 3),
            price_term,
        )
        if len(term_list) < 2:
            volume_df.loc[:, 'cut'] = int(round(float((pmin + pmax) / 2)))
        else:
            for i, v in enumerate(term_list[1:]):
                if i == 0:
                    volume_df.loc[volume_df['price'] <= term_list[1], 'cut'] = int((term_list[0] + term_list[1]) / 2)
                elif i == len(term_list) - 2:
                    volume_df.loc[volume_df['price'] > term_list[i], 'cut'] = int((term_list[i] + term_list[i + 1]) / 2)
                else:
                    volume_df.loc[
                        (volume_df['price'] > term_list[i]) & (volume_df['price'] <= term_list[i + 1]),
                        'cut',
                    ] = int((term_list[i] + term_list[i + 1]) / 2)
    
    
    
    # 그룹바이
    volume_chart = volume_df.groupby(['cut']).sum()[['volume']]
    vol_sum = volume_chart['volume'].sum()
    if vol_sum == 0 or not np.isfinite(vol_sum):
        return empty_chart

    volume_chart.loc[:, 'volume_p'] = volume_chart['volume'] / vol_sum * 100
    volume_chart.loc[:, 'ticker'] = df.iloc[0].ticker
    
    
    df = df.reset_index()
    multi_factor = df.shape[0]/volume_chart['volume_p'].max()
    for i in volume_chart.index:
        tmp = volume_chart[volume_chart.index == i]
        df[i] = None
        df.loc[0:int(tmp.iloc[0]['volume_p']*multi_factor), i] = tmp.index.values[0]
        # df.loc[0:int(tmp.iloc[0]['volume_p']*multi_factor), i] = tmp.index.values[0]
        
    df = df.set_index('date')   
    
    return volume_chart

    
volume_data = {}
print("매물대 지표를 생성하고 있습니다.")
for k, v in tqdm(ohlcv_data.items()):
    vol = gen_tBand(v, 50)
    # 빈 OHLCV는 iloc[0] 불가 — ohlcv_data 키(종목코드)로 저장
    volume_data[k] = vol


#########################

# del ohlcv_data['009150']
########################


### 지표 생성하여 indacator_data 딕셔녀리에 저장

indicators_data = {}

_position_by_pdno = (
    position.set_index("pdno", drop=False) if "pdno" in position.columns else None
)
print("지표를 생성합니다")
for t, d in tqdm(ohlcv_data.items()):
    pos_row = None
    if _position_by_pdno is not None and t in _position_by_pdno.index:
        pos_row = _position_by_pdno.loc[t]
    ord_dt = resolve_ord_dt(t, pos_row)
    if ord_dt is None:
        print(f"[WARN] 티커 {t}: 체결 내역을 찾을 수 없습니다. 기본 날짜 사용")
        ord_dt = biz_day
    indicators_data[t] = build_indicators(d, ord_dt, engine=engine, ticker=t)
    gc.collect()

print("[INFO] 투자자 OSC:")
print(_investor_osc_summary(indicators_data).to_string(index=False))

        
### RS 만들기

### 티커를 가져옴
query = """
select * from krx_ticker
where 기준일 = (select max(기준일) from krx_ticker) and 종목구분 = '보통주';
"""

ticker_list = pd.read_sql(query, con=engine)
ticker_list = ticker_list[['종목코드', '종목명', '업종명']]
ticker_list = drop_excluded(ticker_list, "종목코드")
ticker_list = ticker_list.set_index('종목코드')

### OHLCV를 가져옴
ohlcv_data_all = {}
print("서버에서 OHLCV 데이터를 가젹오고 있습니다.")
_tickers_all = filter_tickers([str(t).zfill(6) for t in ticker_list.index])
if _tickers_all:
    _ph = ",".join(["%s"] * len(_tickers_all))
    _q = f"""
        SELECT ticker, date, open, high, low, close, volume
        FROM krx_ohlcv
        WHERE ticker IN ({_ph})
          AND date >= DATE_SUB((SELECT MAX(date) FROM krx_ohlcv), INTERVAL 1100 DAY)
        ORDER BY ticker, date
    """
    _ohlcv_all = pd.read_sql_query(_q, con=engine, params=tuple(_tickers_all))
    if len(_ohlcv_all) > 0:
        _ohlcv_all["ticker"] = _ohlcv_all["ticker"].astype(str).str.zfill(6)
    _by_ticker = {
        str(t): g.copy()
        for t, g in _ohlcv_all.groupby("ticker", sort=False)
    } if len(_ohlcv_all) > 0 else {}

    for t in tqdm(ticker_list.index):
        _t = str(t).zfill(6)
        if _t in _by_ticker:
            ohlcv = _by_ticker[_t].copy()
        else:
            ohlcv = pd.DataFrame(
                columns=["ticker", "date", "open", "high", "low", "close", "volume"]
            )
        ohlcv.insert(1, "name", ticker_list["종목명"][t])
        ohlcv.insert(2, "sector", ticker_list["업종명"][t])
        ohlcv = ohlcv.set_index("date")
        ohlcv_data_all[t] = ohlcv


### RS 데이터 DB에서 가져오기
print("\n" + "=" * 80)
print("📊 RS 데이터 로드 시작")
print("=" * 80)

rs_start_time = time.time()

# DB에서 최신 날짜의 RS 데이터 가져오기
_RS_SCORE_COLS = ["rs20_score", "rs50_score", "rs120_score", "rs200_score", "rs_score"]
_RS_AVG_DB_COLS = ("rs_20d", "rs_50d", "rs_120d", "rs_200d")


def _rs_scores_from_db(frame: pd.DataFrame) -> pd.DataFrame:
    """krx_relative_strength 행 → 표시용 rs20/50 + 정본 rs_score(20/50/120/200 평균)."""
    out = frame.copy()
    for src, dst in (
        ("rs_20d", "rs20_score"),
        ("rs_50d", "rs50_score"),
        ("rs_120d", "rs120_score"),
        ("rs_200d", "rs200_score"),
    ):
        out[dst] = pd.to_numeric(out[src], errors="coerce") if src in out.columns else np.nan
    out["rs_score"] = rs_avg(frame=out, cols=_RS_AVG_DB_COLS).round(2)
    return out[_RS_SCORE_COLS]


query_max_date = """
    SELECT MAX(date) as max_date
    FROM krx_relative_strength;
"""
max_rs_date = pd.read_sql_query(query_max_date, con=engine)

if len(max_rs_date) > 0 and max_rs_date.iloc[0]['max_date'] is not None:
    latest_date = max_rs_date.iloc[0]['max_date']
    print(f"최신 RS 데이터 날짜: {latest_date}")
    
    # 최신 날짜의 RS 데이터 가져오기 (정본 평균용 20/50/120/200)
    query_rs = """
        SELECT ticker, market_type, rs_10d, rs_20d, rs_50d, rs_120d, rs_200d
        FROM krx_relative_strength
        WHERE date = %s;
    """
    rs_data = pd.read_sql_query(query_rs, con=engine, params=(latest_date,))
    
    if len(rs_data) == 0:
        print("⚠️ 경고: RS 데이터가 없습니다. 최신 날짜로 다시 시도합니다.")
        # 최신 날짜로 다시 조회
        query_rs_all = """
            SELECT ticker, market_type, rs_10d, rs_20d, rs_50d, rs_120d, rs_200d, date
            FROM krx_relative_strength
            ORDER BY date DESC
            LIMIT 10000;
        """
        rs_data_all = pd.read_sql_query(query_rs_all, con=engine)
        if len(rs_data_all) > 0:
            latest_date = rs_data_all.iloc[0]['date']
            print(f"실제 최신 날짜: {latest_date}")
            rs_data = rs_data_all[rs_data_all['date'] == latest_date].copy()
            rs_data = rs_data[
                ["ticker", "market_type", "rs_10d", "rs_20d", "rs_50d", "rs_120d", "rs_200d"]
            ]
    
    if len(rs_data) > 0:
        print(f"RS 데이터 로드 완료: {len(rs_data)}개 종목")
        
        # 코스피/코스닥별로 분리
        rs_kospi_df = rs_data[rs_data['market_type'] == 'KOSPI'].copy()
        rs_kosdaq_df = rs_data[rs_data['market_type'] == 'KOSDAQ'].copy()
        
        # ticker를 인덱스로 설정 + 정본 rs_avg(20/50/120/200)
        if len(rs_kospi_df) > 0:
            rs_kospi_df = rs_kospi_df.set_index('ticker')
            rs_kospi_df = _rs_scores_from_db(rs_kospi_df)
            print(f"코스피 RS 데이터: {len(rs_kospi_df)}개 종목")
        else:
            rs_kospi_df = pd.DataFrame()
            print("⚠️ 코스피 RS 데이터가 없습니다.")
        
        if len(rs_kosdaq_df) > 0:
            rs_kosdaq_df = rs_kosdaq_df.set_index('ticker')
            rs_kosdaq_df = _rs_scores_from_db(rs_kosdaq_df)
            print(f"코스닥 RS 데이터: {len(rs_kosdaq_df)}개 종목")
        else:
            rs_kosdaq_df = pd.DataFrame()
            print("⚠️ 코스닥 RS 데이터가 없습니다.")
        
        # 두 데이터프레임 합치기
        if len(rs_kospi_df) > 0 and len(rs_kosdaq_df) > 0:
            rs_df = pd.concat([rs_kospi_df, rs_kosdaq_df])
        elif len(rs_kospi_df) > 0:
            rs_df = rs_kospi_df
        elif len(rs_kosdaq_df) > 0:
            rs_df = rs_kosdaq_df
        else:
            rs_df = pd.DataFrame(columns=_RS_SCORE_COLS)
            print("⚠️ 경고: RS 데이터프레임이 비어있습니다.")
        
    else:
        print("⚠️ 경고: RS 데이터를 가져올 수 없습니다. 빈 데이터프레임을 생성합니다.")
        rs_kospi_df = pd.DataFrame(columns=_RS_SCORE_COLS)
        rs_kosdaq_df = pd.DataFrame(columns=_RS_SCORE_COLS)
        rs_df = pd.DataFrame(columns=_RS_SCORE_COLS)
else:
    print("⚠️ 경고: RS 데이터가 없습니다. 빈 데이터프레임을 생성합니다.")
    rs_kospi_df = pd.DataFrame(columns=_RS_SCORE_COLS)
    rs_kosdaq_df = pd.DataFrame(columns=_RS_SCORE_COLS)
    rs_df = pd.DataFrame(columns=_RS_SCORE_COLS)

rs_time = time.time() - rs_start_time
print(f"⏱️ RS 데이터 로드 완료: {rs_time:.2f}초")





### 차트 생성


def _indicator_df_aligned_for_chart(raw_df, period, ticker):
    """gen_chart와 동일한 기준(날짜 인덱스, tail(period), 투자자 OSC 보강)으로 정렬된 지표 DF."""
    ticker = str(ticker)
    df = raw_df.copy()
    original_index_is_date = isinstance(df.index, pd.DatetimeIndex) or (df.index.name == "date")
    if "date" in df.columns:
        df = df.set_index("date")
    elif not original_index_is_date:
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            df = df.reset_index()
            if "date" in df.columns:
                df = df.set_index("date")
            else:
                return None
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()]
    df = df.tail(period)
    df, _ = _ensure_investor_osc_on_df(df, ticker)
    if df.empty:
        return None
    required_columns = ["open", "high", "low", "close", "volume"]
    if any(col not in df.columns for col in required_columns):
        return None
    return df.sort_index()


def _comma_int(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    try:
        return f"{int(round(float(x))):,}"
    except (TypeError, ValueError):
        return ""


def _fmt_float_simple(x, nd=2, empty="-"):
    """천단위 구분 없이 소수만 (전일비%, RS 등)."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return empty
    try:
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return empty


def _above_sma_o_x(last, sma_col):
    if sma_col not in last.index:
        return "-"
    try:
        c, s = float(last["close"]), float(last[sma_col])
        if np.isnan(c) or np.isnan(s):
            return "-"
        return "O" if c > s else "X"
    except (TypeError, ValueError):
        return "-"


def _macd_hist_trend(df):
    if len(df) < 3 or "macdhist" not in df.columns:
        return "-"
    try:
        h0 = float(df["macdhist"].iloc[-1])
        h1 = float(df["macdhist"].iloc[-2])
        h2 = float(df["macdhist"].iloc[-3])
        if np.isnan(h0) or np.isnan(h1) or np.isnan(h2):
            return "-"
        if h0 > h1 and h0 > h2:
            return "상승"
        if h0 < h1 and h0 < h2:
            return "하락"
        return "-"
    except Exception:
        return "-"


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
    """연속 두 구간 상승(당일>전일>전전일)."""
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
        return "-"
    except Exception:
        return "-"


def _is_120d_close_high(df):
    if df.empty or "close" not in df.columns:
        return "-"
    win = df["close"].tail(min(120, len(df)))
    try:
        last_c = float(df["close"].iloc[-1])
        mx = float(win.max())
        if np.isnan(last_c) or np.isnan(mx):
            return "-"
        return "O" if last_c >= mx - 1e-9 else "X"
    except Exception:
        return "-"


def _ret_pct_by_date(df) -> dict:
    """OHLCV df → {YYYY-MM-DD: 전일종가 대비 등락률%}."""
    out: dict = {}
    if df is None or getattr(df, "empty", True) or "close" not in getattr(df, "columns", []):
        return out
    work = df.sort_index()
    cl = pd.to_numeric(work["close"], errors="coerce")
    prev = cl.shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        ret = (cl / prev.replace(0, np.nan) - 1.0) * 100.0
    for ts, r in zip(work.index, ret):
        try:
            key = pd.Timestamp(ts).strftime("%Y-%m-%d")
        except Exception:
            continue
        try:
            rv = float(r)
        except (TypeError, ValueError):
            continue
        out[key] = rv if np.isfinite(rv) else float("nan")
    return out


def _load_dashboard_energy_data(engine, tickers_z6):
    """
    KRX_market_analysis와 동일 스키마 가정:
    거래대금 = close*volume, 시장 시총·거래대금은 krx_ticker_sector sector_cd 1001/2001 합산.
    """
    empty = {
        "dates": [],
        "mcap_market": {},
        "tv_market": {},
        "tv_ticker": {},
        "mcap_ticker": {},
    }
    if engine is None or not tickers_z6:
        return empty
    out = dict(empty)
    try:
        drows = pd.read_sql_query(
            "SELECT DISTINCT date FROM krx_ohlcv ORDER BY date DESC LIMIT 3", con=engine
        )
        ds = pd.to_datetime(drows["date"], errors="coerce").dropna().sort_values(ascending=False)
        dates = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in ds.tolist()[:3]]
        out["dates"] = dates
        if not dates:
            return out

        q_mcap_only = """
            SELECT ts.sector_cd, SUM(t.시가총액) AS total_mcap
            FROM krx_ticker t
            INNER JOIN krx_ticker_sector ts ON t.종목코드 = ts.ticker
            WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
              AND t.종목구분 = '보통주'
              AND ts.sector_cd IN ('1001', '2001')
            GROUP BY ts.sector_cd
        """
        _mcdf = pd.read_sql_query(q_mcap_only, con=engine)
        out["mcap_market"] = {str(r["sector_cd"]): float(r["total_mcap"] or 0) for _, r in _mcdf.iterrows()}

        _phd = ",".join(["%s"] * len(dates))
        q_mkt_tv = f"""
            SELECT DATE(o.date) AS d, ts.sector_cd, SUM(o.close * o.volume) AS total_tv
            FROM krx_ohlcv o
            INNER JOIN krx_ticker t ON t.종목코드 = o.ticker
                AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
            INNER JOIN krx_ticker_sector ts ON ts.ticker = o.ticker
            WHERE t.종목구분 = '보통주'
              AND ts.sector_cd IN ('1001', '2001')
              AND DATE(o.date) IN ({_phd})
            GROUP BY DATE(o.date), ts.sector_cd
        """
        _mkdf = pd.read_sql_query(q_mkt_tv, con=engine, params=tuple(dates))
        for _, rr in _mkdf.iterrows():
            _dk = pd.Timestamp(rr["d"]).strftime("%Y-%m-%d")
            out["tv_market"][(_dk, str(rr["sector_cd"]))] = float(rr["total_tv"] or 0.0)

        ut = sorted({str(t).zfill(6) for t in tickers_z6})
        _chunk = 400
        for _i in range(0, len(ut), _chunk):
            chunk = ut[_i : _i + _chunk]
            _pht = ",".join(["%s"] * len(chunk))
            q_tv = f"""
                SELECT o.ticker, DATE(o.date) AS d, SUM(o.close * o.volume) AS tv
                FROM krx_ohlcv o
                WHERE DATE(o.date) IN ({_phd})
                  AND o.ticker IN ({_pht})
                GROUP BY o.ticker, DATE(o.date)
            """
            _bind = tuple(dates + chunk)
            _tvdf = pd.read_sql_query(q_tv, con=engine, params=_bind)
            for _, rr in _tvdf.iterrows():
                _dk = pd.Timestamp(rr["d"]).strftime("%Y-%m-%d")
                out["tv_ticker"][(str(rr["ticker"]).zfill(6), _dk)] = float(rr["tv"] or 0.0)

        if ut:
            _phu = ",".join(["%s"] * len(ut))
            q_mcap_t = f"""
                SELECT 종목코드 AS ticker, 시가총액 AS mcap
                FROM krx_ticker
                WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
                  AND 종목구분 = '보통주'
                  AND 종목코드 IN ({_phu})
            """
            _md = pd.read_sql_query(q_mcap_t, con=engine, params=tuple(ut))
            for _, rr in _md.iterrows():
                out["mcap_ticker"][str(rr["ticker"]).zfill(6)] = float(rr["mcap"] or 0)
    except Exception:
        return dict(empty)
    return out


def _in_market_index(idx, key6):
    if idx is None or len(idx) == 0:
        return False
    if key6 in idx:
        return True
    if key6.lstrip("0") in idx:
        return True
    try:
        return int(key6) in idx
    except (TypeError, ValueError):
        return False


def _rs_lookup(rs_df, key6):
    cands = [key6, key6.lstrip("0") or "0"]
    if key6.isdigit():
        cands.append(int(key6))
    for cand in cands:
        try:
            return rs_df.loc[cand]
        except (KeyError, TypeError, IndexError):
            continue
    return None


def dashboard_column_alignments(columns):
    """Plotly Table: 숫자 열 우측, 기호·텍스트 열 가운데, 나머지 좌측."""
    rights = {
        "기준일",
        "종가",
        "전일비%",
        "ATR14",
        "RS20",
        "RS50",
        "RS평균",
        "MFI",
        "WilliamsR",
        "BB%위치",
        "밴드스퀴즈",
        "당일에너지배율",
        "3일에너지배율",
    }
    centers = {
        "SMA5위",
        "SMA10위",
        "SMA20위",
        "SMA50위",
        "120일신고가",
        "MACD히스토",
        "CSI",
        "기관OSC",
        "외국인OSC",
    }
    return ["right" if c in rights else "center" if c in centers else "left" for c in columns]


def _dashboard_cell_sort_key(column, raw):
    """대시보드 HTML 정렬용 비교값 (JSON 직렬화 가능: float / str / null)."""
    if raw is None:
        return None
    if isinstance(raw, (float, np.floating)) and np.isnan(raw):
        return None
    s = str(raw).strip()
    if s in ("", "-", "nan", "NaN", "None"):
        return None
    if s == "상승":
        return 100.0
    if s == "하락":
        return -100.0
    if s == "매집":
        return 50.0
    if s == "O":
        return 1.0
    if s == "X":
        return 0.0
    if s == "◎":
        return 3.0
    if s == "○":
        return 2.0
    if s == "●":
        return 1.0
    s2 = s.replace(",", "").replace("%", "")
    try:
        return float(s2)
    except ValueError:
        return s


def write_position_dashboard_html(output_path, df):
    """
    헤더 클릭 시 오름/내림차순 토글 정렬, 정렬 후 행 1~7·8~10 배경 구분.
    (Plotly 미사용, 단일 HTML + 내장 스크립트)
    """
    cols = [str(c) for c in df.columns]
    aligns_plotly = dashboard_column_alignments(cols)
    td_class = []
    for a in aligns_plotly:
        if a == "right":
            td_class.append("td-num")
        elif a == "center":
            td_class.append("td-c")
        else:
            td_class.append("td-l")
    rows_payload = []
    for _, r in df.iterrows():
        disp = {}
        sort_map = {}
        for c in cols:
            v = r[c]
            try:
                v_na = pd.isna(v)
            except (ValueError, TypeError):
                v_na = False
            if v_na or v is None:
                disp[c] = ""
                sort_map[c] = None
            else:
                disp[c] = str(v)
                sk = _dashboard_cell_sort_key(c, v)
                if isinstance(sk, float) and (not np.isfinite(sk)):
                    sk = None
                sort_map[c] = sk
        rows_payload.append({"d": disp, "s": sort_map})
    default_sort = "RS평균" if "RS평균" in cols else (cols[0] if cols else "")
    payload = {
        "columns": cols,
        "tdClass": td_class,
        "rows": rows_payload,
        "defaultSort": default_sort,
    }
    json_text = json.dumps(payload, ensure_ascii=False, allow_nan=False).replace("<", "\\u003c")

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>종목 현황 요약 대시보드</title>
  <style>
    body {{ margin: 0; font-family: system-ui, "Segoe UI", Roboto, "Noto Sans KR", sans-serif; background: #f5f5f5; }}
    h1 {{ font-size: 1.1rem; margin: 16px 20px 8px; color: #1565c0; }}
    #hint {{ font-size: 0.85rem; color: #555; margin: 0 20px 12px; }}
    .wrap {{ overflow: auto; margin: 0 12px 24px; border: 1px solid #ccc; border-radius: 6px;
              background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
    table {{ border-collapse: collapse; min-width: 100%; font-size: 12px; }}
    thead th {{
      position: sticky; top: 0; z-index: 2;
      background: #1e88e5; color: #fff; padding: 8px 6px; font-weight: 600;
      cursor: pointer; user-select: none; white-space: nowrap;
      border-bottom: 2px solid #0d47a1;
    }}
    thead th:hover {{ filter: brightness(1.08); }}
    thead th .dir {{ font-size: 0.75em; opacity: 0.9; margin-left: 4px; }}
    tbody td {{ padding: 6px 6px; border-bottom: 1px solid #e0e0e0; vertical-align: middle; }}
    tbody tr.band-top7 {{ background: #e8f5e9 !important; }}
    tbody tr.band-top8-10 {{ background: #fff9c4 !important; }}
    tbody tr:hover {{ outline: 1px solid #90caf9; }}
    .td-num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .td-c {{ text-align: center; }}
    .td-l {{ text-align: left; }}
  </style>
</head>
<body>
  <h1>종목 현황 요약</h1>
  <p id="hint">헤더를 클릭하면 내림차순 ↔ 오름차순으로 정렬됩니다.
    <strong>연두</strong> = 정렬 기준 상위 7개, <strong>노랑</strong> = 그 다음 3개(8~10위).</p>
  <div class="wrap">
    <table id="tbl">
      <thead><tr id="hdr"></tr></thead>
      <tbody id="body"></tbody>
    </table>
  </div>
  <script type="application/json" id="dash-payload">{json_text}</script>
  <script>
  (function() {{
    const P = JSON.parse(document.getElementById('dash-payload').textContent);
    const COLS = P.columns;
    const TDCLASS = P.tdClass;
    const ROWS = P.rows;
    let sortCol = P.defaultSort || (COLS[0] || null);
    let sortAsc = false;

    function isNullSort(v) {{
      return v === null || v === undefined || (typeof v === 'number' && Number.isNaN(v));
    }}

    function cmpVal(a, b) {{
      const na = isNullSort(a), nb = isNullSort(b);
      if (na && nb) return 0;
      if (na) return 1;
      if (nb) return -1;
      if (typeof a === 'number' && typeof b === 'number') return a - b;
      return String(a).localeCompare(String(b), 'ko');
    }}

    function sortedRows() {{
      const col = sortCol;
      if (!col || COLS.length === 0) return ROWS.slice();
      const m = ROWS.map((row, idx) => ({{ row, idx, v: row.s[col] }}));
      m.sort((A, B) => {{
        const c = cmpVal(A.v, B.v);
        if (c !== 0) return sortAsc ? c : -c;
        return A.idx - B.idx;
      }});
      return m.map(x => x.row);
    }}

    function render() {{
      const hdr = document.getElementById('hdr');
      const body = document.getElementById('body');
      hdr.innerHTML = '';
      body.innerHTML = '';
      COLS.forEach((c, j) => {{
        const th = document.createElement('th');
        const dir = (c === sortCol) ? (sortAsc ? '▲' : '▼') : '';
        th.innerHTML = escapeHtml(c) + (dir ? '<span class="dir">' + dir + '</span>' : '');
        th.title = '클릭: 정렬 (같은 열 재클릭 시 오름/내림 전환)';
        th.addEventListener('click', () => {{
          if (sortCol === c) sortAsc = !sortAsc;
          else {{ sortCol = c; sortAsc = false; }}
          render();
        }});
        hdr.appendChild(th);
      }});
      const data = sortedRows();
      data.forEach((row, i) => {{
        const tr = document.createElement('tr');
        if (i < 7) tr.className = 'band-top7';
        else if (i < 10) tr.className = 'band-top8-10';
        COLS.forEach((c, j) => {{
          const td = document.createElement('td');
          td.className = TDCLASS[j] || 'td-l';
          td.textContent = row.d[c] != null ? row.d[c] : '';
          tr.appendChild(td);
        }});
        body.appendChild(tr);
      }});
    }}

    function escapeHtml(t) {{
      const d = document.createElement('div');
      d.textContent = t;
      return d.innerHTML;
    }}

    render();
  }})();
  </script>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def build_position_dashboard_df(
    indicators_data, rs_df, period, rs_kospi_df, rs_kosdaq_df, engine=None
):
    """차트 생성 전 종목별 최신 봉·RS 등 요약 표용 데이터프레임(표시용 문자열 위주)."""
    keys6 = [str(k).zfill(6) for k in indicators_data.keys()]
    energy = _load_dashboard_energy_data(engine, keys6)

    def _sector_cd_for_ticker(key6):
        try:
            if _in_market_index(rs_kospi_df.index, key6):
                return "1001"
            if _in_market_index(rs_kosdaq_df.index, key6):
                return "2001"
        except Exception:
            pass
        return None

    rows = []
    for k, v in indicators_data.items():
        key6 = str(k).zfill(6)
        df = _indicator_df_aligned_for_chart(v, period, key6)
        if df is None or len(df) < 1:
            rows.append(
                {
                    "티커": key6,
                    "종목명": "",
                    "시장": "",
                    "기준일": "",
                    "비고": "데이터 없음",
                    "_rs_sort": np.nan,
                }
            )
            continue
        last = df.iloc[-1]
        name = ""
        try:
            name = str(last.get("name", "") or df.iloc[0].get("name", "") or "")
        except Exception:
            name = ""
        market = ""
        try:
            if _in_market_index(rs_kospi_df.index, key6):
                market = "KOSPI"
            elif _in_market_index(rs_kosdaq_df.index, key6):
                market = "KOSDAQ"
        except Exception:
            market = ""
        d0 = df.index[-1]
        try:
            date_s = pd.Timestamp(d0).strftime("%Y-%m-%d")
        except Exception:
            date_s = str(d0)
        chg_pct = np.nan
        if len(df) >= 2:
            prev_c = float(df.iloc[-2]["close"])
            if prev_c:
                chg_pct = (float(last["close"]) / prev_c - 1.0) * 100.0

        rs_row = _rs_lookup(rs_df, key6)
        if rs_row is not None:
            try:
                rs20 = float(rs_row.rs20_score)
                rs50 = float(rs_row.rs50_score)
                rs_mean = float(rs_row.rs_score)
            except Exception:
                rs20 = rs50 = rs_mean = np.nan
        else:
            rs20 = rs50 = rs_mean = np.nan

        def _f(col, nd=2):
            if col not in last.index:
                return np.nan
            x = last[col]
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return np.nan
            try:
                return round(float(x), nd)
            except (TypeError, ValueError):
                return np.nan

        try:
            close_v = float(last["close"])
        except Exception:
            close_v = np.nan
        try:
            atr_v = float(last["atr14"]) if "atr14" in last.index else np.nan
        except Exception:
            atr_v = np.nan

        sec = _sector_cd_for_ticker(key6)
        er_day_s = "-"
        er3_s = "-"
        dlist = energy.get("dates") or []
        ret_map = _ret_pct_by_date(df)
        if sec and dlist:
            total_mcap = float(energy["mcap_market"].get(sec, 0) or 0)
            mcap_s = energy["mcap_ticker"].get(key6)
            ers = []
            for dstr in dlist:
                total_tv = float(energy["tv_market"].get((dstr, sec), 0) or 0)
                tv_s = energy["tv_ticker"].get((key6, dstr))
                try:
                    if (
                        total_tv <= 0
                        or total_mcap <= 0
                        or tv_s is None
                        or mcap_s is None
                    ):
                        continue
                    tv_f, mcap_f = float(tv_s), float(mcap_s)
                    if tv_f <= 0 or mcap_f <= 0 or not np.isfinite(tv_f) or not np.isfinite(mcap_f):
                        continue
                    tv_pct = tv_f / total_tv * 100.0
                    mcap_pct = mcap_f / total_mcap * 100.0
                    ret = ret_map.get(dstr, np.nan)
                    if (not np.isfinite(ret)) and dstr == dlist[0] and np.isfinite(chg_pct):
                        ret = float(chg_pct)
                    er = float(energy_ratio(tv_pct, mcap_pct, ret))
                except Exception:
                    er = np.nan
                if not np.isnan(er):
                    ers.append(er)
            if ers:
                er_day_s = f"{ers[0]:.3f}"
            if len(ers) >= 3:
                er3_s = f"{float(np.mean(ers[:3])):.3f}"
            elif ers:
                er3_s = f"{float(np.mean(ers)):.3f}"

        row = {
            "티커": key6,
            "종목명": name,
            "시장": market,
            "기준일": date_s,
            "종가": _comma_int(close_v) if not np.isnan(close_v) else "-",
            "전일비%": _fmt_float_simple(chg_pct, 2),
            "SMA5위": _above_sma_o_x(last, "sma5"),
            "SMA10위": _above_sma_o_x(last, "sma10"),
            "SMA20위": _above_sma_o_x(last, "sma20"),
            "SMA50위": _above_sma_o_x(last, "sma50"),
            "ATR14": _comma_int(atr_v) if not np.isnan(atr_v) else "-",
            "RS20": _fmt_float_simple(rs20, 2),
            "RS50": _fmt_float_simple(rs50, 2),
            "RS평균": _fmt_float_simple(rs_mean, 2),
            "MACD히스토": _macd_hist_trend(df),
            "MFI": _fmt_float_simple(_f("mfi", 4), 1),
            "WilliamsR": _fmt_float_simple(_f("willr", 4), 1),
            "BB%위치": _fmt_float_simple(_f("pb", 6), 2),
            "밴드스퀴즈": _fmt_float_simple(_f("band20_q", 6), 2),
            "CSI": _csi_grade(last),
            "기관OSC": _osc_two_day_rise(df["inst_net_osc"]) if "inst_net_osc" in df.columns else "-",
            "외국인OSC": _osc_two_day_rise(df["frgn_net_osc"]) if "frgn_net_osc" in df.columns else "-",
            "당일에너지배율": er_day_s,
            "3일에너지배율": er3_s,
            "120일신고가": _is_120d_close_high(df),
            "_rs_sort": rs_mean,
        }
        rows.append(row)

    rows.sort(
        key=lambda r: (
            0 if pd.notna(r.get("_rs_sort")) else 1,
            -float(r["_rs_sort"]) if pd.notna(r.get("_rs_sort")) else 0.0,
        )
    )
    for r in rows:
        r.pop("_rs_sort", None)
    out = pd.DataFrame(rows)
    return out


def gen_chart(df, sector_df, rs_df, period, trade_data=None):
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
    try:
        rs20 = float(rs_df.loc[ticker].rs20_score)
        rs50 = float(rs_df.loc[ticker].rs50_score)
        rs = float(rs_df.loc[ticker].rs_score)
    except (KeyError, AttributeError):
        rs20 = rs50 = rs = 0.0
    
    cpN = df.iloc[0]['name']

    fileN = ticker + '.html'
    
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
        mom_df[c+'_mom'] = talib.ROC(sector_df[c], timeperiod=10)
    mom_df_10 = mom_df.tail(1).copy()
    
    mom_by_period = {}  # period -> DataFrame (columns = sector, one row)
    for period in mom_periods:
        m = pd.DataFrame()
        for c in sector_df.columns:
            m[c+'_mom'] = talib.ROC(sector_df[c], timeperiod=period)
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
    title_line1 = [cpN + ' : ' + ticker]
    if theme_title_segment:
        title_line1.append(theme_title_segment.strip())  # 앞뒤 공백 제거
    if industry_title_segment:
        title_line1.append(industry_title_segment.strip())  # 앞뒤 공백 제거
    
    # 둘째 줄: 기술적 지표 정보만 (들여쓰기)
    title_line2 = [
        'Bottom ' + _title_level('bottom') + ' | Top ' + _title_level('top'),
        'SMA ' + str(df.iloc[-1].sma20) + ' | ATR ' + str(df.iloc[-1].atr14),
        'RS20 ' + str(rs20) + ', RS50 ' + str(rs50) + ', RS ' + str(rs)
    ]
    
    # 테마명 첫 글자까지의 들여쓰기 계산
    # "종목명 : 티커 | " 까지의 길이를 계산
    first_part = cpN + ' : ' + ticker + ' | '
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
    _spec_row = [{"secondary_y": False}, {"secondary_y": False}]
    fig = make_subplots(
        rows=_n_rows, cols=2,
        row_heights=_row_heights,
        column_widths=(0.9, 0.1),
        shared_xaxes=True,
        shared_yaxes=False,
        vertical_spacing=0.02,
        horizontal_spacing=0.01,
        specs=[_spec_row] * _n_rows,
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
                    x=df.index, y=df["inst_net_osc"], name="기관순매수 OSC",
                    line=dict(color="#2E86DE", width=1.8),
                ),
                go.Scatter(
                    x=df.index, y=df["frgn_net_osc"], name="외국인순매수 OSC",
                    line=dict(color="#E74C3C", width=1.8),
                ),
            ]
            row7_panel = "investor"
        except (KeyError, TypeError):
            investor_graphs = []

    if investor_graphs:
        for g in investor_graphs:
            fig.add_trace(g, _inv_row, 1)


    # Row 1, Col 2: y축 레이블 숨기기
    fig.update_yaxes(showticklabels=False, row=1, col=2)
    
    # Row 2, Col 2: y축 레이블 숨기기
    fig.update_yaxes(showticklabels=False, row=2, col=2)
    
    # Row 6, Col 1: CSI (-10~10)
    if row6_panel == "csi":
        fig.update_yaxes(autorange=True, row=_csi_row, col=1, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
        fig.add_hline(y=0, row=_csi_row, col=1, line=dict(color='rgba(128, 128, 128, 0.5)', width=1, dash='dash'))

    # Row 7, Col 1: 투자자 OSC (0~100)
    if row7_panel == "investor" and _inv_row is not None:
        fig.update_yaxes(range=[0, 100], row=_inv_row, col=1, showgrid=True, gridwidth=1, gridcolor='rgba(128, 128, 128, 0.2)')
        fig.add_hline(y=80, row=_inv_row, col=1, line=dict(color='rgba(255, 107, 107, 0.5)', width=1, dash='dash'))
        fig.add_hline(y=20, row=_inv_row, col=1, line=dict(color='rgba(78, 205, 196, 0.5)', width=1, dash='dash'))
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
    
    # holidays를 datetime 형식으로 변환 (안전하게 처리)
    holidays_datetime = []
    for h in holidays:
        try:
            # 문자열 형식 확인 및 변환
            if isinstance(h, str) and len(h) == 10 and h.count('-') == 2:
                # 올바른 형식 (YYYY-MM-DD)
                holidays_datetime.append(pd.to_datetime(h, format='%Y-%m-%d').strftime('%Y-%m-%d'))
            else:
                # 다른 형식이거나 이미 datetime인 경우
                holidays_datetime.append(pd.to_datetime(h).strftime('%Y-%m-%d'))
        except Exception as e:
            print(f"[WARN] 휴일 변환 실패: {h}, 오류: {e}")
            continue
    
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]), #hide weekends
            dict(values=holidays_datetime)  # hide holidays
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
        # print(f"[SUCCESS] 차트 생성 완료: {fileN}")  # tqdm 진행 상태 바와 충돌 방지
        webbrowser.open(f'file:///{abs_path}')
    except Exception as e:
        print(f"[ERROR] 차트 생성/저장 실패 ({ticker}): {e}")
        import traceback
        traceback.print_exc()
        raise
    



today = date.today()
folder_name = today.strftime('%Y-%m-%d')

# 폴더 경로 설정 (현재 디렉토리 내에 폴더 생성)
# folder_path = os.path.join(folder_name, fileN)
folder_path = os.path.join('C:\\Users\\hachi\\OneDrive\\01. Trading\\picked\\KRX', folder_name)

os.makedirs(folder_path, exist_ok=True)

os.chdir(folder_path)

# --- 차트 열기 전: 보유(픽) 종목 요약 대시보드 (콘솔 + HTML) ---
_dashboard_df = build_position_dashboard_df(
    indicators_data, rs_df, CHART_PERIOD_DAYS, rs_kospi_df, rs_kosdaq_df, engine=engine
)
print("\n" + "=" * 100)
print("📋 종목 현황 요약 대시보드 (차트와 동일 기준일·지표 tail)")
print("=" * 100)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 20)
print(_dashboard_df.to_string(index=False))
pd.reset_option("display.max_columns")
pd.reset_option("display.width")
pd.reset_option("display.max_colwidth")

_dash_path = os.path.join(folder_path, "00_종목요약_대시보드.html")
try:
    write_position_dashboard_html(_dash_path, _dashboard_df)
    webbrowser.open("file:///" + os.path.abspath(_dash_path).replace("\\", "/"))
except Exception as _e:
    print(f"[WARN] 대시보드 HTML 저장/열기 실패: {_e}")

for k, v in tqdm(indicators_data.items(), desc="차트 생성 중", position=0, leave=False):
    if v is None or v.empty or "close" not in v.columns:
        print(f"[WARN] 차트 스킵 (티커 {k}): 지표 데이터 없음 또는 close 컬럼 없음")
        continue

    # 지수 가져오기
    query = """select sector_cd, sector_nm from krx_ticker_sector where ticker = '{}';"""
    query = query.format(k)
    
    sector = pd.read_sql_query(query, con=engine)
    sector = sector.set_index('sector_cd')
    
    sector_df = pd.DataFrame(v["close"])
    
    for s in sector.index:
        q = """select date, close from krx_index_ohlcv where ticker = '{}';""".format(s)
        close = pd.read_sql_query(q, con=engine)
        close = close.set_index('date')
        close.columns = [sector.loc[s].sector_nm]
        
        sector_df = pd.merge(sector_df, close, left_index=True, right_index=True, how='left')
    
    # Sector Performance 그래프: 해당 종목(close) + 코스피 또는 코스닥 메인 지수만 출력 (코스피 대형주, 전기전자, 코스닥 우량기업 등 섹터/세부지수 제외)
    try:
        market = 'KOSPI' if k in rs_kospi_df.index else 'KOSDAQ'
        first_col = sector_df.columns[0]  # 해당 종목 close
        market_index_col = None
        for c in sector_df.columns[1:]:  # 첫 컬럼(종목) 제외하고 검사
            cs = str(c).strip()
            cu = cs.upper()
            # 메인 지수만: 정확히 '코스피'/'KOSPI' 또는 '코스닥'/'KOSDAQ' (공백·추가문자 없이, '대형주'·'전기전자'·'우량기업' 등 제외)
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
    
    # 매수/매도 데이터 가져오기 (지표 단계에서 캐시된 결과 재사용)
    trade_data = None
    try:
        trade_data = get_all_tradeHist(k)
        if trade_data is not None and not trade_data.empty:
            print(f"[DEBUG] 티커 {k}: 매수/매도 데이터 {len(trade_data)}건 가져옴")
            # print(f"[DEBUG] 티커 {k}: 컬럼 목록: {trade_data.columns.tolist()}")
            # if len(trade_data) > 0:
            #     print(f"[DEBUG] 티커 {k}: 첫 번째 행 샘플:\n{trade_data.iloc[0].to_dict()}")
    except Exception as e:
        print(f"매수/매도 데이터 가져오기 실패 ({k}): {e}")
        import traceback
        traceback.print_exc()
        trade_data = None
    
    query = """select * from krx_ohlcv_week where ticker = '{}';"""
    query = query.format(k)
    df_week = pd.read_sql_query(query, con=engine)
    
    try:
        gen_chart(v, sector_df, rs_df, CHART_PERIOD_DAYS, trade_data=trade_data)
    except Exception as e:
        print(f"[ERROR] 차트 생성 중 오류 발생 ({k}): {e}")
        import traceback
        traceback.print_exc()
        continue

