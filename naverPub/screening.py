"""
naverPub 스크리닝: 51.Picking_KRX run_screening 이식 (talib 없이 pandas/numpy).
DB ohlcv/rs + 시총 2000억↑ 필터. selected_stocks에서 패턴 3회+ 종목 차트용.
"""
from __future__ import annotations

import logging
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from db import engine

warnings.filterwarnings("ignore", message="DataFrame is highly fragmented")

log = logging.getLogger("naverPub.screening")

MCAP_MIN = 200_000_000_000  # 2,000억
HISTORY_DAYS = 420
CSI_LENGTH = 20
CHART_WINDOW = 250
MA_WARMUP = 120
FIGSIZE = (10, 6.5)
FIG_DPI = 140


def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n), min_periods=int(n)).mean()


def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=int(n), adjust=False, min_periods=int(n)).mean()


def _tr(h, l, c) -> pd.Series:
    prev = c.shift(1)
    return pd.concat([(h - l), (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)


def _atr(h, l, c, n: int) -> pd.Series:
    return _tr(h, l, c).ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def _rolling_max(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n), min_periods=int(n)).max()


def _rolling_min(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n), min_periods=int(n)).min()


def _rolling_sum(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n), min_periods=int(n)).sum()


def _minmax(s: pd.Series, n: int):
    return _rolling_min(s, n), _rolling_max(s, n)


def _maxindex(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n), min_periods=int(n)).apply(lambda x: float(np.argmax(x)), raw=True)


def _minindex(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n), min_periods=int(n)).apply(lambda x: float(np.argmin(x)), raw=True)


def _linreg_angle(s: pd.Series, n: int) -> pd.Series:
    x = np.arange(n, dtype=float)

    def _fn(y):
        if np.any(~np.isfinite(y)):
            return np.nan
        coef = np.polyfit(x, y, 1)
        return float(np.degrees(np.arctan(coef[0])))

    return s.rolling(int(n), min_periods=int(n)).apply(_fn, raw=True)


def _bbands(close: pd.Series, n: int = 20, nbdev: float = 2.0):
    mid = _sma(close, n)
    std = close.rolling(n, min_periods=n).std(ddof=0)
    return mid + nbdev * std, mid, mid - nbdev * std


def _plus_di(h, l, c, n=14):
    up = h.diff()
    dn = -l.diff()
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    tr = _tr(h, l, c)
    atr = tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    pdm = pd.Series(plus_dm, index=h.index).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 * pdm / atr.replace(0, np.nan)


def _minus_di(h, l, c, n=14):
    up = h.diff()
    dn = -l.diff()
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = _tr(h, l, c)
    atr = tr.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    mdm = pd.Series(minus_dm, index=h.index).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 * mdm / atr.replace(0, np.nan)


def _adx(h, l, c, n=14):
    pdi = _plus_di(h, l, c, n)
    mdi = _minus_di(h, l, c, n)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    return dx.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()


def _mfi(h, l, c, v, n=14):
    tp = (h + l + c) / 3.0
    rmf = tp * v
    delta = tp.diff()
    pos = rmf.where(delta > 0, 0.0)
    neg = rmf.where(delta < 0, 0.0)
    pos_sum = pos.rolling(n, min_periods=n).sum()
    neg_sum = neg.rolling(n, min_periods=n).sum()
    ratio = pos_sum / neg_sum.replace(0, np.nan)
    return 100 - (100 / (1 + ratio))


def _willr(h, l, c, n=14):
    hh = _rolling_max(h, n)
    ll = _rolling_min(l, n)
    return -100 * (hh - c) / (hh - ll).replace(0, np.nan)


def _macd(close, fast=12, slow=26, signal=9):
    macd = _ema(close, fast) - _ema(close, slow)
    sig = _ema(macd, signal)
    return macd, sig, macd - sig


def _obv(close, volume):
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def get_indicators(d, tradeHist=None):
    """51번 get_indicators 동등 (talib → pandas)."""
    d = d.copy()
    if "date" in d.columns:
        d = d.set_index("date")
    if not isinstance(d.index, pd.DatetimeIndex):
        d.index = pd.to_datetime(d.index)

    h, l, c, v = d["high"], d["low"], d["close"], d["volume"]
    d["typical"] = (h + l + c) / 3.0

    for n in (2, 5, 10, 20, 30, 50, 60, 120, 150, 200, 250):
        d[f"sma{n}"] = _sma(c, n)
    d["ema20"] = _ema(c, 20)

    for n in (5, 10, 20, 50, 125, 250):
        d[f"max{n}"] = _rolling_max(h, n)
        d[f"min{n}"] = _rolling_min(l, n)
        d[f"mid{n}"] = (d[f"max{n}"] + d[f"min{n}"]) / 2.0

    for n in (4, 10, 14, 20, 30):
        d[f"atr{n}"] = _atr(h, l, c, n)

    d["tr"] = _tr(h, l, c)
    for n in (7, 14, 20):
        d[f"mtr{n}"] = _rolling_max(d["tr"], n)

    d["maxP_index"] = _maxindex(c, 60)
    d["minP_index"] = _minindex(c, 60)
    d["maxV_index"] = _maxindex(v, 60)
    d["slope20"] = _linreg_angle(d["typical"], 20)
    d["slope30"] = _linreg_angle(d["typical"], 30)
    d["slope40"] = _linreg_angle(d["typical"], 40)

    up, mid, dn = _bbands(c, 20, 2)
    d["bol20_up"], d["bol20_ma"], d["bol20_dn"] = up, mid, dn
    d["band20_w"] = (d.bol20_up - d.bol20_dn) / d.bol20_ma.replace(0, np.nan)
    d["band20_w_min"], d["band20_w_max"] = _minmax(d["band20_w"], 125)
    d["band20_q"] = (d.band20_w - d.band20_w_min) / (d.band20_w_max - d.band20_w_min).replace(0, np.nan)

    d["atr_w"] = d.atr14 / c.replace(0, np.nan)
    d["atr_w_min"], d["atr_w_max"] = _minmax(d["atr_w"], 125)
    d["atr_q"] = (d.atr_w - d.atr_w_min) / (d.atr_w_max - d.atr_w_min).replace(0, np.nan)

    up, mid, dn = _bbands(c, 50, 2)
    d["bol50_up"], d["bol50_ma"], d["bol50_dn"] = up, mid, dn
    d["band50_w"] = (d.bol50_up - d.bol50_dn) / d.bol50_ma.replace(0, np.nan)
    up, mid, dn = _bbands(c, 120, 2)
    d["bol120_up"], d["bol120_ma"], d["bol120_dn"] = up, mid, dn
    d["band120_w"] = (d.bol120_up - d.bol120_dn) / d.bol120_ma.replace(0, np.nan)

    d["dsprt20"] = d.sma2 - d.sma20
    d["dsprt50"] = d.sma2 - d.sma50
    d["dsprt120"] = d.sma2 - d.sma120

    _atr10 = d.atr10.replace(0, np.nan)
    _atr20 = d.atr20.replace(0, np.nan)
    _atr30 = d.atr30.replace(0, np.nan)
    d["csi20"] = ((h - d.sma20) / _atr20 + (l - d.sma20) / _atr20) / 2
    d["csi10"] = ((h - d.sma10) / _atr10 + (l - d.sma10) / _atr10) / 2
    d["csi30"] = ((h - d.sma30) / _atr30 + (l - d.sma30) / _atr30) / 2
    d["csi20_max"] = _rolling_max(d.csi20, 20)

    _csi_sma = _sma(c, CSI_LENGTH)
    _csi_atr = _atr(h, l, c, CSI_LENGTH)
    cs = (c - _csi_sma) / _csi_atr.replace(0, np.nan)
    d["csi"] = _sma(cs, 2)
    d["csi_fast"] = _ema(cs, 10)
    d["csi_slow"] = _ema(cs, 20)

    d["di_P"] = _plus_di(h, l, c, 14)
    d["di_M"] = _minus_di(h, l, c, 14)
    d["adx"] = _adx(h, l, c, 14)

    macd, macdsignal, macdhist = _macd(c)
    d["macd"], d["macdsignal"], d["macdhist"] = macd, macdsignal, macdhist

    d["ch_up1"] = d.sma20 + d.atr14
    d["ch_dn1"] = d.sma20 - d.atr14
    d["ch_up2"] = d.ema20 + d.atr14 * 2
    d["ch_dn2"] = d.ema20 - d.atr14 * 2
    d["ch_up3"] = d.ema20 + d.atr14 * 3
    d["ch_dn3"] = d.ema20 - d.atr14 * 3
    d["ch_up4"] = d.ema20 + d.atr14 * 4
    d["ch_dn4"] = d.ema20 - d.atr14 * 4
    d["ch_w"] = (d.ch_up2 - d.ch_dn2) / d.ema20.replace(0, np.nan)
    d["minmax_w"] = (d.max20 - d.min20) / d.mid20.replace(0, np.nan)
    d["minmax5"] = d.max5 - d.min5
    d["vol_mtr"] = (d.mtr14 / c) * 100
    d["vol_atr"] = (d.atr14 / c) * 100

    d["vol_sma5"] = _sma(v, 5)
    d["vol_sma20"] = _sma(v, 20)
    d["vol_sma50"] = _sma(v, 50)
    d["vol_sum5"] = _rolling_sum(v, 5)
    d["vol_sum20"] = _rolling_sum(v, 20)
    d["vol_sum50"] = _rolling_sum(v, 50)
    d["vol_sum5_sma5"] = _sma(d.vol_sum5, 5)
    d["vol_sum20_sma20"] = _sma(d.vol_sum20, 20)
    d["vol_max5"] = _rolling_max(v, 5)
    d["vol_max20"] = _rolling_max(v, 20)
    d["vol_max50"] = _rolling_max(v, 50)

    z = (d["volume"] == 0) & (d["close"] != 0)
    if z.any():
        d.loc[z, "open"] = d.loc[z, "close"]
        d.loc[z, "high"] = d.loc[z, "close"]
        d.loc[z, "low"] = d.loc[z, "close"]

    d["obv"] = _obv(c, v)
    d["vol_vol50"] = v / d.vol_sma50.replace(0, np.nan)
    d["max_vol50"] = _rolling_max(d.vol_vol50, 50)

    for n in (7, 14, 21, 30, 40, 50):
        d[f"box{n}"] = _rolling_max(h, n) - _rolling_min(l, n)

    d["pb"] = (c - d.bol20_dn) / (d.bol20_up - d.bol20_dn).replace(0, np.nan)
    d["pm"] = (c - d.min20) / (d.max20 - d.min20).replace(0, np.nan)
    d["sma_score"] = d["pm"]
    d["pc"] = (c - d.ch_dn2) / (d.ch_up2 - d.ch_dn2).replace(0, np.nan)
    d["pb_max"] = _rolling_max(d.pb, 20)
    d["pb_min"] = _rolling_min(d.pb, 20)
    d["mfi"] = _mfi(h, l, c, v, 14)
    d["willr"] = _willr(h, l, c, 14)
    return d.copy()


def gen_tBand(df, period):
    """매물대 (51번과 동일)."""
    df = df.reset_index()
    if "date" in df.columns:
        df = df.set_index("date")
    df = df.tail(period).copy()
    span = (df["high"] - df["low"]).astype(float)
    df.loc[:, "3q"] = np.round(df["high"] - span * 0.25, -2).astype(int)
    df.loc[:, "1q"] = np.round(df["low"] + span * 0.25, -2).astype(int)
    df.loc[:, "open_v"] = df["volume"] * 0.2
    df.loc[:, "high_v"] = df["volume"] * 0.1
    df.loc[:, "low_v"] = df["volume"] * 0.1
    df.loc[:, "close_v"] = df["volume"] * 0.2
    df.loc[:, "3q_v"] = df["volume"] * 0.2
    df.loc[:, "1q_v"] = df["volume"] * 0.2
    volume_df = None
    for col in ["open", "high", "low", "close", "3q", "1q"]:
        tmp = df[[col, col + "_v"]].copy()
        tmp.columns = ["price", "volume"]
        volume_df = tmp if volume_df is None else pd.concat([volume_df, tmp], axis=0)
    price_term = np.int64((volume_df["price"].max() - volume_df["price"].min()) / 10) or 1
    term_list = np.arange(
        volume_df["price"].min(),
        volume_df["price"].max() + int(price_term / 3) + 1,
        price_term,
    )
    volume_df = volume_df.copy()
    volume_df.loc[:, "cut"] = 0
    for i, _v in enumerate(term_list[1:]):
        if i == 0:
            volume_df.loc[volume_df["price"] <= term_list[1], "cut"] = int((term_list[0] + term_list[1]) / 2)
        elif i == len(term_list) - 2:
            volume_df.loc[volume_df["price"] > term_list[i], "cut"] = int((term_list[i] + term_list[i + 1]) / 2)
        else:
            m = (volume_df["price"] > term_list[i]) & (volume_df["price"] <= term_list[i + 1])
            volume_df.loc[m, "cut"] = int((term_list[i] + term_list[i + 1]) / 2)
    volume_chart = volume_df.groupby(["cut"]).sum()[["volume"]]
    volume_chart.loc[:, "volume_p"] = volume_chart["volume"] / volume_chart["volume"].sum() * 100
    volume_chart.loc[:, "ticker"] = df.iloc[0].ticker if "ticker" in df.columns else ""
    volume_chart["volume_p_cum"] = volume_chart["volume_p"].cumsum()
    return volume_chart



def run_screening(indicators_data, volume_data, rs_df, ticker_list, audit_ticker):
    """종목 스크리닝 (screening_loop.py 내장)"""
    debug_counts = {"total_indicators": len(indicators_data), "passed_basic_filter": 0, "passed_atr_filter": 0, "selected_by_pattern": 0}
    selected_stocks = []
    selected_stock11 = ['p11']
    selected_stock12 = ['p12']
    selected_stock13 = ['p13']
    selected_stock14 = ['p14']
    selected_stock15 = ['p15']
    selected_stock16 = ['p16']
    selected_stock17 = ['p17']
    selected_stock21 = ['p21']
    selected_stock22 = ['p22']
    selected_stock23 = ['p23']
    selected_stock24 = ['p24']
    selected_stock25 = ['p25']
    selected_stock26 = ['p26']
    selected_stock27 = ['p27']
    selected_stock28 = ['p28']
    selected_stock29a = ['p29']
    selected_stock29b = ['p29']
    selected_stock31 = ['p31']
    selected_stock32 = ['p32']
    selected_stock33 = ['p33']
    selected_stock34 = ['p34']
    selected_stock35 = ['p35']
    selected_stock36 = ['p36']
    selected_stock41 = ['p41']
    selected_stock42 = ['p42']
    selected_stock43 = ['p43']
    selected_stock51 = ['p51']
    selected_stock52 = ['p52']
    selected_stock53 = ['p53']
    selected_stock54 = ['p54']
    selected_stock55 = ['p55']
    selected_stock61 = ['p61']
    selected_stock71 = ['p71']
    selected_stock81 = ['p81']
    selected_stock91 = ['p91']
    selected_stock92 = ['p92']
    selected_stock93 = ['p93']


    for k, i in indicators_data.items():
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

        if len(i) >= 120 and i.iloc[-1].open > 0 and _tk in ticker_list.index and ticker_list.loc[_tk]['시가총액'] > 200000000000\
            and _tk not in audit_ticker:
                debug_counts['passed_basic_filter'] += 1
                ticker = i.iloc[0].ticker
                close = i.iloc[-1].close

                if (idc[-1].atr14/close) < 0.1 and idc[-1].mtr7/close < 0.2 and idc[-1].box7/close < 0.3:
                    debug_counts['passed_atr_filter'] += 1

                    #########################################################################################################################
                    ###########          이동평균선   #######################################################################################3

                    ### 이평선 정배열
                    ### 50일, 120일, 200일 이평선 상승 중
                    ### 20일 박스 내 0.75 - 0.9 사이
                    ### 125일 박스 내 0.75 이상

                    # 0으로 나누기 방지 추가
                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                            and idc[-1].sma50 > idc[-6].sma50 and idc[-1].sma120 > idc[-13].sma120 and idc[-1].sma200 > idc[-21].sma200\
                                and (idc[-1].max20 - idc[-1].min20) > 0 and (close - idc[-1].min20)/(idc[-1].max20 - idc[-1].min20) < 0.9\
                                    and (idc[-1].max20 - idc[-1].min20) > 0 and (close - idc[-1].min20)/(idc[-1].max20 - idc[-1].min20) > 0.75\
                                        and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75:

                                        selected_stock11.append(i)

                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p11')
                                        listed.insert(0,i.index[-1])

                                        selected_stocks.append(listed)


                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                        and idc[-1].sma50 > idc[-6].sma50 and idc[-1].sma120 > idc[-13].sma120 and idc[-1].sma200 > idc[-21].sma200\
                            and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75\
                                and (idc[-1].csi10 < 0 or idc[-1].csi20 < 0 or idc[-1].csi30 < 0):

                            
                                    selected_stock12.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p12')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)

                        ### 이평선 정배열
                        ### 50일 이평선 상승 중
                        ### 20일 박스 내 0.75 - 0.9 사이
                        ### 125일 박스 내 0.75 이상
                
                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                        and idc[-1].sma50 > idc[-6].sma50 and idc[-1].sma120 > idc[-13].sma120 and idc[-1].sma200 > idc[-21].sma200\
                            and (idc[-1].max20 - idc[-1].min20) > 0 and (close - idc[-1].min20)/(idc[-1].max20 - idc[-1].min20) < 0.9\
                                and (idc[-1].max20 - idc[-1].min20) > 0 and (close - idc[-1].min20)/(idc[-1].max20 - idc[-1].min20) > 0.75\
                                    and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75\
                                        and (idc[-21].vol_sum20 > idc[-41].vol_sum20*2 or idc[-21].vol_sum50 > idc[-71].vol_sum50*2)\
                                            and idc[-1].vol_sum20 < idc[-21].vol_sum20:
                        
                                                selected_stock13.append(i)
                                    
                                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                                listed.insert(0,'p13')
                                                listed.insert(0,i.index[-1])
                                    
                                                selected_stocks.append(listed)

                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                        and idc[-1].sma50 > idc[-6].sma50 and idc[-1].sma120 > idc[-6].sma120 and idc[-1].sma200 > idc[-6].sma200\
                            and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75\
                                and (idc[-1].csi10 < 0 or idc[-1].csi20 < 0 or idc[-1].csi30 < 0)\
                                    and (idc[-21].vol_sum20 > idc[-41].vol_sum20*2 or idc[-21].vol_sum50 > idc[-71].vol_sum50*2)\
                                        and idc[-1].vol_sum20 < idc[-21].vol_sum20:
                                        
                                                selected_stock14.append(i)
                                    
                                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                                listed.insert(0,'p14')
                                                listed.insert(0,i.index[-1])
                                    
                                                selected_stocks.append(listed)


                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                        and idc[-1].sma_score > 0.75\
                            and idc[-1].close < idc[-1].sma20 + idc[-1].atr14:
                                        
                                                selected_stock15.append(i)
                                    
                                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                                listed.insert(0,'p15')
                                                listed.insert(0,i.index[-1])
                                    
                                                selected_stocks.append(listed)


                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                        and (idc[-1].close < idc[-1].sma20 + idc[-1].atr14*0.5 or idc[-1].open < idc[-1].sma20 + idc[-1].atr14*0.5)\
                            and (idc[-1].close > idc[-1].sma20 - idc[-1].atr14*0.5 or idc[-1].open > idc[-1].sma20 - idc[-1].atr14*0.5)\
                                and idc[-1].sma50 > idc[-6].sma50 and idc[-1].sma120 > idc[-13].sma120 and idc[-1].sma200 > idc[-21].sma200\
                                    and (idc[-1].min20 - idc[-1].min50)/(idc[-1].max50 - idc[-1].min50) > 0.5:
                                        # and (idc[-1].min50 - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.5:
                                # and 
                                        
                                                selected_stock16.append(i)
                                    
                                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                                listed.insert(0,'p16')
                                                listed.insert(0,i.index[-1])
                                    
                                                selected_stocks.append(listed)


                           #########################################################################################################################
                        ###########          거래량 급증 상승 → 축소 횡보 → 재상승 기대 (p16)   ############################################
                
                        ### P16: Volume Surge → Consolidation → Breakout Ready
                        # 1. 1단계: 거래량 급증을 동반한 상승 (20-40일 전)
                        # 2. 2단계: 거래량 축소 및 횡보/소폭 조정 (최근 10-20일)
                        # 3. 3단계: 재돌파 준비 (현재 위치)
                        # 4. 이동평균선 지지
                        # 5. RS Rating 70 이상
                
                    if len(i) >= 100:
                            try:
                                # === 1단계: 거래량 급증 상승 구간 확인 (30~50일 전) ===
                
                                # 상승 시작점 (50일 전)
                                surge_start_price = idc[-50].close if len(i) >= 50 else idc[-40].close
                
                                # 상승 고점 (25~35일 전 구간)
                                surge_high_price = idc[-35:-25].high.max() if len(i) >= 35 else idc[-30:-20].high.max()
                
                                # 상승률 계산
                                if surge_start_price > 0:
                                    surge_gain = (surge_high_price - surge_start_price) / surge_start_price * 100
                                else:
                                    surge_gain = 0
                
                                # 상승 구간 평균 거래량 (50~30일 전)
                                vol_surge_period = idc[-50:-30].volume.mean() if len(i) >= 50 else idc[-40:-25].volume.mean()
                
                                # 상승 전 평균 거래량 (80~50일 전) - 비교 기준
                                vol_before_surge = idc[-80:-50].volume.mean() if len(i) >= 80 else idc[-60:-40].volume.mean()
                
                                # 거래량 급증 비율
                                if vol_before_surge > 0:
                                    vol_surge_ratio = vol_surge_period / vol_before_surge
                                else:
                                    vol_surge_ratio = 0
                
                                # === 2단계: 횡보/조정 구간 확인 (최근 10~25일) ===
                
                                # 횡보 구간 고점/저점
                                consol_high = idc[-25:-1].high.max()
                                consol_low = idc[-25:-1].low.min()
                
                                # 횡보 구간 변동폭
                                if consol_high > 0:
                                    consol_range = (consol_high - consol_low) / consol_high * 100
                                else:
                                    consol_range = 100
                
                                # 횡보 구간 평균 거래량 (최근 25일)
                                vol_consol = idc[-25:-1].volume.mean()
                
                                # 거래량 축소 확인
                                if vol_surge_period > 0:
                                    vol_contraction = vol_consol / vol_surge_period
                                else:
                                    vol_contraction = 1
                
                                # === 3단계: 현재 위치 확인 ===
                
                                current_price = idc[-1].close
                
                                # 고점 대비 현재가 위치
                                if surge_high_price > 0:
                                    price_from_high = (surge_high_price - current_price) / surge_high_price * 100
                                else:
                                    price_from_high = 100
                
                                # 횡보 구간 내 위치
                                if (consol_high - consol_low) > 0:
                                    price_position_in_consol = (current_price - consol_low) / (consol_high - consol_low)
                                else:
                                    price_position_in_consol = 0
                
                                # === 4단계: 이동평균선 확인 ===
                
                                # 20일선 위
                                above_ma20 = current_price > idc[-1].sma20
                
                                # 50일선 상승 중 또는 평탄
                                ma50_support = idc[-1].sma50 >= idc[-10].sma50 * 0.98
                
                                # 이동평균선 정배열 여부
                                ma_aligned = idc[-1].sma20 > idc[-1].sma50
                
                                # === 5단계: 최근 거래량 증가 징후 ===
                
                                # 최근 5일 평균 거래량
                                vol_recent_5d = idc[-5:].volume.mean()
                
                                # 횡보 구간 거래량 대비
                                if vol_consol > 0:
                                    vol_recent_pickup = vol_recent_5d / vol_consol
                                else:
                                    vol_recent_pickup = 0
                
                                # === 6단계: 최종 조건 ===
                
                                # 조건 체크
                                if surge_gain >= 15\
                                    and vol_surge_ratio >= 1.3\
                                        and consol_range <= 20\
                                            and vol_contraction <= 0.70\
                                                and price_from_high <= 15\
                                                    and price_position_in_consol >= 0.4\
                                                        and above_ma20\
                                                            and ma50_support\
                                                                and _tk in rs_df.index\
                                                                and rs_df.loc[_tk].rs_score >= 70\
                                                                    and current_price > consol_high * 0.90:
                    
                                    selected_stock17.append(i)
                    
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p17')
                                    listed.insert(0,i.index[-1])
                    
                                    selected_stocks.append(listed)

                            except Exception as e:
                                pass

                        #########################################################################################################################
                        ###########          신고가 돌파   #######################################################################################


                        # 1. 50일 최고가 경신
                        # 2. 음봉 만듬
        
                    if idc[-1].max5 > idc[-6].max50\
                        and idc[-1].close > idc[-1].max5 - idc[-1].atr14*1\
                            and idc[-1].close < idc[-1].open:

               
                                    selected_stock21.append(i)    
    
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p21')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)

                        ## High and Tight Flag
                        # 1. 50일 만에 90% 이상 급 상승
                        # 2. 상승 후 타이트한 조정
                    max_min_diff_50 = 0
                    max_min_diff_5 = 0
                    if idc[-1].max10 > 0 and idc[-1].min50 > 0:
                        max_min_diff_50 = idc[-1].max50 - idc[-1].min50
                        max_min_diff_5 = idc[-1].max5 - idc[-1].min5
                    if max_min_diff_50 > 0 and max_min_diff_5 > 0 and idc[-1].min50 > 0:
                            if idc[-1].max10 / idc[-1].min50 > 1.9\
                                and idc[-1].high < idc[-1].max10:
                                # and (close - idc[-1].min5)/max_min_diff_5 < 0.9\
                                    # and (close - idc[-1].min5)/max_min_diff_5 > 0.75:
                        
                                        selected_stock22.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p22')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)

                    # 1. 52주 신고가

                    # rs_df 체크를 안전하게 처리
                    has_rs_data = len(rs_df) > 0 and _tk in rs_df.index
                
                    ticker_in_stock23 = False
                    ticker_in_stock24 = False
                    ticker_in_stock26 = False

                    if has_rs_data:
                        if idc[-1].max20 > idc[-21].max250\
                            and idc[-1].close > idc[-1].sma20 and idc[-1].close < idc[-1].sma10\
                                and (rs20S > 80 and rs50S > 80)\
                                    and (idc[-1].band20_q < 1 or idc[-1].pb < 1):

                                  selected_stock23.append(i)

                                  listed = i.iloc[-1][['ticker', 'name']].to_list()
                                  listed.insert(0,'p23')
                                  listed.insert(0,i.index[-1])

                                  selected_stocks.append(listed)

                        # 1. 52주 신고가
                        # selected_stock23에 ticker가 이미 있는지 확인 (DataFrame에서 ticker 추출)
                        ticker_in_stock23 = False
                    try:
                        for item in selected_stock23:
                            if item != 'p23' and hasattr(item, 'iloc'):
                                if str(item.iloc[-1].ticker) == str(k):
                                    ticker_in_stock23 = True
                                    break
                    except Exception:
                        pass

                    if has_rs_data or ticker_in_stock23:
                        if idc[-1].max20 > idc[-21].max250\
                            and idc[-1].close < idc[-1].sma20 + idc[-1].atr14\
                                and idc[-1].close > idc[-1].sma20 - idc[-1].atr14\
                                    and idc[-1].close > idc[-1].sma50\
                                        and (rs20S > 85 or rs50S > 85):
                                    # and rsS > 70:

             
                                  selected_stock24.append(i)    
  
                                  listed = i.iloc[-1][['ticker', 'name']].to_list()
                                  listed.insert(0,'p24')
                                  listed.insert(0,i.index[-1])
                      
                                  selected_stocks.append(listed)

                        # selected_stock24에 ticker가 이미 있는지 확인
                        ticker_in_stock24 = False
                    try:
                        for item in selected_stock24:
                            if item != 'p24' and hasattr(item, 'iloc'):
                                if str(item.iloc[-1].ticker) == str(k):
                                    ticker_in_stock24 = True
                                    break
                    except Exception:
                        pass
                
                    if has_rs_data or ticker_in_stock23 or ticker_in_stock24:
                        if idc[-1].max50 > idc[-51].max250\
                            and idc[-1].close < idc[-1].sma20 + idc[-1].atr14\
                                and idc[-1].close > idc[-1].sma20 - idc[-1].atr14\
                                    and idc[-1].close > idc[-1].sma50\
                                        and (rs20S > 85 or rs50S > 85):
                                    # and rsS > 70:

             
                                  selected_stock25.append(i)    
  
                                  listed = i.iloc[-1][['ticker', 'name']].to_list()
                                  listed.insert(0,'p25')
                                  listed.insert(0,i.index[-1])
                      
                                  selected_stocks.append(listed)

                        # rs_df 체크를 안전하게 처리
                    if len(rs_df) > 0 and _tk in rs_df.index:
                        if idc[-1].max10 > idc[-11].max50\
                            and idc[-1].close < idc[-1].sma20 + idc[-1].atr14\
                                and idc[-1].close > idc[-1].sma20 - idc[-1].atr14\
                                    and idc[-1].close > idc[-1].sma50\
                                        and (rs20S > 85 or rs50S > 85):
                                    # and rsS > 70:

             
                                  selected_stock26.append(i)    
  
                                  listed = i.iloc[-1][['ticker', 'name']].to_list()
                                  listed.insert(0,'p26')
                                  listed.insert(0,i.index[-1])
                      
                                  selected_stocks.append(listed)

                        # selected_stock26에 ticker가 이미 있는지 확인
                        ticker_in_stock26 = False
                    try:
                        for item in selected_stock26:
                            if item != 'p26' and hasattr(item, 'iloc'):
                                if str(item.iloc[-1].ticker) == str(k):
                                    ticker_in_stock26 = True
                                    break
                    except Exception:
                        pass
                
                    if (len(rs_df) == 0 or _tk not in rs_df.index) and not ticker_in_stock26:
                        pass
                    else:
                        if idc[-1].max10 > idc[-11].max125\
                            and idc[-1].close < idc[-1].sma20 + idc[-1].atr14\
                                and idc[-1].close > idc[-1].sma20 - idc[-1].atr14\
                                    and idc[-1].close > idc[-1].sma50\
                                        and (rs20S > 85 or rs50S > 85):
                                    # and rsS > 70:

             
                                  selected_stock27.append(i)    
  
                                  listed = i.iloc[-1][['ticker', 'name']].to_list()
                                  listed.insert(0,'p27')
                                  listed.insert(0,i.index[-1])
                      
                                  selected_stocks.append(listed)    


                    if ((idc[-1].close > idc[-2].max50 and idc[-2].close < idc[-2].max50)\
                        or (idc[-1].close > idc[-2].max125 and idc[-2].close < idc[-2].max125)\
                            or (idc[-1].close > idc[-2].max250 and idc[-2].close < idc[-2].max250))\
                                and (rs20S > 80 or rs50S > 80):
                                    # and idc[-1].band20_q == 1:
                                # and rsS > 70:

                              selected_stock28.append(i)    
  
                              listed = i.iloc[-1][['ticker', 'name']].to_list()
                              listed.insert(0,'p28')
                              listed.insert(0,i.index[-1])
                  
                              selected_stocks.append(listed)    

                    if idc[-1].max5 > idc[-6].max125 and idc[-1].close > idc[-6].max125\
                        and (rs20S > 80 or rs50S > 80)\
                            and idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200:
                            # and idc[-1].band20_q < 1:
                                # and rsS > 70:

                                    if len(selected_stock29a) < 50:      
                                        selected_stock29a.append(i)    
                                    else:
                                        selected_stock29b.append(i)   
  
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p29')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)    

                      
                      
                        #########################################################################################################################
                        ###########          볼린저 밴드   #######################################################################################

                        ## Bolinger band sqeeze
                        ## 볼린저밴드 폭이 최근 125일 중 하위 10% 미만
                 
                    if idc[-1].band20_q < 0.1\
                        and idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                            and idc[-1].sma50 > idc[-6].sma50\
                                and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.5:
               
                                        selected_stock31.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p31')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)


                        ### 이동평균 정배열 상태에서 볼린저 밴드 하단 근접
                    if idc[-1].pb < 0.3\
                        and idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200\
                            and idc[-1].sma50 > idc[-6].sma50\
                                and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75:
                                # and idc[-1].box7*2 < idc[-1].box50\

               
                                        selected_stock32.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p32')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)
                            
                        ## 볼린저 밴드 돌파 + 50일 신고가 돌파
                        # 볼린저 밴드 돌파 후 밴드 안쪽으로 회귀

                    if idc[-1].max5 > idc[-1].bol20_up and idc[-1].max5 > idc[-6].max50\
                        and idc[-1].pb < 1 and idc[-1].pb > 0.8\
                            and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200:

                                        selected_stock33.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p33')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)
                            

                        ## 볼린저 밴드 돌파 + 50일 신고가 돌파
                        ## 밴드 폭 20% 미만

                    if idc[-1].max5 > idc[-6].bol20_up and idc[-1].max5 > idc[-6].max50\
                        and idc[-1].band20_q < 0.2\
                            and idc[-1].pb < 1.2 and idc[-1].pb > 0.8:
                        # and idc[-6].bol20_up < idc[-6].sma20 + idc[-6].atr14*2\
                            # and idc[-1].pb > 0.8\
                                # and idc[-1].close < idc[-1].open:
                                        selected_stock34.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p34')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)
             

                        ## 볼린저 밴드 근접

                    if idc[-1].pb_max < 1\
                        and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75:
                
                
                        # idc[-1].close < idc[-1].bol20_up and idc[-2].close < idc[-2].bol20_up and idc[-3].close < idc[-3].bol20_up\
                        #     and idc[-1].close > idc[-1].bol20_up - idc[-1].atr14\
                        #         and idc[-2].close < idc[-2].bol20_up - idc[-2].atr14\
                        #             and idc[-3].close < idc[-3].bol20_up - idc[-3].atr14\
                        #                 and (idc[-1].max125 - idc[-1].min125) > 0 and (close - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.5:
                                        selected_stock35.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p35')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)                                    


                        ## 볼린저 밴드 스퀴즈
                        ## 밴드 폭 20% 미만

                    if idc[-2].band20_q < 0.2 and idc[-1].band20_q > idc[-2].band20_q\
                        and _tk in rs_df.index\
                            and rs_df.loc[_tk].rs_score > 80:
                        # and idc[-6].bol20_up < idc[-6].sma20 + idc[-6].atr14*2\
                            # and idc[-1].pb > 0.8\
                                # and idc[-1].close < idc[-1].open:
                                        selected_stock36.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p36')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)



                        #########################################################################################################################
                        ###########          매물대 돌파   #######################################################################################

                        ## 매물대 돌파
                        ## 30% 이상 매물이 몰려있는 매물대를 돌파함

                    if _tk in volume_data.keys():
                        vd = volume_data[_tk]
                        if idc[-1].close > vd['volume_p'].idxmax()\
                                and idc[-6].close < vd['volume_p'].idxmax()\
                                    and vd['volume_p'].max() > 40:
                                                selected_stock41.append(i)
                                    
                                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                                listed.insert(0,'p41')
                                                listed.insert(0,i.index[-1])
                                    
                                                selected_stocks.append(listed)

                        ## 매물대 돌파
                        if idc[-1].close > vd['volume_p'].idxmax()\
                                and idc[-6].close < vd['volume_p'].idxmax()\
                                    and vd.loc[vd['volume_p'].idxmax()].volume_p_cum > 80:
                                                selected_stock42.append(i)
                                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                                listed.insert(0,'p42')
                                                listed.insert(0,i.index[-1])
                                                selected_stocks.append(listed)

                    if (idc[-1].max20 - idc[-1].min20) < (idc[-1].max50 - idc[-1].min50)\
                            and (idc[-1].max50 - idc[-1].min50) < (idc[-1].max125 - idc[-1].min125)\
                                and (idc[-1].min20 - idc[-1].min125)/(idc[-1].max125 - idc[-1].min125) > 0.75\
                                    and (idc[-1].close - idc[-1].min20)/(idc[-1].max20 - idc[-1].min20) < 0.9\
                                        and idc[-1].sma50 > idc[-1].sma120 and idc[-1].sma120 > idc[-1].sma200:
                         

         
                                  selected_stock43.append(i)    
  
                                  listed = i.iloc[-1][['ticker', 'name']].to_list()
                                  listed.insert(0,'p43')
                                  listed.insert(0,i.index[-1])
                      
                                  selected_stocks.append(listed)        

                        ### Cup with handle
                    if idc[-1].sma50 > idc[-1].sma150 and idc[-1].sma150 > idc[-1].sma200\
                        and idc[-1].sma200 > idc[-21].sma200\
                            and idc[-1].min5 > idc[-1].max125 * 0.75\
                                and idc[-1].min5 > idc[-1].min125 * 1.25\
                                    and idc[-1].close > idc[-2].max20 * 0.9\
                                        and idc[-1].max5 < idc[-6].max50 * 1.1\
                                            and _tk in rs_df.index\
                                            and rs_df.loc[_tk].rs_score > 90:
                                    selected_stock51.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p51')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)    


                        #########################################################################################################################
                        ###########          Cup with Handle (마크 미너비니)   #################################################################
                
                        ### P52: 마크 미너비니의 Cup with Handle 전략 (완화+ 버전)
                        # Trend Template + SEPA 원칙 기반 (더욱 완화하여 초기 패턴 포착)
                        # 1. 컵 형성: 최소 30일 이상
                        # 2. 컵 깊이: 5-70% (더욱 완화)
                        # 3. 핸들: 컵 높이의 상위 40% 구간
                        # 4. 핸들 깊이: 5-25% (더욱 완화)
                        # 5. 거래량: 바닥과 핸들에서 감소 경향
                        # 6. Trend Template 조건 5개 이상 (더욱 완화)
                        # 7. RS Rating 65 이상 (더욱 완화)
                
                    if len(i) >= 120:
                        # try:
                            # === 1단계: 컵(Cup) 패턴 찾기 ===
                
                            # 컵 왼쪽 고점 (100~150일 전 구간에서 최고점)
                            cup_left_window_start = min(150, len(i)-1)
                            cup_left_window_end = min(70, len(i)-1)
                            cup_left_high = idc[-cup_left_window_start:-cup_left_window_end].high.max()
                
                            # 컵 바닥 (왼쪽 고점 이후 ~ 최근 15일 전)
                            cup_bottom = idc[-70:-15].low.min()
                
                            # 컵 오른쪽 (최근 15일 내 고점)
                            cup_right_high = idc[-15:].high.max()
                
                            # 컵 깊이 계산
                            if cup_left_high > 0:
                                cup_depth_pct = (cup_left_high - cup_bottom) / cup_left_high * 100
                    
                                # === 2단계: 핸들(Handle) 패턴 찾기 ===
                    
                                # 핸들 구간 (최근 5~15일)
                                handle_high = idc[-15:-1].high.max()
                                handle_low = idc[-15:-1].low.min()
                    
                                # 핸들 깊이
                                handle_depth_pct = (handle_high - handle_low) / cup_left_high * 100
                    
                                # 핸들 위치 (컵 바닥으로부터의 상대적 위치)
                                cup_height = cup_left_high - cup_bottom
                                if cup_height > 0:
                                    handle_position_pct = (handle_low - cup_bottom) / cup_height * 100
                                else:
                                    handle_position_pct = 0
                    
                                # === 3단계: 거래량 패턴 분석 ===
                    
                                # 컵 왼쪽 거래량 (고점 부근)
                                vol_left = idc[-100:-70].volume.mean() if len(i) >= 100 else idc[-70:-50].volume.mean()
                    
                                # 컵 바닥 거래량 (저점 부근)
                                vol_bottom = idc[-50:-20].volume.mean()
                    
                                # 핸들 거래량 (최근)
                                vol_handle = idc[-15:-1].volume.mean()
                    
                                # 최근 거래량 급증 여부
                                vol_recent = idc[-1].volume
                    
                                # 거래량 감소 비율
                                vol_drying_1 = (vol_left - vol_bottom) / vol_left if vol_left > 0 else 0
                                vol_drying_2 = (vol_bottom - vol_handle) / vol_bottom if vol_bottom > 0 else 0
                                vol_surge = vol_recent / vol_handle if vol_handle > 0 else 0
                    
                                # === 4단계: Trend Template 검증 (8개 조건) ===
                    
                                tt_conditions = 0
                    
                                # TT1: 현재가 > 150일, 200일 이평
                                if idc[-1].close > idc[-1].sma150 and idc[-1].close > idc[-1].sma200:
                                    tt_conditions += 1
                    
                                # TT2: 150일 이평 > 200일 이평
                                if idc[-1].sma150 > idc[-1].sma200:
                                    tt_conditions += 1
                    
                                # TT3: 200일 이평선 상승 중
                                if idc[-1].sma200 > idc[-21].sma200:
                                    tt_conditions += 1
                    
                                # TT4: 50일 이평 > 150일, 200일 이평
                                if idc[-1].sma50 > idc[-1].sma150 and idc[-1].sma50 > idc[-1].sma200:
                                    tt_conditions += 1
                    
                                # TT5: 현재가 > 50일 이평
                                if idc[-1].close > idc[-1].sma50:
                                    tt_conditions += 1
                    
                                # TT6: 52주 저점보다 20% 이상 상승 (더욱 완화)
                                week52_low = idc[-200:].low.min() if len(i) >= 200 else idc[-120:].low.min()
                                if idc[-1].close > week52_low * 1.20:
                                    tt_conditions += 1
                    
                                # TT7: 52주 고점의 65% 이상 (더욱 완화)
                                week52_high = idc[-200:].high.max() if len(i) >= 200 else idc[-120:].high.max()
                                if week52_high > 0 and idc[-1].close >= week52_high * 0.65:
                                    tt_conditions += 1
                    
                                # TT8: RS Rating 65 이상 (더욱 완화)
                                if _tk in rs_df.index and rs_df.loc[_tk].rs_score >= 65:
                                    tt_conditions += 1
                    
                                # === 5단계: 피벗 포인트 (매수 시점) 계산 ===
                    
                                pivot_point = handle_high
                                buy_point = pivot_point * 1.001  # 0.1% 돌파
                                current_vs_pivot = idc[-1].close / pivot_point
                    
                                # === 6단계: 최종 조건 검증 (더욱 완화) ===
                    
                                # 미너비니 Cup with Handle 조건 (더욱 완화)
                                if 5 <= cup_depth_pct <= 70\
                                    and 5 <= handle_depth_pct <= 25\
                                        and handle_position_pct >= 40\
                                            and tt_conditions >= 5\
                                                and _tk in rs_df.index\
                                                and rs_df.loc[_tk].rs_score >= 65\
                                                    and vol_drying_1 >= 0.05\
                                                        and vol_drying_2 >= 0.02\
                                                            and 0.80 <= current_vs_pivot <= 1.10\
                                                                and idc[-1].close > idc[-1].sma50:
                        
                                    selected_stock52.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p52')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)
                        
                        # except Exception as e:
                            # pass


                        #########################################################################################################################
                        ###########          High Tight Flag (HTF) - 미너비니가 가장 선호하는 패턴   #########################################
                
                        ### P53: High Tight Flag (HTF) - 완화 버전
                        # 1. 6-8주(30-40일) 이내 60% 이상 급등 (완화)
                        # 2. 3-5주간 타이트한 조정 (8-30%) (완화)
                        # 3. 조정 중 거래량 감소
                        # 4. 이동평균선 정배열 유지
                        # 5. RS Rating 75 이상 (완화)
                        # 6. 고점 부근에서 횡보 후 재돌파
                
                    if len(i) >= 80:
                        # try:
                            # === 1단계: 급등 확인 (최근 25~50일) ===
                
                            # 급등 전 가격 (25~50일 전)
                            surge_start_price = idc[-50:-40].close.min() if len(i) >= 50 else idc[-40:-30].close.min()
                
                            # 급등 후 고점 (최근 10~25일)
                            surge_high = idc[-25:-8].high.max()
                
                            # 급등률 계산
                            if surge_start_price > 0:
                                surge_pct = (surge_high - surge_start_price) / surge_start_price * 100
                    
                                # === 2단계: 타이트한 조정 확인 (최근 8~20일) ===
                    
                                flag_high = idc[-20:-1].high.max()
                                flag_low = idc[-20:-1].low.min()
                    
                                # 조정 깊이
                                if surge_high > 0:
                                    flag_depth_pct = (flag_high - flag_low) / surge_high * 100
                        
                                    # 현재가 위치 (고점 대비)
                                    price_from_high = (surge_high - idc[-1].close) / surge_high * 100
                        
                                    # === 3단계: 거래량 패턴 ===
                        
                                    # 급등 구간 거래량
                                    vol_surge = idc[-40:-15].volume.mean()
                        
                                    # 조정 구간 거래량
                                    vol_flag = idc[-15:-1].volume.mean()
                        
                                    # 거래량 감소율
                                    vol_decrease = (vol_surge - vol_flag) / vol_surge if vol_surge > 0 else 0
                        
                                    # === 4단계: 이동평균선 체크 ===
                        
                                    ma_aligned = idc[-1].sma20 > idc[-1].sma50
                        
                                    # === 5단계: 최종 조건 (완화) ===
                        
                                    if surge_pct >= 60\
                                        and 8 <= flag_depth_pct <= 30\
                                            and price_from_high <= 20\
                                                and vol_decrease >= 0.20\
                                                    and ma_aligned\
                                                        and idc[-1].close > idc[-1].sma20\
                                                            and _tk in rs_df.index\
                                                            and rs_df.loc[_tk].rs_score >= 75\
                                                                and idc[-1].close > surge_high * 0.85:
                            
                                        selected_stock53.append(i)
                            
                                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                                        listed.insert(0,'p53')
                                        listed.insert(0,i.index[-1])
                            
                                        selected_stocks.append(listed)
                            
                        # except Exception as e:
                            # pass


                        #########################################################################################################################
                        ###########          VCP (Volatility Contraction Pattern)   ##########################################################
                
                        ### P54: VCP - 변동성 축소 패턴
                        # 1. 3단계 이상의 수축 (Contraction)
                        # 2. 각 수축마다 변동폭 감소 (T1 > T2 > T3)
                        # 3. 각 수축마다 거래량 감소
                        # 4. 이동평균선 상승 지지
                        # 5. RS Rating 75 이상
                        # 6. 마지막 수축이 가장 타이트
                
                    if len(i) >= 120:
                        # try:
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
                            volatility_contracting = (t1_range > t2_range * 0.9 and t2_range > t3_range * 0.9)
                
                            # 거래량도 감소 경향인지
                            volume_contracting = (t1_vol > t2_vol * 0.9 and t2_vol > t3_vol * 0.9)
                
                            # 마지막 수축이 충분히 타이트한지 (완화)
                            is_tight = t3_range < 12
                
                            # === 3단계: 베이스 높이 확인 ===
                
                            # 전체 베이스 깊이
                            base_high = idc[-50:].high.max()
                            base_low = idc[-50:].low.min()
                            base_depth = (base_high - base_low) / base_high * 100 if base_high > 0 else 0
                
                            # === 4단계: 이동평균선 지지 ===
                
                            # 50일선 상승 중 또는 평탄
                            ma50_rising = idc[-1].sma50 >= idc[-10].sma50 * 0.98
                
                            # 현재가가 주요 이평선 위
                            above_ma = idc[-1].close > idc[-1].sma20
                
                            # === 5단계: 최종 조건 (완화) ===
                
                            if volatility_contracting\
                                and volume_contracting\
                                    and is_tight\
                                        and base_depth <= 50\
                                            and ma50_rising\
                                                and above_ma\
                                                    and _tk in rs_df.index\
                                                    and rs_df.loc[_tk].rs_score >= 70\
                                                        and idc[-1].close > base_high * 0.80:
                    
                                selected_stock54.append(i)
                    
                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                listed.insert(0,'p54')
                                listed.insert(0,i.index[-1])
                    
                                selected_stocks.append(listed)
                    
                        # except Exception as e:
                            # pass


                        #########################################################################################################################
                        ###########          Flat Base Breakout   ################################################################################
                
                        ### P55: Flat Base (평평한 베이스) 돌파 - 완화++ 버전
                        # 1. 최소 3주(15일) 이상 횡보 (더욱 완화)
                        # 2. 변동폭 25% 이내 (더욱 완화)
                        # 3. 이전에 상승 추세 존재 (10% 이상)
                        # 4. 횡보 중 거래량 감소 (조건 완화)
                        # 5. RS Rating 70 이상 (더욱 완화)
                        # 6. 주요 이평선 근처에서 형성
                        # 7. 돌파 근접
                
                    if len(i) >= 100:
                        # try:
                            # === 1단계: 이전 상승 추세 확인 ===
                
                            # 베이스 전 가격 (70일 전)
                            pre_base_price = idc[-70].close if len(i) >= 70 else idc[-50].close
                
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
                            vol_pre_base = idc[-70:-35].volume.mean() if len(i) >= 70 else idc[-50:-25].volume.mean()
                
                            # 베이스 중 거래량 (35~8일 전)
                            vol_during_base = idc[-35:-8].volume.mean()
                
                            # 최근 거래량 (최근 8일)
                            vol_recent = idc[-8:].volume.mean()
                
                            # 거래량 감소 후 증가 (더욱 완화)
                            vol_dried = vol_during_base < vol_pre_base * 0.85  # 15% 감소면 OK
                            vol_increasing = vol_recent > vol_during_base * 1.05  # 5% 증가면 OK
                
                            # === 4단계: 이동평균선 배열 ===
                
                            # 주요 이평선 정배열 또는 지지 (더욱 완화)
                            ma_setup = idc[-1].close > idc[-1].sma50 * 0.95  # 50일선 근처면 OK
                
                            # 베이스가 20일선 근처에서 형성 (더욱 완화)
                            base_above_ma20 = base_low > idc[-25].sma20 * 0.90  # 10% 아래까지 허용
                
                            # === 5단계: 돌파 확인 ===
                
                            # 최근 고점 테스트 중 (더욱 완화)
                            near_breakout = idc[-1].close > base_high * 0.85
                
                            # 피벗 포인트
                            pivot = base_high
                
                            # === 6단계: 최종 조건 (더욱 완화) ===
                
                            if pre_base_gain >= 10\
                                and base_range_pct <= 25\
                                    and vol_dried\
                                        and ma_setup\
                                            and base_above_ma20\
                                                and _tk in rs_df.index\
                                                and rs_df.loc[_tk].rs_score >= 70\
                                                    and near_breakout\
                                                        and idc[-1].sma50 >= idc[-10].sma50 * 0.95:
                    
                                selected_stock55.append(i)
                    
                                listed = i.iloc[-1][['ticker', 'name']].to_list()
                                listed.insert(0,'p55')
                                listed.insert(0,i.index[-1])
                    
                                selected_stocks.append(listed)
                    
                        # except Exception as e:
                            # pass

                        #########################################################################################################################
                        ###########          Pre-Rally Setup (P61 - 최종 실용 버전)   #########################################################
                
                        ### P61: 상승 직전 준비 단계 - 20개 내외 안정적 선정
                        # 
                        # 단계적 조합 테스트 결과:
                        # - 조합 4 (ADX 포함): 9개
                        # - 조합 5 (MA20 포함): 7개  ← 여기 근처 목표
                        # - 조합 6 (MFI 포함): 6개
                        # 
                        # 선정 전략:
                        # - 핵심 6개 조건 (AND)
                        # - 보조 조건은 완화 또는 OR
                
                    if len(i) >= 120:
                            # try:
                                current_price = idc[-1].close
                    
                                # === 핵심 조건 6개 (모두 AND - 필수) ===
                    
                                # 1. Band Squeeze: < 0.20 (완화)
                                if not ('band20_q' in i.columns and idc[-1].band20_q < 0.20):
                                    continue
                    
                                # 2. 가격 위치: 0.10-0.50 (완화)
                                if not all(col in i.columns for col in ['close', 'max20', 'min20']):
                                    continue
                                if (idc[-1].max20 - idc[-1].min20) <= 0:
                                    continue
                                position = (current_price - idc[-1].min20) / (idc[-1].max20 - idc[-1].min20)
                                if not (0.10 <= position <= 0.50):
                                    continue
                    
                                # 3. DMI 우위: DI+ - DI- > 3 (완화)
                                if not all(col in i.columns for col in ['di_P', 'di_M']):
                                    continue
                                if not (idc[-1].di_P - idc[-1].di_M > 3):
                                    continue
                    
                                # 4. ADX: > 20 (완화)
                                if not ('adx' in i.columns and idc[-1].adx > 20):
                                    continue
                    
                                # 5. MA20 근처: ±10% (완화)
                                if not ('sma20' in i.columns and idc[-1].sma20 > 0):
                                    continue
                                ma20_dist_pct = (current_price - idc[-1].sma20) / idc[-1].sma20 * 100
                                if not (-10 <= ma20_dist_pct <= 10):
                                    continue
                    
                                # 6. MFI: 40-90 (완화)
                                if not ('mfi' in i.columns and 40 <= idc[-1].mfi <= 90):
                                    continue
                    
                                # === 보조 조건 (3개 중 2개 이상 만족) ===
                    
                                support_count = 0
                    
                                # 7. Williams %R: -80 ~ -20 (선택)
                                if 'willr' in i.columns and -80 <= idc[-1].willr <= -20:
                                    support_count += 1
                    
                                # 8. 이평선 정배열: 20>50>120 (선택)
                                if all(col in i.columns for col in ['sma20', 'sma50', 'sma120']):
                                    if idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120:
                                        support_count += 1
                    
                                # 9. 거래량 증가: 5일 > 20일 (선택, 완화)
                                if all(col in i.columns for col in ['vol_sum5', 'vol_sum20']):
                                    vol_avg5 = idc[-1].vol_sum5 / 5
                                    vol_avg20 = idc[-1].vol_sum20 / 20
                                    if vol_avg5 > vol_avg20 * 0.95:  # 거의 동등 이상이면 OK
                                        support_count += 1
                    
                                # 보조 조건 2개 이상 만족 시 선정
                                if support_count >= 2:
                                    selected_stock61.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p61')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)
                    
                            # except Exception as e:
                                # pass


                        ### volitility
                    if idc[-1].mtr7 > idc[-8].atr14*1.5\
                        and idc[-1].close < idc[-1].max5\
                            and idc[-1].sma20 > idc[-1].sma50 and idc[-1].sma50 > idc[-1].sma120\
                                and idc[-1].sma120 > idc[-1].sma200:
                        
                        selected_stock71.append(i)
                
                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                        listed.insert(0,'p71')
                        listed.insert(0,i.index[-1])
                
                        selected_stocks.append(listed)    

                        ### disparity
                    if idc[-1].band20_q > idc[-2].band20_q\
                        and idc[-1].csi > idc[-2].csi:
                        
                        selected_stock81.append(i)
                
                        listed = i.iloc[-1][['ticker', 'name']].to_list()
                        listed.insert(0,'p81')
                        listed.insert(0,i.index[-1])
                
                        selected_stocks.append(listed)    

 

                        ### RS
                    if _tk in rs_df.index\
                        and rs_df.loc[_tk].rs50_score > 95\
                            and rs_df.loc[_tk].rs20_score < 95 and rs_df.loc[_tk].rs20_score > 50\
                                and rs_df.loc[_tk].rs_score > 90:
                        
                                    selected_stock91.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p91')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)    

                    if idc[-1].band20_q < 0.2 and idc[-1].atr_q < 0.2\
                        and (idc[-1].max10 > idc[-11].max50 or idc[-1].max10 > idc[-11].max125\
                             or idc[-1].max20 > idc[-21].max50 or idc[-1].max20 > idc[-21].max125\
                                 or idc[-1].max50 > idc[-51].max50 or idc[-1].max50 > idc[-51].max125):                              
                
                                    selected_stock92.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p92')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)    

                    if idc[-1].band20_q < 0.8 and idc[-1].band20_q > idc[-6].band20_q:
                                # and idc[-1].csi_fast > idc[-1].csi_slow:
                
                                    selected_stock93.append(i)
                        
                                    listed = i.iloc[-1][['ticker', 'name']].to_list()
                                    listed.insert(0,'p93')
                                    listed.insert(0,i.index[-1])
                        
                                    selected_stocks.append(listed)

    result = {"p11": [x for x in selected_stock11 if hasattr(x,"iloc")], "p12": [x for x in selected_stock12 if hasattr(x,"iloc")], "p13": [x for x in selected_stock13 if hasattr(x,"iloc")], "p14": [x for x in selected_stock14 if hasattr(x,"iloc")], "p15": [x for x in selected_stock15 if hasattr(x,"iloc")], "p16": [x for x in selected_stock16 if hasattr(x,"iloc")], "p17": [x for x in selected_stock17 if hasattr(x,"iloc")], "p21": [x for x in selected_stock21 if hasattr(x,"iloc")], "p22": [x for x in selected_stock22 if hasattr(x,"iloc")], "p23": [x for x in selected_stock23 if hasattr(x,"iloc")], "p24": [x for x in selected_stock24 if hasattr(x,"iloc")], "p25": [x for x in selected_stock25 if hasattr(x,"iloc")], "p26": [x for x in selected_stock26 if hasattr(x,"iloc")], "p27": [x for x in selected_stock27 if hasattr(x,"iloc")], "p28": [x for x in selected_stock28 if hasattr(x,"iloc")], "p29a": [x for x in selected_stock29a if hasattr(x,"iloc")], "p29b": [x for x in selected_stock29b if hasattr(x,"iloc")], "p31": [x for x in selected_stock31 if hasattr(x,"iloc")], "p32": [x for x in selected_stock32 if hasattr(x,"iloc")], "p33": [x for x in selected_stock33 if hasattr(x,"iloc")], "p34": [x for x in selected_stock34 if hasattr(x,"iloc")], "p35": [x for x in selected_stock35 if hasattr(x,"iloc")], "p36": [x for x in selected_stock36 if hasattr(x,"iloc")], "p41": [x for x in selected_stock41 if hasattr(x,"iloc")], "p42": [x for x in selected_stock42 if hasattr(x,"iloc")], "p43": [x for x in selected_stock43 if hasattr(x,"iloc")], "p51": [x for x in selected_stock51 if hasattr(x,"iloc")], "p52": [x for x in selected_stock52 if hasattr(x,"iloc")], "p53": [x for x in selected_stock53 if hasattr(x,"iloc")], "p54": [x for x in selected_stock54 if hasattr(x,"iloc")], "p55": [x for x in selected_stock55 if hasattr(x,"iloc")], "p61": [x for x in selected_stock61 if hasattr(x,"iloc")], "p71": [x for x in selected_stock71 if hasattr(x,"iloc")], "p81": [x for x in selected_stock81 if hasattr(x,"iloc")], "p91": [x for x in selected_stock91 if hasattr(x,"iloc")], "p92": [x for x in selected_stock92 if hasattr(x,"iloc")], "p93": [x for x in selected_stock93 if hasattr(x,"iloc")]}

    return {
        "result": result,
        "debug_counts": debug_counts,
        "selected_stocks": selected_stocks,
    }




def _load_universe(as_of: date) -> pd.DataFrame:
    eng = engine()
    df = pd.read_sql(
        """
        SELECT ticker, name, mcap
        FROM ohlcv
        WHERE date = %s AND mcap IS NOT NULL AND mcap > %s
          AND open IS NOT NULL AND open > 0
        """,
        eng,
        params=(as_of, MCAP_MIN),
    )
    if df.empty:
        return pd.DataFrame(columns=["시가총액", "종목명"])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    out = df.set_index("ticker")
    out["시가총액"] = pd.to_numeric(out["mcap"], errors="coerce")
    out["종목명"] = out["name"]
    return out[["시가총액", "종목명"]]


def _load_ohlcv_map(tickers: list[str], as_of: date) -> dict[str, pd.DataFrame]:
    if not tickers:
        return {}
    eng = engine()
    start = as_of - timedelta(days=HISTORY_DAYS)
    out: dict[str, pd.DataFrame] = {}
    chunk = 200
    for i in range(0, len(tickers), chunk):
        part = tickers[i : i + chunk]
        ph = ",".join(["%s"] * len(part))
        q = f"""
            SELECT ticker, date, open, high, low, close, volume, name, mcap
            FROM ohlcv
            WHERE date BETWEEN %s AND %s AND ticker IN ({ph})
            ORDER BY ticker, date
        """
        df = pd.read_sql(q, eng, params=(start, as_of, *part))
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        df["ticker"] = df["ticker"].astype(str).str.zfill(6)
        for c in ("open", "high", "low", "close", "volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        for tk, g in df.groupby("ticker"):
            g = g.sort_values("date").set_index("date")
            if len(g) < 120:
                continue
            last = g.iloc[-1]
            if any(pd.isna(last[c]) or float(last[c]) <= 0 for c in ("open", "high", "low", "close")):
                continue
            g = g.copy()
            g["name"] = last.get("name") or tk
            out[str(tk)] = g
    return out


def _load_rs_df(as_of: date) -> pd.DataFrame:
    eng = engine()
    df = pd.read_sql(
        "SELECT ticker, rs_10, rs_20, rs_50 FROM rs WHERE date = %s",
        eng,
        params=(as_of,),
    )
    if df.empty:
        return pd.DataFrame(columns=["rs20_score", "rs50_score", "rs_score"])
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)
    for c in ("rs_10", "rs_20", "rs_50"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["rs20_score"] = df["rs_20"]
    df["rs50_score"] = df["rs_50"]
    df["rs_score"] = df[["rs_10", "rs_20", "rs_50"]].mean(axis=1).round(2)
    return df.set_index("ticker")[["rs20_score", "rs50_score", "rs_score"]]


def _compute_indicators_map(ohlcv_map: dict[str, pd.DataFrame], workers: int = 6) -> dict[str, pd.DataFrame]:
    out = {}

    def _one(item):
        tk, raw = item
        try:
            d = raw.copy()
            d["ticker"] = tk
            if "name" not in d.columns:
                d["name"] = tk
            return tk, get_indicators(d)
        except Exception as e:
            log.debug("indicator fail %s: %s", tk, e)
            return tk, None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(_one, it) for it in ohlcv_map.items()]):
            tk, ind = fut.result()
            if ind is not None and len(ind) >= 120:
                out[tk] = ind
    return out


def _compute_volume_map(ohlcv_map: dict[str, pd.DataFrame], workers: int = 4) -> dict[str, pd.DataFrame]:
    out = {}

    def _one(item):
        tk, raw = item
        try:
            if len(raw) > 0 and float(raw.iloc[-1]["volume"] or 0) > 0:
                d = raw.copy()
                d["ticker"] = tk
                return tk, gen_tBand(d, 50)
        except Exception:
            pass
        return tk, None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed([ex.submit(_one, it) for it in ohlcv_map.items()]):
            tk, vb = fut.result()
            if vb is not None:
                out[tk] = vb
    return out


def run_screening_pipeline(as_of: date) -> dict:
    universe = _load_universe(as_of)
    log.info("스크리닝 유니버스(시총>%d억): %d종", MCAP_MIN // 100_000_000, len(universe))
    if universe.empty:
        return {"selected_stocks": [], "debug_counts": {}, "result": {}}
    tickers = list(universe.index)
    ohlcv_map = _load_ohlcv_map(tickers, as_of)
    log.info("스크리닝 OHLCV 로드: %d종", len(ohlcv_map))
    indicators = _compute_indicators_map(ohlcv_map)
    log.info("스크리닝 지표 계산: %d종", len(indicators))
    volume_data = _compute_volume_map(ohlcv_map)
    rs_df = _load_rs_df(as_of)
    return run_screening(indicators, volume_data, rs_df, universe, [])


def count_patterns(selected_stocks: list) -> pd.DataFrame:
    if not selected_stocks:
        return pd.DataFrame(columns=["티커", "종목명", "패턴수", "패턴목록"])
    by_tk: dict[str, set] = defaultdict(set)
    names: dict[str, str] = {}
    for row in selected_stocks:
        if not row or len(row) < 4:
            continue
        pat, tk, nm = str(row[1]), str(row[2]).zfill(6), str(row[3] or "")
        if not str(pat).startswith("p"):
            continue
        by_tk[tk].add(str(pat))
        if nm:
            names[tk] = nm
    rows = [
        {"티커": tk, "종목명": names.get(tk, tk), "패턴수": len(pats), "패턴목록": ",".join(sorted(pats))}
        for tk, pats in by_tk.items()
    ]
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["패턴수", "티커"], ascending=[False, True]).reset_index(drop=True)


def select_chart_candidates(
    selected_stocks: list,
    pick_df: Optional[pd.DataFrame] = None,
    min_patterns: int = 3,
) -> pd.DataFrame:
    cnt = count_patterns(selected_stocks)
    if cnt.empty:
        return cnt
    cnt = cnt[cnt["패턴수"] >= int(min_patterns)].copy()
    if cnt.empty:
        return cnt
    if pick_df is not None and not pick_df.empty and "티커" in pick_df.columns:
        p = pick_df.copy()
        p["티커"] = p["티커"].astype(str).str.zfill(6)
        score_map = (
            p.set_index("티커")["picking점수"] if "picking점수" in p.columns else pd.Series(dtype=float)
        )
        cnt["picking점수"] = cnt["티커"].map(score_map)
    else:
        cnt["picking점수"] = np.nan
    cnt["picking점수"] = pd.to_numeric(cnt["picking점수"], errors="coerce").fillna(-1)
    return cnt.sort_values(["패턴수", "picking점수"], ascending=[False, False]).reset_index(drop=True)


def _setup_korean_font():
    try:
        from content_volatility import _setup_korean_font as _sf

        return _sf()
    except Exception:
        import matplotlib.pyplot as plt

        plt.rcParams["axes.unicode_minus"] = False
        return "sans-serif"


def plot_stock_candle_chart(ohlc: pd.DataFrame, title: str, out_path: Path) -> Path:
    import matplotlib.pyplot as plt
    import mplfinance as mpf
    from matplotlib.lines import Line2D

    _setup_korean_font()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if ohlc is None or ohlc.empty or len(ohlc) < 5:
        fig, ax = plt.subplots(figsize=FIGSIZE, dpi=FIG_DPI)
        ax.text(0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return out_path

    full = ohlc.sort_index().copy()
    full = full.tail(CHART_WINDOW + MA_WARMUP)
    plot_src = pd.DataFrame(
        {
            "Open": pd.to_numeric(full["open"], errors="coerce"),
            "High": pd.to_numeric(full["high"], errors="coerce"),
            "Low": pd.to_numeric(full["low"], errors="coerce"),
            "Close": pd.to_numeric(full["close"], errors="coerce"),
            "Volume": pd.to_numeric(full["volume"], errors="coerce"),
        },
        index=pd.to_datetime(full.index),
    ).dropna(subset=["Open", "High", "Low", "Close"])

    ma20 = plot_src["Close"].rolling(20, min_periods=20).mean()
    ma50 = plot_src["Close"].rolling(50, min_periods=50).mean()
    ma120 = plot_src["Close"].rolling(120, min_periods=120).mean()
    plot_df = plot_src.tail(CHART_WINDOW).copy()
    ma20_p = ma20.reindex(plot_df.index)
    ma50_p = ma50.reindex(plot_df.index)
    ma120_p = ma120.reindex(plot_df.index)

    font_name = plt.rcParams.get("font.family", "sans-serif")
    if isinstance(font_name, (list, tuple)):
        font_name = font_name[0] if font_name else "sans-serif"
    mc = mpf.make_marketcolors(
        up="#d32f2f",
        down="#1565c0",
        edge="inherit",
        wick={"up": "#d32f2f", "down": "#1565c0"},
        volume={"up": "#d32f2f", "down": "#1565c0"},
        ohlc="inherit",
    )
    style = mpf.make_mpf_style(
        marketcolors=mc,
        facecolor="white",
        gridstyle=":",
        y_on_right=False,
        rc={"font.family": font_name, "axes.unicode_minus": False, "figure.facecolor": "white"},
    )
    mav_colors = ["#ef6c00", "#2e7d32", "#5e35b1"]
    addplots = [
        mpf.make_addplot(ma20_p, color=mav_colors[0], width=1.4),
        mpf.make_addplot(ma50_p, color=mav_colors[1], width=1.4),
        mpf.make_addplot(ma120_p, color=mav_colors[2], width=1.4),
    ]
    fig, axes = mpf.plot(
        plot_df,
        type="candle",
        style=style,
        addplot=addplots,
        volume=True,
        figsize=FIGSIZE,
        returnfig=True,
        datetime_format="%y-%m-%d",
        title=title,
        ylabel="가격",
        ylabel_lower="거래량",
    )
    ax = axes[0] if isinstance(axes, (list, np.ndarray)) else axes
    handles = [
        Line2D([0], [0], color=mav_colors[0], lw=1.5, label="MA20"),
        Line2D([0], [0], color=mav_colors[1], lw=1.5, label="MA50"),
        Line2D([0], [0], color=mav_colors[2], lw=1.5, label="MA120"),
    ]
    ax.legend(handles=handles, loc="upper left", fontsize=9, framealpha=0.92)
    try:
        from content_volatility import _apply_horizontal_date_ticks

        for a in axes if isinstance(axes, (list, np.ndarray)) else [ax]:
            try:
                _apply_horizontal_date_ticks(a, format_dates=False)
            except Exception:
                pass
    except Exception:
        pass
    fig.set_size_inches(*FIGSIZE)
    fig.set_dpi(FIG_DPI)
    fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def build_screening_charts(
    as_of: date,
    out_dir: Path,
    pick_df: Optional[pd.DataFrame] = None,
    min_patterns: int = 3,
) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("pick_chart_*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    try:
        scr = run_screening_pipeline(as_of)
    except Exception as e:
        log.exception("스크리닝 실패: %s", e)
        return {
            "candidates": pd.DataFrame(),
            "chart_paths": [],
            "articles": [],
            "selected_count": 0,
            "pass_count": 0,
            "debug": {"error": str(e)},
            "error": str(e),
        }

    selected = scr.get("selected_stocks") or []
    debug = scr.get("debug_counts") or {}
    cands = select_chart_candidates(selected, pick_df=pick_df, min_patterns=min_patterns)
    log.info(
        "스크리닝 통과(패턴≥%d): %d종 (selected_rows=%d, atr_pass=%s)",
        min_patterns,
        len(cands),
        len(selected),
        debug.get("passed_atr_filter"),
    )

    chart_paths: list[Path] = []
    articles: list[dict] = []
    if cands.empty:
        return {
            "candidates": cands,
            "chart_paths": [],
            "articles": [],
            "selected_count": len(selected),
            "pass_count": 0,
            "debug": debug,
        }

    tickers = cands["티커"].tolist()
    ohlcv_map = _load_ohlcv_map(tickers, as_of)
    for _, row in cands.iterrows():
        tk = str(row["티커"])
        nm = str(row.get("종목명") or tk)
        npat = int(row["패턴수"])
        score = row.get("picking점수")
        score_s = "-" if score is None or (isinstance(score, float) and score < 0) else f"{float(score):.1f}"
        title = f"{nm}({tk}) · 선택패턴수 {npat} · picking점수 {score_s}"
        png = out_dir / f"pick_chart_{tk}.png"
        plot_stock_candle_chart(ohlcv_map.get(tk), title, png)
        chart_paths.append(png)
        articles.append({"title": title, "text": f"패턴 {row.get('패턴목록', '')}", "png": png})

    return {
        "candidates": cands,
        "chart_paths": chart_paths,
        "articles": articles,
        "selected_count": len(selected),
        "pass_count": len(cands),
        "debug": debug,
    }
