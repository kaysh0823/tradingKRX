"""
지표 정본 모듈.

정본: .cursor/rules/project-structure.mdc
루트 `indicators_core.py` 와 `naverPub/indicators_core.py` 는 문자 그대로 동일해야 한다.
정의 변경 시 양쪽 저장소를 함께 수정한다.

정본 정의:
- ATR = Wilder 평활(RMA, alpha=1/period). talib.ATR 와 값 동일.
- 에너지배율 = (거래대금 시장내 비중)/(시총 시장내 비중) × (1 + tanh(수익률% / 15))
- RS 평균 = 가중평균 0.4·rs_200 + 0.3·rs_120 + 0.2·rs_50 + 0.1·rs_20  (rs_10 제외, 결측 시 재정규화)
- Talent = 전일종가 대비 일간등락률 ≥ +10% 일수 비중을 20/50/120에 0.5/0.3/0.2 가중합성
- Band Width raw = (BB상단−하단)/중심 = (2·nσ·std)/SMA (window=20, nσ=2, std ddof=1)
- Band Width q = (raw − rollmin(raw,125)) / (rollmax − rollmin)  → 0~1
- 투자자 OSC = 5일 누적 net_val의 stochastic(20, smooth=2).
  기관=6000+3000+3100 (7050 아님), 외국인=9000 단독 (9001 제외)
"""
from __future__ import annotations

import re
from typing import Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd

ArrayLike = Union[pd.Series, np.ndarray, list, float, int]

ENERGY_DIR_K = 15.0
TALENT_UP = 0.10
TALENT_WINDOWS = (20, 50, 120)
TALENT_WEIGHTS = (0.5, 0.3, 0.2)
RS_AVG_COLS = ("rs_20", "rs_50", "rs_120", "rs_200")
RS_AVG_COLS_D = ("rs_20d", "rs_50d", "rs_120d", "rs_200d")
# 가중평균 정본 (합=1). 키는 기간 정규화명; frame 컬럼 rs_20 / rs_20d 모두 매핑.
RS_AVG_WEIGHTS = {"rs_20": 0.1, "rs_50": 0.2, "rs_120": 0.3, "rs_200": 0.4}
_RS_PERIODS_DESC = (200, 120, 50, 20)  # 매칭 시 긴 기간 우선(200 ⊃ 20 방지)

INVESTOR_OSC_PERIOD = 20
INVESTOR_OSC_CUM_DAYS = 5  # 누적일 5일 통일 (수량 차트 10일 → 금액 OSC 5일)
INVESTOR_OSC_SMOOTH = 2
INVESTOR_INST_CODES = ("6000", "3000", "3100")  # 기관 = 연기금등+투신+사모 (7050 아님)
INVESTOR_FOREIGN_CODES = ("9000",)  # 외국인 = 9000 단독 (기타외국인 9001 제외)


def true_range(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
) -> pd.Series:
    """TR = max(H-L, |H-prevC|, |L-prevC|). 전일 없으면 NaN (talib.TRANGE 동일)."""
    h = pd.to_numeric(high, errors="coerce")
    l = pd.to_numeric(low, errors="coerce")
    c = pd.to_numeric(close, errors="coerce")
    prev = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    return tr.where(prev.notna())


def atr_wilder(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> pd.Series:
    """
    Wilder ATR (RMA). talib.ATR 과 값 일치.

    시딩: 첫 유효 ATR = 첫 `period`개 TR의 SMA.
    이후: ATR_t = (ATR_{t-1} * (period-1) + TR_t) / period
    (= ewm(alpha=1/period, adjust=False) with SMA seed)
    """
    tr = true_range(high, low, close)
    n = int(period)
    if n <= 0:
        raise ValueError("period must be positive")
    x = tr.to_numpy(dtype=float)
    out = np.full(len(x), np.nan, dtype=float)
    if len(x) == 0:
        return pd.Series(out, index=tr.index, dtype=float)

    # TR[0] 은 보통 NaN → 첫 finite부터 period개로 시드 (talib 관행)
    start = 0
    while start < len(x) and not np.isfinite(x[start]):
        start += 1
    if start + n > len(x):
        return pd.Series(out, index=tr.index, dtype=float)

    seed = x[start : start + n]
    if not np.all(np.isfinite(seed)):
        return pd.Series(out, index=tr.index, dtype=float)

    seed_i = start + n - 1
    out[seed_i] = float(np.mean(seed))
    for i in range(seed_i + 1, len(x)):
        if not np.isfinite(x[i]) or not np.isfinite(out[i - 1]):
            continue
        out[i] = (out[i - 1] * (n - 1) + x[i]) / n
    return pd.Series(out, index=tr.index, dtype=float)


def energy_ratio_pure(
    tv_share: ArrayLike,
    mcap_share: ArrayLike,
) -> Union[float, pd.Series, np.ndarray]:
    """순수 에너지배율 = 거래대금비중 / 시총비중 (동일 단위)."""
    tv = np.asarray(tv_share, dtype=float)
    mc = np.asarray(mcap_share, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        pure = np.where((mc > 0) & np.isfinite(mc) & np.isfinite(tv), tv / mc, np.nan)
    if np.ndim(pure) == 0:
        return float(pure) if np.isfinite(pure) else float("nan")
    if isinstance(tv_share, pd.Series):
        return pd.Series(pure, index=tv_share.index, dtype=float)
    return pure


def energy_ratio(
    tv_share: ArrayLike,
    mcap_share: ArrayLike,
    ret_pct: Optional[ArrayLike] = None,
    k: float = ENERGY_DIR_K,
) -> Union[float, pd.Series, np.ndarray]:
    """
    에너지배율 = 순수비율 × (1 + tanh(수익률% / K)), K=15.

    ret_pct 가 None 이면 순수비율만 반환.
    수익률 결측/비유한 값은 방향계수 0 (×1.0).
    """
    pure = energy_ratio_pure(tv_share, mcap_share)
    if ret_pct is None:
        return pure
    ret = np.asarray(ret_pct, dtype=float)
    direction = np.where(np.isfinite(ret), np.tanh(ret / float(k)), 0.0)
    pure_arr = np.asarray(pure, dtype=float)
    out = pure_arr * (1.0 + direction)
    if np.ndim(out) == 0:
        return float(out) if np.isfinite(out) else float("nan")
    if isinstance(tv_share, pd.Series):
        return pd.Series(out, index=tv_share.index, dtype=float)
    return out


def _rs_period_key(col: str) -> Optional[str]:
    """컬럼명 → 'rs_20'|'rs_50'|'rs_120'|'rs_200'. rs_10 등은 None."""
    s = str(col).lower()
    # 200|120|50|20 긴 기간 우선 — rs_20 ⊂ rs_200 부분일치 방지
    m = re.search(r"(?:rs[_-]?)?(200|120|50|20)d?", s)
    if not m:
        return None
    return f"rs_{m.group(1)}"


def _rs_weighted_from_pairs(
    pairs: list[tuple[str, ArrayLike]],
    *,
    index=None,
) -> Union[float, pd.Series]:
    """
    (period_key, values) 목록 → 가중평균.
    결측은 skipna + 남은 가중치로 재정규화.
    """
    if not pairs:
        if index is not None:
            return pd.Series(np.nan, index=index, dtype=float)
        return float("nan")

    series_list: list[tuple[float, pd.Series]] = []
    scalars: list[tuple[float, float]] = []
    any_series = False
    for key, raw in pairs:
        w = float(RS_AVG_WEIGHTS.get(key, 0.0))
        if w <= 0:
            continue
        if raw is None:
            continue
        if np.isscalar(raw) or (isinstance(raw, (float, int, np.floating, np.integer)) and not isinstance(raw, (pd.Series, np.ndarray))):
            try:
                x = float(raw)
            except (TypeError, ValueError):
                continue
            scalars.append((w, x))
            continue
        s = pd.to_numeric(pd.Series(raw), errors="coerce")
        series_list.append((w, s))
        any_series = True

    if not any_series and not scalars:
        if index is not None:
            return pd.Series(np.nan, index=index, dtype=float)
        return float("nan")

    if not any_series:
        num = 0.0
        den = 0.0
        for w, x in scalars:
            if np.isfinite(x):
                num += w * x
                den += w
        return float(num / den) if den > 0 else float("nan")

    # 시리즈(+선택 스칼라를 상수 시리즈로)
    aligned: list[tuple[float, pd.Series]] = list(series_list)
    if scalars:
        base_idx = series_list[0][1].index
        for w, x in scalars:
            aligned.append((w, pd.Series(x, index=base_idx, dtype=float)))

    idx = index
    if idx is None:
        idx = aligned[0][1].index
    num = pd.Series(0.0, index=idx, dtype=float)
    den = pd.Series(0.0, index=idx, dtype=float)
    for w, s in aligned:
        s = s.reindex(idx)
        ok = s.notna() & np.isfinite(s.to_numpy(dtype=float, na_value=np.nan))
        vals = s.to_numpy(dtype=float, na_value=np.nan)
        num = num + pd.Series(np.where(ok, w * vals, 0.0), index=idx)
        den = den + pd.Series(np.where(ok, w, 0.0), index=idx)
    out = num / den.replace(0.0, np.nan)
    return out.astype(float)


def rs_avg(
    rs_20: ArrayLike = None,
    rs_50: ArrayLike = None,
    rs_120: ArrayLike = None,
    rs_200: ArrayLike = None,
    *,
    frame: Optional[pd.DataFrame] = None,
    cols: Sequence[str] = RS_AVG_COLS,
) -> Union[float, pd.Series]:
    """
    RS 가중평균 (rs_10 제외).

    rs_avg = 0.4·rs_200 + 0.3·rs_120 + 0.2·rs_50 + 0.1·rs_20
    결측 컬럼은 skipna 후 남은 가중치로 재정규화.
    frame+cols(rs_20 / rs_20d 등) 또는 개별 인자. 루트·naverPub 함께 수정.
    """
    if frame is not None:
        pairs: list[tuple[str, ArrayLike]] = []
        seen: set[str] = set()
        for c in cols:
            if c not in frame.columns:
                continue
            key = _rs_period_key(c)
            if key is None or key in seen:
                continue
            seen.add(key)
            pairs.append((key, frame[c]))
        return _rs_weighted_from_pairs(pairs, index=frame.index)

    pairs = []
    for key, v in (
        ("rs_20", rs_20),
        ("rs_50", rs_50),
        ("rs_120", rs_120),
        ("rs_200", rs_200),
    ):
        if v is None:
            continue
        pairs.append((key, v))
    return _rs_weighted_from_pairs(pairs)


def daily_ret_from_close(close: pd.Series) -> pd.Series:
    """전일종가 대비 일간 등락률(소수, 예: +10% → 0.10)."""
    cl = pd.to_numeric(close, errors="coerce")
    prev = cl.shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        return (cl / prev.replace(0, np.nan)) - 1.0


def talent_up_mask(close: pd.Series, thr: float = TALENT_UP) -> pd.Series:
    """전일종가 대비 등락률 ≥ thr 인 날 True."""
    ret = daily_ret_from_close(close)
    prev = pd.to_numeric(close, errors="coerce").shift(1)
    valid = prev.notna() & (prev > 0) & pd.to_numeric(close, errors="coerce").notna()
    return (ret >= float(thr)) & valid


def talent_up_count(close: pd.Series, window: int, thr: float = TALENT_UP) -> float:
    """최근 window 거래일 중 +thr 이상 일수."""
    up = talent_up_mask(close, thr=thr)
    if up.empty:
        return float("nan")
    return float(int(up.tail(int(window)).sum()))


def talent_up_share(close: pd.Series, window: int, thr: float = TALENT_UP) -> float:
    """최근 window 거래일 중 +thr 이상 일수 비중 = count / window."""
    n = talent_up_count(close, window, thr=thr)
    if not np.isfinite(n):
        return float("nan")
    return float(n) / float(window)


def talent_score(
    close: pd.Series,
    thr: float = TALENT_UP,
    windows: Sequence[int] = TALENT_WINDOWS,
    weights: Sequence[float] = TALENT_WEIGHTS,
) -> dict:
    """
    Talent 합성 점수 + 개별 비중/일수.

    score = Σ (n_w / w) * weight_w
    기본: 20/50/120 × 0.5/0.3/0.2
    """
    wins = tuple(int(w) for w in windows)
    wts = tuple(float(w) for w in weights)
    if len(wins) != len(wts):
        raise ValueError("windows and weights length mismatch")
    shares = [talent_up_share(close, w, thr=thr) for w in wins]
    counts = [talent_up_count(close, w, thr=thr) for w in wins]
    score = 0.0
    for s, wt in zip(shares, wts):
        if np.isfinite(s):
            score += float(s) * wt
    out = {"score": float(score), "shares": shares, "counts": counts}
    if len(wins) >= 3:
        out.update(
            {
                "share_20": shares[0],
                "share_50": shares[1],
                "share_120": shares[2],
                "n20": counts[0],
                "n50": counts[1],
                "n120": counts[2],
            }
        )
    return out


def bollinger_band_width(
    close: pd.Series,
    window: int = 20,
    n_sigma: float = 2.0,
) -> pd.Series:
    """
    Band Width raw = (BB상단 − 하단) / 중심 = (2·nσ·std) / SMA(window).

    std = rolling sample std (ddof=1). 정본: screening/51 band20_w.
    루트·naverPub indicators_core 함께 수정.
    """
    s = pd.to_numeric(close, errors="coerce")
    w = int(window)
    mid = s.rolling(w, min_periods=w).mean()
    std = s.rolling(w, min_periods=w).std(ddof=1)
    return (2.0 * float(n_sigma) * std) / mid.replace(0, np.nan)


def bollinger_band_width_q(
    close: pd.Series,
    window: int = 20,
    n_sigma: float = 2.0,
    lookback: int = 125,
) -> pd.Series:
    """
    Band Width 정규화 q ∈ [0,1].

    q = (raw − rollmin(raw, lookback)) / (rollmax − rollmin).
    정본: screening/51 band20_q. 루트·naverPub indicators_core 함께 수정.
    """
    raw = bollinger_band_width(close, window=window, n_sigma=n_sigma)
    lb = int(lookback)
    rmin = raw.rolling(lb, min_periods=lb).min()
    rmax = raw.rolling(lb, min_periods=lb).max()
    denom = (rmax - rmin).replace(0, np.nan)
    return (raw - rmin) / denom


def stochastic_osc(
    series: pd.Series,
    period: int = INVESTOR_OSC_PERIOD,
    smooth: int = INVESTOR_OSC_SMOOTH,
) -> pd.Series:
    """
    Stochastic oscillator 0~100.

    lo=rolling(period).min(), hi=rolling(period).max()
    raw = 100*(s-lo)/(hi-lo)  ((hi-lo)==0 → NaN)
    return EMA(raw, span=smooth).clip(0,100)
    """
    s = pd.to_numeric(series, errors="coerce")
    n = int(period)
    lo = s.rolling(n, min_periods=n).min()
    hi = s.rolling(n, min_periods=n).max()
    raw = 100.0 * (s - lo) / (hi - lo).replace(0, np.nan)
    return raw.ewm(span=int(smooth), adjust=False).mean().clip(0, 100)


def investor_net_osc(
    net_val_series: pd.Series,
    cum_days: int = INVESTOR_OSC_CUM_DAYS,
    period: int = INVESTOR_OSC_PERIOD,
    smooth: int = INVESTOR_OSC_SMOOTH,
) -> pd.Series:
    """결측 0 채움 → rolling(cum_days, min_periods=1).sum() → stochastic_osc. 금액(net_val)."""
    s = pd.to_numeric(net_val_series, errors="coerce").fillna(0)
    cum = s.rolling(int(cum_days), min_periods=1).sum()
    return stochastic_osc(cum, period=period, smooth=smooth)


def investor_osc_frame(
    long_df: pd.DataFrame,
    groups: Optional[Mapping[str, Sequence[str]]] = None,
) -> pd.DataFrame:
    """
    투자자 롱테이블 → 일자 인덱스 OSC.

    long_df 컬럼: [date, invst_tp_cd, net_val]  (금액 기준. net_qty 사용 금지)
    groups 기본: inst_net_osc=6000+3000+3100, frgn_net_osc=9000
    """
    if groups is None:
        groups = {
            "inst_net_osc": INVESTOR_INST_CODES,
            "frgn_net_osc": INVESTOR_FOREIGN_CODES,
        }
    cols = list(groups.keys())
    if long_df is None or getattr(long_df, "empty", True):
        return pd.DataFrame(columns=cols)
    df = long_df.copy()
    if "date" not in df.columns or "invst_tp_cd" not in df.columns or "net_val" not in df.columns:
        raise ValueError("long_df must have columns: date, invst_tp_cd, net_val")
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
    piv.index = pd.to_datetime(piv.index, errors="coerce").normalize()
    piv = piv[~piv.index.isna()].sort_index()
    out = pd.DataFrame(index=piv.index)
    for name, codes in groups.items():
        code_list = [str(c).strip() for c in codes]
        net = None
        for c in code_list:
            part = pd.to_numeric(piv[c], errors="coerce") if c in piv.columns else pd.Series(0.0, index=piv.index)
            net = part if net is None else net.add(part, fill_value=0)
        out[name] = investor_net_osc(net if net is not None else pd.Series(0.0, index=piv.index))
    return out[cols]
