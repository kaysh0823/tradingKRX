"""
지표 정본 모듈.

정본: .cursor/rules/project-structure.mdc
루트 `indicators_core.py` 와 `naverPub/indicators_core.py` 는 문자 그대로 동일해야 한다.
정의 변경 시 양쪽 저장소를 함께 수정한다.

정본 정의:
- ATR = Wilder 평활(RMA, alpha=1/period). talib.ATR 와 값 동일.
- 에너지배율 = (거래대금 시장내 비중)/(시총 시장내 비중) × (1 + tanh(수익률% / 15))
- RS 평균 = mean(rs_20, rs_50, rs_120, rs_200)  (rs_10 제외)
- Talent = 전일종가 대비 일간등락률 ≥ +10% 일수 비중을 20/50/120에 0.5/0.3/0.2 가중합성
- Band Width raw = (BB상단−하단)/중심 = (2·nσ·std)/SMA (window=20, nσ=2, std ddof=1)
- Band Width q = (raw − rollmin(raw,125)) / (rollmax − rollmin)  → 0~1
"""
from __future__ import annotations

from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd

ArrayLike = Union[pd.Series, np.ndarray, list, float, int]

ENERGY_DIR_K = 15.0
TALENT_UP = 0.10
TALENT_WINDOWS = (20, 50, 120)
TALENT_WEIGHTS = (0.5, 0.3, 0.2)
RS_AVG_COLS = ("rs_20", "rs_50", "rs_120", "rs_200")
RS_AVG_COLS_D = ("rs_20d", "rs_50d", "rs_120d", "rs_200d")


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
    RS 평균 = mean(rs_20, rs_50, rs_120, rs_200). rs_10 제외.

    frame+cols 로 DataFrame 행평균, 또는 개별 시리즈/스칼라 인자.
    """
    if frame is not None:
        use = [c for c in cols if c in frame.columns]
        if not use:
            return pd.Series(np.nan, index=frame.index, dtype=float)
        return frame[use].apply(pd.to_numeric, errors="coerce").mean(axis=1, skipna=True)

    parts = []
    for v in (rs_20, rs_50, rs_120, rs_200):
        if v is None:
            continue
        parts.append(pd.to_numeric(v, errors="coerce"))
    if not parts:
        return float("nan")
    if all(np.isscalar(p) or (isinstance(p, float)) for p in parts):
        arr = np.asarray([float(p) for p in parts], dtype=float)
        return float(np.nanmean(arr)) if np.any(np.isfinite(arr)) else float("nan")
    df = pd.concat([pd.Series(p) for p in parts], axis=1)
    return df.mean(axis=1, skipna=True)


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
