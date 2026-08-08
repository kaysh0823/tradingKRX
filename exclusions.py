"""
전역 제외 종목 목록.

수집(OHLCV/지수/ETF PDF/투자자/RS·talent 적재)은 유지하고,
분석·표·그래프 유니버스에서만 제외한다.

변경 시 루트 exclusions.py 와 naverPub/exclusions.py 를 함께 수정(문자 동일).
"""
from __future__ import annotations

from typing import Iterable

# 수집은 유지, 분석/표/그래프에서만 제외. 변경 시 양쪽 저장소 함께 수정.
EXCLUDED_TICKERS: dict[str, str] = {
    # "005930": "삼성전자",   # 예시(실제 제외할 종목만)
}


def _norm(t) -> str:
    """티커 정규화: 숫자 티커는 6자리 zero-pad, 그 외(영문 혼합 등)는 strip만."""
    s = str(t).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


EXCLUDED: set[str] = {_norm(t) for t in EXCLUDED_TICKERS}


def drop_excluded(df, col: str = "ticker"):
    """DataFrame에서 제외 티커 행 제거. col 없거나 df 비면 그대로 반환."""
    if df is None or not hasattr(df, "columns") or col not in df.columns:
        return df
    if getattr(df, "empty", False):
        return df
    return df[~df[col].astype(str).map(_norm).isin(EXCLUDED)]


def filter_tickers(tickers: Iterable | None):
    """list/set 등 티커 시퀀스에서 제외 종목 제거 → list 반환."""
    if tickers is None:
        return []
    return [t for t in tickers if _norm(str(t)) not in EXCLUDED]
