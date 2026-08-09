"""
전역 제외 종목 목록.

수집(OHLCV/지수/ETF PDF/투자자/RS·talent 적재)은 유지하고,
분석·표·그래프 유니버스에서만 제외한다.

변경 시 루트 exclusions.py 와 naverPub/exclusions.py 를 함께 수정(문자 동일).
"""
from __future__ import annotations

import re
from typing import Iterable

# 수집은 유지, 분석/표/그래프에서만 제외. 변경 시 양쪽 저장소 함께 수정.
EXCLUDED_TICKERS: dict[str, str] = {
    # "005930": "삼성전자",   # 예시(실제 제외할 종목만)
    "079940": "가비아",
    "121440": "골프존홀딩스",
}

_SPAC_RE = re.compile(r"스팩|제[0-9]+호")
# 우선주: …우 / …2우B / …우C / (전환) / 우선주
_PREF_RE = re.compile(r"(?:우선주|\(전환\)|(?:\d)?우[A-Z]?)$")


def _norm(t) -> str:
    """티커 정규화: 숫자 티커는 6자리 zero-pad, 그 외(영문 혼합 등)는 strip만."""
    s = str(t).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.zfill(6) if s.isdigit() else s


EXCLUDED: set[str] = {_norm(t) for t in EXCLUDED_TICKERS}


def is_common_stock(name) -> bool:
    """
    종목명 기반 보통주 판정.
    제외: 스팩('스팩'|제N호), 리츠(endswith '리츠'), 우선주(끝 '우'/'우B'/'(전환)' 등).
    루트 krx_diff '기타'는 이름만으로 완전 재현 불가 → 이름 휴리스틱으로 맞춤.
    """
    if name is None:
        return False
    try:
        # NaN
        if isinstance(name, float) and name != name:
            return False
    except Exception:
        pass
    s = str(name).strip()
    if not s or s.lower() in ("nan", "none"):
        return False
    if _SPAC_RE.search(s):
        return False
    if s.endswith("리츠"):
        return False
    if _PREF_RE.search(s):
        return False
    return True


def drop_excluded(df, col: str = "ticker"):
    """DataFrame에서 제외 티커 행 제거. col 없거나 df 비면 그대로 반환."""
    if df is None or not hasattr(df, "columns") or col not in df.columns:
        return df
    if getattr(df, "empty", False):
        return df
    return df[~df[col].astype(str).map(_norm).isin(EXCLUDED)]


def drop_non_common(df, name_col: str = "name"):
    """DataFrame에서 보통주가 아닌 종목(이름 휴리스틱) 행 제거. name_col 없으면 그대로."""
    if df is None or not hasattr(df, "columns") or name_col not in df.columns:
        return df
    if getattr(df, "empty", False):
        return df
    return df[df[name_col].map(is_common_stock)]


def filter_common_stock_df(df, ticker_col: str = "ticker", name_col: str = "name"):
    """전역 제외(filter_tickers 계열) + 보통주(이름) 필터."""
    return drop_non_common(drop_excluded(df, ticker_col), name_col)


def filter_tickers(tickers: Iterable | None):
    """list/set 등 티커 시퀀스에서 제외 종목 제거 → list 반환."""
    if tickers is None:
        return []
    return [t for t in tickers if _norm(str(t)) not in EXCLUDED]
