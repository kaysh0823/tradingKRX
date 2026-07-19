"""
Picking: 에너지배율·RS·주가위치·talent Top50 순위 점수를 합산한 통합 랭킹.

배점(합산용): 1위=250, 50위=200, 선형 보간
  score = 250 - (rank-1) * (50/49)
표 표시: 4지표 컬럼은 원본 값 (Top50 밖 '-'), picking점수는 환산 점수 합.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from content_market import MARKETS, build_all_market

log = logging.getLogger("naverPub.content_picking")

SCORE_TOP = 250.0
SCORE_BOTTOM = 200.0
TOP_N = 50
SCORE_STEP = (SCORE_TOP - SCORE_BOTTOM) / (TOP_N - 1)  # 50/49

# (market_key, display_col, raw_source_cols 우선순위, round_digits)
METRIC_SPECS = (
    ("energy", "에너지배율", ("3일에너지배율",), 2),
    ("rs", "RS", ("rs_120", "rs_avg"), 2),
    ("pos", "주가위치", ("주가위치",), 2),
    ("talent", "talent", ("talent 지수",), 3),
)

PICK_COLS = [
    "순위",
    "티커",
    "종목명",
    "현재가",
    "picking점수",
    "에너지배율",
    "RS",
    "주가위치",
    "talent",
    "신고가여부",
]


def rank_to_score(rank: int) -> float:
    """1→250, 50→200. 범위 밖은 0."""
    try:
        r = int(rank)
    except (TypeError, ValueError):
        return 0.0
    if r < 1 or r > TOP_N:
        return 0.0
    return SCORE_TOP - (r - 1) * SCORE_STEP


def _raw_metric_value(row: pd.Series, source_cols: tuple[str, ...]):
    """원본 지표 값. 우선순위 컬럼 중 첫 유효값."""
    for c in source_cols:
        if c not in row.index:
            continue
        v = row.get(c)
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            continue
        try:
            if pd.isna(v):
                continue
        except (TypeError, ValueError):
            pass
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    # RS 표에 rs_avg 없으면 rs_20~200 평균
    if "rs_120" in source_cols:
        parts = []
        for c in ("rs_20", "rs_50", "rs_120", "rs_200"):
            if c in row.index and pd.notna(row.get(c)):
                try:
                    parts.append(float(row.get(c)))
                except (TypeError, ValueError):
                    pass
        if parts:
            return float(np.mean(parts))
    return np.nan


def _high_label_map(market: dict) -> dict[str, str]:
    """티커 → '250일' 등 (최장 신고가 구간)."""
    out: dict[str, str] = {}
    for mkt in MARKETS:
        hdf = (market.get(mkt) or {}).get("high")
        if hdf is None or hdf.empty or "티커" not in hdf.columns:
            continue
        for _, r in hdf.iterrows():
            tk = str(r.get("티커") or "").strip()
            if not tk:
                continue
            label = str(r.get("달성구간") or "")
            m = re.match(r"(\d+)일", label)
            text = f"{m.group(1)}일" if m else (label.strip() or "-")
            prev = out.get(tk)
            if prev is None:
                out[tk] = text
                continue
            try:
                if int(re.match(r"(\d+)", text).group(1)) > int(re.match(r"(\d+)", prev).group(1)):
                    out[tk] = text
            except (AttributeError, ValueError, TypeError):
                out[tk] = text
    return out


def build_picking_rank(
    as_of: Optional[date] = None,
    market: Optional[dict] = None,
    top_n: int = TOP_N,
) -> pd.DataFrame:
    """
    시장별 4지표 Top50 순위 → 환산점수 합산 → 통합 TopN.
    표 컬럼(에너지배율/RS/주가위치/talent)에는 원본 값을 넣음.
    """
    if market is None:
        market = build_all_market(as_of)

    bag: dict[str, dict] = {}

    for mkt in MARKETS:
        block = market.get(mkt) or {}
        for key, disp_col, src_cols, digits in METRIC_SPECS:
            df = block.get(key)
            if df is None or df.empty:
                continue
            if "티커" not in df.columns or "순위" not in df.columns:
                continue
            for _, row in df.iterrows():
                tk = str(row.get("티커") or "").strip()
                if not tk:
                    continue
                sc = rank_to_score(row.get("순위"))
                if sc <= 0:
                    continue
                raw = _raw_metric_value(row, src_cols)
                if tk not in bag:
                    bag[tk] = {
                        "티커": tk,
                        "종목명": row.get("종목명") or "",
                        "현재가": row.get("현재가"),
                        "_sc_에너지배율": 0.0,
                        "_sc_RS": 0.0,
                        "_sc_주가위치": 0.0,
                        "_sc_talent": 0.0,
                        "에너지배율": np.nan,
                        "RS": np.nan,
                        "주가위치": np.nan,
                        "talent": np.nan,
                        "_dig_에너지배율": 2,
                        "_dig_RS": 2,
                        "_dig_주가위치": 2,
                        "_dig_talent": 3,
                    }
                sc_key = f"_sc_{disp_col}"
                prev_sc = float(bag[tk].get(sc_key) or 0.0)
                if sc >= prev_sc:
                    bag[tk][sc_key] = sc
                    if raw is not None and np.isfinite(raw):
                        bag[tk][disp_col] = round(float(raw), int(digits))
                    else:
                        bag[tk][disp_col] = np.nan
                if row.get("종목명"):
                    bag[tk]["종목명"] = row.get("종목명")
                if row.get("현재가") is not None and pd.notna(row.get("현재가")):
                    bag[tk]["현재가"] = row.get("현재가")

    high_map = _high_label_map(market)
    rows = []
    for d in bag.values():
        total = float(
            d["_sc_에너지배율"] + d["_sc_RS"] + d["_sc_주가위치"] + d["_sc_talent"]
        )
        if total <= 0:
            continue
        rows.append(
            {
                "티커": d["티커"],
                "종목명": d["종목명"],
                "현재가": d["현재가"],
                "picking점수": round(total, 1),
                "에너지배율": d["에너지배율"],
                "RS": d["RS"],
                "주가위치": d["주가위치"],
                "talent": d["talent"],
                "신고가여부": high_map.get(d["티커"], "-"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=PICK_COLS)

    out = pd.DataFrame(rows)
    out = out.sort_values(
        ["picking점수", "현재가"],
        ascending=[False, False],
        na_position="last",
    ).head(int(top_n)).reset_index(drop=True)
    out.insert(0, "순위", range(1, len(out) + 1))
    return out[PICK_COLS]


def build_all_picking(
    as_of: Optional[date] = None,
    market: Optional[dict] = None,
) -> dict[str, pd.DataFrame]:
    """렌더용: {'pick': DataFrame}."""
    df = build_picking_rank(as_of=as_of, market=market)
    log.info("Picking Top%d: %d종", TOP_N, len(df))
    return {"pick": df}
