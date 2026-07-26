"""
종목 선정: 에너지배율·RS·주가위치·talent Top50 순위 점수를 가중 합산한 이중 랭킹.

배점: 1위=250, 50위=50, 선형 보간
  score = 250 - (rank-1) * (200/49)
표 표시: 4지표 컬럼은 원본 값, picking점수는 유형별 가중합.
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
SCORE_BOTTOM = 50.0
TOP_N = 50
SCORE_STEP = (SCORE_TOP - SCORE_BOTTOM) / (TOP_N - 1)  # 200/49

# (market_key, display_col, raw_source_cols 우선순위, round_digits)
METRIC_SPECS = (
    ("energy", "에너지배율", ("3일에너지배율",), 2),
    ("rs", "RS", ("rs_120", "rs_avg"), 2),
    ("pos", "주가위치", ("주가위치",), 2),
    ("talent", "talent", ("talent 지수",), 3),
)

METRIC_COLS = ("에너지배율", "RS", "주가위치", "talent")

# 장기모멘텀형(추세추종) / 단기모멘텀형(급등)
WEIGHT_SETS: dict[str, dict[str, float]] = {
    "long": {
        "RS": 0.50,
        "주가위치": 0.30,
        "talent": 0.10,
        "에너지배율": 0.10,
    },
    "short": {
        "주가위치": 0.40,
        "RS": 0.20,
        "talent": 0.20,
        "에너지배율": 0.20,
    },
}

PICK_TYPE_META = (
    ("long", "장기 모멘텀", "추세추종 · RS 0.50 / 주가위치 0.30 / talent 0.10 / 에너지 0.10"),
    ("short", "단기 모멘텀", "급등 · 주가위치 0.40 / RS 0.20 / talent 0.20 / 에너지 0.20"),
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
    """1→250, 50→50. 범위 밖은 0."""
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
    """티커 → '250일' 등 (최장 종가 신고가 구간)."""
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


def _collect_metric_bag(market: dict) -> dict[str, dict]:
    """시장별 4지표 Top50 → 티커별 환산점수·원값 가방."""
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
    return bag


def build_picking_rank(
    as_of: Optional[date] = None,
    market: Optional[dict] = None,
    top_n: int = TOP_N,
    weight_key: str = "long",
    bag: Optional[dict[str, dict]] = None,
    high_map: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """
    유형별 가중합 TopN.
    4지표 중 한 번도 Top50에 못 든 종목은 bag에 없어 자동 제외.
    """
    if market is None and bag is None:
        market = build_all_market(as_of)
    if bag is None:
        bag = _collect_metric_bag(market or {})
    if high_map is None:
        high_map = _high_label_map(market or {})

    weights = WEIGHT_SETS.get(weight_key) or WEIGHT_SETS["long"]
    rows = []
    for d in bag.values():
        total = 0.0
        for col in METRIC_COLS:
            total += float(d.get(f"_sc_{col}") or 0.0) * float(weights.get(col, 0.0))
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
    """렌더용: {'long': DataFrame, 'short': DataFrame}."""
    if market is None:
        market = build_all_market(as_of)
    bag = _collect_metric_bag(market)
    high_map = _high_label_map(market)
    out: dict[str, pd.DataFrame] = {}
    for key, title, _cap in PICK_TYPE_META:
        df = build_picking_rank(
            as_of=as_of,
            market=market,
            weight_key=key,
            bag=bag,
            high_map=high_map,
        )
        out[key] = df
        log.info("%s Top%d: %d종", title, TOP_N, len(df))
    return out
