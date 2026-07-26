# -*- coding: utf-8 -*-
"""
ETF PDF 구성비중 시각화 (33. ETF_PDF_GRAPH_1.0)
================================================================================
`krx_etf_pdf` 데이터를 읽어 ETF별 구성비중을 4개 시점 표로 비교합니다.

  · DB최근일자 / 3거래일 전 / 1주일 전(T-5) / 2주일 전(T-10)
  · 연속 시점 대비 변화는 셀 안 () 와 그라데이션으로 표시
  · 맨 위: 전체 합산 요약 순위 (비중 / 3일·1주·2주 변화)

출력: results/etf_pdf_weight_dashboard_{기준일}.html

■ 필요: pip install pandas pymysql sqlalchemy
"""

from __future__ import annotations

import html
import inspect
import os
import webbrowser
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────────
DB_URL = "mysql+pymysql://root:GloriaDahn03240701@127.0.0.1:3306/kor_stock_db"
PDF_TABLE = "krx_etf_pdf"
WEIGHT_COL = "시가총액기준 구성비중"

START_DATE: date | str | None = None
END_DATE: date | str | None = None
ETF_FILTER: list[str] | None = None

# (표시명, DB 최근일 기준 거래일 오프셋)
SNAPSHOT_DEFS: list[tuple[str, int]] = [
    ("DB최근", 0),
    ("3일전", 3),
    ("일주일전", 5),
    ("2주일전", 10),
]
# 연속 비교: (현재 컬럼, 직전 컬럼) — 현재 셀에 (현재−직전) 표시
CHAIN_COMPARE: list[tuple[str, str]] = [
    ("DB최근", "3일전"),
    ("3일전", "일주일전"),
    ("일주일전", "2주일전"),
]

TOP_N = 20                 # ETF별 표에 표시할 구성종목 수 (최근 비중 기준)
SUMMARY_TOP_N = 20         # 상단 전체 요약 순위 행 수
OPEN_BROWSER = True
# 그라데이션: |변화| 이 값(pp)일 때 최대 채도
GRAD_MAX_PP = 5.0
# 비중 상위(제외) 요약에서 빼는 종목 (티커 / 종목명 키워드)
EXCLUDE_FROM_WEIGHT_RANK: set[str] = {"005930", "000660"}  # 삼성전자, SK하이닉스
EXCLUDE_NAME_KEYWORDS: tuple[str, ...] = ("삼성전자", "하이닉스")


def _script_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        for fi in inspect.stack():
            p = getattr(fi, "filename", "") or ""
            if "ETF_PDF_GRAPH" in p.replace("\\", "/") or "ETF_PDF" in p.replace("\\", "/"):
                return os.path.dirname(os.path.abspath(p))
        wd = os.getcwd()
        for name in ("33. ETF_PDF_GRAPH_1.0.py", "ETF_PDF_GRAPH_1.0.py"):
            if os.path.isfile(os.path.join(wd, name)):
                return wd
        for sub in ("30. ETF", "ETF"):
            d = os.path.join(wd, sub)
            for name in ("33. ETF_PDF_GRAPH_1.0.py", "ETF_PDF_GRAPH_1.0.py"):
                if os.path.isfile(os.path.join(d, name)):
                    return d
        return wd


RESULTS_DIR = os.path.join(_script_dir(), "results")


# ─────────────────────────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────────────────────────
def load_pdf_from_db(
    start_date=None,
    end_date=None,
    etf_codes: list[str] | None = None,
) -> pd.DataFrame:
    engine = create_engine(DB_URL)
    where = ["1=1"]
    params: dict = {}

    if start_date is not None:
        where.append("`수집일자` >= :start_date")
        params["start_date"] = pd.to_datetime(start_date).date()
    if end_date is not None:
        where.append("`수집일자` <= :end_date")
        params["end_date"] = pd.to_datetime(end_date).date()
    if etf_codes:
        codes = [str(c).strip() for c in etf_codes]
        placeholders = ", ".join(f":c{i}" for i in range(len(codes)))
        where.append(f"`ETF코드` IN ({placeholders})")
        for i, c in enumerate(codes):
            params[f"c{i}"] = c

    sql = text(f"""
        SELECT
            `ETF코드`, `ETF명`, `수집일자`, `티커`, `구성종목명`,
            `계약수`, `금액`, `시가총액`, `{WEIGHT_COL}` AS 비중
        FROM `{PDF_TABLE}`
        WHERE {' AND '.join(where)}
        ORDER BY `ETF코드`, `수집일자`, `{WEIGHT_COL}` DESC
    """)

    df = pd.read_sql(sql, con=engine, params=params)
    if df.empty:
        return df

    df["수집일자"] = pd.to_datetime(df["수집일자"])
    df["ETF코드"] = df["ETF코드"].astype(str).str.strip()
    df["티커"] = df["티커"].astype(str).str.strip()
    df["구성종목명"] = df["구성종목명"].astype(str).str.strip()
    df["ETF명"] = df["ETF명"].fillna(df["ETF코드"]).astype(str).str.strip()
    df["비중"] = pd.to_numeric(df["비중"], errors="coerce")
    return df


def _etf_display_name(g: pd.DataFrame, code: str) -> str:
    names = g["ETF명"].dropna().astype(str)
    names = names[names.str.len() > 0]
    if not names.empty:
        return f"{code} · {names.iloc[-1]}"
    return code


# ─────────────────────────────────────────────────────────────
# 스냅샷
# ─────────────────────────────────────────────────────────────
def resolve_snapshot_dates(all_dates: list) -> dict[str, pd.Timestamp | None]:
    dates = sorted(pd.to_datetime(all_dates).unique())
    if not dates:
        return {name: None for name, _ in SNAPSHOT_DEFS}
    out: dict[str, pd.Timestamp | None] = {}
    latest_idx = len(dates) - 1
    for name, offset in SNAPSHOT_DEFS:
        idx = latest_idx - int(offset)
        out[name] = dates[idx] if idx >= 0 else None
    return out


def snapshot_meta_html(snaps: dict[str, pd.Timestamp | None]) -> str:
    bits = []
    for name, _ in SNAPSHOT_DEFS:
        d = snaps.get(name)
        if d is None:
            bits.append(f"<span class='snap miss'>{name}: 데이터 부족</span>")
        else:
            bits.append(f"<span class='snap'>{name}: {pd.Timestamp(d).strftime('%Y-%m-%d')}</span>")
    return " · ".join(bits)


def build_snapshot_wide(
    etf_df: pd.DataFrame,
    snaps: dict[str, pd.Timestamp | None],
    top_n: int | None = TOP_N,
) -> pd.DataFrame:
    """
    행=구성종목, 열=스냅샷 비중 + 연속 비교 Δ.
    특정 시점에 없으면 비중 0으로 보고 편입/이탈 변화를 계산합니다.
    """
    latest = snaps.get("DB최근")
    if latest is None:
        return pd.DataFrame()

    frames = []
    for name, d in snaps.items():
        if d is None:
            continue
        sub = etf_df[etf_df["수집일자"] == d][["티커", "구성종목명", "비중"]].copy()
        sub = sub.rename(columns={"비중": name})
        frames.append(sub)

    if not frames:
        return pd.DataFrame()

    wide = frames[0]
    for f in frames[1:]:
        wide = pd.merge(wide, f, on=["티커", "구성종목명"], how="outer")

    for name, _ in SNAPSHOT_DEFS:
        if name not in wide.columns:
            wide[name] = np.nan
        wide[name] = pd.to_numeric(wide[name], errors="coerce")

    # 원본 결측 여부 (표시용: 실제 미편입 vs 0)
    for name, _ in SNAPSHOT_DEFS:
        wide[f"_miss_{name}"] = wide[name].isna()

    # 비교용: 결측=0 (이탈→비중축소, 신규→비중확대)
    for cur, prev in CHAIN_COMPARE:
        # 스냅샷 자체가 없으면(날짜 데이터 부족) 해당 비교는 NaN 유지
        if snaps.get(cur) is None or snaps.get(prev) is None:
            wide[f"chainΔ_{cur}"] = np.nan
        else:
            wide[f"chainΔ_{cur}"] = wide[cur].fillna(0.0) - wide[prev].fillna(0.0)

    for name, _ in SNAPSHOT_DEFS:
        if name == "DB최근":
            continue
        if snaps.get(name) is None:
            wide[f"vs최근_{name}"] = np.nan
        else:
            wide[f"vs최근_{name}"] = wide["DB최근"].fillna(0.0) - wide[name].fillna(0.0)

    if top_n is not None:
        latest_rank = wide.sort_values("DB최근", ascending=False, na_position="last")
        pick = set(latest_rank.head(top_n)["티커"].tolist())
        # 이탈·큰 변화 종목도 포함
        for col in ("vs최근_3일전", "vs최근_일주일전", "vs최근_2주일전", "chainΔ_DB최근"):
            if col not in wide.columns:
                continue
            tmp = wide.dropna(subset=[col]).copy()
            if tmp.empty:
                continue
            tmp["_abs"] = tmp[col].abs()
            pick.update(tmp.sort_values("_abs", ascending=False).head(max(8, top_n // 2))["티커"].tolist())
        # 당일 없고 과거에만 있는 종목(이탈)은 반드시 후보에
        exited = wide[wide["_miss_DB최근"] & wide[[n for n, _ in SNAPSHOT_DEFS if n != "DB최근"]].notna().any(axis=1)]
        pick.update(exited["티커"].tolist())
        wide = wide[wide["티커"].isin(pick)].copy()

    wide["라벨"] = wide["티커"].astype(str) + " " + wide["구성종목명"].astype(str)
    # 정렬: 당일 비중 높은 순, 이탈(당일 없음)은 변화 절대값 큰 순으로 뒤에
    wide["_sort_w"] = wide["DB최근"].fillna(-1e9)
    wide["_sort_exit"] = wide["_miss_DB최근"].astype(int)
    if "vs최근_3일전" in wide.columns:
        wide["_sort_chg"] = wide["vs최근_3일전"].fillna(0).abs()
    else:
        wide["_sort_chg"] = 0.0
    wide = wide.sort_values(
        ["_sort_exit", "_sort_w", "_sort_chg"],
        ascending=[True, False, False],
    ).reset_index(drop=True)
    return wide


# ─────────────────────────────────────────────────────────────
# 그라데이션 / 셀 렌더
# ─────────────────────────────────────────────────────────────
def _delta_bg(delta: float | None, max_pp: float = GRAD_MAX_PP) -> str:
    """비중확대=초록, 비중축소=빨강, |Δ|에 비례한 배경."""
    if delta is None or (isinstance(delta, float) and np.isnan(delta)):
        return ""
    mag = min(abs(float(delta)) / max_pp, 1.0)
    alpha = 0.12 + 0.48 * mag
    if delta > 0:
        return f"background: rgba(34, 140, 80, {alpha:.3f});"
    if delta < 0:
        return f"background: rgba(200, 45, 45, {alpha:.3f});"
    return "background: rgba(120,120,120,0.08);"


def _fmt_weight_cell(weight, delta, missing: bool = False) -> tuple[str, str]:
    """
    Returns (inner_html, style).
    이탈(당일 미편입): 0.000 (−과거비중) + '이탈' 표시.
    """
    if missing or weight is None or (isinstance(weight, float) and np.isnan(weight)):
        w_show = 0.0
        tag = ' <span class="tag-exit">이탈</span>' if missing else ""
        if delta is None or (isinstance(delta, float) and np.isnan(delta)):
            return f"0.000{tag}", _delta_bg(None) if not missing else _delta_bg(-1e-9)
        d = float(delta)
        sign = "+" if d >= 0 else ""
        cls = "up" if d > 0 else ("down" if d < 0 else "flat")
        inner = f'0.000 <span class="dlt {cls}">({sign}{d:.3f})</span>{tag}'
        return inner, _delta_bg(d)

    wtxt = f"{float(weight):.3f}"
    if delta is None or (isinstance(delta, float) and np.isnan(delta)):
        return wtxt, ""
    d = float(delta)
    sign = "+" if d >= 0 else ""
    cls = "up" if d > 0 else ("down" if d < 0 else "flat")
    inner = f'{wtxt} <span class="dlt {cls}">({sign}{d:.3f})</span>'
    return inner, _delta_bg(d)


def render_etf_weight_table(wide: pd.DataFrame, snaps: dict) -> str:
    if wide.empty:
        return "<p class='muted'>스냅샷 표: 데이터 없음</p>"

    ths = ["<th>순위</th>", "<th>티커</th>", "<th>구성종목명</th>"]
    for name, _ in SNAPSHOT_DEFS:
        d = snaps.get(name)
        if d is None:
            ths.append(f"<th>{html.escape(name)}<br><span class='th-sub'>데이터 부족</span></th>")
        else:
            ths.append(
                f"<th>{html.escape(name)}<br><span class='th-sub'>"
                f"{pd.Timestamp(d).strftime('%Y-%m-%d')}</span></th>"
            )
    thead = "<tr>" + "".join(ths) + "</tr>"

    chain_delta_for = {cur: f"chainΔ_{cur}" for cur, _prev in CHAIN_COMPARE}

    rows = []
    for i, r in wide.iterrows():
        rank = i + 1
        tds = [
            f"<td class='num'>{rank}</td>",
            f"<td class='code'>{html.escape(str(r['티커']))}</td>",
            f"<td>{html.escape(str(r['구성종목명']))}</td>",
        ]
        for name, _ in SNAPSHOT_DEFS:
            w = r.get(name, np.nan)
            # 스냅샷 날짜 자체가 없으면 비교/표시 불가
            if snaps.get(name) is None:
                tds.append("<td class='wcell'>—</td>")
                continue
            miss = bool(r.get(f"_miss_{name}", False))
            dcol = chain_delta_for.get(name)
            delta = r.get(dcol, np.nan) if dcol else np.nan
            if name == "2주일전":
                delta = np.nan
            # '이탈' 표시는 당일(DB최근)에만 — 과거에만 있던 종목
            inner, style = _fmt_weight_cell(w, delta, missing=(miss and name == "DB최근"))
            style_attr = f' style="{style}"' if style else ""
            tds.append(f"<td class='wcell'{style_attr}>{inner}</td>")
        rows.append("<tr>" + "".join(tds) + "</tr>")

    return (
        "<table class='tbl weight-tbl'><thead>"
        + thead
        + "</thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
        + "<p class='hint'>셀 색: 직전 시점 대비 비중확대=초록 / 비중축소=빨강 (농도=|변화|). "
        "괄호=직전 대비 pp 변화. "
        "과거에만 있고 당일에 없으면 <strong>이탈</strong>(당일 비중 0)으로 표시·집계합니다. "
        "DB최근←3일전, 3일전←일주일전, 일주일전←2주일전.</p>"
    )


# ─────────────────────────────────────────────────────────────
# 전체 요약 순위
# ─────────────────────────────────────────────────────────────
def build_universe_snapshot_frame(df: pd.DataFrame, snaps: dict) -> pd.DataFrame:
    """모든 ETF 구성종목을 이어붙인 wide (요약용, top_n 제한 없음)."""
    parts = []
    for code, g in df.groupby("ETF코드", sort=True):
        etf_dates = set(g["수집일자"].tolist())
        etf_snaps = dict(snaps)
        etf_snaps["DB최근"] = g["수집일자"].max()
        for name, _ in SNAPSHOT_DEFS:
            if name == "DB최근":
                continue
            common_d = snaps.get(name)
            etf_snaps[name] = common_d if (common_d is not None and common_d in etf_dates) else None
        wide = build_snapshot_wide(g, etf_snaps, top_n=None)
        if wide.empty:
            continue
        wide = wide.copy()
        wide.insert(0, "ETF코드", str(code))
        wide.insert(1, "ETF명", _etf_display_name(g, str(code)))
        parts.append(wide)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _rank_table_html(
    frame: pd.DataFrame,
    value_col: str,
    title: str,
    top_n: int = SUMMARY_TOP_N,
    as_delta: bool = False,
    direction: str | None = None,
) -> str:
    """
    direction: None=값 내림차순(비중),
               'up'=비중확대(양수) 큰 순, 'down'=비중축소(음수) 절대값 큰 순
    """
    if frame.empty or value_col not in frame.columns:
        return f"<div class='rank-card'><h3>{html.escape(title)}</h3><p class='muted'>데이터 없음</p></div>"

    sub = frame.dropna(subset=[value_col]).copy()
    if sub.empty:
        return f"<div class='rank-card'><h3>{html.escape(title)}</h3><p class='muted'>데이터 없음</p></div>"

    if as_delta:
        if direction == "up":
            sub = sub[sub[value_col] > 0].copy()
            sub = sub.sort_values(value_col, ascending=False).head(top_n)
        elif direction == "down":
            sub = sub[sub[value_col] < 0].copy()
            sub = sub.sort_values(value_col, ascending=True).head(top_n)  # 더 음수가 상위
        else:
            sub["_key"] = sub[value_col].abs()
            sub = sub.sort_values("_key", ascending=False).head(top_n)
    else:
        sub = sub.sort_values(value_col, ascending=False).head(top_n)

    if sub.empty:
        return f"<div class='rank-card'><h3>{html.escape(title)}</h3><p class='muted'>해당 방향 데이터 없음</p></div>"

    rows = []
    for i, (_, r) in enumerate(sub.iterrows(), start=1):
        val = r[value_col]
        if as_delta:
            sign = "+" if val >= 0 else ""
            exit_tag = ""
            if bool(r.get("_miss_DB최근", False)):
                exit_tag = ' <span class="tag-exit">이탈</span>'
            vhtml = f'<span class="{"up" if val > 0 else "down"}">{sign}{val:.3f}</span>{exit_tag}'
            style = _delta_bg(val)
        else:
            vhtml = f"{val:.3f}"
            style = ""
        style_attr = f' style="{style}"' if style else ""
        rows.append(
            "<tr>"
            f"<td class='num'>{i}</td>"
            f"<td class='etf'>{html.escape(str(r.get('ETF명', r.get('ETF코드', ''))))}</td>"
            f"<td class='code'>{html.escape(str(r['티커']))}</td>"
            f"<td>{html.escape(str(r['구성종목명']))}</td>"
            f"<td class='wcell'{style_attr}>{vhtml}</td>"
            "</tr>"
        )

    card_cls = "rank-card"
    if direction == "up":
        card_cls += " rank-up"
    elif direction == "down":
        card_cls += " rank-down"

    return f"""
<div class="{card_cls}">
  <h3>{html.escape(title)}</h3>
  <table class="tbl">
    <thead><tr><th>#</th><th>ETF</th><th>티커</th><th>구성종목</th><th>값</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</div>
"""


def render_summary_section(universe: pd.DataFrame) -> str:
    if universe.empty:
        return "<p class='muted'>전체 요약: 데이터 없음</p>"

    # 삼성전자·하이닉스 제외 유니버스
    excl = universe.copy()
    ticker_ok = ~excl["티커"].astype(str).isin(EXCLUDE_FROM_WEIGHT_RANK)
    name_s = excl["구성종목명"].astype(str)
    name_ok = pd.Series(True, index=excl.index)
    for kw in EXCLUDE_NAME_KEYWORDS:
        name_ok &= ~name_s.str.contains(kw, na=False)
    universe_ex = excl[ticker_ok & name_ok].copy()

    cards = [
        _rank_table_html(universe, "DB최근", f"비중 상위 (DB최근) TOP{SUMMARY_TOP_N}", as_delta=False),
        _rank_table_html(
            universe_ex,
            "DB최근",
            f"비중 상위 · 삼성전자·하이닉스 제외 TOP{SUMMARY_TOP_N}",
            as_delta=False,
        ),
    ]
    for col, label in (
        ("vs최근_3일전", "3일전 대비"),
        ("vs최근_일주일전", "일주일전 대비"),
        ("vs최근_2주일전", "2주일전 대비"),
    ):
        cards.append(
            _rank_table_html(
                universe, col, f"{label} · 비중확대 TOP{SUMMARY_TOP_N}", as_delta=True, direction="up"
            )
        )
        cards.append(
            _rank_table_html(
                universe, col, f"{label} · 비중축소 TOP{SUMMARY_TOP_N}", as_delta=True, direction="down"
            )
        )

    return (
        "<section class='summary-block' id='summary'>"
        "<h2>전체 요약 순위</h2>"
        "<p class='meta'>모든 ETF 구성종목을 합쳐 순위를 매깁니다. "
        "변화는 <strong>비중확대</strong>·<strong>비중축소</strong>를 각각 따로 집계합니다 "
        "(값 = DB최근 − 과거, pp; 미편입은 0으로 처리 → 이탈은 비중축소, 신규는 비중확대). "
        "비중 상위(제외)는 삼성전자·SK하이닉스를 빼고 집계합니다.</p>"
        "<div class='rank-grid'>"
        + "".join(cards)
        + "</div></section>"
    )


# ─────────────────────────────────────────────────────────────
# HTML 대시보드
# ─────────────────────────────────────────────────────────────
def _etf_snaps_for_group(g: pd.DataFrame, snaps: dict) -> dict:
    etf_dates = set(g["수집일자"].tolist())
    etf_snaps = dict(snaps)
    etf_snaps["DB최근"] = g["수집일자"].max()
    for name, _ in SNAPSHOT_DEFS:
        if name == "DB최근":
            continue
        common_d = snaps.get(name)
        etf_snaps[name] = common_d if (common_d is not None and common_d in etf_dates) else None
    return etf_snaps


def build_dashboard_html(df: pd.DataFrame) -> str:
    if df.empty:
        return "<html><body><h1>데이터 없음</h1></body></html>"

    all_dates = sorted(df["수집일자"].unique())
    snaps = resolve_snapshot_dates(all_dates)
    latest = snaps.get("DB최근")
    latest_str = pd.Timestamp(latest).strftime("%Y-%m-%d") if latest is not None else "-"

    snap_info = []
    for name, offset in SNAPSHOT_DEFS:
        d = snaps.get(name)
        if d is None:
            snap_info.append(f"{name}(T-{offset}): 없음")
        else:
            snap_info.append(f"{name}(T-{offset}): {pd.Timestamp(d).strftime('%Y-%m-%d')}")
    print("· 스냅샷:", " | ".join(snap_info))

    universe = build_universe_snapshot_frame(df, snaps)
    summary_html = render_summary_section(universe)

    sections: list[str] = []
    nav_links: list[str] = ['<a href="#summary">전체 요약</a>']

    for code, g in df.groupby("ETF코드", sort=True):
        title = _etf_display_name(g, str(code))
        etf_snaps = _etf_snaps_for_group(g, snaps)
        wide = build_snapshot_wide(g, etf_snaps, TOP_N)
        nav_links.append(f'<a href="#etf-{code}">{html.escape(title)}</a>')
        sections.append(f"""
<section class="etf-block" id="etf-{code}">
  <h2>{html.escape(title)}</h2>
  <p class="meta">{snapshot_meta_html(etf_snaps)}</p>
  <h3>스냅샷 비중 (연속 시점 대비 변화)</h3>
  {render_etf_weight_table(wide, etf_snaps)}
</section>
""")

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>ETF PDF 구성비중 스냅샷</title>
  <style>
    :root {{
      --bg: #f4f6f8; --card: #fff; --text: #1a1d21; --muted: #5c6570;
      --line: #e2e6ea; --accent: #0b6e4f;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: "Pretendard","Malgun Gothic",sans-serif;
      background: linear-gradient(165deg,#eef3f0 0%,var(--bg) 45%,#e8eef5 100%);
      color: var(--text); line-height: 1.45;
    }}
    header {{
      padding: 24px 28px 12px; border-bottom: 1px solid var(--line);
      background: rgba(255,255,255,.82); backdrop-filter: blur(8px);
      position: sticky; top: 0; z-index: 10;
    }}
    h1 {{ margin: 0 0 6px; font-size: 1.5rem; letter-spacing: -.02em; }}
    h2 {{ margin: 0 0 4px; font-size: 1.15rem; }}
    h3 {{ margin: 10px 0 8px; font-size: .95rem; color: var(--muted); }}
    .sub {{ color: var(--muted); font-size: .92rem; margin: 0; }}
    .nav {{ display:flex; flex-wrap:wrap; gap:8px 12px; margin-top:12px; max-height:110px; overflow:auto; }}
    .nav a {{
      text-decoration:none; color:var(--accent); font-size:.8rem;
      background:#e7f3ee; padding:4px 10px; border-radius:4px;
    }}
    .nav a:hover {{ background:#d3ebe2; }}
    main {{ padding: 18px 28px 48px; max-width: 1400px; margin: 0 auto; }}
    .summary-block, .etf-block {{
      background: var(--card); border: 1px solid var(--line);
      border-radius: 10px; padding: 16px; margin: 20px 0;
      box-shadow: 0 8px 24px rgba(20,35,50,.05);
    }}
    .rank-grid {{
      display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px;
    }}
    @media (max-width: 980px) {{ .rank-grid {{ grid-template-columns: 1fr; }} }}
    .rank-card {{
      border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px;
      background: #fafbfc;
    }}
    .rank-card.rank-up {{ border-color: #b7e0c5; background: #f4fbf6; }}
    .rank-card.rank-down {{ border-color: #ecc0c0; background: #fdf6f6; }}
    .rank-card h3 {{ margin: 0 0 8px; font-size: .88rem; color: var(--text); }}
    .meta {{ color: var(--muted); font-size: .84rem; margin: 0 0 10px; }}
    .hint {{ color: var(--muted); font-size: .78rem; margin: 6px 0 0; }}
    .snap {{ display:inline-block; margin-right: 6px; }}
    .snap.miss {{ color: #b00020; }}
    .muted {{ color: var(--muted); }}
    table.tbl {{ width:100%; border-collapse:collapse; font-size:.82rem; margin-bottom:8px; }}
    table.tbl th, table.tbl td {{ border-bottom:1px solid var(--line); padding:6px 8px; text-align:left; }}
    table.tbl th {{ background:#f3f5f7; font-weight:600; vertical-align:bottom; }}
    .th-sub {{ font-weight:500; color:var(--muted); font-size:.75rem; }}
    td.num, td.code {{ font-variant-numeric: tabular-nums; white-space: nowrap; }}
    td.etf {{ font-size: .78rem; max-width: 180px; }}
    td.wcell {{ font-variant-numeric: tabular-nums; white-space: nowrap; text-align: right; }}
    .dlt {{ font-size: .78em; margin-left: 2px; }}
    .dlt.up {{ color: #157a3c; }}
    .dlt.down {{ color: #b00020; }}
    .dlt.flat {{ color: #666; }}
    .tag-exit {{
      font-size: .72em; font-weight: 600; color: #b00020;
      margin-left: 4px; padding: 1px 5px; border-radius: 3px;
      background: rgba(200,45,45,.12);
    }}
    .up {{ color: #157a3c; font-weight: 600; }}
    .down {{ color: #b00020; font-weight: 600; }}
  </style>
</head>
<body>
  <header>
    <h1>ETF PDF 구성비중 스냅샷</h1>
    <p class="sub">기준(DB최근) {latest_str} · ETF {df['ETF코드'].nunique()}개
       · 비교: DB최근 / 3일전(T-3) / 일주일전(T-5) / 2주일전(T-10)
       · <code>{PDF_TABLE}</code></p>
    <p class="sub">{snapshot_meta_html(snaps)}</p>
    <nav class="nav">{''.join(nav_links)}</nav>
  </header>
  <main>
    {summary_html}
    {''.join(sections)}
  </main>
</body>
</html>
"""


def main():
    print("ETF PDF 비중 스냅샷 표 생성...")
    df = load_pdf_from_db(START_DATE, END_DATE, ETF_FILTER)
    if df.empty:
        print("⚠️ krx_etf_pdf에 데이터가 없습니다. 32. ETF_PDF_v2.0.py 로 먼저 수집하세요.")
        return

    print(
        f"· 로드: {len(df)}행 / ETF {df['ETF코드'].nunique()}개 / "
        f"일자 {df['수집일자'].nunique()}개 "
        f"({df['수집일자'].min().date()} ~ {df['수집일자'].max().date()})"
    )

    html_out = build_dashboard_html(df)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    latest = df["수집일자"].max().strftime("%Y%m%d")
    out_path = os.path.join(RESULTS_DIR, f"etf_pdf_weight_dashboard_{latest}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_out)
    print(f"· 저장: {out_path}")

    if OPEN_BROWSER:
        webbrowser.open("file:///" + out_path.replace("\\", "/"))


if __name__ == "__main__":
    main()
