"""표 PNG + 본문 md/html 렌더 (HTML → Playwright Chromium 스크린샷)."""
from __future__ import annotations

import html
import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import OUTPUTS_DIR
from content_market import energy_ratio_font_color

log = logging.getLogger("naverPub.render")

_TEXT_COLS = {"종목명", "달성구간", "티커"}
_DEFAULT_INT_COLS = {"순위", "거래대금순위", "현재가"}
_NARROW_COLS = {"순위", "티커"}
MCAP_COL = "시총(조원)"
TALENT_UD_COLS = {"20일 내 상승/하락", "50일 내 상승/하락", "120일 내 상승/하락"}
_DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")

CHG_UP = "#d32f2f"
CHG_DOWN = "#1565c0"
CHG_ZERO = "#212121"
GRAD_LIGHT = "#ffffff"
GRAD_DARK = "#ffd8a8"
HEADER_BG = "#1f3864"

_TABLE_CSS = f"""
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  padding: 8px;
  background: #ffffff;
  font-family: 'Malgun Gothic', 'NanumGothic', 'Nanum Gothic', sans-serif;
  color: #212121;
}}
#capture {{
  display: inline-block;
  max-width: 960px;
  background: #ffffff;
}}
.title {{
  font-size: 16px;
  font-weight: bold;
  margin: 0 0 8px 0;
  padding: 0;
  line-height: 1.3;
}}
.empty {{
  font-size: 13px;
  padding: 16px 10px;
  border-top: 2px solid {HEADER_BG};
  border-bottom: 2px solid {HEADER_BG};
  margin: 0;
}}
table.snap {{
  border-collapse: collapse;
  border-spacing: 0;
  width: auto;
  max-width: 960px;
  font-size: 13px;
  border-top: 2px solid {HEADER_BG};
  border-bottom: 2px solid {HEADER_BG};
}}
table.snap th,
table.snap td {{
  padding: 6px 10px;
  border: none;
  border-bottom: 1px solid #e3e8ef;
  vertical-align: middle;
  white-space: nowrap;
}}
table.snap thead th {{
  background: {HEADER_BG};
  color: #ffffff;
  font-weight: bold;
  font-size: 13px;
  padding: 8px 10px;
  border-bottom: none;
  text-align: center;
}}
table.snap tbody tr:nth-child(odd) td {{ background-color: #ffffff; }}
table.snap tbody tr:nth-child(even) td {{ background-color: #f7f9fc; }}
table.snap tbody tr:last-child td {{ border-bottom: none; }}
td.num, th.num {{ text-align: right; }}
td.txt, th.txt {{ text-align: left; }}
td.ctr, th.ctr {{ text-align: center; }}
td.narrow, th.narrow {{ width: 1%; white-space: nowrap; }}
td.name {{ white-space: normal; max-width: 240px; }}
span.name-fit {{ white-space: nowrap; line-height: 1.2; }}
span.name-fit.fs12 {{ font-size: 12px; }}
span.name-fit.fs11 {{ font-size: 11px; }}
span.name-fit.fs10 {{ font-size: 10px; }}
span.name-fit.fs9 {{ font-size: 9px; }}
span.tag-in {{
  font-size: 10px;
  color: #d32f2f;
  font-weight: bold;
  margin-left: 5px;
  vertical-align: middle;
}}
span.tag-out {{
  font-size: 10px;
  color: #1565c0;
  font-weight: bold;
  margin-left: 5px;
  vertical-align: middle;
}}
span.ud-up {{ color: {CHG_UP}; font-size: 12px; }}
span.ud-dn {{ color: {CHG_DOWN}; font-size: 12px; }}
span.ud-sep {{ color: #9e9e9e; font-size: 12px; margin: 0 1px; }}
span.ud-em {{ font-size: 14px; font-weight: 700; }}
.footnote {{
  font-size: 11px;
  color: #616161;
  margin: 6px 0 0 0;
  line-height: 1.4;
}}
table.snap thead th.chg-group {{
  border-bottom: 1px solid rgba(255,255,255,0.35);
}}
table.snap thead th.sub {{
  font-size: 12px;
  font-weight: bold;
  padding: 6px 8px;
}}
"""


def _chg_font_color(v) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return CHG_ZERO
    if not np.isfinite(x) or x == 0:
        return CHG_ZERO
    if x > 0:
        return CHG_UP
    return CHG_DOWN


def _hex_to_rgb(h: str) -> tuple[float, float, float]:
    h = h.lstrip("#")
    return tuple(int(h[i : i + 2], 16) / 255.0 for i in (0, 2, 4))


def _rgb_to_hex(rgb: tuple[float, float, float]) -> str:
    return "#{:02x}{:02x}{:02x}".format(
        int(round(rgb[0] * 255)),
        int(round(rgb[1] * 255)),
        int(round(rgb[2] * 255)),
    )


def _lerp_hex(c0: str, c1: str, t: float) -> str:
    t = float(np.clip(t, 0.0, 1.0))
    a = np.array(_hex_to_rgb(c0))
    b = np.array(_hex_to_rgb(c1))
    return _rgb_to_hex(tuple(a + (b - a) * t))


def _gradient_color(v, vmin: float, vmax: float, light: str = GRAD_LIGHT, dark: str = GRAD_DARK) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return light
    if not np.isfinite(x) or not np.isfinite(vmin) or not np.isfinite(vmax) or vmax <= vmin:
        return light
    return _lerp_hex(light, dark, (x - vmin) / (vmax - vmin))


def _fmt_cell(v, *, kind: str = "auto", digits: int = 2) -> str:
    try:
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            return "-"
        if isinstance(v, str) and v.strip() in ("", "-"):
            return "-"
        if pd.isna(v):
            return "-"
    except (TypeError, ValueError):
        pass
    if isinstance(v, str) and v == "-":
        return "-"
    try:
        if kind == "int":
            return f"{int(round(float(v))):,}"
        if kind == "float":
            return f"{float(v):,.{digits}f}"
        if isinstance(v, (int, np.integer)):
            return f"{int(v):,}"
        if isinstance(v, (float, np.floating)):
            return f"{float(v):,.{digits}f}"
        return str(v)
    except (TypeError, ValueError):
        return str(v) if v is not None else "-"


def _col_align_class(name: str) -> str:
    if name == "달성구간" or name in TALENT_UD_COLS:
        return "ctr"
    if name in _TEXT_COLS:
        return "txt"
    return "num"


def _col_extra_class(name: str) -> str:
    parts = []
    if name in _NARROW_COLS:
        parts.append("narrow")
    if name == "종목명":
        parts.append("name")
    return " ".join(parts)


def _name_font_class(name: str) -> str:
    """긴 종목명만 폰트 축소 클래스 반환 (짧으면 빈 문자열 = 기본 크기)."""
    n = len(str(name).strip())
    if n <= 10:
        return ""
    if n <= 12:
        return "fs12"
    if n <= 14:
        return "fs11"
    if n <= 17:
        return "fs10"
    return "fs9"


def _talent_ud_html(v) -> str:
    """'5/2' → 상승(빨강) / 하락(파랑), 많은 쪽 폰트 강조."""
    s = "" if v is None or (isinstance(v, float) and not np.isfinite(v)) else str(v).strip()
    m = re.match(r"^(\d+)\s*/\s*(\d+)$", s)
    if not m:
        return html.escape(s or "-")
    up, dn = int(m.group(1)), int(m.group(2))
    up_cls = "ud-up" + (" ud-em" if up > dn else "")
    dn_cls = "ud-dn" + (" ud-em" if dn > up else "")
    return (
        f'<span class="{up_cls}">{up}</span>'
        f'<span class="ud-sep">/</span>'
        f'<span class="{dn_cls}">{dn}</span>'
    )


def _build_table_document(
    title: str,
    raw: Optional[pd.DataFrame],
    display: Optional[pd.DataFrame],
    *,
    chg_font_cols: set[str],
    gradient_cols: dict[str, tuple[str, str]],
    energy_font_cols: set[str],
    grad_range: dict[str, tuple[float, float]],
) -> str:
    title_esc = html.escape(title)
    if raw is None or display is None or raw.empty:
        body = f'<div id="capture"><div class="title">{title_esc}</div><p class="empty">데이터 없음</p></div>'
    else:
        ths = []
        for c in display.columns:
            cls = f"{_col_align_class(c)} {_col_extra_class(c)}".strip()
            ths.append(f'<th class="{cls}">{html.escape(str(c))}</th>')
        rows_html = []
        for rr in range(len(display)):
            tds = []
            for c in display.columns:
                col = str(c)
                cls = f"{_col_align_class(col)} {_col_extra_class(col)}".strip()
                style_parts = []
                raw_v = raw.iloc[rr][col]
                if col in gradient_cols and col in grad_range:
                    light, dark = gradient_cols[col]
                    vmin, vmax = grad_range[col]
                    bg = _gradient_color(raw_v, vmin, vmax, light, dark)
                    style_parts.append(f"background-color:{bg}")
                color = None
                if col in chg_font_cols:
                    color = _chg_font_color(raw_v)
                if col in energy_font_cols:
                    try:
                        color = energy_ratio_font_color(float(raw_v))
                    except (TypeError, ValueError):
                        pass
                if color:
                    style_parts.append(f"color:{color}")
                style = f' style="{";".join(style_parts)}"' if style_parts else ""
                if col in TALENT_UD_COLS:
                    cell = _talent_ud_html(raw_v)
                else:
                    cell = html.escape(str(display.iloc[rr][col]))
                tds.append(f'<td class="{cls}"{style}>{cell}</td>')
            rows_html.append("<tr>" + "".join(tds) + "</tr>")
        body = (
            f'<div id="capture"><div class="title">{title_esc}</div>'
            f'<table class="snap"><thead><tr>{"".join(ths)}</tr></thead>'
            f'<tbody>{"".join(rows_html)}</tbody></table></div>'
        )
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<style>{_TABLE_CSS}</style></head><body>{body}</body></html>"
    )


def _build_etf_table_document(
    title: str,
    raw: pd.DataFrame,
    display: pd.DataFrame,
    *,
    col_dates: list[str],
    chg_cols: list[str],
    gradient_col: Optional[str],
    grad_range: dict[str, tuple[float, float]],
    footnotes: Optional[list[str]] = None,
) -> str:
    """2단 헤더(비중변화 병합) + 편입/편출 소형 태그."""
    from content_etf import CHG_PREV, CHG_1W, CHG_2W, RET_10D

    title_esc = html.escape(title)
    if raw is None or display is None or raw.empty:
        return (
            "<!DOCTYPE html><html><head><meta charset='utf-8'>"
            f"<style>{_TABLE_CSS}</style></head><body>"
            f'<div id="capture"><div class="title">{title_esc}</div>'
            f'<p class="empty">데이터 없음</p></div></body></html>'
        )

    # thead row1: rowspan=2 for fixed cols + date cols, colspan=3 for 비중변화
    fixed_pre = ["순위", "종목명", "티커"]
    fixed_post = [RET_10D]
    chg_cols = chg_cols or [CHG_PREV, CHG_1W, CHG_2W]

    r1 = []
    for c in fixed_pre + col_dates:
        cls = f"{_col_align_class(c)} {_col_extra_class(c)}".strip()
        r1.append(f'<th class="{cls}" rowspan="2">{html.escape(str(c))}</th>')
    r1.append(f'<th class="ctr chg-group" colspan="{len(chg_cols)}">비중변화</th>')
    for c in fixed_post:
        cls = f"{_col_align_class(c)} {_col_extra_class(c)}".strip()
        r1.append(f'<th class="{cls}" rowspan="2">{html.escape(str(c))}</th>')

    r2 = []
    for c in chg_cols:
        r2.append(f'<th class="num sub">{html.escape(str(c))}</th>')

    order = fixed_pre + col_dates + chg_cols + fixed_post
    chg_set = set(chg_cols) | {RET_10D}
    rows_html = []
    has_status = "_status" in raw.columns
    for rr in range(len(display)):
        tds = []
        for col in order:
            if col not in display.columns:
                continue
            cls = f"{_col_align_class(col)} {_col_extra_class(col)}".strip()
            style_parts = []
            raw_v = raw.iloc[rr][col] if col in raw.columns else None
            if col in grad_range:
                vmin, vmax = grad_range[col]
                bg = _gradient_color(raw_v, vmin, vmax, GRAD_LIGHT, GRAD_DARK)
                style_parts.append(f"background-color:{bg}")
            if col in chg_set:
                style_parts.append(f"color:{_chg_font_color(raw_v)}")
            style = f' style="{";".join(style_parts)}"' if style_parts else ""

            if col == "종목명":
                name_raw = str(display.iloc[rr][col])
                name = html.escape(name_raw)
                fs = _name_font_class(name_raw)
                fit_cls = f"name-fit {fs}".strip() if fs else "name-fit"
                tag = ""
                if has_status:
                    st = raw.iloc[rr].get("_status") or ""
                    if st == "편입":
                        tag = ' <span class="tag-in">편입</span>'
                    elif st == "편출":
                        tag = ' <span class="tag-out">편출</span>'
                # 긴 이름만 폰트 축소 — 표 구조/컬럼 폭은 변경하지 않음
                tds.append(
                    f'<td class="{cls}"{style}>'
                    f'<span class="{fit_cls}">{name}</span>{tag}</td>'
                )
            else:
                tds.append(
                    f'<td class="{cls}"{style}>{html.escape(str(display.iloc[rr][col]))}</td>'
                )
        rows_html.append("<tr>" + "".join(tds) + "</tr>")

    fn_html = ""
    if footnotes:
        items = "".join(f"<div>· {html.escape(f)}</div>" for f in footnotes)
        fn_html = f'<div class="footnote">{items}</div>'

    body = (
        f'<div id="capture"><div class="title">{title_esc}</div>'
        f'<table class="snap"><thead><tr>{"".join(r1)}</tr><tr>{"".join(r2)}</tr></thead>'
        f'<tbody>{"".join(rows_html)}</tbody></table>{fn_html}</div>'
    )
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<style>{_TABLE_CSS}</style></head><body>{body}</body></html>"
    )


def dataframe_to_png(
    df: pd.DataFrame,
    out_path: Path,
    title: str,
    *,
    page=None,
    int_cols: Optional[set[str]] = None,
    chg_font_cols: Optional[set[str]] = None,
    gradient_cols: Optional[dict[str, tuple[str, str]]] = None,
    energy_font_cols: Optional[set[str]] = None,
    float_digits: Optional[dict[str, int]] = None,
    col_widths: Optional[list[float]] = None,
) -> Path:
    """
    HTML 표 → Playwright 요소 스크린샷(PNG).
    page가 있으면 재사용, 없으면 단발 브라우저 기동.
    """
    _ = col_widths  # HTML 자동 레이아웃 사용
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    int_cols = set(int_cols or set()) | (_DEFAULT_INT_COLS & set(df.columns if df is not None else []))
    chg_font_cols = set(chg_font_cols or set())
    # 스펙: 은은한 주황 그라데이션으로 통일
    raw_grad = gradient_cols or {}
    gradient_cols = {k: (GRAD_LIGHT, GRAD_DARK) for k in raw_grad}
    energy_font_cols = set(energy_font_cols or set())
    float_digits = dict(float_digits or {})

    raw = None
    display = None
    grad_range: dict[str, tuple[float, float]] = {}

    if df is not None and not df.empty:
        raw = df.copy()
        if "현재가" in raw.columns:
            int_cols.add("현재가")
        display = pd.DataFrame(index=raw.index)
        for c in raw.columns:
            digits = float_digits.get(c, 2)
            if c in int_cols:
                kind = "int"
            elif pd.api.types.is_numeric_dtype(raw[c]) or c in (
                MCAP_COL,
                "시가총액",
                "1주대비",
                "2주대비",
                "전일대비",
                "전일 대비",
                "1주 전 대비",
                "2주 전 대비",
                "talent 지수",
            ):
                kind = "float"
            else:
                kind = "auto"
            display[c] = [_fmt_cell(v, kind=kind, digits=digits) for v in raw[c].tolist()]
        for c in gradient_cols:
            if c not in raw.columns:
                continue
            s = pd.to_numeric(raw[c], errors="coerce")
            if s.notna().any():
                grad_range[c] = (float(s.min()), float(s.max()))

    doc = _build_table_document(
        title,
        raw,
        display,
        chg_font_cols=chg_font_cols,
        gradient_cols=gradient_cols,
        energy_font_cols=energy_font_cols,
        grad_range=grad_range,
    )

    def _shot(pg) -> None:
        pg.set_content(doc, wait_until="load")
        pg.locator("#capture").screenshot(path=str(out_path), type="png")

    if page is not None:
        _shot(page)
    else:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                ctx = browser.new_context(
                    device_scale_factor=2,
                    viewport={"width": 1400, "height": 900},
                )
                pg = ctx.new_page()
                _shot(pg)
                ctx.close()
            finally:
                browser.close()
    return out_path


def dataframe_to_png_etf(
    df: pd.DataFrame,
    out_path: Path,
    title: str,
    *,
    page,
    col_cur: str,
    col_prev: str,
    col_w1: str,
    col_w2: str,
    footnotes: Optional[list[str]] = None,
) -> Path:
    """ETF PDF 전용: 2단 헤더 + 편입/편출 태그."""
    from content_etf import CHG_PREV, CHG_1W, CHG_2W, RET_10D

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if df is None or df.empty:
        doc = _build_etf_table_document(
            title, pd.DataFrame(), pd.DataFrame(),
            col_dates=[], chg_cols=[CHG_PREV, CHG_1W, CHG_2W],
            gradient_col=None, grad_range={}, footnotes=footnotes,
        )
        page.set_content(doc, wait_until="load")
        page.locator("#capture").screenshot(path=str(out_path), type="png")
        return out_path

    raw = df.copy()
    col_dates = [col_cur, col_prev, col_w1, col_w2]
    chg_cols = [CHG_PREV, CHG_1W, CHG_2W]
    show_cols = ["순위", "종목명", "티커"] + col_dates + chg_cols + [RET_10D]
    # display는 _status 제외
    display = pd.DataFrame(index=raw.index)
    int_cols = {"순위"}
    for c in show_cols:
        if c not in raw.columns:
            display[c] = "-"
            continue
        if c in int_cols:
            kind = "int"
        elif c == "종목명" or c == "티커":
            kind = "auto"
        else:
            kind = "float"
        display[c] = [_fmt_cell(v, kind=kind) for v in raw[c].tolist()]

    grad_range = {}
    if col_cur in raw.columns:
        s = pd.to_numeric(raw[col_cur], errors="coerce")
        if s.notna().any():
            grad_range[col_cur] = (float(s.min()), float(s.max()))

    doc = _build_etf_table_document(
        title,
        raw,
        display,
        col_dates=col_dates,
        chg_cols=chg_cols,
        gradient_col=col_cur,
        grad_range=grad_range,
        footnotes=footnotes,
    )
    page.set_content(doc, wait_until="load")
    page.locator("#capture").screenshot(path=str(out_path), type="png")
    return out_path

def _fmt_num(v, digits=2) -> str:
    try:
        if v is None or (isinstance(v, float) and not np.isfinite(v)):
            return "-"
        if isinstance(v, str) and v == "-":
            return "-"
        return f"{float(v):,.{digits}f}"
    except (TypeError, ValueError):
        return "-"


def narrative_energy(df: pd.DataFrame, as_of: str) -> str:
    if df is None or df.empty:
        return f"{as_of} 기준 에너지배율 상위 종목 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", top.get("티커", ""))
    er3 = _fmt_num(top.get("3일에너지배율"))
    er1 = _fmt_num(top.get("당일에너지배율"))
    return (
        f"{as_of} 거래대금 상위 종목 중 3일 에너지배율 1위는 {name}입니다. "
        f"3일 배율 {er3}, 당일 배율 {er1}로 시총 대비 자금 유입이 두드러집니다. "
        f"에너지배율은 거래대금 시장비중을 시총 시장비중으로 나눈 값입니다. "
        f"시총 단위는 조(원)입니다."
    )


def narrative_highs(df: pd.DataFrame, as_of: str) -> str:
    if df is None or df.empty:
        return f"{as_of} 기준 신고가 달성 종목이 없습니다."
    return (
        f"{as_of} 기준 최장 구간 신고가 {len(df)}종입니다. "
        f"구간대비(%)는 해당 구간 최저가 대비 상승률입니다."
    )


def narrative_lows(df: pd.DataFrame, as_of: str) -> str:
    if df is None or df.empty:
        return f"{as_of} 기준 신저가 달성 종목이 없습니다."
    return (
        f"{as_of} 기준 최장 구간 신저가 {len(df)}종입니다. "
        f"구간대비(%)는 해당 구간 최고가 대비 하락률입니다."
    )


def narrative_rs(df: pd.DataFrame, as_of: str) -> str:
    if df is None or df.empty:
        return f"{as_of} 기준 RS 순위 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", "")
    return (
        f"{as_of} RS(상대강도) 1위는 {name}입니다. "
        f"RS는 rs_20·50·120·200 백분위 평균입니다."
    )


def narrative_talent(df: pd.DataFrame, as_of: str) -> str:
    if df is None or df.empty:
        return f"{as_of} 기준 Talent 순위 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", "")
    idx = _fmt_num(top.get("talent 지수"), digits=3)
    return (
        f"{as_of} Talent 지수 1위는 {name}(지수 {idx})입니다. "
        f"시총 5,000억원 이상 종목 대상이며, "
        f"지수 = (20일상승일수/20)×0.5 + (50일/50)×0.3 + (120일/120)×0.2 "
        f"(상승일 = 일간 등락률 +10% 이상)입니다."
    )


def narrative_etf_pdf(item: dict, as_of: str) -> str:
    df = item.get("df")
    name = item.get("etf_name", "")
    if df is None or df.empty:
        return f"{as_of} {name} PDF 구성 데이터가 없습니다."
    top = df.iloc[0]
    col_cur = item.get("col_cur")
    w = top.get(col_cur) if col_cur else None
    parts = [
        f"{as_of} {name} PDF 1위는 {top.get('종목명', '')}"
        f"(비중 {_fmt_num(w)}%)입니다."
    ]
    if not item.get("prev_available", True):
        parts.append("전일 데이터 없음 — 편입/편출 판정을 생략했습니다.")
    else:
        parts.append(
            f"편입 {int(item.get('n_in') or 0)}종, 편출 {int(item.get('n_out') or 0)}종"
            f"(전일 {item.get('date_prev')})."
        )
    fns = item.get("footnotes") or []
    if fns:
        parts.append("각주: " + "; ".join(fns))
    return " ".join(parts)


def _etf_sheet_df(df: pd.DataFrame) -> pd.DataFrame:
    """xlsx/csv용: _status → 상태 컬럼, 내부 컬럼 제거."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "_status" in out.columns:
        # 종목명 다음에 상태
        cols = list(out.columns)
        cols.remove("_status")
        insert_at = cols.index("종목명") + 1 if "종목명" in cols else 1
        status = out["_status"].replace({"": pd.NA, None: pd.NA})
        out = out.drop(columns=["_status"])
        cols = list(out.columns)
        out.insert(insert_at, "상태", status)
    return out


def _ymd(as_of: str) -> str:
    return as_of.replace("-", "")


def _safe_sheet_name(name: str, used: set[str]) -> str:
    base = re.sub(r"[\\/*?:\[\]]", "_", str(name))[:31] or "sheet"
    out = base
    i = 1
    while out in used:
        suffix = f"_{i}"
        out = (base[: 31 - len(suffix)] + suffix)[:31]
        i += 1
    used.add(out)
    return out


def export_tables_xlsx(path: Path, sheets: dict[str, pd.DataFrame]) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    used: set[str] = set()
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if not sheets:
            pd.DataFrame({"info": ["데이터 없음"]}).to_excel(writer, sheet_name="empty", index=False)
        else:
            for name, df in sheets.items():
                sn = _safe_sheet_name(name, used)
                (df if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sn, index=False)
    return path


def export_tables_csv(out_dir: Path, sheets: dict[str, pd.DataFrame]) -> list[Path]:
    csv_dir = Path(out_dir) / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, (name, df) in enumerate(sheets.items(), start=1):
        safe = re.sub(r"[^\w\-]+", "_", str(name), flags=re.UNICODE).strip("_")[:60] or f"sheet{i}"
        p = csv_dir / f"{i:02d}_{safe}.csv"
        (df if df is not None else pd.DataFrame()).to_csv(p, index=False, encoding="utf-8-sig")
        paths.append(p)
    return paths


def _write_body(out_dir: Path, md_parts: list[str], html_parts: list[str]) -> tuple[Path, Path]:
    html_parts = list(html_parts) + ["</body></html>"]
    md_path = out_dir / "body.md"
    html_path = out_dir / "body.html"
    md_path.write_text("\n".join(md_parts), encoding="utf-8")
    html_path.write_text("\n".join(html_parts), encoding="utf-8")
    return md_path, html_path


def _html_header(title: str) -> list[str]:
    return [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        f"<title>{html.escape(title)}</title>",
        "<style>body{font-family:'Malgun Gothic','NanumGothic',sans-serif;max-width:960px;margin:24px auto;padding:0 12px}"
        "img{max-width:100%;height:auto;border:1px solid #ddd} h2{margin-top:1.2em;margin-bottom:0.4em}"
        "p{margin:0.4em 0} .warn{color:#b71c1c;font-weight:bold}</style>",
        "</head><body>",
        f"<h1>{html.escape(title)}</h1>",
    ]


def render_daily_snapshot(
    as_of: str,
    market: dict[str, pd.DataFrame],
    out_dir: Optional[Path] = None,
) -> dict:
    """데일리 스냅샷 전용: outputs/daily_snapshot_{YYYYMMDD}/"""
    from playwright.sync_api import sync_playwright

    ymd = _ymd(as_of)
    out_dir = Path(out_dir or (OUTPUTS_DIR / f"daily_snapshot_{ymd}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("0*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    articles = []
    title = f"데일리 스냅샷 {as_of}"
    html_parts = _html_header(title)
    md_parts = [f"# {title}\n"]
    sheets: dict[str, pd.DataFrame] = {}
    soft = (GRAD_LIGHT, GRAD_DARK)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(device_scale_factor=2, viewport={"width": 1400, "height": 2400})
            page = ctx.new_page()

            energy = market.get("energy")
            png = dataframe_to_png(
                energy,
                out_dir / "01_energy.png",
                f"에너지배율 순위 ({as_of})",
                page=page,
                int_cols={"순위", "거래대금순위", "현재가"},
                chg_font_cols={"당일상승률"},
                gradient_cols={"거래대금(억)": soft, MCAP_COL: soft},
                energy_font_cols={"당일에너지배율", "3일에너지배율"},
            )
            text = narrative_energy(energy, as_of)
            md_parts.append(f"## 에너지배율 순위\n\n{text}\n\n![에너지배율]({png.name})\n")
            html_parts.append(f"<h2>에너지배율 순위</h2><p>{html.escape(text)}</p><p><img src='{png.name}'/></p>")
            articles.append({"key": "01_energy", "title": "에너지배율 순위", "png": png, "text": text, "df": energy})
            sheets["에너지배율"] = energy if energy is not None else pd.DataFrame()

            highs = market.get("high")
            png = dataframe_to_png(
                highs,
                out_dir / "02_high.png",
                f"신고가 ({as_of})",
                page=page,
                int_cols={"현재가"},
                chg_font_cols={"당일상승률", "구간대비(%)"},
                gradient_cols={MCAP_COL: soft},
            )
            text = narrative_highs(highs, as_of)
            md_parts.append(f"## 신고가\n\n{text}\n\n![신고가]({png.name})\n")
            html_parts.append(f"<h2>신고가</h2><p>{html.escape(text)}</p><p><img src='{png.name}'/></p>")
            articles.append({"key": "02_high", "title": "신고가", "png": png, "text": text, "df": highs})
            sheets["신고가"] = highs if highs is not None else pd.DataFrame()

            lows = market.get("low")
            png = dataframe_to_png(
                lows,
                out_dir / "03_low.png",
                f"신저가 ({as_of})",
                page=page,
                int_cols={"현재가"},
                chg_font_cols={"당일상승률", "구간대비(%)"},
                gradient_cols={MCAP_COL: soft},
            )
            text = narrative_lows(lows, as_of)
            md_parts.append(f"## 신저가\n\n{text}\n\n![신저가]({png.name})\n")
            html_parts.append(f"<h2>신저가</h2><p>{html.escape(text)}</p><p><img src='{png.name}'/></p>")
            articles.append({"key": "03_low", "title": "신저가", "png": png, "text": text, "df": lows})
            sheets["신저가"] = lows if lows is not None else pd.DataFrame()

            talent = market.get("talent")
            png = dataframe_to_png(
                talent,
                out_dir / "04_talent.png",
                f"Talent 순위 Top50 ({as_of})",
                page=page,
                int_cols={"순위", "현재가"},
                chg_font_cols={"당일상승률"},
                gradient_cols={"talent 지수": soft, MCAP_COL: soft},
                float_digits={"talent 지수": 3},
            )
            text = narrative_talent(talent, as_of)
            md_parts.append(f"## Talent 순위\n\n{text}\n\n![Talent]({png.name})\n")
            html_parts.append(f"<h2>Talent 순위 Top50</h2><p>{html.escape(text)}</p><p><img src='{png.name}'/></p>")
            articles.append({"key": "04_talent", "title": "Talent 순위 Top50", "png": png, "text": text, "df": talent})
            sheets["Talent"] = talent if talent is not None else pd.DataFrame()

            rs = market.get("rs")
            png = dataframe_to_png(
                rs,
                out_dir / "05_rs.png",
                f"RS 순위 Top50 ({as_of})",
                page=page,
                int_cols={"순위", "현재가"},
                gradient_cols={MCAP_COL: soft},
            )
            text = narrative_rs(rs, as_of)
            md_parts.append(f"## RS 순위\n\n{text}\n\n![RS]({png.name})\n")
            html_parts.append(f"<h2>RS 순위 Top50</h2><p>{html.escape(text)}</p><p><img src='{png.name}'/></p>")
            articles.append({"key": "05_rs", "title": "RS 순위 Top50", "png": png, "text": text, "df": rs})
            sheets["RS"] = rs if rs is not None else pd.DataFrame()

            ctx.close()
        finally:
            browser.close()

    md_path, html_path = _write_body(out_dir, md_parts, html_parts)
    xlsx = export_tables_xlsx(out_dir / f"daily_snapshot_{ymd}.xlsx", sheets)
    csvs = export_tables_csv(out_dir, sheets)
    log.info("데일리 스냅샷 출력: %s (xlsx=%s csv=%d)", out_dir, xlsx.name, len(csvs))
    return {
        "out_dir": out_dir,
        "md": md_path,
        "html": html_path,
        "xlsx": xlsx,
        "csv": csvs,
        "articles": articles,
        "kind": "daily_snapshot",
    }


def render_active_etf_pdf(
    as_of: str,
    etf: dict,
    out_dir: Optional[Path] = None,
) -> dict:
    """액티브 ETF PDF 전용: outputs/active_etf_pdf_{YYYYMMDD}/"""
    from playwright.sync_api import sync_playwright

    ymd = _ymd(as_of)
    out_dir = Path(out_dir or (OUTPUTS_DIR / f"active_etf_pdf_{ymd}"))
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("0*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    articles = []
    title = f"액티브 ETF PDF {as_of}"
    html_parts = _html_header(title)
    md_parts = [f"# {title}\n"]
    sheets: dict[str, pd.DataFrame] = {}

    note = etf.get("note")
    if note:
        md_parts.append(f"> **{note}** — 편입/편출 판정을 생략했습니다.\n")
        html_parts.append(
            f"<p class='warn'>{html.escape(str(note))} — 편입/편출 판정 생략</p>"
        )

    # 스냅샷 날짜 안내
    if etf.get("date_cur"):
        md_parts.append(
            f"스냅샷: 당일 `{etf.get('date_cur')}` / 전일 `{etf.get('date_prev')}` / "
            f"1주전 `{etf.get('date_w1')}` / 2주전 `{etf.get('date_w2')}`\n"
        )

    by_etf = etf.get("by_etf") or []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(device_scale_factor=2, viewport={"width": 1400, "height": 2400})
            page = ctx.new_page()

            if not by_etf:
                md_parts.append("액티브 ETF PDF 데이터가 없습니다.\n")
                html_parts.append("<p>액티브 ETF PDF 데이터가 없습니다.</p>")
            else:
                for i, item in enumerate(by_etf, start=1):
                    key = f"{i:02d}_etf_{item['etf_ticker']}"
                    etitle = f"{item['etf_name']} ({item['etf_ticker']})"
                    png = dataframe_to_png_etf(
                        item["df"],
                        out_dir / f"{key}.png",
                        f"{etitle} PDF ({as_of})",
                        page=page,
                        col_cur=item.get("col_cur") or "-",
                        col_prev=item.get("col_prev") or "-",
                        col_w1=item.get("col_w1") or "-",
                        col_w2=item.get("col_w2") or "-",
                        footnotes=item.get("footnotes") or [],
                    )
                    text = narrative_etf_pdf(item, as_of)
                    md_parts.append(f"### {etitle}\n\n{text}\n\n![{etitle}]({png.name})\n")
                    html_parts.append(
                        f"<h3>{html.escape(etitle)}</h3><p>{html.escape(text)}</p>"
                        f"<p><img src='{png.name}'/></p>"
                    )
                    articles.append(
                        {"key": key, "title": etitle, "png": png, "text": text, "df": item["df"]}
                    )
                    sheets[f"{item['etf_ticker']}_{item['etf_name'][:20]}"] = _etf_sheet_df(item["df"])

            ctx.close()
        finally:
            browser.close()

    md_path, html_path = _write_body(out_dir, md_parts, html_parts)
    xlsx = export_tables_xlsx(out_dir / f"active_etf_pdf_{ymd}.xlsx", sheets)
    csvs = export_tables_csv(out_dir, sheets)
    log.info("액티브 ETF PDF 출력: %s (xlsx=%s csv=%d)", out_dir, xlsx.name, len(csvs))
    return {
        "out_dir": out_dir,
        "md": md_path,
        "html": html_path,
        "xlsx": xlsx,
        "csv": csvs,
        "articles": articles,
        "kind": "active_etf_pdf",
    }


def render_bundle(
    as_of: str,
    market: dict[str, pd.DataFrame],
    etf: dict,
    out_dir: Optional[Path] = None,
) -> dict:
    """데일리+ETF를 각각 독립 폴더로 생성 (단독 실행 함수의 조합)."""
    root = Path(out_dir) if out_dir else OUTPUTS_DIR
    ymd = _ymd(as_of)
    daily = render_daily_snapshot(as_of, market, root / f"daily_snapshot_{ymd}")
    etf_bundle = render_active_etf_pdf(as_of, etf, root / f"active_etf_pdf_{ymd}")
    return {
        "out_dir": root,
        "daily": daily,
        "etf": etf_bundle,
        "articles": (daily.get("articles") or []) + (etf_bundle.get("articles") or []),
        "md": daily.get("md"),
        "html": daily.get("html"),
    }
