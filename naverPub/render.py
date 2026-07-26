"""표 PNG + 본문 md/html 렌더 (HTML → Playwright Chromium 스크린샷)."""
from __future__ import annotations

import html
import logging
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import OUTPUTS_DIR, OUTPUT_WIDTH_PX, RENDER_SCALE
from content_market import RET_3D_COL, energy_ratio_font_color

log = logging.getLogger("naverPub.render")

_TEXT_COLS = {"종목명", "달성구간", "티커", "신고가여부"}
_DEFAULT_INT_COLS = {"순위", "거래대금순위", "현재가"}
_NARROW_COLS = {"순위", "티커"}
MCAP_COL = "시총(조원)"
TALENT_UD_COLS = {"20일 ↑/↓", "50일 ↑/↓", "120일 ↑/↓"}
# 구 컬럼명 호환
TALENT_UD_COLS |= {"20일 내 상승/하락", "50일 내 상승/하락", "120일 내 상승/하락"}
_DATE_COL_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")

CHG_UP = "#d32f2f"
CHG_DOWN = "#1565c0"
CHG_ZERO = "#212121"
GRAD_LIGHT = "#ffffff"
GRAD_DARK = "#ffd8a8"
HEADER_BG = "#1f3864"

# 표 PNG 최소 가로(CSS px). device_scale 적용 시 ≈ OUTPUT_WIDTH_PX
_TABLE_CSS_WIDTH = max(400, int(round(OUTPUT_WIDTH_PX / max(1, RENDER_SCALE))))

_TABLE_CSS = f"""
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  padding: 8px;
  background: #ffffff;
  font-family: 'Malgun Gothic', 'NanumGothic', 'Nanum Gothic', sans-serif;
  color: #212121;
  overflow: visible;
}}
#capture {{
  display: inline-block;
  min-width: {_TABLE_CSS_WIDTH}px;
  width: max-content;
  max-width: none;
  overflow: visible;
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
  width: max-content;
  max-width: none;
  table-layout: auto;
  font-size: 13px;
  border-top: 2px solid {HEADER_BG};
  border-bottom: 2px solid {HEADER_BG};
}}
table.snap.compact {{
  font-size: 12px;
}}
table.snap th,
table.snap td {{
  padding: 6px 10px;
  border: none;
  border-bottom: 1px solid #e3e8ef;
  vertical-align: middle;
  white-space: nowrap;
}}
table.snap.compact th,
table.snap.compact td {{
  padding: 4px 5px;
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
table.snap.compact thead th {{
  font-size: 11px;
  padding: 5px 4px;
}}
table.snap tbody tr:nth-child(odd) td {{ background-color: #ffffff; }}
table.snap tbody tr:nth-child(even) td {{ background-color: #f7f9fc; }}
table.snap tbody tr:last-child td {{ border-bottom: none; }}
td.num, th.num {{ text-align: right; }}
td.txt, th.txt {{ text-align: left; }}
td.ctr, th.ctr {{ text-align: center; }}
td.narrow, th.narrow {{ width: 1%; white-space: nowrap; padding-left: 5px; padding-right: 5px; }}
td.ud, th.ud {{ width: 1%; white-space: nowrap; padding-left: 3px; padding-right: 3px; }}
td.name {{ white-space: nowrap; max-width: 200px; }}
table.snap.compact td.name {{ max-width: 140px; }}
td.mcap, th.mcap {{ padding-left: 8px; padding-right: 10px; }}
table.snap.compact td.mcap,
table.snap.compact th.mcap {{ padding-left: 6px; padding-right: 8px; }}
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
span.ud-up {{ color: {CHG_UP}; font-size: 11px; }}
span.ud-dn {{ color: {CHG_DOWN}; font-size: 11px; }}
span.ud-sep {{ color: #9e9e9e; font-size: 11px; margin: 0 1px; }}
span.ud-em {{ font-size: 12px; font-weight: 700; }}
table.snap.compact span.ud-up,
table.snap.compact span.ud-dn,
table.snap.compact span.ud-sep {{ font-size: 10px; }}
table.snap.compact span.ud-em {{ font-size: 11px; font-weight: 700; }}
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
    if name in ("달성구간", "신고가여부") or name in TALENT_UD_COLS:
        return "ctr"
    if name in _TEXT_COLS:
        return "txt"
    return "num"


def _col_extra_class(name: str) -> str:
    parts = []
    if name in _NARROW_COLS:
        parts.append("narrow")
    if name in TALENT_UD_COLS:
        parts.append("ud")
    if name == "종목명":
        parts.append("name")
    if name == MCAP_COL or name == "시총(조원)":
        parts.append("mcap")
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
        compact = bool(set(display.columns) & TALENT_UD_COLS)
        tbl_cls = "snap compact" if compact else "snap"
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
                elif col == "종목명":
                    name_raw = str(display.iloc[rr][col])
                    name = html.escape(name_raw)
                    fs = _name_font_class(str(raw_v) if raw_v is not None else name_raw)
                    fit_cls = f"name-fit {fs}".strip() if fs else "name-fit"
                    cell = f'<span class="{fit_cls}">{name}</span>'
                else:
                    cell = html.escape(str(display.iloc[rr][col]))
                tds.append(f'<td class="{cls}"{style}>{cell}</td>')
            rows_html.append("<tr>" + "".join(tds) + "</tr>")
        body = (
            f'<div id="capture"><div class="title">{title_esc}</div>'
            f'<table class="{tbl_cls}"><thead><tr>{"".join(ths)}</tr></thead>'
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
    gradient_fixed_range: Optional[dict[str, tuple[float, float]]] = None,
) -> Path:
    """
    HTML 표 → Playwright 요소 스크린샷(PNG).
    page가 있으면 재사용, 없으면 단발 브라우저 기동.
    gradient_fixed_range: 컬럼별 (vmin, vmax) 고정 — 예: 주가위치 (0,1).
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
    gradient_fixed_range = dict(gradient_fixed_range or {})

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
                "주가위치",
                "picking점수",
                "에너지배율",
                "RS",
                "talent",
            ):
                kind = "float"
            else:
                kind = "auto"
            display[c] = [_fmt_cell(v, kind=kind, digits=digits) for v in raw[c].tolist()]
        for c in gradient_cols:
            if c not in raw.columns:
                continue
            if c in gradient_fixed_range:
                grad_range[c] = gradient_fixed_range[c]
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
        # 최소폭 뷰포트 → 실제 #capture 크기로 확장 후 전체 스크린샷 (우측·하단 잘림 방지)
        base_w = max(_TABLE_CSS_WIDTH + 48, 500)
        base_h = 2400
        pg.set_viewport_size({"width": base_w, "height": base_h})
        pg.set_content(doc, wait_until="load")
        loc = pg.locator("#capture")
        dims = pg.evaluate(
            """() => {
              const el = document.querySelector('#capture');
              if (!el) return {w: 800, h: 600};
              const rect = el.getBoundingClientRect();
              const w = Math.ceil(Math.max(el.scrollWidth, el.offsetWidth, rect.width));
              const h = Math.ceil(Math.max(el.scrollHeight, el.offsetHeight, rect.height));
              return {w, h};
            }"""
        )
        need_w = max(int(dims.get("w") or base_w) + 48, base_w)
        need_h = min(max(int(dims.get("h") or 600) + 48, base_h), 12000)
        if need_w > base_w or need_h > base_h:
            pg.set_viewport_size({"width": need_w, "height": need_h})
        loc.screenshot(path=str(out_path), type="png")

    if page is not None:
        _shot(page)
    else:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = _launch_chromium(p)
            try:
                ctx = browser.new_context(
                    device_scale_factor=REPORT_DEVICE_SCALE,
                    viewport={"width": _TABLE_CSS_WIDTH + 48, "height": 900},
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


def narrative_tv(df: pd.DataFrame, as_of: str, market: str = "") -> str:
    label = f"{market} " if market else ""
    if df is None or df.empty:
        return f"{as_of} 기준 {label}거래대금 상위 종목 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", "")
    tv = _fmt_num(top.get("거래대금(억)"), digits=1)
    chg = _fmt_num(top.get("당일상승률"), digits=2)
    return (
        f"{as_of} {label}거래대금 1위는 {name}({tv}억, 등락 {chg}%)입니다. "
        f"당일 거래대금 내림차순 Top50입니다."
    )


def narrative_energy(df: pd.DataFrame, as_of: str, market: str = "") -> str:
    label = f"{market} " if market else ""
    if df is None or df.empty:
        return f"{as_of} 기준 {label}에너지배율 상위 종목 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", top.get("티커", ""))
    er3 = _fmt_num(top.get("3일에너지배율"))
    er1 = _fmt_num(top.get("당일에너지배율"))
    return (
        f"{as_of} {label}거래대금 상위 종목 중 3일 에너지배율 1위는 {name}입니다. "
        f"3일 배율 {er3}, 당일 배율 {er1}로 시총 대비 자금 유입이 두드러집니다. "
        f"에너지배율은 거래대금 시장비중÷시총 시장비중에 등락 방향(tanh)을 반영한 값입니다. "
        f"시총 단위는 조(원)입니다."
    )


def narrative_highs(df: pd.DataFrame, as_of: str, market: str = "") -> str:
    label = f"{market} " if market else ""
    if df is None or df.empty:
        return f"{as_of} 기준 {label}종가 신고가 달성 종목이 없습니다."
    return (
        f"{as_of} 기준 {label}최장 구간 종가 신고가 {len(df)}종입니다. "
        f"구간대비(%)는 해당 구간 종가 최저 대비 상승률입니다."
    )


def narrative_rs(df: pd.DataFrame, as_of: str, market: str = "") -> str:
    label = f"{market} " if market else ""
    if df is None or df.empty:
        return f"{as_of} 기준 {label}RS 순위 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", "")
    return (
        f"{as_of} {label}RS(상대강도) 1위는 {name}입니다. "
        f"시총 5,000억원 이상·해당 시장 내 백분위 기준이며, "
        f"RS는 rs_20·50·120·200 백분위 평균입니다."
    )


def narrative_talent(df: pd.DataFrame, as_of: str, market: str = "") -> str:
    label = f"{market} " if market else ""
    if df is None or df.empty:
        return f"{as_of} 기준 {label}Talent 순위 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", "")
    idx = _fmt_num(top.get("talent 지수"), digits=3)
    return (
        f"{as_of} {label}Talent 지수 1위는 {name}(지수 {idx})입니다. "
        f"시총 5,000억원 이상·해당 시장 내 순위이며, "
        f"지수 = (20일상승일수/20)×0.5 + (50일/50)×0.3 + (120일/120)×0.2 "
        f"(상승일 = 일간 등락률 +10% 이상)입니다."
    )


def narrative_price_pos(df: pd.DataFrame, as_of: str, market: str = "") -> str:
    label = f"{market} " if market else ""
    if df is None or df.empty:
        return f"{as_of} 기준 {label}주가위치 순위 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", "")
    pos = _fmt_num(top.get("주가위치"), digits=2)
    return (
        f"{as_of} {label}주가위치 1위는 {name}(위치 {pos})입니다. "
        f"시총 5,000억원 이상·120거래일 고가·저가 기준이며, "
        f"주가위치 = (현재가−최저)/(최고−최저) 입니다."
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


def _write_doc(
    out_dir: Path,
    md_parts: list[str],
    html_parts: list[str],
    stem: str,
) -> tuple[Path, Path]:
    """md는 항상 기록. html_parts가 있으면 구형 조립 HTML, 없으면 md만."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{stem}.md"
    html_path = out_dir / f"{stem}.html"
    md_path.write_text("\n".join(md_parts), encoding="utf-8")
    if html_parts is not None:
        html_path.write_text("\n".join(list(html_parts) + ["</body></html>"]), encoding="utf-8")
    return md_path, html_path


def _write_md_only(out_dir: Path, md_parts: list[str], stem: str) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{stem}.md"
    md_path.write_text("\n".join(md_parts), encoding="utf-8")
    return md_path


def _weight_rank_lines_html(df: Optional[pd.DataFrame], n: int = 3) -> str:
    """비중 확대/축소 metric: 1~3위 줄 HTML (raw placeholder)."""
    if df is None or df.empty:
        return '<div class="metric-ranks"><div class="r1">-</div></div>'
    lines = []
    for i, (_, r) in enumerate(df.head(n).iterrows()):
        name = html.escape(str(r.get("종목명") or "-").strip() or "-")
        etf = html.escape(str(r.get("ETF") or "").strip() or "-")
        chg = _fmt_num(r.get("전일 대비"))
        cls = "r1" if i == 0 else "r23"
        lines.append(
            f'<div class="{cls}">'
            f'<span class="rk-name">{name}</span>'
            f'<span class="rk-sep"> · </span>'
            f'<span class="rk-meta">{etf} · {chg}%p</span>'
            f"</div>"
        )
    return '<div class="metric-ranks">' + "".join(lines) + "</div>"


def _as_of_short(as_of: str) -> str:
    s = str(as_of).replace("-", "").replace(".", "")[:8]
    if len(s) == 8 and s.isdigit():
        return f"{s[4:6]}-{s[6:8]}"
    return str(as_of)


def _top_name(df: Optional[pd.DataFrame], col: str = "종목명") -> str:
    if df is None or df.empty:
        return "-"
    v = df.iloc[0].get(col, "")
    return str(v).strip() if v is not None and str(v).strip() else "-"


def _count_day_stocks(as_of: str) -> int:
    try:
        from db import engine

        ymd = _ymd(as_of)
        d = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}"
        df = pd.read_sql(
            "SELECT COUNT(*) AS c FROM ohlcv WHERE date = %s",
            engine(),
            params=(d,),
        )
        return int(df.iloc[0]["c"] or 0)
    except Exception:
        return 0


REPORT_VIEWPORT_W = OUTPUT_WIDTH_PX
CHROMIUM_LAUNCH_ARGS = ["--disable-dev-shm-usage", "--no-sandbox"]
REPORT_DEVICE_SCALE = RENDER_SCALE

# 폴더별 단일 디자인 HTML (텔레그램 전송·잔재 정리 시 보존)
DESIGN_HTML_NAMES = {
    "tickers": "stocks.html",
    "martket": "market.html",
    "pick": "pick.html",
    "etfs": "etf.html",
}

# --force 재생성 시 예전 HTML·MD 파일명 제거
LEGACY_OUTPUT_FILES = {
    "tickers": ("market.html", "market.md"),
    "martket": ("volatility.html", "volatility.md"),
}


def _launch_chromium(playwright):
    return playwright.chromium.launch(headless=True, args=list(CHROMIUM_LAUNCH_ARGS))


def cleanup_publish_artifacts(out_dir: Path) -> None:
    """
    구 조각/캡처·예전 파일명 잔재 삭제.
    폴더별 단일 디자인 HTML(stocks/market/etf/pick.html)은 보존.
    """
    import shutil

    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        return
    folder = out_dir.name
    keep_name = DESIGN_HTML_NAMES.get(folder)
    keep_html = {keep_name} if keep_name else set(DESIGN_HTML_NAMES.values())
    for name in LEGACY_OUTPUT_FILES.get(folder, ()):
        legacy = out_dir / name
        if legacy.is_file():
            try:
                legacy.unlink()
            except OSError as e:
                log.warning("레거시 삭제 실패(%s): %s", legacy, e)
    cap = out_dir / "capture"
    if cap.exists():
        try:
            shutil.rmtree(cap)
        except OSError as e:
            log.warning("capture/ 삭제 실패(%s): %s", cap, e)
    for pat in ("*_full.*", "*_sec*.png", ".capture_tmp_*.html"):
        for old in out_dir.glob(pat):
            try:
                if old.is_file():
                    old.unlink()
            except OSError:
                pass
    for old in out_dir.glob("*.html"):
        if old.name in keep_html:
            continue
        stem = old.stem
        if stem.startswith(("tickers_", "market_", "etf_", "pick_", "stocks_", "volatility_")):
            try:
                old.unlink()
            except OSError:
                pass


def build_etf_cross_rankings(by_etf: list[dict], top_n: int = 30) -> dict[str, pd.DataFrame]:
    """전 ETF 합산: 보유 상위 / 비중 확대 / 비중 축소."""
    from content_etf import CHG_PREV

    rows = []
    for item in by_etf or []:
        df = item.get("df")
        if df is None or df.empty:
            continue
        col_cur = item.get("col_cur")
        etf_name = item.get("etf_name") or item.get("etf_ticker") or ""
        etf_ticker = item.get("etf_ticker") or ""
        for _, r in df.iterrows():
            w = r.get(col_cur) if col_cur else None
            chg = r.get(CHG_PREV)
            try:
                w_f = float(w) if w is not None and pd.notna(w) else np.nan
            except (TypeError, ValueError):
                w_f = np.nan
            try:
                c_f = float(chg) if chg is not None and pd.notna(chg) else np.nan
            except (TypeError, ValueError):
                c_f = np.nan
            rows.append(
                {
                    "ETF": etf_name,
                    "ETF코드": etf_ticker,
                    "종목명": r.get("종목명", ""),
                    "티커": r.get("티커", ""),
                    "비중(%)": w_f,
                    "전일 대비": c_f,
                    "상태": r.get("_status", "") or "",
                }
            )
    empty = {
        "holdings": pd.DataFrame(),
        "weight_up": pd.DataFrame(),
        "weight_down": pd.DataFrame(),
    }
    if not rows:
        return empty
    all_df = pd.DataFrame(rows)
    hold = (
        all_df.dropna(subset=["비중(%)"])
        .sort_values("비중(%)", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    hold.insert(0, "순위", range(1, len(hold) + 1))
    up = (
        all_df.dropna(subset=["전일 대비"])
        .sort_values("전일 대비", ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )
    up.insert(0, "순위", range(1, len(up) + 1))
    down = (
        all_df.dropna(subset=["전일 대비"])
        .sort_values("전일 대비", ascending=True)
        .head(top_n)
        .reset_index(drop=True)
    )
    down.insert(0, "순위", range(1, len(down) + 1))
    return {"holdings": hold, "weight_up": up, "weight_down": down}


def _etf_cross_table_cols(df: pd.DataFrame, *, with_status: bool) -> list[str]:
    """기존 교차랭킹 컬럼 유지 + ETF코드를 ETF명 왼쪽에만 추가."""
    preferred = ["순위", "ETF코드", "ETF", "종목명", "티커", "비중(%)", "전일 대비"]
    if with_status:
        preferred.append("상태")
    if df is None or df.empty:
        return preferred
    cols = [c for c in preferred if c in df.columns]
    for c in df.columns:
        if c not in cols and not str(c).startswith("_"):
            cols.append(c)
    return cols


def day_root(as_of: str) -> Path:
    """outputs/YYYYMMDD/"""
    return OUTPUTS_DIR / _ymd(as_of)


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
    market: dict,
    out_dir: Optional[Path] = None,
) -> dict:
    """시장 스냅샷: 지표별(코스피→코스닥) PNG + stocks.md/html."""
    from playwright.sync_api import sync_playwright

    from content_market import MARKET_LABELS, MARKETS, MCAP_COL as MCAP, PRICE_POS_COL
    from design_html import chart_card, date_iso, date_kr, write_design_html

    ymd = _ymd(as_of)
    out_dir = Path(out_dir or (day_root(as_of) / "tickers"))
    out_dir.mkdir(parents=True, exist_ok=True)
    cleanup_publish_artifacts(out_dir)
    for old in out_dir.glob("0*.png"):
        try:
            old.unlink()
        except OSError:
            pass
    for old in out_dir.glob("*_low_*.png"):
        try:
            old.unlink()
        except OSError:
            pass
    for old in out_dir.glob("*low*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    articles = []
    title = f"시장 스냅샷 {as_of}"
    md_parts = [f"# {title}\n"]
    sheets: dict[str, pd.DataFrame] = {}
    soft = (GRAD_LIGHT, GRAD_DARK)
    section_html: list[str] = []
    energy_tops: dict[str, tuple[str, str]] = {}

    if any(k in market for k in ("energy", "high", "rs", "tv")):
        market = {"KOSPI": market, "KOSDAQ": {}}

    # ①거래대금 → ②에너지 → ③RS → ④주가위치 → ⑤talent → ⑥신고가
    metrics_spec = [
        {
            "id": "tv",
            "prefix": "01_tv",
            "key": "tv",
            "title": "거래대금순위",
            "desc": "시장별 당일 거래대금 내림차순 Top50입니다.",
            "caption": "당일 거래대금 Top50 · 당일·3일 상승률",
            "tag": "Turnover",
            "kicker": "Section 01",
            "accent": "var(--orange)",
        },
        {
            "id": "energy",
            "prefix": "02_energy",
            "key": "energy",
            "title": "에너지배율",
            "desc": "시장별 거래대금 상위 50 → 방향반영 3일 에너지배율 순입니다.",
            "caption": "거래대금 상위50 → 방향반영 3일 에너지배율 · tanh(K=15)",
            "tag": "Flow",
            "kicker": "Section 02",
            "accent": "var(--blue)",
        },
        {
            "id": "rs",
            "prefix": "03_rs",
            "key": "rs",
            "title": "RS Top50",
            "desc": "시총 5,000억 이상, 시장 내 상대강도 상위 50종입니다.",
            "caption": "시총 5,000억↑ · rs 백분위 · 당일·3일 상승률",
            "tag": "RS",
            "kicker": "Section 03",
            "accent": "var(--violet)",
        },
        {
            "id": "pos",
            "prefix": "04_pos",
            "key": "pos",
            "title": "주가위치 Top50",
            "desc": "120일 고·저가 위치 상위 50종(+20·50일 종가 위치)입니다.",
            "caption": "120일 고저 위치 정렬 · 20/50일 종가 위치 병기",
            "tag": "Position",
            "kicker": "Section 04",
            "accent": "var(--cyan)",
        },
        {
            "id": "talent",
            "prefix": "05_talent",
            "key": "talent",
            "title": "Talent Top50",
            "desc": "시총 5,000억 이상 Talent 지수 상위 50종입니다.",
            "caption": "시총 5,000억↑ · 시장내 순위 · +10% 상승일 가중",
            "tag": "Talent",
            "kicker": "Section 05",
            "accent": "var(--orange)",
        },
        {
            "id": "high",
            "prefix": "06_high",
            "key": "high",
            "title": "신고가",
            "desc": "시장별 종가 기준 최장 구간 신고가 달성 종목입니다.",
            "caption": "종가 신고가(50/120/200) · 구간 종가 최저 대비",
            "tag": "High",
            "kicker": "Section 06",
            "accent": "var(--blue)",
        },
    ]

    with sync_playwright() as p:
        browser = _launch_chromium(p)
        try:
            ctx = browser.new_context(
                device_scale_factor=REPORT_DEVICE_SCALE,
                viewport={"width": _TABLE_CSS_WIDTH + 48, "height": 2400},
            )
            page = ctx.new_page()

            for spec in metrics_spec:
                md_parts.append(f"## {spec['title']}\n")
                cards: list[str] = []
                for mi, mkt in enumerate(MARKETS):
                    mkt_l = mkt.lower()
                    label = MARKET_LABELS.get(mkt, mkt)
                    block = market.get(mkt) or {}
                    df = block.get(spec["key"])
                    png_name = f"{spec['prefix']}_{mkt_l}.png"
                    num = f"{mi + 1:02d}"
                    num_style = (
                        ""
                        if mkt == "KOSPI"
                        else "background:rgba(80,227,194,.12);border-color:rgba(80,227,194,.18)"
                    )

                    kw: dict = {
                        "page": page,
                        "int_cols": {"순위", "현재가"},
                        "chg_font_cols": {"당일상승률", RET_3D_COL},
                        "gradient_cols": {MCAP: soft},
                    }
                    if spec["key"] == "tv":
                        kw["gradient_cols"] = {"거래대금(억)": soft, MCAP: soft}
                        text = narrative_tv(df, as_of, market=label)
                        card_title = f"{label} 거래대금순위"
                    elif spec["key"] == "energy":
                        kw["int_cols"] = {"순위", "거래대금순위", "현재가"}
                        kw["gradient_cols"] = {"거래대금(억)": soft, MCAP: soft}
                        kw["energy_font_cols"] = {"당일에너지배율", "3일에너지배율"}
                        kw["chg_font_cols"] = {"당일상승률", RET_3D_COL}
                        text = narrative_energy(df, as_of, market=label)
                        card_title = f"{label} 에너지배율"
                    elif spec["key"] == "rs":
                        text = narrative_rs(df, as_of, market=label)
                        card_title = f"{label} RS Top50"
                    elif spec["key"] == "pos":
                        kw["gradient_cols"] = {
                            PRICE_POS_COL: soft,
                            "20일 주가위치": soft,
                            "50일 주가위치": soft,
                            MCAP: soft,
                        }
                        kw["float_digits"] = {
                            PRICE_POS_COL: 2,
                            "20일 주가위치": 2,
                            "50일 주가위치": 2,
                        }
                        kw["gradient_fixed_range"] = {
                            PRICE_POS_COL: (0.0, 1.0),
                            "20일 주가위치": (0.0, 1.0),
                            "50일 주가위치": (0.0, 1.0),
                        }
                        kw["chg_font_cols"] = {"당일상승률", RET_3D_COL}
                        text = narrative_price_pos(df, as_of, market=label)
                        card_title = f"{label} 주가위치 Top50"
                    elif spec["key"] == "talent":
                        kw["gradient_cols"] = {"talent 지수": soft, MCAP: soft}
                        kw["float_digits"] = {"talent 지수": 3}
                        kw["chg_font_cols"] = {"당일상승률", RET_3D_COL}
                        text = narrative_talent(df, as_of, market=label)
                        card_title = f"{label} Talent Top50"
                    else:  # high
                        kw["chg_font_cols"] = {"당일상승률", RET_3D_COL, "구간대비(%)"}
                        text = narrative_highs(df, as_of, market=label)
                        card_title = f"{label} 신고가"
                        kw["int_cols"] = {"현재가"}

                    caption = spec["caption"]
                    if spec["key"] == "high":
                        caption = f"{0 if df is None else len(df)}종 · {spec['caption']}"

                    png = dataframe_to_png(
                        df,
                        out_dir / png_name,
                        f"{card_title} ({as_of})",
                        **kw,
                    )
                    md_parts.append(
                        f"### {label}\n\n{text}\n\n![{card_title}]({png.name})\n"
                    )
                    articles.append(
                        {
                            "key": f"{spec['prefix']}_{mkt_l}",
                            "title": card_title,
                            "png": png,
                            "text": text,
                            "df": df,
                        }
                    )
                    sheets[f"{spec['title']}_{label}"] = (
                        df if df is not None else pd.DataFrame()
                    )
                    if spec["key"] == "energy":
                        energy_tops[mkt] = (
                            _top_name(df),
                            _fmt_num(df.iloc[0].get("3일에너지배율"))
                            if df is not None and not df.empty
                            else "-",
                        )
                    cards.append(
                        chart_card(
                            num,
                            card_title,
                            caption,
                            png.name,
                            tag=f"{label} · {spec['tag']}",
                            wide=True,
                            number_style=num_style,
                            narrative=text,
                        )
                    )

                section_html.append(
                    f'<section class="section" id="{spec["id"]}">'
                    f'<div class="section-head"><div>'
                    f'<div class="section-kicker" style="color:{spec["accent"]}">{spec["kicker"]}</div>'
                    f'<h2>{html.escape(spec["title"])}</h2>'
                    f'<p class="section-desc">{html.escape(spec["desc"])}</p>'
                    f"</div>"
                    f'<div class="market-badge" style="color:{spec["accent"]}">'
                    f'<i></i>KOSPI → KOSDAQ</div></div>'
                    f'<div class="chart-grid stack">{"".join(cards)}</div>'
                    f"</section>"
                )

            ctx.close()
        finally:
            browser.close()

    md_path = _write_md_only(out_dir, md_parts, "stocks")
    ek, ev = energy_tops.get("KOSPI", ("-", "-"))
    dk, dv = energy_tops.get("KOSDAQ", ("-", "-"))
    html_path = write_design_html(
        "market_design.html",
        out_dir / "stocks.html",
        {
            "DATE": date_iso(as_of),
            "DATE_KR": date_kr(as_of),
            "AS_OF_SHORT": _as_of_short(as_of),
            "TOTAL_STOCKS": f"{_count_day_stocks(as_of):,}",
            "ENERGY_TOP_KOSPI": ek,
            "ENERGY_TOP_VAL_KOSPI": ev,
            "ENERGY_TOP_KOSDAQ": dk,
            "ENERGY_TOP_VAL_KOSDAQ": dv,
            "MARKET_SECTIONS_HTML": "\n".join(section_html),
        },
    )
    cleanup_publish_artifacts(out_dir)
    xlsx = export_tables_xlsx(out_dir / f"tickers_{ymd}.xlsx", sheets)
    csvs = export_tables_csv(out_dir, sheets)
    log.info("tickers 출력: %s (md=%s xlsx=%s csv=%d)", out_dir, md_path.name, xlsx.name, len(csvs))
    return {
        "out_dir": out_dir,
        "md": md_path,
        "html": html_path,
        "xlsx": xlsx,
        "csv": csvs,
        "articles": articles,
        "kind": "tickers",
    }


def render_active_etf_pdf(
    as_of: str,
    etf: dict,
    out_dir: Optional[Path] = None,
) -> dict:
    """액티브 ETF PDF: outputs/YYYYMMDD/etfs/ (PNG + etf.md / 디자인 etf.html)."""
    from playwright.sync_api import sync_playwright

    from design_html import chart_card, date_iso, date_kr, write_design_html

    ymd = _ymd(as_of)
    out_dir = Path(out_dir or (day_root(as_of) / "etfs"))
    out_dir.mkdir(parents=True, exist_ok=True)
    cleanup_publish_artifacts(out_dir)
    for old in out_dir.glob("0*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    articles = []
    title = f"액티브 ETF PDF {as_of}"
    md_parts = [f"# {title}\n"]
    sheets: dict[str, pd.DataFrame] = {}

    note = etf.get("note")
    if note:
        md_parts.append(f"> **{note}** — 편입/편출 판정을 생략했습니다.\n")

    if etf.get("date_cur"):
        md_parts.append(
            f"스냅샷: 당일 `{etf.get('date_cur')}` / 전일 `{etf.get('date_prev')}` / "
            f"1주전 `{etf.get('date_w1')}` / 2주전 `{etf.get('date_w2')}`\n"
        )

    by_etf = etf.get("by_etf") or []
    rankings = build_etf_cross_rankings(by_etf, top_n=30)
    soft = (GRAD_LIGHT, GRAD_DARK)
    pdf_cards: list[str] = []

    with sync_playwright() as p:
        browser = _launch_chromium(p)
        try:
            ctx = browser.new_context(
                device_scale_factor=REPORT_DEVICE_SCALE,
                viewport={"width": _TABLE_CSS_WIDTH + 48, "height": 2400},
            )
            page = ctx.new_page()

            hold = rankings["holdings"]
            dataframe_to_png(
                hold[_etf_cross_table_cols(hold, with_status=False)] if not hold.empty else hold,
                out_dir / "00_top_holdings.png",
                f"보유 비중 Top ({as_of})",
                page=page,
                int_cols={"순위"},
                chg_font_cols={"전일 대비"},
                gradient_cols={"비중(%)": soft},
            )
            up = rankings["weight_up"]
            dataframe_to_png(
                up[_etf_cross_table_cols(up, with_status=True)] if not up.empty else up,
                out_dir / "00_weight_up.png",
                f"비중 확대 Top ({as_of})",
                page=page,
                int_cols={"순위"},
                chg_font_cols={"전일 대비"},
                gradient_cols={"비중(%)": soft},
            )
            down = rankings["weight_down"]
            dataframe_to_png(
                down[_etf_cross_table_cols(down, with_status=True)] if not down.empty else down,
                out_dir / "00_weight_down.png",
                f"비중 축소 Top ({as_of})",
                page=page,
                int_cols={"순위"},
                chg_font_cols={"전일 대비"},
                gradient_cols={"비중(%)": soft},
            )

            if not by_etf:
                md_parts.append("액티브 ETF PDF 데이터가 없습니다.\n")
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
                    articles.append(
                        {"key": key, "title": etitle, "png": png, "text": text, "df": item["df"]}
                    )
                    sheets[f"{item['etf_ticker']}_{item['etf_name'][:20]}"] = _etf_sheet_df(item["df"])
                    pdf_cards.append(
                        chart_card(
                            number=f"{i:02d}",
                            title=etitle,
                            caption=text[:120] + ("…" if len(text) > 120 else ""),
                            src=png.name,
                            tag="PDF",
                            wide=True,
                            number_style=(
                                "background:rgba(167,139,250,.12);border-color:rgba(167,139,250,.18)"
                                if i % 2 == 0
                                else ""
                            ),
                        )
                    )

            ctx.close()
        finally:
            browser.close()

    md_path = _write_md_only(out_dir, md_parts, "etf")

    up_top = _top_name(rankings["weight_up"])
    down_top = _top_name(rankings["weight_down"])
    up_ranks = _weight_rank_lines_html(rankings["weight_up"], 3)
    down_ranks = _weight_rank_lines_html(rankings["weight_down"], 3)

    html_path = write_design_html(
        "etf_design.html",
        out_dir / "etf.html",
        {
            "DATE": date_iso(as_of),
            "DATE_KR": date_kr(as_of),
            "AS_OF_SHORT": _as_of_short(as_of),
            "ETF_COUNT": str(len(by_etf)),
            "WEIGHT_UP_TOP": up_top,
            "WEIGHT_DOWN_TOP": down_top,
            "WEIGHT_UP_RANKS_HTML": up_ranks,
            "WEIGHT_DOWN_RANKS_HTML": down_ranks,
            "DATE_CUR": str(etf.get("date_cur") or "-"),
            "DATE_PREV": str(etf.get("date_prev") or "-"),
            "DATE_W1": str(etf.get("date_w1") or "-"),
            "DATE_W2": str(etf.get("date_w2") or "-"),
            "ETF_PDF_CARDS_HTML": "\n".join(pdf_cards)
            if pdf_cards
            else '<p class="section-desc">ETF PDF 데이터가 없습니다.</p>',
        },
    )
    cleanup_publish_artifacts(out_dir)
    xlsx = export_tables_xlsx(out_dir / f"etfs_{ymd}.xlsx", sheets)
    csvs = export_tables_csv(out_dir, sheets)
    log.info("etfs 출력: %s (md=%s xlsx=%s csv=%d)", out_dir, md_path.name, xlsx.name, len(csvs))
    return {
        "out_dir": out_dir,
        "md": md_path,
        "html": html_path,
        "xlsx": xlsx,
        "csv": csvs,
        "articles": articles,
        "kind": "etfs",
    }


def render_picking(
    as_of: str,
    market: dict,
    out_dir: Optional[Path] = None,
) -> dict:
    """장기/단기 모멘텀 Picking Top50 + 스크리닝 차트: outputs/YYYYMMDD/pick/"""
    from content_picking import PICK_COLS, PICK_TYPE_META, build_all_picking
    from datetime import datetime as _dt
    from design_html import chart_card, date_iso, date_kr, write_design_html
    from playwright.sync_api import sync_playwright
    from screening import build_screening_charts

    ymd = _ymd(as_of)
    as_of_d = _dt.strptime(ymd, "%Y%m%d").date()
    out_dir = Path(out_dir) if out_dir else day_root(as_of) / "pick"
    out_dir.mkdir(parents=True, exist_ok=True)
    cleanup_publish_artifacts(out_dir)

    tables = build_all_picking(as_of=None, market=market)
    soft = (GRAD_LIGHT, GRAD_DARK)
    md_parts = [f"# 종목 선정 ({as_of})\n"]
    articles: list[dict] = []
    sheets: dict[str, pd.DataFrame] = {}
    float_digits = {
        "picking점수": 1,
        "에너지배율": 2,
        "RS": 2,
        "주가위치": 2,
        "talent": 3,
    }
    pick_cards: list[str] = []

    with sync_playwright() as p:
        browser = _launch_chromium(p)
        try:
            ctx = browser.new_context(
                device_scale_factor=REPORT_DEVICE_SCALE,
                viewport={"width": _TABLE_CSS_WIDTH + 48, "height": 2400},
            )
            page = ctx.new_page()
            for i, (key, title, caption) in enumerate(PICK_TYPE_META, start=1):
                df = tables.get(key)
                png_name = f"{i:02d}_picking_{key}.png"
                if df is None or df.empty:
                    text = f"{as_of} 기준 {title} 후보가 없습니다."
                    md_parts.append(f"## {title}\n\n{text}\n")
                    dataframe_to_png(
                        pd.DataFrame(columns=PICK_COLS),
                        out_dir / png_name,
                        f"{title} ({as_of})",
                        page=page,
                    )
                    pick_cards.append(
                        chart_card(
                            number=f"{i:02d}",
                            title=title,
                            caption=caption,
                            src=png_name,
                            tag="Pick",
                            wide=True,
                            narrative=text,
                            number_style="background:rgba(167,139,250,.12);border-color:rgba(167,139,250,.18)",
                        )
                    )
                    continue
                text = narrative_picking(df, as_of, title=title)
                md_parts.append(f"## {title}\n\n{text}\n")
                dataframe_to_png(
                    df,
                    out_dir / png_name,
                    f"{title} ({as_of})",
                    page=page,
                    int_cols={"순위", "현재가"},
                    gradient_cols={"picking점수": soft},
                    energy_font_cols={"에너지배율"},
                    float_digits=float_digits,
                )
                md_parts.append(f"![{title}]({png_name})\n")
                articles.append({"title": f"{title} ({as_of})", "text": text, "png": out_dir / png_name})
                sheets[title] = df.copy()
                pick_cards.append(
                    chart_card(
                        number=f"{i:02d}",
                        title=title,
                        caption=caption,
                        src=png_name,
                        tag="Pick",
                        wide=True,
                        narrative=text,
                        number_style="background:rgba(167,139,250,.12);border-color:rgba(167,139,250,.18)",
                    )
                )
            ctx.close()
        finally:
            browser.close()

    df_long = tables.get("long")
    df_short = tables.get("short")
    # 스크리닝 정렬용: 장기 picking점수 우선 (대상·패턴 로직은 변경 없음)
    df_for_screen = df_long if df_long is not None and not df_long.empty else df_short

    long_n = 0 if df_long is None or df_long.empty else len(df_long)
    short_n = 0 if df_short is None or df_short.empty else len(df_short)
    count = f"{long_n}/{short_n}"
    if df_long is not None and not df_long.empty:
        top_name = str(df_long.iloc[0].get("종목명") or "-")
        top_score = _fmt_num(df_long.iloc[0].get("picking점수"), digits=1)
    else:
        top_name, top_score = "-", "-"
    if df_short is not None and not df_short.empty:
        short_top = str(df_short.iloc[0].get("종목명") or "-")
        short_score = _fmt_num(df_short.iloc[0].get("picking점수"), digits=1)
    else:
        short_top, short_score = "-", "-"

    # --- 스크리닝 차트 (패턴 ≥3) ---
    md_parts.append("\n## 스크리닝 통과 차트\n")
    try:
        screen = build_screening_charts(as_of_d, out_dir, pick_df=df_for_screen, min_patterns=3)
    except Exception as e:
        log.exception("pick 스크리닝 차트 실패: %s", e)
        screen = {
            "candidates": pd.DataFrame(),
            "chart_paths": [],
            "articles": [],
            "pass_count": 0,
            "error": str(e),
        }

    pass_n = int(screen.get("pass_count") or 0)
    log.info("스크리닝 통과 종목 수: %d", pass_n)
    screen_cards: list[str] = []
    if pass_n <= 0:
        empty_msg = "해당 종목 없음"
        md_parts.append(f"{empty_msg}\n")
        screen_cards.append(f'<p class="section-desc">{html.escape(empty_msg)}</p>')
    else:
        cands = screen.get("candidates")
        if cands is not None and not cands.empty:
            sheets["Screening"] = cands.copy()
        for i, art in enumerate(screen.get("articles") or [], start=1):
            png = Path(art["png"])
            title = art.get("title") or png.stem
            caption = art.get("text") or ""
            md_parts.append(f"### {title}\n\n{caption}\n\n![{title}]({png.name})\n")
            articles.append(art)
            screen_cards.append(
                chart_card(
                    number=f"{i:02d}",
                    title=title,
                    caption=caption[:140] + ("…" if len(caption) > 140 else ""),
                    src=png.name,
                    tag="Screen",
                    wide=True,
                    number_style="background:rgba(80,227,194,.12);border-color:rgba(80,227,194,.18)",
                )
            )

    md_path = _write_md_only(out_dir, md_parts, "pick")
    html_path = write_design_html(
        "picking_design.html",
        out_dir / "pick.html",
        {
            "DATE": date_iso(as_of),
            "DATE_KR": date_kr(as_of),
            "AS_OF_SHORT": _as_of_short(as_of),
            "PICK_COUNT": count,
            "PICK_TOP": top_name,
            "PICK_TOP_SCORE": top_score,
            "PICK_SHORT_TOP": short_top,
            "PICK_SHORT_SCORE": short_score,
            "PICK_CARD_HTML": "\n".join(pick_cards),
            "SCREEN_COUNT": str(pass_n),
            "SCREEN_CHARTS_HTML": "\n".join(screen_cards),
        },
    )
    cleanup_publish_artifacts(out_dir)

    xlsx = export_tables_xlsx(out_dir / f"pick_{ymd}.xlsx", sheets or {"Picking": pd.DataFrame()})
    csvs = export_tables_csv(out_dir, sheets or {"Picking": pd.DataFrame()})
    log.info(
        "pick 출력: %s (md=%s xlsx=%s csv=%d screen=%d)",
        out_dir,
        md_path.name,
        xlsx.name,
        len(csvs),
        pass_n,
    )
    return {
        "out_dir": out_dir,
        "md": md_path,
        "html": html_path,
        "xlsx": xlsx,
        "csv": csvs,
        "articles": articles,
        "kind": "pick",
        "df": df_long,
        "tables": tables,
        "screen": screen,
    }


def narrative_picking(df: pd.DataFrame, as_of: str, title: str = "Picking") -> str:
    if df is None or df.empty:
        return f"{as_of} 기준 {title} 데이터가 없습니다."
    top = df.iloc[0]
    name = top.get("종목명", top.get("티커", ""))
    score = _fmt_num(top.get("picking점수"), digits=1)
    high = top.get("신고가여부") or "-"
    parts = []
    for col, label, dig in (
        ("에너지배율", "3일에너지", 2),
        ("RS", "RS120", 2),
        ("주가위치", "주가위치", 2),
        ("talent", "talent", 3),
    ):
        v = top.get(col)
        if v is not None and pd.notna(v):
            parts.append(f"{label} {_fmt_num(v, digits=dig)}")
    detail = ", ".join(parts) if parts else "지표 원값 없음"
    nh = f", 종가 신고가 {high}" if high and high != "-" else ""
    return (
        f"{as_of} {title} 1위는 {name}({score}점). "
        f"원값: {detail}{nh}. "
        f"picking점수는 Top50 순위 환산(1위 250~50위 50)에 유형별 가중치를 곱한 가중합입니다."
    )


def render_bundle(
    as_of: str,
    market: dict[str, pd.DataFrame],
    etf: dict,
    out_dir: Optional[Path] = None,
) -> dict:
    """outputs/YYYYMMDD/{tickers,etfs,martket,pick}/"""
    from content_volatility import render_market_volatility
    from datetime import datetime as _dt

    root = Path(out_dir) if out_dir else day_root(as_of)
    tickers = render_daily_snapshot(as_of, market, root / "tickers")
    as_of_d = _dt.strptime(_ymd(as_of), "%Y%m%d").date()
    vol = render_market_volatility(as_of_d, root / "martket")
    pick = render_picking(as_of, market, root / "pick")
    etfs = render_active_etf_pdf(as_of, etf, root / "etfs")
    return {
        "out_dir": root,
        "tickers": tickers,
        "martket": vol,
        "pick": pick,
        "etfs": etfs,
        "articles": (tickers.get("articles") or [])
        + (vol.get("articles") or [])
        + (pick.get("articles") or [])
        + (etfs.get("articles") or []),
    }
