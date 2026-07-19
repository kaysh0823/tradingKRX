"""디자인 HTML 템플릿 로드·플레이스홀더 치환 (단일 파일 인라인 CSS)."""
from __future__ import annotations

import html
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Optional, Union

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_PLACEHOLDER_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


def date_kr(d: Union[date, datetime, str]) -> str:
    if isinstance(d, str):
        s = d.replace("-", "").replace(".", "")[:8]
        try:
            d = datetime.strptime(s, "%Y%m%d").date()
        except ValueError:
            return str(d)
    elif isinstance(d, datetime):
        d = d.date()
    return f"{d.year}년 {d.month}월 {d.day}일"


def date_iso(d: Union[date, datetime, str]) -> str:
    if isinstance(d, str):
        s = d.replace("-", "").replace(".", "")[:8]
        try:
            d = datetime.strptime(s, "%Y%m%d").date()
        except ValueError:
            return str(d)
    elif isinstance(d, datetime):
        d = d.date()
    return d.isoformat()


def load_base_css() -> str:
    p = TEMPLATES_DIR / "base.css"
    return p.read_text(encoding="utf-8") if p.is_file() else ""


def load_template(name: str) -> str:
    return (TEMPLATES_DIR / name).read_text(encoding="utf-8")


def chart_card(
    number: str,
    title: str,
    caption: str,
    src: str,
    tag: str = "",
    wide: bool = True,
    number_style: str = "",
    narrative: str = "",
) -> str:
    wide_cls = " wide" if wide else ""
    num_attr = f' style="{html.escape(number_style)}"' if number_style else ""
    tag_html = f'<div class="chart-tag">{html.escape(tag)}</div>' if tag else ""
    narr = ""
    if narrative:
        narr = f'<p class="chart-narrative">{html.escape(narrative)}</p>'
    return (
        f'<article class="chart-card{wide_cls}">'
        f'<div class="chart-head">'
        f'<div class="chart-number"{num_attr}>{html.escape(number)}</div>'
        f'<div class="chart-title-wrap">'
        f"<h3>{html.escape(title)}</h3>"
        f'<div class="chart-caption">{html.escape(caption)}</div>'
        f"</div>{tag_html}</div>"
        f'<div class="chart-body">'
        f'<img src="{html.escape(src)}" alt="{html.escape(title)}" onerror="showFallback(this)" />'
        f'<div class="image-fallback"><div><strong>이미지 파일을 찾을 수 없습니다.</strong>'
        f"HTML과 같은 폴더에 <code>{html.escape(src)}</code>를 두세요.</div></div>"
        f"</div>{narr}</article>"
    )


def fill_template(template_name: str, mapping: Mapping[str, Any]) -> str:
    """
    {{KEY}} 치환. BASE_CSS·*_HTML 키는 raw, 나머지는 html.escape.
    미치환 플레이스홀더는 '-'로.
    """
    tpl = load_template(template_name)
    raw_keys = {"BASE_CSS"} | {k for k in mapping if str(k).endswith("_HTML")}
    values = {"BASE_CSS": load_base_css()}
    for k, v in mapping.items():
        if v is None:
            values[k] = ""
        elif k in raw_keys:
            values[k] = str(v)
        else:
            values[k] = html.escape(str(v))

    def _repl(m: re.Match) -> str:
        key = m.group(1)
        if key in values:
            return values[key]
        return "-"

    return _PLACEHOLDER_RE.sub(_repl, tpl)


def write_design_html(
    template_name: str,
    out_path: Path,
    mapping: Mapping[str, Any],
) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(fill_template(template_name, mapping), encoding="utf-8")
    return out_path
