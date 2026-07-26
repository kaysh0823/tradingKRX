"""텔레그램 봇 전송. 토큰 미설정 시 스킵. 폴더별 원본 PNG + 디자인 HTML(sendDocument)."""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable, Optional

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

log = logging.getLogger("naverPub.notify")

API = "https://api.telegram.org/bot{token}/{method}"
SEND_GAP_SEC = 0.5

# day_root 하위 폴더 전송 순서 · (폴더명, 제목, 디자인 HTML 파일명)
NOTIFY_FOLDERS: list[tuple[str, str, str]] = [
    ("tickers", "📊 시장 스냅샷", "stocks.html"),
    ("martket", "📈 마켓 변동성", "market.html"),
    ("pick", "🎯 종목 선정", "pick.html"),
    ("etfs", "🧺 액티브 ETF", "etf.html"),
]


def enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _post(method: str, **kwargs) -> dict:
    url = API.format(token=TELEGRAM_BOT_TOKEN, method=method)
    r = requests.post(url, timeout=120, **kwargs)
    r.raise_for_status()
    return r.json()


def send_text(text: str, parse_mode: Optional[str] = None) -> bool:
    if not enabled():
        log.info("Telegram 미설정 — 메시지 스킵")
        return False
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]}
    if parse_mode:
        data["parse_mode"] = parse_mode
    try:
        _post("sendMessage", data=data)
        return True
    except Exception as e:
        log.error("Telegram text 실패: %s", e)
        return False


def send_document(path: Path, caption: str = "") -> bool:
    """sendDocument — 원본 무압축(~50MB)."""
    if not enabled():
        log.info("Telegram 미설정 — document 스킵")
        return False
    path = Path(path)
    if not path.is_file():
        log.warning("파일 없음: %s", path)
        return False
    try:
        with open(path, "rb") as f:
            _post(
                "sendDocument",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": (caption or "")[:1024]},
                files={"document": (path.name, f)},
            )
        return True
    except Exception as e:
        log.error("Telegram document 실패(%s): %s", path.name, e)
        return False


def list_top_pngs(folder: Path) -> list[Path]:
    """폴더 최상위 *.png만 (서브폴더·capture 제외), 이름 오름차순."""
    folder = Path(folder)
    if not folder.is_dir():
        return []
    return sorted(
        [p for p in folder.glob("*.png") if p.is_file()],
        key=lambda p: p.name,
    )


def count_top_pngs(day_root: Path) -> dict[str, int]:
    out = {}
    for folder_name, _title, _html in NOTIFY_FOLDERS:
        out[folder_name] = len(list_top_pngs(Path(day_root) / folder_name))
    return out


def design_html_path(folder: Path, html_name: str) -> Optional[Path]:
    p = Path(folder) / html_name
    return p if p.is_file() else None


def notify_day_outputs(
    day_root: Path,
    as_of: str,
    *,
    summary: str,
    screen_pass: Optional[int] = None,
    errors: Optional[list] = None,
) -> None:
    """
    폴더별 최상위 *.png(이름순) → 단일 디자인 HTML 을 전부 sendDocument.
    """
    if not enabled():
        log.info("Telegram 미설정 — notify 전체 스킵")
        return

    day_root = Path(day_root)
    png_counts = count_top_pngs(day_root)
    total_png = sum(png_counts.values())
    html_flags: dict[str, bool] = {}
    for folder_name, _title, html_name in NOTIFY_FOLDERS:
        html_flags[folder_name] = design_html_path(day_root / folder_name, html_name) is not None

    head = summary.strip() if summary else f"naverPub {as_of} 완료"
    lines = [head, ""]
    if screen_pass is not None:
        lines.append(f"스크리닝 통과: {screen_pass}종")
    lines.append("원본 PNG + 디자인 HTML (sendDocument):")
    for folder_name, title, _html in NOTIFY_FOLDERS:
        html_mark = "HTML O" if html_flags.get(folder_name) else "HTML X"
        lines.append(f"  · {title}: PNG {png_counts.get(folder_name, 0)}장 · {html_mark}")
    lines.append(f"PNG 합계: {total_png}장")
    send_text("\n".join(lines))
    time.sleep(SEND_GAP_SEC)

    sent = 0
    failed = 0
    for folder_name, title, html_name in NOTIFY_FOLDERS:
        folder = day_root / folder_name
        pngs = list_top_pngs(folder)
        html_path = design_html_path(folder, html_name)
        send_text(f"{title} · {as_of}")
        time.sleep(SEND_GAP_SEC)

        if not pngs and html_path is None:
            log.info("전송할 PNG/HTML 없음: %s", folder)
            continue

        for i, img in enumerate(pngs, start=1):
            mb = img.stat().st_size / (1024 * 1024)
            caption = f"{title} PNG ({i}/{len(pngs)}) · {img.name} ({mb:.1f}MB)"
            ok = send_document(img, caption=caption)
            if ok:
                sent += 1
            else:
                failed += 1
                log.warning("전송 실패 계속: %s", img)
            time.sleep(SEND_GAP_SEC)

        if html_path is not None:
            mb = html_path.stat().st_size / (1024 * 1024)
            caption = f"{title} HTML · {html_path.name} ({mb:.1f}MB)"
            ok = send_document(html_path, caption=caption)
            if ok:
                sent += 1
            else:
                failed += 1
                log.warning("전송 실패 계속: %s", html_path)
            time.sleep(SEND_GAP_SEC)
        else:
            log.info("디자인 HTML 없음(스킵): %s/%s", folder_name, html_name)

    if errors:
        send_text("⚠️ 일부 실패:\n" + "\n".join(f"- {e}" for e in errors))
        time.sleep(SEND_GAP_SEC)

    log.info(
        "Telegram 전송 완료 sent=%d failed=%d total_png=%d html=%d",
        sent,
        failed,
        total_png,
        sum(1 for v in html_flags.values() if v),
    )


def notify_bundle(articles: Iterable[dict], summary: str, errors: Optional[list] = None) -> None:
    """하위 호환: articles 경로 무시. summary만 보내고 종료(구 호출 대비)."""
    log.warning("notify_bundle은 deprecated — notify_day_outputs 사용 권장")
    if not enabled():
        log.info("Telegram 미설정 — notify 전체 스킵")
        return
    send_text(summary)
    if errors:
        send_text("⚠️ 일부 실패:\n" + "\n".join(f"- {e}" for e in errors))
