"""텔레그램 봇 전송. 토큰 미설정 시 스킵."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

log = logging.getLogger("naverPub.notify")

API = "https://api.telegram.org/bot{token}/{method}"


def enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _post(method: str, **kwargs) -> dict:
    url = API.format(token=TELEGRAM_BOT_TOKEN, method=method)
    r = requests.post(url, timeout=60, **kwargs)
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


def send_photo(path: Path, caption: str = "") -> bool:
    if not enabled():
        log.info("Telegram 미설정 — 사진 스킵")
        return False
    path = Path(path)
    if not path.is_file():
        log.warning("파일 없음: %s", path)
        return False
    try:
        with open(path, "rb") as f:
            _post(
                "sendPhoto",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1024]},
                files={"photo": f},
            )
        return True
    except Exception as e:
        log.error("Telegram photo 실패: %s", e)
        return False


def notify_bundle(articles: Iterable[dict], summary: str, errors: Optional[list] = None) -> None:
    if not enabled():
        log.info("Telegram 미설정 — notify 전체 스킵")
        return
    send_text(summary)
    for a in articles:
        png = a.get("png")
        caption = f"{a.get('title', '')}\n{a.get('text', '')}"[:1024]
        if png:
            send_photo(png, caption=caption)
    if errors:
        send_text("⚠️ 일부 실패:\n" + "\n".join(f"- {e}" for e in errors))
