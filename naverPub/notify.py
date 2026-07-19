"""텔레그램 봇 전송. 토큰 미설정 시 스킵. _sec* 섹션 PNG만 전송."""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable, Optional

import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

log = logging.getLogger("naverPub.notify")

API = "https://api.telegram.org/bot{token}/{method}"
PHOTO_MAX_BYTES = 10 * 1024 * 1024
PHOTO_MAX_SUM_PX = 9000  # width+height (텔레그램 실질 한도 ~10000 여유)
PHOTO_MAX_ASPECT = 15.0  # max/min (텔레그램 실질 한도 20:1 여유)
SEND_GAP_SEC = 0.5

# day_root 하위 폴더 전송 순서
NOTIFY_FOLDERS: list[tuple[str, str]] = [
    ("tickers", "📊 시장 스냅샷"),
    ("martket", "📈 마켓 변동성"),
    ("pick", "🎯 Picking"),
    ("etfs", "🧺 액티브 ETF"),
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


def send_photo(path: Path, caption: str = "") -> bool:
    """저수준 sendPhoto."""
    if not enabled():
        return False
    path = Path(path)
    if not path.is_file():
        log.warning("파일 없음: %s", path)
        return False
    try:
        with open(path, "rb") as f:
            _post(
                "sendPhoto",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": (caption or "")[:1024]},
                files={"photo": (path.name, f)},
            )
        return True
    except Exception as e:
        log.error("Telegram photo 실패(%s): %s", path.name, e)
        return False


def send_document(path: Path, caption: str = "") -> bool:
    """sendDocument — 원본 무압축, 비율·픽셀 제한 없음(~50MB)."""
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


def _image_size_px(path: Path) -> tuple[int, int]:
    """(width, height). 실패 시 (0,0)."""
    try:
        from PIL import Image

        Image.MAX_IMAGE_PIXELS = max(getattr(Image, "MAX_IMAGE_PIXELS", 0) or 0, 300_000_000)
        with Image.open(path) as im:
            return int(im.size[0]), int(im.size[1])
    except Exception as e:
        log.warning("이미지 크기 읽기 실패(%s): %s", path.name, e)
        return 0, 0


def _unsuitable_for_photo(path: Path) -> tuple[bool, str]:
    """
    sendPhoto 부적합이면 (True, reason).
    - 10MB 초과
    - width+height > 9000
    - max/min > 15
    """
    size = path.stat().st_size
    if size > PHOTO_MAX_BYTES:
        return True, f"size={size / (1024 * 1024):.1f}MB>10MB"
    w, h = _image_size_px(path)
    if w <= 0 or h <= 0:
        # 치수 모를 때는 photo 시도 허용 (실패 시 document 폴백)
        return False, ""
    if (w + h) > PHOTO_MAX_SUM_PX:
        return True, f"w+h={w + h}>{PHOTO_MAX_SUM_PX} ({w}x{h})"
    aspect = max(w, h) / float(min(w, h))
    if aspect > PHOTO_MAX_ASPECT:
        return True, f"aspect={aspect:.1f}>15 ({w}x{h})"
    return False, ""


def send_image(path: Path, caption: str = "") -> bool:
    """
    sendPhoto 적합 여부 선판정 → 부적합/400이면 원본 sendDocument.
    리사이즈 재시도 없음(세로 긴 표는 비율 제한을 리사이즈로 해결 불가).
    """
    if not enabled():
        log.info("Telegram 미설정 — 이미지 스킵")
        return False
    path = Path(path)
    if not path.is_file():
        log.warning("파일 없음: %s", path)
        return False

    bad, reason = _unsuitable_for_photo(path)
    if bad:
        log.info("sendPhoto 부적합 → sendDocument: %s (%s)", path.name, reason)
        return send_document(path, caption=caption)

    if send_photo(path, caption=caption):
        return True
    log.info("sendPhoto 400/실패 → sendDocument 폴백: %s", path.name)
    return send_document(path, caption=caption)


def list_sec_pngs(folder: Path) -> list[Path]:
    """파일명에 '_sec' 포함 PNG만, 이름 오름차순."""
    folder = Path(folder)
    if not folder.is_dir():
        return []
    files = [
        p
        for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() == ".png" and "_sec" in p.name
    ]
    return sorted(files, key=lambda p: p.name)


def count_sec_pngs(day_root: Path) -> dict[str, int]:
    out = {}
    for folder_name, _title in NOTIFY_FOLDERS:
        out[folder_name] = len(list_sec_pngs(Path(day_root) / folder_name))
    return out


def notify_day_outputs(
    day_root: Path,
    as_of: str,
    *,
    summary: str,
    screen_pass: Optional[int] = None,
    errors: Optional[list] = None,
) -> None:
    """
    outputs/YYYYMMDD/{tickers,martket,pick,etfs} 의 _sec*.png 만 전송.
    폴더마다 제목 메시지 후 섹션 이미지 순차 전송.
    """
    if not enabled():
        log.info("Telegram 미설정 — notify 전체 스킵")
        return

    day_root = Path(day_root)
    sec_counts = count_sec_pngs(day_root)
    total_sec = sum(sec_counts.values())

    head = summary.strip() if summary else f"naverPub {as_of} 완료"
    lines = [head, ""]
    if screen_pass is not None:
        lines.append(f"스크리닝 통과: {screen_pass}종")
    lines.append("섹션 이미지(_sec):")
    for folder_name, title in NOTIFY_FOLDERS:
        lines.append(f"  · {title}: {sec_counts.get(folder_name, 0)}장")
    lines.append(f"합계: {total_sec}장")
    send_text("\n".join(lines))
    time.sleep(SEND_GAP_SEC)

    sent = 0
    failed = 0
    for folder_name, title in NOTIFY_FOLDERS:
        folder = day_root / folder_name
        files = list_sec_pngs(folder)
        send_text(f"{title} · {as_of}")
        time.sleep(SEND_GAP_SEC)
        if not files:
            log.info("전송할 _sec 없음: %s", folder)
            continue
        for i, png in enumerate(files, start=1):
            caption = f"{title} ({i}/{len(files)}) · {png.name}"
            ok = send_image(png, caption=caption)
            if ok:
                sent += 1
            else:
                failed += 1
                log.warning("전송 실패 계속: %s", png)
            time.sleep(SEND_GAP_SEC)

    if errors:
        send_text("⚠️ 일부 실패:\n" + "\n".join(f"- {e}" for e in errors))
        time.sleep(SEND_GAP_SEC)

    log.info("Telegram 전송 완료 sent=%d failed=%d total_sec=%d", sent, failed, total_sec)


def notify_bundle(articles: Iterable[dict], summary: str, errors: Optional[list] = None) -> None:
    """하위 호환: articles 경로 무시. summary만 보내고 종료(구 호출 대비)."""
    log.warning("notify_bundle은 deprecated — notify_day_outputs 사용 권장")
    if not enabled():
        log.info("Telegram 미설정 — notify 전체 스킵")
        return
    send_text(summary)
    if errors:
        send_text("⚠️ 일부 실패:\n" + "\n".join(f"- {e}" for e in errors))
