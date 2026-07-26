"""naverPub 설정 — 모든 시크릿은 .env에서 로드. 하드코딩 금지."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=True)

# ── MySQL (VPS 로컬) ──────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "naverpub")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "naverpub")

# ── KRX 로그인 ────────────────────────────────────────────────
KRX_ID = os.getenv("KRX_ID", "")
KRX_PW = os.getenv("KRX_PW", "")

# ── Telegram (옵션) ───────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ── 경로·동작 ─────────────────────────────────────────────────
OUTPUTS_DIR = Path(os.getenv("OUTPUTS_DIR", str(ROOT / "outputs")))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "60"))
BIZDAY_NET_RETRIES = int(os.getenv("BIZDAY_NET_RETRIES", "3"))
BIZDAY_NET_RETRY_WAIT = float(os.getenv("BIZDAY_NET_RETRY_WAIT", "5"))
BIZDAY_LOOKBACK = int(os.getenv("BIZDAY_LOOKBACK", "10"))
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "1.0"))

# 한글 폰트 (Linux: NanumGothic, Windows: Malgun Gothic)
FONT_FAMILY = os.getenv("FONT_FAMILY", "NanumGothic")

# Playwright 렌더 배율 (표·조각 캡처 device_scale_factor). 기본 3.
RENDER_SCALE = int(os.getenv("RENDER_SCALE", "3"))

# 그래프·표 PNG 목표 가로 픽셀 (텔레그램 나열 시 폭 통일)
OUTPUT_WIDTH_PX = int(os.getenv("OUTPUT_WIDTH_PX", "1400"))
# (legacy) matplotlib DPI — 변동성 차트는 Plotly+kaleido(RENDER_SCALE) 사용
FIG_DPI = int(os.getenv("FIG_DPI", "140"))

# 로컬→VPS 이관용 (migrate_initial.py)
LOCAL_DB_HOST = os.getenv("LOCAL_DB_HOST", "127.0.0.1")
LOCAL_DB_PORT = int(os.getenv("LOCAL_DB_PORT", "3306"))
LOCAL_DB_USER = os.getenv("LOCAL_DB_USER", "root")
LOCAL_DB_PASSWORD = os.getenv("LOCAL_DB_PASSWORD", "")
LOCAL_DB_NAME = os.getenv("LOCAL_DB_NAME", "kor_stock_db")


def db_config() -> dict:
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USER,
        "passwd": DB_PASSWORD,
        "db": DB_NAME,
        "charset": "utf8mb4",
    }


def db_url() -> str:
    return (
        f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )


def require_krx_credentials() -> None:
    if not (KRX_ID and KRX_PW):
        raise RuntimeError("KRX_ID / KRX_PW 가 .env에 없습니다.")
