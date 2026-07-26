"""
루트 공통 환경설정 (.env).
각 스크립트에서: from env_config import load_project_env, require_env, db_url, db_connect_kwargs

Spyder 셀(cwd 임의)·F5·CLI 모두에서 find_repo_root() 로 루트를 찾는다.
부트스트랩(import 전)은 동일 규칙의 인라인 사본을 쓴다.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List, Optional, Union

_LOADED = False
_MARKERS = ("env_config.py", ".env", ".git")


def _is_repo_root(path: Path) -> bool:
    return any((path / m).exists() for m in _MARKERS)


def _walk_up_for_root(start: Path) -> Optional[Path]:
    try:
        start = Path(start).expanduser().resolve()
    except Exception:
        return None
    if not start.exists():
        return None
    if start.is_file():
        start = start.parent
    for p in [start, *start.parents]:
        if _is_repo_root(p):
            return p
    return None


def _candidate_starts(
    *,
    start: Optional[Path] = None,
    file_hint: Optional[Path] = None,
) -> List[Path]:
    """탐색 시작점 목록 (우선순위 순, 중복 제거는 호출측)."""
    out: List[Path] = []

    if start is not None:
        out.append(Path(start))

    env_root = os.environ.get("REPO_ROOT", "").strip()
    if env_root:
        out.append(Path(env_root))

    if file_hint is not None:
        out.append(Path(file_hint))
    else:
        try:
            here = Path(__file__).resolve()
            out.append(here if here.is_dir() else here.parent)
        except NameError:
            pass

    # Spyder 셀: 프레임에 열린 .py 경로가 남는 경우
    try:
        import inspect

        for fi in inspect.stack():
            fn = getattr(fi, "filename", None) or ""
            if not fn or fn.startswith("<"):
                continue
            try:
                p = Path(fn).resolve()
            except Exception:
                continue
            if p.suffix.lower() == ".py" and p.is_file():
                out.append(p.parent)
    except Exception:
        pass

    out.append(Path.cwd())

    for item in sys.path:
        if not item or item == ".":
            continue
        try:
            p = Path(item)
            if p.is_dir():
                out.append(p)
        except Exception:
            continue

    return out


def find_repo_root(
    start: Optional[Union[Path, str]] = None,
    *,
    file_hint: Optional[Union[Path, str]] = None,
) -> Path:
    """
    프로젝트 루트 탐색.
    우선순위: REPO_ROOT → __file__/file_hint → inspect 프레임 → cwd → sys.path
    각 후보에서 env_config.py / .env / .git 마커를 상향 탐색.
    """
    tried: List[str] = []
    seen = set()

    env_root = os.environ.get("REPO_ROOT", "").strip()
    if env_root:
        er = Path(env_root).expanduser()
        try:
            er = er.resolve()
        except Exception as e:
            raise RuntimeError(
                f"REPO_ROOT 경로를 해석할 수 없습니다: {env_root!r} ({e})\n"
                "REPO_ROOT 환경변수를 리포 루트로 지정하거나 F5로 실행하세요"
            ) from e
        tried.append(str(er))
        if not er.is_dir():
            raise RuntimeError(
                f"REPO_ROOT 가 디렉터리가 아닙니다: {er}\n"
                "REPO_ROOT 환경변수를 리포 루트로 지정하거나 F5로 실행하세요"
            )
        if _is_repo_root(er):
            return er
        found = _walk_up_for_root(er)
        if found:
            return found
        raise RuntimeError(
            f"REPO_ROOT={er} 에서 마커(env_config.py / .env / .git)를 찾지 못했습니다.\n"
            "REPO_ROOT 환경변수를 리포 루트로 지정하거나 F5로 실행하세요"
        )

    starts = _candidate_starts(
        start=Path(start) if start is not None else None,
        file_hint=Path(file_hint) if file_hint is not None else None,
    )
    for c in starts:
        try:
            key = str(Path(c).expanduser().resolve())
        except Exception:
            key = str(c)
        if key in seen:
            continue
        seen.add(key)
        tried.append(key)
        found = _walk_up_for_root(Path(c))
        if found:
            return found

    raise RuntimeError(
        "프로젝트 루트를 찾지 못했습니다 (env_config.py / .env / .git).\n"
        f"탐색 후보:\n  - " + "\n  - ".join(tried) + "\n"
        "REPO_ROOT 환경변수를 리포 루트로 지정하거나 F5로 실행하세요"
    )


def find_project_root(start: Optional[Path] = None) -> Path:
    """하위 호환 별칭 → find_repo_root."""
    return find_repo_root(start=start)


_ROOT = find_repo_root()


def project_root() -> Path:
    return _ROOT


def load_project_env(dotenv_path: Optional[Path] = None) -> Path:
    """루트 .env 로드. 반환: 사용한 .env 경로(없으면 루트/.env 후보 경로)."""
    global _LOADED, _ROOT
    try:
        from dotenv import load_dotenv
    except ImportError as e:
        raise RuntimeError(
            "python-dotenv 가 필요합니다. pip install python-dotenv"
        ) from e

    if dotenv_path is not None:
        path = Path(dotenv_path)
        _ROOT = find_repo_root(start=path.parent if path.is_file() else path)
    else:
        _ROOT = find_repo_root()
        path = _ROOT / ".env"

    if path.is_file():
        load_dotenv(path, override=False)
    else:
        load_dotenv(override=False)
    _LOADED = True
    return path


def require_env(key: str) -> str:
    if not _LOADED:
        load_project_env()
    val = os.getenv(key)
    if val is None or str(val).strip() == "":
        raise RuntimeError(
            f"환경변수 {key} 가 설정되지 않았습니다. "
            f"프로젝트 루트 .env 를 확인하세요 ({_ROOT / '.env'})."
        )
    return str(val).strip()


def getenv_or(key: str, default: str = "") -> str:
    if not _LOADED:
        load_project_env()
    v = os.getenv(key)
    return default if v is None or str(v).strip() == "" else str(v).strip()


def db_url(*, require: bool = True) -> str:
    """
    DB_URL 이 있으면 그대로, 없으면 DB_USER/PASSWORD/HOST/PORT/NAME 조합.
    """
    if not _LOADED:
        load_project_env()
    url = os.getenv("DB_URL")
    if url and str(url).strip():
        return str(url).strip()
    if require:
        user = require_env("DB_USER")
        password = require_env("DB_PASSWORD")
        host = require_env("DB_HOST")
        port = getenv_or("DB_PORT", "3306")
        name = require_env("DB_NAME")
    else:
        user = getenv_or("DB_USER", "root")
        password = getenv_or("DB_PASSWORD", "")
        host = getenv_or("DB_HOST", "127.0.0.1")
        port = getenv_or("DB_PORT", "3306")
        name = getenv_or("DB_NAME", "kor_stock_db")
        if not password:
            raise RuntimeError("DB_PASSWORD 또는 DB_URL 이 필요합니다.")
    from urllib.parse import quote_plus

    return f"mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{name}"


def db_connect_kwargs() -> dict:
    """pymysql.connect(**kwargs) 용."""
    return {
        "user": require_env("DB_USER"),
        "passwd": require_env("DB_PASSWORD"),
        "host": require_env("DB_HOST"),
        "port": int(getenv_or("DB_PORT", "3306")),
        "db": require_env("DB_NAME"),
        "charset": "utf8",
    }
