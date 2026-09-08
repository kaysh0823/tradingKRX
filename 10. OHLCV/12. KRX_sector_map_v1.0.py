# -*- coding: utf-8 -*-
"""
KRX 전종목 업종/섹터 마스터 구축 (수동 실행).

투자맵(investingmap) 수집 + sector/sector_rules.csv 규칙으로
krx_sector_master / krx_stock_sector_map 를 적재한다.
11번 OHLCV 파이프라인에 연결하지 않는다 — Spyder F5 단독 실행.
주의: krx_ticker_sector(sector_cd) 는 11번 RS 유니버스용 기존 테이블이다. 사용 금지.
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urlunparse


def _find_repo_root():
    """env_config.find_repo_root 와 동일 규칙 (import 전용 인라인)."""
    markers = ("env_config.py", ".env", ".git")

    def _is_root(p: Path) -> bool:
        return any((p / m).exists() for m in markers)

    def _walk_up(start: Path):
        try:
            start = Path(start).expanduser().resolve()
        except Exception:
            return None
        if not start.exists():
            return None
        if start.is_file():
            start = start.parent
        for p in [start, *start.parents]:
            if _is_root(p):
                return p
        return None

    tried = []
    seen = set()
    _nl = chr(10)
    _hint = _nl + "REPO_ROOT 환경변수를 리포 루트로 지정하거나 F5로 실행하세요"

    env_root = os.environ.get("REPO_ROOT", "").strip()
    if env_root:
        er = Path(env_root).expanduser()
        try:
            er = er.resolve()
        except Exception as e:
            raise RuntimeError(
                "REPO_ROOT 경로를 해석할 수 없습니다: {!r} ({}){}".format(
                    env_root, e, _hint
                )
            ) from e
        tried.append(str(er))
        if not er.is_dir():
            raise RuntimeError(
                "REPO_ROOT 가 디렉터리가 아닙니다: {}{}".format(er, _hint)
            )
        if _is_root(er):
            return er
        found = _walk_up(er)
        if found:
            return found
        raise RuntimeError(
            "REPO_ROOT={} 에서 마커(env_config.py / .env / .git)를 찾지 못했습니다.{}".format(
                er, _hint
            )
        )

    starts = []
    try:
        here = Path(__file__).resolve()
        starts.append(here if here.is_dir() else here.parent)
    except NameError:
        pass
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
                starts.append(p.parent)
    except Exception:
        pass
    starts.append(Path.cwd())
    for item in sys.path:
        if not item or item == ".":
            continue
        try:
            p = Path(item)
            if p.is_dir():
                starts.append(p)
        except Exception:
            continue

    for c in starts:
        try:
            key = str(Path(c).expanduser().resolve())
        except Exception:
            key = str(c)
        if key in seen:
            continue
        seen.add(key)
        tried.append(key)
        found = _walk_up(Path(c))
        if found:
            return found

    raise RuntimeError(
        "프로젝트 루트를 찾지 못했습니다 (env_config.py / .env / .git)."
        + _nl
        + "탐색 후보:"
        + _nl
        + "  - "
        + (_nl + "  - ").join(tried)
        + _hint
    )


_ROOT = _find_repo_root()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from env_config import load_project_env, require_env, db_url, db_connect_kwargs

load_project_env()

import csv
from collections import defaultdict

import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
IMAP_BASE = "https://investingmap.kr"
IMAP_INDEX_JSON = "/data/search_index.json"
IMAP_SLEEP_SEC = 1.0  # 예의상 요청 간 대기 — 줄이지 말 것
IMAP_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
SNAPSHOT_DATE = None  # None 이면 today
SECTOR_RULES_CSV = os.path.join(_ROOT, "sector", "sector_rules.csv")
IMAP_ALIAS_CSV = os.path.join(_ROOT, "sector", "imap_sector_alias.csv")

BATCH_SIZE = 1000
IMAP_MAP_HREF_RE = re.compile(r"korea_[^/\"'?#]*_map\.html", re.IGNORECASE)
SECTOR_TITLE_RE = re.compile(
    r"한국\s+(.+?)\s+(?:산업\s+)?투자\s+지도",
    re.UNICODE,
)
_UNIV_SQL = """
    SELECT 종목코드 FROM krx_ticker
    WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
      AND 종목구분 = '보통주'
"""


def _snapshot_date() -> date:
    if SNAPSHOT_DATE is None:
        return date.today()
    if isinstance(SNAPSHOT_DATE, date):
        return SNAPSHOT_DATE
    return datetime.strptime(str(SNAPSHOT_DATE), "%Y-%m-%d").date()


def make_sector_key(sector_name: str, chain_name: Optional[str]) -> str:
    """chain 이 있으면 '{sector}_{chain}', 없으면 sector.
    key 용 chain 에서 · / 공백 제거. 원본 chain 은 master 에 보존.
    """
    sn = (sector_name or "").strip()
    if not sn:
        raise ValueError("sector_name 이 비어 있습니다")
    cn = (chain_name or "").strip()
    if not cn:
        return sn
    cn_key = cn.replace("·", "").replace("/", "").replace(" ", "")
    return f"{sn}_{cn_key}"


def load_imap_sector_alias(path: str) -> Dict[str, str]:
    """imap_sector_alias.csv → {imap_sector: major}."""
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"투자맵 섹터 별칭 파일이 없습니다: {path}\n"
            "sector/imap_sector_alias.csv 를 준비한 뒤 다시 실행하세요."
        )
    alias: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required_cols = {"imap_sector", "major"}
        if reader.fieldnames is None:
            raise RuntimeError(f"별칭표 헤더가 없습니다: {path}")
        got = {c.strip() for c in reader.fieldnames}
        missing = sorted(required_cols - got)
        if missing:
            raise RuntimeError(
                f"별칭표 컬럼 누락 {missing}: {path} (필요: imap_sector,major)"
            )
        for i, row in enumerate(reader, start=2):
            imap_sector = (row.get("imap_sector") or "").strip()
            major = (row.get("major") or "").strip()
            if not imap_sector or not major:
                raise RuntimeError(
                    f"별칭표 {i}행에 빈 값: imap_sector={imap_sector!r} major={major!r}"
                )
            alias[imap_sector] = major
    print(f"투자맵 섹터 별칭 {len(alias)}건")
    return alias


def resolve_imap_major(
    imap_sector: str,
    alias: Dict[str, str],
    missing_out: set,
) -> str:
    """투자맵 섹터 한글명 → 표준 major. 미등록 시 원본 사용·경고."""
    name = (imap_sector or "").strip()
    major = alias.get(name)
    if major is None:
        print(f"⚠️ 투자맵 섹터 '{name}' 별칭 없음 — 원본명 사용")
        missing_out.add(name)
        return name
    return major


def _imap_get(url: str, *, retries: int = 2, timeout: int = 20) -> requests.Response:
    """실패 시 retries 회 재시도(총 1+retries 회). 최종 실패는 예외."""
    last_err: Optional[BaseException] = None
    attempts = 1 + max(0, int(retries))
    for i in range(attempts):
        try:
            res = requests.get(url, headers={"User-Agent": IMAP_UA}, timeout=timeout)
            res.raise_for_status()
            res.encoding = "utf-8"          # ← 없으면 latin-1 로 추정되어 헤더가 전부 깨진다
            return res
        except Exception as e:
            last_err = e
            if i < attempts - 1:
                time.sleep(IMAP_SLEEP_SEC)
    assert last_err is not None
    raise last_err


def _clean_cell(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def _norm_header(text: str) -> str:
    """헤더 비교용: 공백·`·` 제거."""
    return _clean_cell(text).replace(" ", "").replace("·", "")


def _extract_sector_name_ko(h1_or_title: str) -> str:
    raw = _clean_cell(h1_or_title)
    raw = re.sub(r"[\U0001F1E0-\U0001F1FF]+", "", raw)  # 국기 이모지
    raw = _clean_cell(raw)
    m = SECTOR_TITLE_RE.search(raw)
    if m:
        return _clean_cell(m.group(1))
    # 폴백: '투자 지도' 앞 토큰
    m2 = re.search(r"(.+?)\s*투자\s*지도", raw)
    if m2:
        name = _clean_cell(m2.group(1))
        if name.startswith("한국"):
            name = _clean_cell(name[2:])
        name = re.sub(r"\s*산업\s*$", "", name).strip()
        if name:
            return name
    return raw


def fetch_imap_index() -> Dict[str, Dict[str, str]]:
    """GET /data/search_index.json → {ticker: {name, slug}}."""
    url = urljoin(IMAP_BASE.rstrip("/") + "/", IMAP_INDEX_JSON.lstrip("/"))
    resp = _imap_get(url)
    data = resp.json()
    out: Dict[str, Dict[str, str]] = {}
    if not isinstance(data, list):
        raise RuntimeError(f"search_index.json 형식이 list 가 아닙니다: {type(data)}")
    for item in data:
        if not isinstance(item, dict):
            continue
        t = str(item.get("t") or "").strip()
        if not t:
            continue
        out[t] = {
            "name": str(item.get("k") or "").strip(),
            "slug": str(item.get("s") or "").strip(),
        }
    return out


def fetch_imap_sector_pages() -> List[str]:
    """인덱스 HTML 의 a[href] 중 korea_*_map.html 패턴 URL 목록 (절대경로)."""
    url = urljoin(IMAP_BASE.rstrip("/") + "/", "index.html?lang=ko")
    resp = _imap_get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    found: List[str] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        # query/hash 제거 후 패턴 검사
        path_only = href.split("?", 1)[0].split("#", 1)[0]
        if not IMAP_MAP_HREF_RE.search(path_only):
            continue
        abs_url = urljoin(url, href)
        # 정규화: fragment 제거, 동일 path 중복 제거
        parts = urlparse(abs_url)
        norm = urlunparse((parts.scheme, parts.netloc, parts.path, "", "", ""))
        if norm in seen:
            continue
        seen.add(norm)
        found.append(norm)
    found.sort()
    return found


def parse_imap_map_page(
    html_text: str,
) -> Tuple[str, str, List[str], List[Dict[str, Any]]]:
    """맵 HTML 파싱.

    반환: (sector_slug, sector_name, chains, rows)
    rows 항목: ticker, chain_name, detail_name, product
    """
    soup = BeautifulSoup(html_text, "html.parser")
    body = soup.find("body")
    if body is None:
        raise RuntimeError("body 태그가 없습니다")
    sector_slug = (body.get("data-sector") or "").strip()
    if not sector_slug:
        raise RuntimeError('body data-sector 가 없습니다')

    h1 = soup.find("h1")
    title = soup.find("title")
    title_text = ""
    if h1 is not None:
        title_text = h1.get_text(" ", strip=True)
    if not title_text and title is not None:
        title_text = title.get_text(" ", strip=True)
    sector_name = _extract_sector_name_ko(title_text)
    if not sector_name:
        raise RuntimeError("섹터 한글명을 추출하지 못했습니다")

    chains: List[str] = []
    seen_chain = set()
    for el in soup.find_all(attrs={"data-filter-chain": True}):
        ch = _clean_cell(str(el.get("data-filter-chain") or ""))
        if not ch or ch.lower() == "all":
            continue
        if ch not in seen_chain:
            seen_chain.add(ch)
            chains.append(ch)

    table = soup.find("table")
    if table is None:
        raise RuntimeError("table 이 없습니다")

    first_tr = table.find("tr")
    if first_tr is None:
        raise RuntimeError("table 헤더 행이 없습니다")
    headers = [_clean_cell(th.get_text(" ", strip=True)) for th in first_tr.find_all("th")]
    if not headers:
        headers = [_clean_cell(td.get_text(" ", strip=True)) for td in first_tr.find_all("td")]
    headers_norm = [_norm_header(h) for h in headers]

    def _find_col(pred) -> Optional[int]:
        for i, hn in enumerate(headers_norm):
            if pred(hn):
                return i
        return None

    # chain: '벨류체인' 포함 → 없으면 '섹터' 정확일치 ('섹터요약' 제외)
    idx_chain = _find_col(lambda hn: "벨류체인" in hn)
    if idx_chain is None:
        idx_chain = _find_col(lambda hn: hn == "섹터")
    # detail: '유형'으로 끝 → 없으면 '섹터요약' → 없으면 None
    idx_detail = _find_col(lambda hn: hn.endswith("유형"))
    if idx_detail is None:
        idx_detail = _find_col(lambda hn: hn == "섹터요약")
    # product: '주요제품'으로 시작 → 없으면 '핵심테마' → 없으면 None
    idx_product = _find_col(lambda hn: hn.startswith("주요제품"))
    if idx_product is None:
        idx_product = _find_col(lambda hn: hn == "핵심테마")

    if idx_chain is None:
        raise RuntimeError(f"벨류체인/섹터 컬럼을 찾지 못함. headers={headers}")

    rows: List[Dict[str, Any]] = []
    for tr in table.find_all("tr", attrs={"data-ticker": True}):
        ticker = str(tr.get("data-ticker") or "").strip()
        if not ticker:
            continue
        cells = tr.find_all(["td", "th"])

        def _cell(i: Optional[int]) -> str:
            if i is None or i < 0 or i >= len(cells):
                return ""
            return _clean_cell(cells[i].get_text(" ", strip=True))

        chain_name = _cell(idx_chain)
        detail_name = _cell(idx_detail) or None
        product = _cell(idx_product) or None
        rows.append(
            {
                "ticker": ticker,
                "chain_name": chain_name or None,
                "detail_name": detail_name,
                "product": product,
            }
        )
        # 정적 HTML 에 filter-chip 이 없어도 행 체인으로 목록 보강
        if chain_name and chain_name not in seen_chain:
            seen_chain.add(chain_name)
            chains.append(chain_name)

    return sector_slug, sector_name, chains, rows


def ensure_sector_tables(mycursor, con) -> None:
    mycursor.execute(
        """
        CREATE TABLE IF NOT EXISTS krx_sector_master (
            sector_key   VARCHAR(96)  NOT NULL,
            sector_name  VARCHAR(64)  NOT NULL,
            chain_name   VARCHAR(64)  NULL,
            sector_slug  VARCHAR(32)  NULL,
            source       VARCHAR(20)  NOT NULL,
            sort_order   INT          NULL,
            PRIMARY KEY (sector_key),
            INDEX idx_sm_source (source),
            INDEX idx_sm_sector (sector_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    mycursor.execute(
        """
        CREATE TABLE IF NOT EXISTS krx_stock_sector_map (
            ticker        VARCHAR(10)  NOT NULL,
            sector_key    VARCHAR(96)  NOT NULL,
            detail_name   VARCHAR(128) NULL,
            product       VARCHAR(255) NULL,
            source        VARCHAR(20)  NOT NULL,
            priority      TINYINT      NOT NULL,
            is_primary    TINYINT(1)   NOT NULL DEFAULT 0,
            snapshot_date DATE         NOT NULL,
            `rank`        TINYINT      NOT NULL DEFAULT 99,
            PRIMARY KEY (ticker, sector_key, source),
            INDEX idx_ssm_ticker (ticker),
            INDEX idx_ssm_key (sector_key),
            INDEX idx_ssm_primary (ticker, is_primary),
            INDEX idx_ssm_rank (ticker, `rank`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    con.commit()

    mycursor.execute("SHOW COLUMNS FROM krx_stock_sector_map")
    cols = {row[0] for row in mycursor.fetchall()}
    if "rank" not in cols:
        mycursor.execute(
            """
            ALTER TABLE krx_stock_sector_map
            ADD COLUMN `rank` TINYINT NOT NULL DEFAULT 99
            """
        )
        con.commit()
        mycursor.execute("SHOW COLUMNS FROM krx_stock_sector_map")
        cols = {row[0] for row in mycursor.fetchall()}

    required = {
        "ticker",
        "sector_key",
        "detail_name",
        "product",
        "source",
        "priority",
        "is_primary",
        "snapshot_date",
        "rank",
    }
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(
            "krx_stock_sector_map 테이블이 업종 마스터 스키마가 아닙니다 "
            f"(누락 컬럼: {missing}). "
            "테이블을 확인하고 스키마를 맞춘 뒤 다시 실행하세요."
        )


def _chunked(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def upsert_sector_master(mycursor, con, rows: List[Tuple]) -> int:
    """rows: (sector_key, sector_name, chain_name, sector_slug, source, sort_order)"""
    if not rows:
        return 0
    sql = """
        INSERT INTO krx_sector_master
            (sector_key, sector_name, chain_name, sector_slug, source, sort_order)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            sector_name = VALUES(sector_name),
            chain_name  = VALUES(chain_name),
            sector_slug = VALUES(sector_slug),
            source      = VALUES(source),
            sort_order  = VALUES(sort_order)
    """
    n = 0
    for batch in _chunked(rows, BATCH_SIZE):
        mycursor.executemany(sql, list(batch))
        n += len(batch)
    con.commit()
    return n


def upsert_ticker_sector(mycursor, con, rows: List[Tuple]) -> int:
    """rows: (ticker, sector_key, detail_name, product, source, priority,
    is_primary, snapshot_date, rank)"""
    if not rows:
        return 0
    sql = """
        INSERT INTO krx_stock_sector_map
            (ticker, sector_key, detail_name, product, source, priority,
             is_primary, snapshot_date, `rank`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            detail_name   = VALUES(detail_name),
            product       = VALUES(product),
            priority      = VALUES(priority),
            is_primary    = VALUES(is_primary),
            snapshot_date = VALUES(snapshot_date),
            `rank`        = VALUES(`rank`)
    """
    n = 0
    for batch in _chunked(rows, BATCH_SIZE):
        mycursor.executemany(sql, list(batch))
        n += len(batch)
    con.commit()
    return n


def load_sector_rules(
    path: str,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """sector_rules.csv → {(source, label): {major, sub, sector_key, weight}}."""
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"규칙표 파일이 없습니다: {path}\n"
            "sector/sector_rules.csv 를 준비한 뒤 다시 실행하세요."
        )
    rules: Dict[Tuple[str, str], Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required_cols = {"source", "label", "major", "sub", "weight"}
        if reader.fieldnames is None:
            raise RuntimeError(f"규칙표 헤더가 없습니다: {path}")
        got = {c.strip() for c in reader.fieldnames}
        missing = sorted(required_cols - got)
        if missing:
            raise RuntimeError(
                f"규칙표 컬럼 누락 {missing}: {path} (필요: source,label,major,sub,weight)"
            )
        for i, row in enumerate(reader, start=2):
            source = (row.get("source") or "").strip()
            label = (row.get("label") or "").strip()
            major = (row.get("major") or "").strip()
            sub = (row.get("sub") or "").strip()
            w_raw = (row.get("weight") or "").strip()
            if not source or not label or not major or not sub:
                raise RuntimeError(
                    f"규칙표 {i}행에 빈 값: source={source!r} label={label!r} "
                    f"major={major!r} sub={sub!r}"
                )
            if source not in ("krx", "naver_industry", "naver_theme"):
                raise RuntimeError(
                    f"규칙표 {i}행 source 허용값 아님: {source!r} "
                    "(krx|naver_industry|naver_theme)"
                )
            try:
                weight = int(w_raw)
            except ValueError as e:
                raise RuntimeError(
                    f"규칙표 {i}행 weight 정수 아님: {w_raw!r}"
                ) from e
            key = (source, label)
            rules[key] = {
                "major": major,
                "sub": sub,
                "sector_key": f"{major}_{sub}",
                "weight": weight,
            }
    majors = {v["major"] for v in rules.values()}
    keys = {v["sector_key"] for v in rules.values()}
    print(
        f"규칙 {len(rules)}건 / 대분류 {len(majors)}개 / sector_key {len(keys)}개"
    )
    return rules


def collect_ticker_labels(
    mycursor,
    universe: set,
) -> Tuple[
    Dict[str, List[Tuple[str, str]]],
    Dict[Tuple[str, str], int],
    Dict[str, str],
    set,
]:
    """종목별 라벨 + 라벨별 종목 수 (universe=보통주만).

    반환:
      labels_by_ticker: {ticker: [(source, label), ...]}
      label_n_tickers: {(source, label): 종목수}  — universe 내만
      ticker_names: {ticker: 종목명}
      excluded_tickers: 소스 라벨에만 있고 universe 밖인 ticker 집합
    """
    labels_by_ticker: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    label_n_tickers: Dict[Tuple[str, str], int] = {}
    ticker_names: Dict[str, str] = {}
    excluded_tickers: set = set()

    mycursor.execute(
        """
        SELECT 종목코드, 종목명, 업종명
        FROM krx_ticker
        WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
          AND 종목구분 = '보통주'
        """
    )
    krx_label_tickers: Dict[str, set] = defaultdict(set)
    for code, name, upjong in mycursor.fetchall():
        t = str(code).strip()
        if t not in universe:
            excluded_tickers.add(t)
            continue
        ticker_names[t] = str(name or "").strip()
        lab = str(upjong or "").strip()
        if lab:
            labels_by_ticker[t].append(("krx", lab))
            krx_label_tickers[lab].add(t)
    for lab, ts in krx_label_tickers.items():
        label_n_tickers[("krx", lab)] = len(ts)
    print(f"✓ krx 업종명 라벨: 종목 {len(ticker_names)} / 라벨종류 {len(krx_label_tickers)}")

    mycursor.execute(
        """
        SELECT ticker, industry_name
        FROM krx_industry_stock
        WHERE update_date = (SELECT MAX(update_date) FROM krx_industry_stock)
          AND industry_name IS NOT NULL
          AND industry_name <> ''
          AND ticker IS NOT NULL
          AND ticker <> ''
        """
    )
    ind_label_tickers: Dict[str, set] = defaultdict(set)
    n_ind = 0
    for ticker, industry_name in mycursor.fetchall():
        t = str(ticker).strip()
        lab = str(industry_name).strip()
        if not t or not lab:
            continue
        if t not in universe:
            excluded_tickers.add(t)
            continue
        labels_by_ticker[t].append(("naver_industry", lab))
        ind_label_tickers[lab].add(t)
        n_ind += 1
    for lab, ts in ind_label_tickers.items():
        label_n_tickers[("naver_industry", lab)] = len(ts)
    print(f"✓ naver_industry 라벨: {n_ind}행 / 라벨종류 {len(ind_label_tickers)}")

    mycursor.execute(
        """
        SELECT ticker, theme_name
        FROM krx_theme_stock
        WHERE update_date = (SELECT MAX(update_date) FROM krx_theme_stock)
          AND theme_name IS NOT NULL
          AND theme_name <> ''
          AND ticker IS NOT NULL
          AND ticker <> ''
        """
    )
    th_label_tickers: Dict[str, set] = defaultdict(set)
    n_th = 0
    for ticker, theme_name in mycursor.fetchall():
        t = str(ticker).strip()
        lab = str(theme_name).strip()
        if not t or not lab:
            continue
        if t not in universe:
            excluded_tickers.add(t)
            continue
        labels_by_ticker[t].append(("naver_theme", lab))
        th_label_tickers[lab].add(t)
        n_th += 1
    for lab, ts in th_label_tickers.items():
        label_n_tickers[("naver_theme", lab)] = len(ts)
    print(f"✓ naver_theme 라벨: {n_th}행 / 라벨종류 {len(th_label_tickers)}")

    return labels_by_ticker, label_n_tickers, ticker_names, excluded_tickers


def apply_sector_rules(
    labels_by_ticker: Dict[str, List[Tuple[str, str]]],
    rules: Dict[Tuple[str, str], Dict[str, Any]],
    imap_ticker_rows: List[Tuple],
    label_n_tickers: Dict[Tuple[str, str], int],
) -> Tuple[
    Dict[str, List[Dict[str, Any]]],
    List[Tuple[str, str, int]],
    Dict[Tuple[str, str], int],
]:
    """규칙 + 투자맵 적용 → 종목별 후보(중복 sector_key 는 max weight).

    imap_ticker_rows: cell2 튜플
      (ticker, sector_key, detail_name, product, source, priority, is_primary, snap)

    반환:
      by_ticker: {ticker: [mapping dict, ...]}  (아직 rank 미부여)
      unmapped: [(source, label, n_tickers), ...] 정렬됨
      imap_key_n: {sector_key: 종목수}  (투자맵 특이도용)
    """
    # 투자맵 sector_key 별 종목 수
    imap_key_tickers: Dict[str, set] = defaultdict(set)
    imap_by_ticker: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in imap_ticker_rows:
        ticker, sk, detail, product, source = row[0], row[1], row[2], row[3], row[4]
        if source != "investingmap":
            continue
        imap_key_tickers[sk].add(ticker)
        imap_by_ticker[ticker].append(
            {
                "sector_key": sk,
                "source": "investingmap",
                "weight": 4,
                "label": sk,
                "detail_name": detail,
                "product": product,
            }
        )
    imap_key_n = {k: len(v) for k, v in imap_key_tickers.items()}

    # 미매핑 집계용: 규칙에 없는 (source, label) 의 종목
    unmapped_tickers: Dict[Tuple[str, str], set] = defaultdict(set)

    by_ticker: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    # by_ticker[ticker][sector_key] = best mapping

    def _keep_best(ticker: str, m: Dict[str, Any]) -> None:
        sk = m["sector_key"]
        cur = by_ticker[ticker].get(sk)
        if cur is None or int(m["weight"]) > int(cur["weight"]):
            by_ticker[ticker][sk] = m
        elif int(m["weight"]) == int(cur["weight"]):
            # 동일 weight 면 특이도(라벨 종목수) 낮은 쪽, 그다음 source 사전순
            n_new = int(m.get("label_n", 10**9))
            n_old = int(cur.get("label_n", 10**9))
            if n_new < n_old or (n_new == n_old and m["source"] < cur["source"]):
                by_ticker[ticker][sk] = m

    for ticker, pairs in labels_by_ticker.items():
        for source, label in pairs:
            rule = rules.get((source, label))
            if rule is None:
                unmapped_tickers[(source, label)].add(ticker)
                continue
            ln = int(label_n_tickers.get((source, label), 10**9))
            _keep_best(
                ticker,
                {
                    "sector_key": rule["sector_key"],
                    "source": source,
                    "weight": int(rule["weight"]),
                    "label": label,
                    "label_n": ln,
                    "detail_name": None,
                    "product": None,
                    "major": rule["major"],
                    "sub": rule["sub"],
                },
            )

    for ticker, maps in imap_by_ticker.items():
        for m in maps:
            m = dict(m)
            m["label_n"] = int(imap_key_n.get(m["sector_key"], 10**9))
            # major/sub 분해 (마지막 _ 기준이 아니라 원본 make_sector_key 형태)
            sk = m["sector_key"]
            if "_" in sk:
                major, sub = sk.split("_", 1)
            else:
                major, sub = sk, None
            m["major"] = major
            m["sub"] = sub
            _keep_best(ticker, m)

    ranked_lists: Dict[str, List[Dict[str, Any]]] = {}
    for ticker, sk_map in by_ticker.items():
        items = list(sk_map.values())
        items.sort(
            key=lambda m: (
                -int(m["weight"]),
                int(m.get("label_n", 10**9)),
                str(m["sector_key"]),
            )
        )
        for i, m in enumerate(items, start=1):
            m["rank"] = i
            m["is_primary"] = 1 if i == 1 else 0
        ranked_lists[ticker] = items

    unmapped = [
        (src, lab, len(ts))
        for (src, lab), ts in unmapped_tickers.items()
    ]
    unmapped.sort(key=lambda x: (-x[2], x[0], x[1]))
    return ranked_lists, unmapped, imap_key_n


def ensure_sector_views(mycursor, con) -> None:
    mycursor.execute(
        """
        CREATE OR REPLACE VIEW v_ticker_sector_primary AS
        SELECT
            ts.ticker,
            ts.sector_key,
            m.sector_name,
            m.chain_name,
            ts.detail_name,
            ts.source,
            ts.priority
        FROM krx_stock_sector_map ts
        JOIN krx_sector_master m ON m.sector_key = ts.sector_key
        WHERE ts.is_primary = 1
        """
    )
    mycursor.execute(
        """
        CREATE OR REPLACE VIEW v_ticker_sector_wide AS
        SELECT ticker,
          MAX(CASE WHEN `rank`=1 THEN sector_key END) AS sector1,
          MAX(CASE WHEN `rank`=2 THEN sector_key END) AS sector2,
          MAX(CASE WHEN `rank`=3 THEN sector_key END) AS sector3
        FROM krx_stock_sector_map
        GROUP BY ticker
        """
    )
    con.commit()


def print_validation_report(
    mycursor,
    snapshot_date: date,
    rule_stats: Dict[str, Any],
    unmapped: List[Tuple[str, str, int]],
) -> None:
    print("=" * 60)
    print("검증 리포트")
    print("=" * 60)

    n_univ = int(rule_stats.get("universe", 0))
    n_excl = int(rule_stats.get("excluded_label_tickers", 0))
    print(f"· 유니버스 {n_univ}종목 / 소스 라벨에만 존재해 제외한 ticker {n_excl}개")

    alias_missing = sorted(rule_stats.get("imap_alias_missing") or [])
    print(f"· 투자맵 섹터 별칭 미등록 {len(alias_missing)}건: 목록")
    for name in alias_missing:
        print(f"    {name}")

    rule_majors = set(rule_stats.get("rule_majors") or [])
    mycursor.execute(
        """
        SELECT DISTINCT m.sector_name
        FROM krx_stock_sector_map ts
        JOIN krx_sector_master m ON m.sector_key = ts.sector_key
        WHERE ts.snapshot_date = %s AND ts.`rank` = 1
        ORDER BY m.sector_name
        """,
        (snapshot_date,),
    )
    rank1_majors = [str(r[0]) for r in mycursor.fetchall()]
    unknown_majors = sorted(m for m in rank1_majors if m not in rule_majors)
    print(
        f"· rank1 대분류가 규칙표 {len(rule_majors)}개 대분류에 없는 것: "
        f"{len(unknown_majors)}건 목록"
    )
    for name in unknown_majors:
        print(f"    {name}")

    n_tk = int(rule_stats.get("tickers", 0))
    n_rows = int(rule_stats.get("rows", 0))
    avg = (float(n_rows) / n_tk) if n_tk else 0.0
    print(f"· 규칙 적용: 종목 {n_tk} / 매핑 행 {n_rows} / 종목당 평균 {avg:.2f}개")

    mycursor.execute(
        """
        SELECT m.sector_name, COUNT(DISTINCT ts.ticker) AS n
        FROM krx_stock_sector_map ts
        JOIN krx_sector_master m ON m.sector_key = ts.sector_key
        WHERE ts.snapshot_date = %s AND ts.`rank` = 1
        GROUP BY m.sector_name
        ORDER BY n DESC, m.sector_name ASC
        LIMIT 20
        """,
        (snapshot_date,),
    )
    print("· rank1 대분류 분포 Top 20 (종목 수)")
    for name, n in mycursor.fetchall():
        print(f"    {n:5d}  {name}")

    mycursor.execute(
        """
        SELECT sector_key, COUNT(DISTINCT ticker) AS n
        FROM krx_stock_sector_map
        WHERE snapshot_date = %s AND `rank` = 1
        GROUP BY sector_key
        ORDER BY n DESC, sector_key ASC
        LIMIT 30
        """,
        (snapshot_date,),
    )
    print("· rank1 sector_key 분포 Top 30")
    for sk, n in mycursor.fetchall():
        print(f"    {n:5d}  {sk}")

    mycursor.execute(
        """
        SELECT COUNT(DISTINCT ticker)
        FROM krx_stock_sector_map
        WHERE snapshot_date = %s AND `rank` = 1 AND sector_key = '기타_기타'
        """,
        (snapshot_date,),
    )
    n_etc = int(mycursor.fetchone()[0])
    print(f"· sector1 이 '기타_기타' 인 종목 수: {n_etc}")

    mycursor.execute(
        """
        SELECT t.종목코드, t.종목명
        FROM krx_ticker t
        LEFT JOIN krx_stock_sector_map ts ON ts.ticker = t.종목코드
        WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
          AND t.종목구분 = '보통주'
          AND ts.ticker IS NULL
        ORDER BY t.종목코드
        """
    )
    missing = mycursor.fetchall()
    print(f"· 매핑 0건 종목 수: {len(missing)} + 상위 20개 목록")
    for code, name in missing[:20]:
        print(f"    {code}  {name}")

    print("· 미매핑 라벨 Top 30 (규칙 보강용)")
    for src, lab, n in unmapped[:30]:
        print(f"    {n:5d}  [{src}] {lab}")
    print("=" * 60)


# %% 1) 테이블 준비
print("=" * 60)
print("1) 섹터 테이블 준비")
print("=" * 60)

snap = _snapshot_date()
print(f"snapshot_date = {snap}")

con = pymysql.connect(**db_connect_kwargs())
mycursor = con.cursor()
engine = create_engine(db_url())

ensure_sector_tables(mycursor, con)
print("✓ krx_sector_master / krx_stock_sector_map 준비 완료")

universe = set(pd.read_sql_query(_UNIV_SQL, con=engine)["종목코드"].astype(str))
print(f"✓ 보통주 유니버스: {len(universe)}종목")


# %% 2) 투자맵 수집
print("=" * 60)
print("2) 투자맵(investingmap) 수집")
print("=" * 60)

imap_index = fetch_imap_index()
print(f"✓ search_index.json: {len(imap_index)}종목")

map_urls = fetch_imap_sector_pages()
print(f"✓ 지도 URL {len(map_urls)}개 발견")

imap_alias = load_imap_sector_alias(IMAP_ALIAS_CSV)
imap_alias_missing: set = set()

master_rows: List[Tuple] = []
ticker_rows: List[Tuple] = []
master_seen = set()
imap_tickers = set()
imap_chains = set()
imap_sectors_ok = 0
empty_chain_rows = 0

for i, map_url in enumerate(map_urls):
    slug_hint = urlparse(map_url).path.rstrip("/").split("/")[-1]
    try:
        if i > 0:
            time.sleep(IMAP_SLEEP_SEC)
        resp = _imap_get(map_url, retries=2, timeout=20)
        sector_slug, sector_name, chains, rows = parse_imap_map_page(resp.text)
        major = resolve_imap_major(sector_name, imap_alias, imap_alias_missing)
        imap_sectors_ok += 1
        for ch in chains:
            imap_chains.add((major, ch))
            sk = make_sector_key(major, ch)
            if sk not in master_seen:
                master_seen.add(sk)
                master_rows.append(
                    (sk, major, ch, sector_slug, "investingmap", None)
                )
        # 섹터 자체(체인 없음) 마스터도 보존
        sk0 = make_sector_key(major, None)
        if sk0 not in master_seen:
            master_seen.add(sk0)
            master_rows.append(
                (sk0, major, None, sector_slug, "investingmap", None)
            )

        for r in rows:
            ticker = r["ticker"]
            chain_name = r.get("chain_name")
            if not chain_name:
                empty_chain_rows += 1
            sk = make_sector_key(major, chain_name)
            if sk not in master_seen:
                master_seen.add(sk)
                master_rows.append(
                    (
                        sk,
                        major,
                        chain_name,
                        sector_slug,
                        "investingmap",
                        None,
                    )
                )
            imap_tickers.add(ticker)
            ticker_rows.append(
                (
                    ticker,
                    sk,
                    r.get("detail_name"),
                    r.get("product"),
                    "investingmap",
                    4,  # weight=4 (priority 컬럼에 저장)
                    0,
                    snap,
                )
            )
        print(
            f"  · {sector_slug}: {sector_name} → {major} / "
            f"rows={len(rows)} / chains={len(chains)}"
        )
    except Exception as e:
        print(f"⚠️ {slug_hint} 수집 실패: {e}")

n_m = upsert_sector_master(mycursor, con, master_rows)
print(f"✓ 투자맵 master upsert: {n_m}행 (ticker map 은 규칙 적용 단계에서 일괄 적재)")
print(
    f"· 투자맵 수집 요약: 섹터 {imap_sectors_ok}개 / 종목 {len(imap_tickers)}개 / "
    f"밸류체인 {len(imap_chains)}종 / 빈 체인 행 {empty_chain_rows}"
)


# %% 3) 규칙표 기반 매핑 적재
print("=" * 60)
print("3) 규칙표 기반 매핑 (krx / naver_industry / naver_theme + investingmap)")
print("=" * 60)

rules = load_sector_rules(SECTOR_RULES_CSV)
labels_by_ticker, label_n_tickers, ticker_names, excluded_label_tickers = (
    collect_ticker_labels(mycursor, universe)
)
# 투자맵도 보통주 유니버스만
ticker_rows_univ = [r for r in ticker_rows if str(r[0]) in universe]
ranked_by_ticker, unmapped_labels, _imap_key_n = apply_sector_rules(
    labels_by_ticker, rules, ticker_rows_univ, label_n_tickers
)
# 적재 직전 한 번 더 필터
ranked_by_ticker = {
    t: maps for t, maps in ranked_by_ticker.items() if str(t) in universe
}
if len(ranked_by_ticker) > len(universe):
    print(
        f"⚠️ 적재 종목 수({len(ranked_by_ticker)})가 "
        f"유니버스({len(universe)})를 초과합니다"
    )

# 규칙 sector_key → master
rule_master: List[Tuple] = []
rule_master_seen = set()
for rule in rules.values():
    sk = rule["sector_key"]
    if sk in rule_master_seen:
        continue
    rule_master_seen.add(sk)
    rule_master.append(
        (sk, rule["major"], rule["sub"], None, "sector_rules", None)
    )
# 투자맵·규칙 적용 결과의 sector_key 도 master 보강
for maps in ranked_by_ticker.values():
    for m in maps:
        sk = m["sector_key"]
        if sk in rule_master_seen or sk in master_seen:
            continue
        rule_master_seen.add(sk)
        rule_master.append(
            (
                sk,
                m.get("major") or sk,
                m.get("sub"),
                None,
                m["source"],
                None,
            )
        )

n_m3 = upsert_sector_master(mycursor, con, rule_master)
print(f"✓ 규칙/보강 master upsert: {n_m3}행")

# 스냅샷 덮어쓰기: 맵 테이블 비운 뒤 랭킹 결과 일괄 insert
mycursor.execute("SET SQL_SAFE_UPDATES=0")
mycursor.execute("DELETE FROM krx_stock_sector_map")
con.commit()

final_rows: List[Tuple] = []
for ticker, maps in ranked_by_ticker.items():
    if str(ticker) not in universe:
        continue
    for m in maps:
        final_rows.append(
            (
                ticker,
                m["sector_key"],
                m.get("detail_name"),
                m.get("product"),
                m["source"],
                int(m["weight"]),  # priority 컬럼 = weight
                int(m["is_primary"]),
                snap,
                int(m["rank"]),
            )
        )

loaded_tickers = {str(r[0]) for r in final_rows}
if len(loaded_tickers) > len(universe):
    print(
        f"⚠️ 적재 종목 수({len(loaded_tickers)})가 "
        f"유니버스({len(universe)})를 초과합니다"
    )

n_t3 = upsert_ticker_sector(mycursor, con, final_rows)
print(f"✓ krx_stock_sector_map 적재: {n_t3}행 / 종목 {len(loaded_tickers)}")

rule_stats = {
    "tickers": len(loaded_tickers),
    "rows": n_t3,
    "universe": len(universe),
    "excluded_label_tickers": len(excluded_label_tickers),
    "imap_alias_missing": sorted(imap_alias_missing),
    "rule_majors": sorted({v["major"] for v in rules.values()}),
}


# %% 4) 뷰 · 검증
print("=" * 60)
print("4) 뷰 / 검증")
print("=" * 60)

ensure_sector_views(mycursor, con)
print("✓ VIEW v_ticker_sector_primary / v_ticker_sector_wide 생성·갱신")

print_validation_report(mycursor, snap, rule_stats, unmapped_labels)

mycursor.close()
con.close()
engine.dispose()
print("완료.")
