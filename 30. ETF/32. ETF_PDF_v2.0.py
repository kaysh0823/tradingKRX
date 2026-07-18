"""
KRX [13108] PDF 수집 + 구성비중 스냅샷 대시보드 (32. ETF_PDF_v2.0)
=========================================================================================
1) PDF 자동 다운로드 → CSV + MySQL(`krx_etf_pdf`) 적재
2) DB 기준 구성비중 스냅샷 HTML 대시보드 생성

종목코드(ETF) 딕셔너리 {코드: 종목명} × 수집일자 조합을 조회·저장한 뒤,
DB최근 / 3일전 / 일주일전 / 2주일전 비중 비교표를 만듭니다.

동작 방식 (KRX 정보데이터시스템 내부 API를 그대로 사용):
  · 로그인      : POST MDCCOMS001D1.cmd  (중복로그인 시 skipDup=Y 재전송)
  · 6자리→ISIN  : getJsonData.cmd + bld=dbms/comm/finder/finder_secuprodisu
  · PDF 조회    : getJsonData.cmd + bld=dbms/MDC/STAT/standard/MDCSTAT05001

■ 필요 패키지 :  pip install requests pandas pymysql sqlalchemy
■ 로그인      :  로그인이 필요하면 아래 환경변수를 '본인 PC'에 설정하세요.
                 (코드에 직접 비밀번호를 넣지 마세요. 없으면 비로그인으로 동작)
       Windows(PowerShell) :  setx KRX_ID "아이디"   /   setx KRX_PW "비번"  (후 새 터미널)
       macOS/Linux         :  export KRX_ID="아이디" ; export KRX_PW="비번"
"""

from __future__ import annotations

import html
import os
import time
import inspect
import webbrowser
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pymysql
import requests
from sqlalchemy import create_engine, text

# pw = os.getenv("KRX_PW")
# print("설정됨:", pw is not None, "| 길이:", len(pw) if pw else 0,
#       "| 앞뒤 공백:", pw != pw.strip() if pw else "-")
# print("ID:", os.getenv("KRX_ID"))


def _script_dir():
    """Spyder F5 / Jupyter 셀 / 일반 실행에서 스크립트 디렉터리 반환."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        for fi in inspect.stack():
            p = getattr(fi, "filename", "") or ""
            if "ETF_PDF" in p.replace("\\", "/"):
                return os.path.dirname(os.path.abspath(p))
        wd = os.getcwd()
        for name in ("32. ETF_PDF_v2.0.py", "ETF_PDF_v2.0.py", "32. ETF_PDF_v1.0.py"):
            if os.path.isfile(os.path.join(wd, name)):
                return wd
        etf_dir = os.path.join(wd, "30. ETF")
        for name in ("32. ETF_PDF_v2.0.py", "ETF_PDF_v2.0.py", "32. ETF_PDF_v1.0.py"):
            if os.path.isfile(os.path.join(etf_dir, name)):
                return etf_dir
        etf_dir2 = os.path.join(wd, "ETF")
        for name in ("32. ETF_PDF_v2.0.py", "ETF_PDF_v2.0.py", "32. ETF_PDF_v1.0.py"):
            if os.path.isfile(os.path.join(etf_dir2, name)):
                return etf_dir2
        return wd

# ─────────────────────────────────────────────────────────────
# 설정 : 이 부분만 바꿔서 사용
# ─────────────────────────────────────────────────────────────
ISU_CODES: dict[str, dict[str, str]] = {
    "마켓": {
        '471780': 'TIGER 코리아테크액티브',
        '444200': 'SOL 코리아메가테크액티브',
        '495060': 'TIME 코리아밸류업액티브',
        '442260': '마이티 다이나믹퀀트액티브',
        '385720': 'TIME 코스피액티브',
        '364690': 'KODEX 혁신기술테마액티브',
        '0172Y0': 'ACE K수출핵심TOP10산업액티브',
        '0074K0': 'KoAct K수출핵심기업TOP30액티브',
    },
    "반도체": {
        '494220': 'UNICORN SK하이닉스밸류체인액티브',
        '474590': 'WON 반도체밸류체인액티브',
        '388420': 'RISE 비메모리반도체액티브',
    },
    "조선": {
        '445150': 'KODEX 친환경조선해운액티브',
    },
    "로봇": {
        '445290': 'KODEX 로봇액티브',
    },
    "컬처": {
        '410870': 'TIME K컬처액티브',
    },
    "에너지": {
        '385510': 'KODEX 신재생에너지액티브',
        '422420': 'RISE 2차전지액티브',
        '404120': 'TIME K신재생에너지액티브',
        '482030': 'KoAct 반도체&2차전지핵심소재액티브',
    },
    "바이오": {
        '0000Z0': 'RISE 바이오TOP10액티브',
        '0168K0': 'TIGER 기술이전바이오액티브',
        '463050': 'TIME K바이오액티브',
    },
    "코스닥": {
        '0162Y0': 'TIME 코스닥액티브',
        '0163Y0': 'KoAct 코스닥액티브',
    },
}


def iter_isu_entries() -> list[tuple[str, str, str]]:
    """(그룹, 티커, 이름) — ISU_CODES 정의 순서."""
    out: list[tuple[str, str, str]] = []
    for group, codes in ISU_CODES.items():
        for code, name in codes.items():
            out.append((group, code, name))
    return out


def flat_isu_codes() -> dict[str, str]:
    """티커→이름 (그룹 정의 순 유지)."""
    return {code: name for _, code, name in iter_isu_entries()}


def isu_codes_ordered() -> list[str]:
    """수집·출력용 티커 리스트 (그룹 key 순 → 그룹 내 순)."""
    return [code for _, code, _ in iter_isu_entries()]


def isu_group_of(code: str) -> str | None:
    for group, codes in ISU_CODES.items():
        if code in codes:
            return group
    return None


# 수집일: 최초=최근 2주(INITIAL_LOOKBACK_DAYS), 이후=DB 최신 다음날 ~ KRX 최신 영업일
# TRD_DATES는 main()에서 resolve_trd_dates()로 채움
# 저장: <이 스크립트>/results/krx_pdf_result_{수집일}.csv
# 복수 일자면 min-max (예: krx_pdf_result_20260113-20260114.csv)
_SCRIPT_DIR = _script_dir()
RESULTS_DIR = os.path.join(_SCRIPT_DIR, "results")
SLEEP_SEC = 1.0      # 요청 간 간격(초)
SAVE_TO_DB = True   # False면 CSV만 저장
BIZDAY_LOOKBACK = 10  # KRX 최신 영업일 탐색 최대 일수
INITIAL_LOOKBACK_DAYS = 14  # 최초 수집: 최근 약 2주(캘린더일) 거래일
# True면 DB 최신일 무시하고 최근 2주 강제 수집. 이번 실행 후 False로 되돌릴 것.
FORCE_INITIAL_BACKFILL = False
PDF_HTTP_TIMEOUT = 60          # PDF 조회/영업일 확인 HTTP timeout(초)
BIZDAY_NET_RETRIES = 3         # 네트워크 오류 시 동일 일자 재시도 횟수
BIZDAY_NET_RETRY_WAIT = 5.0    # 재시도 간격(초)

# KRX 휴장일 (KST, YYYY-MM-DD). 주말은 코드에서 별도 제외.
# 연도별 고시·임시공휴일 반영 필요. 안내: https://open.krx.co.kr
KRX_HOLIDAYS = frozenset({
    # 2026
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-02-18", "2026-03-02",
    "2026-05-01", "2026-05-05", "2026-05-25", "2026-06-03", "2026-07-17",
    "2026-08-17", "2026-09-24", "2026-09-25", "2026-10-05", "2026-10-09",
    "2026-12-25", "2026-12-31",
    # 2027
    "2027-01-01", "2027-02-08", "2027-02-09", "2027-03-01", "2027-05-03",
    "2027-05-05", "2027-05-13", "2027-07-19", "2027-08-16", "2027-09-14",
    "2027-09-15", "2027-09-16", "2027-10-04", "2027-10-11", "2027-12-27",
    "2027-12-31",
})

DB_CONFIG = {
    'user': 'root',
    'passwd': 'GloriaDahn03240701',
    'host': '127.0.0.1',
    'db': 'kor_stock_db',
    'charset': 'utf8',
}
PDF_TABLE = 'krx_etf_pdf'
PDF_COLS = [
    'ETF코드', 'ETF명', '수집일자', '티커', '구성종목명',
    '계약수', '금액', '시가총액', '시가총액기준 구성비중',
]


# ─────────────────────────────────────────────────────────────
# 대시보드(구성비중 스냅샷) 설정
# ─────────────────────────────────────────────────────────────
WEIGHT_COL = "시가총액기준 구성비중"
RUN_COLLECT = True      # PDF 수집+DB 적재
RUN_DASHBOARD = True    # 비중 스냅샷 HTML 생성
OPEN_BROWSER = True

DASH_START_DATE = None  # date | str | None — 대시보드 조회 시작일
DASH_END_DATE = None
DASH_ETF_FILTER = None  # list[str] | None — None이면 ISU_CODES 전체(그룹 순)

SNAPSHOT_DEFS = [
    ("DB최근", 0),
    ("3일전", 3),
    ("일주일전", 5),
    ("2주일전", 10),
]
CHAIN_COMPARE = [
    ("DB최근", "3일전"),
    ("3일전", "일주일전"),
    ("일주일전", "2주일전"),
]
TOP_N = 20
SUMMARY_TOP_N = 20
GRAD_MAX_PP = 5.0
EXCLUDE_FROM_WEIGHT_RANK = {"005930", "000660"}
EXCLUDE_NAME_KEYWORDS = ("삼성전자", "하이닉스")


def _db_url() -> str:
    return (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['passwd']}"
        f"@{DB_CONFIG['host']}:3306/{DB_CONFIG['db']}"
    )


# ── KRX 엔드포인트 ────────────────────────────────────────────
BASE       = "https://data.krx.co.kr"
JSON_URL   = f"{BASE}/comm/bldAttendant/getJsonData.cmd"
LOGIN_PAGE = f"{BASE}/contents/MDC/COMS/client/MDCCOMS001.cmd"
LOGIN_JSP  = f"{BASE}/contents/MDC/COMS/client/view/login.jsp?site=mdc"
LOGIN_URL  = f"{BASE}/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
UA         = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
HEADERS = {
    "User-Agent": UA,
    "Referer": f"{BASE}/contents/MDC/MDI/outerLoader/index.cmd",
    "X-Requested-With": "XMLHttpRequest",
}


# ─────────────────────────────────────────────────────────────
# 로그인 (선택)
# ─────────────────────────────────────────────────────────────
def krx_login(session: requests.Session) -> bool:
    """KRX_ID / KRX_PW 환경변수가 있으면 로그인. 성공/불필요 시 True."""
    # uid, upw = os.getenv("KRX_ID"), os.getenv("KRX_PW")
    uid, upw = "hachimitsu79", "GloriaDahn0823$$"
    if not (uid and upw):
        print("· 로그인 정보 없음 → 비로그인으로 진행")
        return True

    # 1) 세션 준비(JSESSIONID 발급)
    session.get(LOGIN_PAGE, headers={"User-Agent": UA}, timeout=15)
    session.get(LOGIN_JSP, headers={"User-Agent": UA, "Referer": LOGIN_PAGE}, timeout=15)

    payload = {"mbrNm": "", "telNo": "", "di": "", "certType": "",
               "mbrId": uid, "pw": upw}
    h = {"User-Agent": UA, "Referer": LOGIN_PAGE}

    data = session.post(LOGIN_URL, data=payload, headers=h, timeout=15).json()
    code = data.get("_error_code", "")

    if code == "CD010":
        print("⚠️ 비밀번호 변경이 필요합니다. krx.co.kr 에서 변경 후 재시도하세요.")
        return False
    if code == "CD011":                       # 중복 로그인 → 강제 진행
        payload["skipDup"] = "Y"
        data = session.post(LOGIN_URL, data=payload, headers=h, timeout=15).json()
        code = data.get("_error_code", "")

    if code == "CD001":
        print("· 로그인 성공")
        return True
    print(f"⚠️ 로그인 실패: {code} / {data.get('_error_message','')}")
    return False


# ─────────────────────────────────────────────────────────────
# 6자리 단축코드 → ISIN(표준코드) 변환
# ─────────────────────────────────────────────────────────────
def build_isin_map(session: requests.Session, market: str = "ALL") -> dict:
    """finder로 전체 ETF/ETN/ELW 목록을 받아 {단축코드: ISIN} 딕셔너리 생성."""
    params = {"bld": "dbms/comm/finder/finder_secuprodisu",
              "mktsel": market, "searchText": ""}
    rows = session.post(JSON_URL, data=params, headers=HEADERS, timeout=15).json()
    rows = rows.get("block1", [])
    return {r["short_code"]: r["full_code"] for r in rows}


# ─────────────────────────────────────────────────────────────
# PDF 조회
# ─────────────────────────────────────────────────────────────
COLS = {
    "COMPST_ISU_CD":        "티커",
    "COMPST_ISU_NM":        "구성종목명",
    "COMPST_ISU_CU1_SHRS":  "계약수",
    "VALU_AMT":             "금액",
    "COMPST_AMT":           "시가총액",
    "COMPST_RTO":           "시가총액기준 구성비중",
}

def fetch_pdf(session: requests.Session, isin: str, date: str) -> pd.DataFrame:
    params = {"bld": "dbms/MDC/STAT/standard/MDCSTAT05001",
              "trdDd": date, "isuCd": isin}
    out = session.post(
        JSON_URL, data=params, headers=HEADERS, timeout=PDF_HTTP_TIMEOUT
    ).json()
    df = pd.DataFrame(out.get("output", []))
    if df.empty:
        return df
    # 누락 컬럼은 None으로 채워 CSV/DB 스키마 유지
    missing = [src for src in COLS if src not in df.columns]
    if missing:
        print(f'  ⚠️ PDF 응답 누락 키 {missing} / 실제키={list(df.columns)}')
    for src, dst in COLS.items():
        if src not in df.columns:
            df[src] = None
    df = df[list(COLS.keys())].rename(columns=COLS)
    return df


def _probe_pdf_has_data(session: requests.Session, isin: str, day_str: str) -> bool:
    """
    PDF에 구성종목 행이 있으면 True, 정상 응답이지만 비어 있으면 False(휴장/무자료).
    Timeout/ConnectionError는 동일 일자 재시도 후 실패 시 RuntimeError (휴장으로 오판하지 않음).
    """
    last_err: Exception | None = None
    for attempt in range(1, BIZDAY_NET_RETRIES + 1):
        try:
            df = fetch_pdf(session, isin, day_str)
            return df is not None and not df.empty
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            print(
                f'  → 네트워크 오류 {day_str} '
                f'({attempt}/{BIZDAY_NET_RETRIES}): {type(e).__name__}: {e}'
            )
            if attempt < BIZDAY_NET_RETRIES:
                time.sleep(BIZDAY_NET_RETRY_WAIT)
    raise RuntimeError(
        f'KRX PDF 조회 실패({day_str}): 네트워크 오류 '
        f'{BIZDAY_NET_RETRIES}회 연속 → 휴장으로 처리하지 않고 중단'
    ) from last_err


# ─────────────────────────────────────────────────────────────
# 수집 일자 범위 (DB 최신일 다음날 ~ KRX 최신 영업일)
# ─────────────────────────────────────────────────────────────
def get_max_pdf_date_from_db():
    """krx_etf_pdf의 MAX(수집일자). 없거나 테이블 비면 None."""
    con = pymysql.connect(**DB_CONFIG)
    try:
        ensure_etf_pdf_table(con)
        with con.cursor() as cur:
            cur.execute(f"SELECT MAX(`수집일자`) FROM `{PDF_TABLE}`")
            row = cur.fetchone()
        if not row or row[0] is None:
            return None
        d = row[0]
        if isinstance(d, datetime):
            return d.date()
        if isinstance(d, date):
            return d
        return pd.to_datetime(d).date()
    finally:
        con.close()


def _ymd_dash(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def is_krx_holiday(d: date) -> bool:
    """캘린더 공휴일/임시공휴일 (주말 제외 — 주말은 weekday로 별도 처리)."""
    return _ymd_dash(d) in KRX_HOLIDAYS


def is_calendar_non_trading_day(d: date) -> bool:
    """주말 또는 KRX_HOLIDAYS."""
    return d.weekday() >= 5 or is_krx_holiday(d)


def pdf_date_exists_in_db(d: date) -> bool:
    """해당 수집일자가 DB에 1행이라도 있으면 True."""
    con = pymysql.connect(**DB_CONFIG)
    try:
        ensure_etf_pdf_table(con)
        with con.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM `{PDF_TABLE}` WHERE `수집일자` = %s LIMIT 1",
                (d,),
            )
            return cur.fetchone() is not None
    finally:
        con.close()


def find_latest_pdf_biz_day(session: requests.Session, isin: str, max_lookback: int = BIZDAY_LOOKBACK) -> str:
    """
    KRX PDF가 조회되는 가장 최근 영업일(YYYYMMDD).
    - 15시 이전: 당일 제외하고 전일부터
    - 주말·KRX_HOLIDAYS: 프로브 없이 건너뛰고 전거래일 탐색
    - 정상 응답 + 빈 데이터만 휴장으로 하루 뒤로
    - 네트워크 오류는 동일 일자 재시도 후 중단
    """
    if datetime.now().hour < 15:
        candidate = date.today() - timedelta(days=1)
        print(f'· 15시 이전 실행 → 영업일 탐색 시작: {candidate.strftime("%Y%m%d")} (당일 제외)')
    else:
        candidate = date.today()

    if is_calendar_non_trading_day(date.today()):
        print(
            f'· 오늘({date.today().strftime("%Y-%m-%d")})은 주말/공휴일 → '
            f'전거래일 PDF 기준으로 수집'
        )

    probed = 0
    # 주말·공휴일 스킵을 감안해 lookback보다 여유 있게 캘린더일 탐색
    for _ in range(max_lookback * 3):
        if probed >= max_lookback:
            break
        day_str = candidate.strftime('%Y%m%d')
        if is_calendar_non_trading_day(candidate):
            reason = "주말" if candidate.weekday() >= 5 else "공휴일"
            print(f'  → {day_str} {reason} → 전일로')
            candidate -= timedelta(days=1)
            continue

        probed += 1
        print(f'KRX PDF 영업일 확인: {day_str} ({probed}/{max_lookback})')
        if _probe_pdf_has_data(session, isin, day_str):
            print(f'· KRX PDF 최신 영업일: {day_str}')
            return day_str
        print(f'  → 데이터 없음(휴장/무자료) → 하루 전으로')
        time.sleep(0.3)
        candidate -= timedelta(days=1)
    raise RuntimeError(f'최근 {max_lookback}영업일 내 KRX PDF 조회 가능일을 찾지 못했습니다.')


def _to_pydate(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return pd.to_datetime(v).date()


def _known_trading_days_from_ohlcv(start_excl: date, end_incl: date) -> list:
    """
    로컬 OHLCV에 존재하는 거래일 (공휴일·주말 제외용).
    start_excl < date <= end_incl. pymysql cursor 사용 (pandas UserWarning 회피).
    """
    sqls = [
        """
        SELECT DISTINCT `date` AS d FROM krx_etf_ohlcv
        WHERE `date` > %s AND `date` <= %s ORDER BY d
        """,
        """
        SELECT DISTINCT `date` AS d FROM krx_ohlcv
        WHERE `date` > %s AND `date` <= %s ORDER BY d
        """,
    ]
    con = pymysql.connect(**DB_CONFIG)
    try:
        with con.cursor() as cur:
            for sql in sqls:
                try:
                    cur.execute(sql, (start_excl, end_incl))
                    rows = cur.fetchall()
                except Exception:
                    continue
                if not rows:
                    continue
                days = [_to_pydate(r[0]) for r in rows]
                if days:
                    return days
        return []
    finally:
        con.close()


def _weekday_range(start_excl: date, end_incl: date) -> list:
    """start_excl < d <= end_incl 인 평일(월~금) 중 공휴일 제외 리스트."""
    days = []
    d = start_excl + timedelta(days=1)
    while d <= end_incl:
        if d.weekday() < 5 and not is_krx_holiday(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def _filter_trading_days_with_pdf(
    session: requests.Session,
    sample_isin: str,
    candidates: list,
    known: set,
    end_d: date,
) -> list:
    """
    후보 중 실제 PDF 조회 가능한 거래일만 남긴다.
    - 공휴일/주말: 제외
    - end_d: 이미 find_latest에서 확인됨 → 채택
    - 그 외: PDF 프로브 (OHLCV에 있어도 공휴일 오인을 막기 위해 검증)
    """
    verified = []
    for d in candidates:
        if is_calendar_non_trading_day(d):
            print(f'  · 주말/공휴일 제외: {d.strftime("%Y%m%d")}')
            continue
        if d == end_d:
            verified.append(d)
            continue
        day_str = d.strftime('%Y%m%d')
        if _probe_pdf_has_data(session, sample_isin, day_str):
            verified.append(d)
        else:
            hint = " (OHLCV엔 있음)" if d in known else ""
            print(f'  · 공휴일/무자료 제외: {day_str}{hint}')
        time.sleep(0.2)
    return verified


def resolve_trd_dates(session: requests.Session, sample_isin: str) -> list:
    """
    수집 대상 일자(YYYYMMDD) 리스트.
    - end: KRX PDF 조회 가능한 최신 영업일 (공휴일이면 전거래일)
    - 이미 DB에 있는 수집일은 제외
    - FORCE_INITIAL_BACKFILL 또는 DB 비어 있으면 최근 INITIAL_LOOKBACK_DAYS 거래일
    - 그 외: (DB 최신일 다음날 ~ end] 거래일
    """
    end_str = find_latest_pdf_biz_day(session, sample_isin)
    end_d = datetime.strptime(end_str, '%Y%m%d').date()
    last = get_max_pdf_date_from_db()

    if FORCE_INITIAL_BACKFILL:
        last = end_d - timedelta(days=INITIAL_LOOKBACK_DAYS)
        print(
            f'· FORCE_INITIAL_BACKFILL=True → DB 무시, 최근 {INITIAL_LOOKBACK_DAYS}일 강제 수집: '
            f'{(last + timedelta(days=1)).strftime("%Y-%m-%d")} ~ {end_d.strftime("%Y-%m-%d")}'
        )
    elif last is None:
        last = end_d - timedelta(days=INITIAL_LOOKBACK_DAYS)
        print(
            f'· DB 수집 이력 없음 → 최초 수집: '
            f'{(last + timedelta(days=1)).strftime("%Y-%m-%d")} ~ {end_d.strftime("%Y-%m-%d")} '
            f'(최근 {INITIAL_LOOKBACK_DAYS}일)'
        )
    else:
        print(f'· DB 최신 수집일: {last.strftime("%Y-%m-%d")} / KRX 최신: {end_d.strftime("%Y-%m-%d")}')
        if last > end_d:
            msg = (
                f'⚠️ 판별 모순: DB 최신 수집일({last.strftime("%Y-%m-%d")}) > '
                f'KRX 최신 영업일({end_d.strftime("%Y-%m-%d")}). '
                f'네트워크 오류를 휴장으로 오판했을 가능성이 큽니다. 수집을 중단합니다.'
            )
            print(msg)
            raise RuntimeError(msg)
        if last == end_d:
            print('· 이미 KRX 최신 영업일까지 적재됨 → 수집 스킵')
            return []

    candidates = _weekday_range(last, end_d)
    known = set(_known_trading_days_from_ohlcv(last, end_d))
    days = _filter_trading_days_with_pdf(session, sample_isin, candidates, known, end_d)
    days = sorted({d for d in days if last < d <= end_d})

    # 이미 DB에 있는 일자는 재수집하지 않음 (FORCE 제외)
    if not FORCE_INITIAL_BACKFILL:
        pending = []
        for d in days:
            if pdf_date_exists_in_db(d):
                print(f'  · 이미 수집됨 → 스킵: {d.strftime("%Y-%m-%d")}')
            else:
                pending.append(d)
        days = pending

    out = [d.strftime('%Y%m%d') for d in days]
    if not out:
        print('· 수집할 신규 영업일 없음 (공휴일·기수집 반영)')
    else:
        print(f'· 수집 대상 일자: {out}')
    return out


# ─────────────────────────────────────────────────────────────
# MySQL 적재 (krx_etf_pdf)
# ─────────────────────────────────────────────────────────────
def _prepare_pdf_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """CSV와 동일 컬럼을 DB용으로 정리 (코드 정규화, 숫자 변환, NaN→None)."""
    out = df.copy()
    for c in PDF_COLS:
        if c not in out.columns:
            out[c] = None
    out = out[PDF_COLS].copy()

    # 숫자만인 코드는 6자리 패딩, 영문 포함(예: 0172Y0)은 그대로
    etf = out['ETF코드'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    etf_num = etf.str.fullmatch(r'\d+', na=False)
    out['ETF코드'] = etf.where(~etf_num, etf.str.zfill(6))

    if 'ETF명' in out.columns:
        out['ETF명'] = out['ETF명'].astype(str).str.strip().replace({'nan': None, 'None': None})

    out['티커'] = out['티커'].astype(str).str.strip()
    # 현금 등 비종목 티커는 그대로 두고, 숫자면 6자리 패딩
    num_mask = out['티커'].str.fullmatch(r'\d+(\.0)?', na=False)
    out.loc[num_mask, '티커'] = (
        out.loc[num_mask, '티커'].str.replace(r'\.0$', '', regex=True).str.zfill(6)
    )
    out['구성종목명'] = out['구성종목명'].astype(str).str.strip().replace({'nan': None})
    out['수집일자'] = pd.to_datetime(out['수집일자'].astype(str).str.replace('-', ''), format='%Y%m%d')

    for col in ('계약수', '금액', '시가총액', '시가총액기준 구성비중'):
        if col not in out.columns:
            out[col] = None
            continue
        s = out[col].astype(str).str.replace(',', '', regex=False).str.replace(r'^\-$', '', regex=True)
        out[col] = pd.to_numeric(s, errors='coerce')

    out = out.replace({np.nan: None, pd.NaT: None})
    return out


def ensure_etf_pdf_table(con) -> None:
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{PDF_TABLE}` (
        `ETF코드` VARCHAR(10) NOT NULL,
        `ETF명` VARCHAR(200),
        `수집일자` DATE NOT NULL,
        `티커` VARCHAR(20) NOT NULL,
        `구성종목명` VARCHAR(200),
        `계약수` DECIMAL(20, 4),
        `금액` DECIMAL(20, 2),
        `시가총액` DECIMAL(20, 2),
        `시가총액기준 구성비중` DECIMAL(12, 6),
        PRIMARY KEY (`ETF코드`, `수집일자`, `티커`),
        INDEX `idx_etf_pdf_date` (`수집일자`),
        INDEX `idx_etf_pdf_etf` (`ETF코드`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with con.cursor() as cur:
        cur.execute(create_sql)

        def _has_col(name: str) -> bool:
            cur.execute(f"SHOW COLUMNS FROM `{PDF_TABLE}` LIKE %s", (name,))
            return cur.fetchone() is not None

        if not _has_col('ETF명'):
            cur.execute(
                f"ALTER TABLE `{PDF_TABLE}` ADD COLUMN `ETF명` VARCHAR(200) NULL AFTER `ETF코드`"
            )
        if not _has_col('시가총액'):
            cur.execute(
                f"ALTER TABLE `{PDF_TABLE}` ADD COLUMN `시가총액` DECIMAL(20, 2) NULL AFTER `금액`"
            )
        # 구 컬럼명 `비중` → `시가총액기준 구성비중`
        if _has_col('비중') and not _has_col('시가총액기준 구성비중'):
            cur.execute(
                f"ALTER TABLE `{PDF_TABLE}` "
                f"CHANGE COLUMN `비중` `시가총액기준 구성비중` DECIMAL(12, 6) NULL"
            )
        elif not _has_col('시가총액기준 구성비중'):
            cur.execute(
                f"ALTER TABLE `{PDF_TABLE}` "
                f"ADD COLUMN `시가총액기준 구성비중` DECIMAL(12, 6) NULL AFTER `시가총액`"
            )
    con.commit()


def save_etf_pdf_to_db(df: pd.DataFrame, batch_size: int = 1000) -> int:
    """PDF DataFrame을 krx_etf_pdf에 upsert. 적재 행 수 반환."""
    if df is None or df.empty:
        print('⚠️ DB 적재 스킵: 데이터 없음')
        return 0

    prepared = _prepare_pdf_df_for_db(df)
    args = prepared.values.tolist()

    insert_sql = f"""
    INSERT INTO `{PDF_TABLE}` (
        `ETF코드`, `ETF명`, `수집일자`, `티커`, `구성종목명`,
        `계약수`, `금액`, `시가총액`, `시가총액기준 구성비중`
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) AS new
    ON DUPLICATE KEY UPDATE
        `ETF명`=new.`ETF명`,
        `구성종목명`=new.`구성종목명`,
        `계약수`=new.`계약수`,
        `금액`=new.`금액`,
        `시가총액`=new.`시가총액`,
        `시가총액기준 구성비중`=new.`시가총액기준 구성비중`;
    """

    con = pymysql.connect(**DB_CONFIG)
    try:
        ensure_etf_pdf_table(con)
        mycursor = con.cursor()
        total_batches = (len(args) + batch_size - 1) // batch_size
        for i in range(0, len(args), batch_size):
            batch = args[i:i + batch_size]
            mycursor.executemany(insert_sql, batch)
            con.commit()
            if total_batches > 1:
                print(f'PDF DB 배치: {i // batch_size + 1}/{total_batches} 완료', end='\r')
        print(f'\n✓ `{PDF_TABLE}` 적재 완료: {len(args)}행')
        return len(args)
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────
# 대시보드: DB 로드 / 스냅샷 / HTML
# ─────────────────────────────────────────────────────────────
def load_pdf_from_db(
    start_date=None,
    end_date=None,
    etf_codes: list[str] | None = None,
) -> pd.DataFrame:
    engine = create_engine(_db_url())
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
    """모든 ETF 구성종목을 이어붙인 wide (요약용, top_n 제한 없음). 그룹 정의 순."""
    parts = []
    present = set(df["ETF코드"].astype(str))
    ordered = [c for c in isu_codes_ordered() if c in present]
    leftovers = sorted(present - set(ordered))
    for code in ordered + leftovers:
        g = df[df["ETF코드"].astype(str) == str(code)]
        if g.empty:
            continue
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

    present = set(df["ETF코드"].astype(str))
    seen: set[str] = set()

    for group, codes in ISU_CODES.items():
        group_codes = [c for c in codes if c in present]
        if not group_codes:
            continue
        gid = f"grp-{html.escape(group)}"
        nav_links.append(f'<a href="#{gid}" class="nav-grp">{html.escape(group)}</a>')
        sections.append(
            f'<h2 class="group-heading" id="{gid}">{html.escape(group)}</h2>'
        )
        for code in group_codes:
            g = df[df["ETF코드"].astype(str) == str(code)]
            if g.empty:
                continue
            seen.add(str(code))
            title = _etf_display_name(g, str(code))
            etf_snaps = _etf_snaps_for_group(g, snaps)
            wide = build_snapshot_wide(g, etf_snaps, TOP_N)
            nav_links.append(f'<a href="#etf-{code}">{html.escape(title)}</a>')
            sections.append(f"""
<section class="etf-block" id="etf-{code}">
  <p class="group-tag">{html.escape(group)}</p>
  <h2>{html.escape(title)}</h2>
  <p class="meta">{snapshot_meta_html(etf_snaps)}</p>
  <h3>스냅샷 비중 (연속 시점 대비 변화)</h3>
  {render_etf_weight_table(wide, etf_snaps)}
</section>
""")

    leftovers = sorted(present - seen)
    if leftovers:
        nav_links.append('<a href="#grp-기타" class="nav-grp">기타</a>')
        sections.append('<h2 class="group-heading" id="grp-기타">기타</h2>')
        for code in leftovers:
            g = df[df["ETF코드"].astype(str) == str(code)]
            if g.empty:
                continue
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
    h2.group-heading {{
      margin: 28px 0 8px; font-size: 1.05rem; color: var(--accent);
      border-bottom: 2px solid var(--accent); padding-bottom: 6px;
    }}
    .group-tag {{
      display: inline-block; margin: 0 0 6px; font-size: .72rem; font-weight: 600;
      color: var(--accent); background: #e7f3ee; padding: 2px 8px; border-radius: 4px;
    }}
    h3 {{ margin: 10px 0 8px; font-size: .95rem; color: var(--muted); }}
    .sub {{ color: var(--muted); font-size: .92rem; margin: 0; }}
    .nav {{ display:flex; flex-wrap:wrap; gap:8px 12px; margin-top:12px; max-height:110px; overflow:auto; }}
    .nav a {{
      text-decoration:none; color:var(--accent); font-size:.8rem;
      background:#e7f3ee; padding:4px 10px; border-radius:4px;
    }}
    .nav a.nav-grp {{
      background:#0b6e4f; color:#fff; font-weight:600;
    }}
    .nav a:hover {{ background:#d3ebe2; }}
    .nav a.nav-grp:hover {{ background:#095a41; color:#fff; }}
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

# ─────────────────────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────────────────────
def collect_etf_pdf() -> bool:
    session = requests.Session()
    if not krx_login(session):
        return False

    isin_map = build_isin_map(session)
    frames, errors = [], []

    # 영업일 판별용 샘플 ISIN (그룹 정의 순 첫 유효 종목)
    sample_isin = None
    for code in isu_codes_ordered():
        sample_isin = isin_map.get(code)
        if sample_isin:
            break
    if not sample_isin:
        print('⚠️ ISIN 변환 가능한 종목이 없어 수집을 중단합니다.')
        return False

    trd_dates = resolve_trd_dates(session, sample_isin)
    if not trd_dates:
        print('\n수집할 신규 영업일이 없습니다. (대시보드는 기존 DB로 진행)')
        return True

    for group, code, etf_name in iter_isu_entries():
        isin = isin_map.get(code)
        if not isin:
            errors.append((code, "-", f"ISIN 변환 실패({etf_name})"))
            print(f"[실패] [{group}] {code} ({etf_name}) : ISIN 변환 실패")
            continue

        for date in trd_dates:
            try:
                df = fetch_pdf(session, isin, date)
                if df.empty:
                    errors.append((code, date, f"빈 데이터({etf_name})"))
                    print(f"[빈값] [{group}] {code} ({etf_name}) / {date}")
                else:
                    df.insert(0, "수집일자", date)
                    df.insert(0, "ETF명", etf_name)
                    df.insert(0, "ETF코드", code)
                    frames.append(df)
                    print(f"[OK]  [{group}] {code} ({etf_name}) / {date} : {len(df)}개 구성종목")
            except Exception as e:
                errors.append((code, date, f"{etf_name}: {e}"))
                print(f"[실패] [{group}] {code} ({etf_name}) / {date} : {e}")
            time.sleep(SLEEP_SEC)

    if frames:
        result = pd.concat(frames, ignore_index=True)
        dates_sorted = sorted(trd_dates)
        if len(dates_sorted) == 1:
            date_tag = dates_sorted[0]
        else:
            date_tag = f"{dates_sorted[0]}-{dates_sorted[-1]}"
        os.makedirs(RESULTS_DIR, exist_ok=True)
        out_file = os.path.join(RESULTS_DIR, f"krx_pdf_result_{date_tag}.csv")
        result.to_csv(out_file, index=False, encoding="utf-8-sig")
        print(f"\n저장 완료: {out_file}  (총 {len(result)}행)")

        if SAVE_TO_DB:
            try:
                save_etf_pdf_to_db(result)
            except Exception as e:
                print(f'⚠️ DB 적재 실패: {e}')
    else:
        print("\n수집된 데이터가 없습니다. 종목코드/날짜/로그인 설정을 확인하세요.")

    if errors:
        print("\n[확인이 필요한 항목]")
        for code, date, msg in errors:
            print(f"  {code} / {date} : {msg}")
    return True


def generate_weight_dashboard(
    start_date=None,
    end_date=None,
    etf_codes: list[str] | None = None,
) -> str | None:
    """krx_etf_pdf → 구성비중 스냅샷 HTML. 저장 경로 반환(실패 시 None)."""
    print("\n" + "=" * 50)
    print("ETF PDF 비중 스냅샷 대시보드 생성")
    print("=" * 50)

    if start_date is None:
        start_date = DASH_START_DATE
    if end_date is None:
        end_date = DASH_END_DATE
    if etf_codes is None:
        etf_codes = DASH_ETF_FILTER if DASH_ETF_FILTER is not None else isu_codes_ordered()

    df = load_pdf_from_db(start_date, end_date, etf_codes)
    if df.empty:
        print("⚠️ krx_etf_pdf에 데이터가 없습니다. 수집 단계를 먼저 확인하세요.")
        return None

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
    return out_path


def main():
    print("ETF PDF v2.0 — 수집 + 비중 대시보드")
    print(f"· RUN_COLLECT={RUN_COLLECT} / RUN_DASHBOARD={RUN_DASHBOARD}")

    if RUN_COLLECT:
        print("\n" + "=" * 50)
        print("1) PDF 수집")
        print("=" * 50)
        ok = collect_etf_pdf()
        if not ok:
            print("⚠️ 수집 단계에서 중단되었습니다.")
            if not RUN_DASHBOARD:
                return
    else:
        print("· 수집 스킵 (RUN_COLLECT=False)")

    if RUN_DASHBOARD:
        generate_weight_dashboard()
    else:
        print("· 대시보드 스킵 (RUN_DASHBOARD=False)")


if __name__ == "__main__":
    main()
