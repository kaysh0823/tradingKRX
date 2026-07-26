"""
KRX [13108] PDF (Portfolio Deposit File) 자동 다운로드  ―  pykrx 미사용 (requests only)
=========================================================================================
종목코드(ETF) 딕셔너리 {코드: 종목명} × 수집일자 리스트의 모든 조합을 반복 조회하여 CSV 저장 + MySQL 적재.

동작 방식 (KRX 정보데이터시스템 내부 API를 그대로 사용):
  · 로그인      : POST MDCCOMS001D1.cmd  (중복로그인 시 skipDup=Y 재전송)
  · 6자리→ISIN  : getJsonData.cmd + bld=dbms/comm/finder/finder_secuprodisu
  · PDF 조회    : getJsonData.cmd + bld=dbms/MDC/STAT/standard/MDCSTAT05001

■ 필요 패키지 :  pip install requests pandas pymysql
■ 로그인      :  로그인이 필요하면 아래 환경변수를 '본인 PC'에 설정하세요.
                 (코드에 직접 비밀번호를 넣지 마세요. 없으면 비로그인으로 동작)
       Windows(PowerShell) :  setx KRX_ID "아이디"   /   setx KRX_PW "비번"  (후 새 터미널)
       macOS/Linux         :  export KRX_ID="아이디" ; export KRX_PW="비번"
"""

import os
import time
import inspect
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
import pymysql
import requests

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
        for name in ("32. ETF_PDF_v1.0.py", "ETF_PDF_v1.0.py"):
            if os.path.isfile(os.path.join(wd, name)):
                return wd
        etf_dir = os.path.join(wd, "30. ETF")
        for name in ("32. ETF_PDF_v1.0.py", "ETF_PDF_v1.0.py"):
            if os.path.isfile(os.path.join(etf_dir, name)):
                return etf_dir
        etf_dir2 = os.path.join(wd, "ETF")
        for name in ("32. ETF_PDF_v1.0.py", "ETF_PDF_v1.0.py"):
            if os.path.isfile(os.path.join(etf_dir2, name)):
                return etf_dir2
        return wd

# ─────────────────────────────────────────────────────────────
# 설정 : 이 부분만 바꿔서 사용
# ─────────────────────────────────────────────────────────────
ISU_CODES: dict[str, str] = {
    '471780': 'TIGER 코리아테크액티브',
    '444200': 'SOL 코리아메가테크액티브',
    '495060': 'TIME 코리아밸류업액티브',
    '442260': '마이티 다이나믹퀀트액티브',
    '494220': 'UNICORN SK하이닉스밸류체인액티브',
    '474590': 'WON 반도체밸류체인액티브',
    '388420': 'RISE 비메모리반도체액티브',
    '385720': 'TIME 코스피액티브',
    '445150': 'KODEX 친환경조선해운액티브',
    '364690': 'KODEX 혁신기술테마액티브',
    '0172Y0': 'ACE K수출핵심TOP10산업액티브',
    '0074K0': 'KoAct K수출핵심기업TOP30액티브',
    '445290': 'KODEX 로봇액티브',
    '410870': 'TIME K컬처액티브',
    '0132D0': 'KoAct 글로벌K컬처밸류체인액티브',
    '385510': 'KODEX 신재생에너지액티브',
    '422420': 'RISE 2차전지액티브',
    '404120': 'TIME K신재생에너지액티브',
    '482030': 'KoAct 반도체&2차전지핵심소재액티브',
    '0000Z0': 'RISE 바이오TOP10액티브',
    '0168K0': 'TIGER 기술이전바이오액티브',
    '463050': 'TIME K바이오액티브',
    '0162Y0': 'TIME 코스닥액티브',
    '0163Y0': 'KoAct 코스닥액티브',
}

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
    out = session.post(JSON_URL, data=params, headers=HEADERS, timeout=15).json()
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


def find_latest_pdf_biz_day(session: requests.Session, isin: str, max_lookback: int = BIZDAY_LOOKBACK) -> str:
    """
    KRX PDF가 조회되는 가장 최근 영업일(YYYYMMDD).
    15시 이전(새벽·오전)에는 캘린더 당일을 쓰지 않고 전일부터 탐색한다.
    (장중·마감 전 당일 PDF가 비어 있거나 불완전할 수 있음)
    """
    if datetime.now().hour < 15:
        candidate = date.today() - timedelta(days=1)
        print(f'· 15시 이전 실행 → 영업일 탐색 시작: {candidate.strftime("%Y%m%d")} (당일 제외)')
    else:
        candidate = date.today()

    for i in range(max_lookback):
        day_str = candidate.strftime('%Y%m%d')
        print(f'KRX PDF 영업일 확인: {day_str} ({i + 1}/{max_lookback})')
        try:
            df = fetch_pdf(session, isin, day_str)
            if df is not None and not df.empty:
                print(f'· KRX PDF 최신 영업일: {day_str}')
                return day_str
        except Exception as e:
            print(f'  → 조회 실패: {e}')
        time.sleep(0.3)
        candidate -= timedelta(days=1)
    raise RuntimeError(f'최근 {max_lookback}일 내 KRX PDF 조회 가능일을 찾지 못했습니다.')


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
    """start_excl < d <= end_incl 인 평일(월~금) 리스트."""
    days = []
    d = start_excl + timedelta(days=1)
    while d <= end_incl:
        if d.weekday() < 5:
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
    평일 후보 중 실제 거래일만 남긴다.
    - OHLCV에 있으면 채택
    - end_d(이미 최신일로 확인됨)면 채택
    - 그 외는 PDF 조회로 확인 (공휴일 제외)
    """
    verified = []
    for d in candidates:
        if d in known or d == end_d:
            verified.append(d)
            continue
        day_str = d.strftime('%Y%m%d')
        try:
            pdf = fetch_pdf(session, sample_isin, day_str)
            if pdf is not None and not pdf.empty:
                verified.append(d)
            else:
                print(f'  · 공휴일/무자료 제외: {day_str}')
        except Exception:
            print(f'  · 공휴일/무자료 제외: {day_str}')
        time.sleep(0.2)
    return verified


def resolve_trd_dates(session: requests.Session, sample_isin: str) -> list:
    """
    수집 대상 일자(YYYYMMDD) 리스트.
    - end: KRX PDF 조회 가능한 최신 영업일 (15시 전이면 전일부터 탐색)
    - FORCE_INITIAL_BACKFILL 또는 DB 비어 있으면 최근 INITIAL_LOOKBACK_DAYS 거래일
    - 그 외: (DB 최신일 다음날 ~ end] 거래일 (공휴일 제외, OHLCV 공백 일자도 PDF로 보충)
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
        if last >= end_d:
            print('· 이미 KRX 최신일까지 적재됨 → 수집 스킵')
            return []

    candidates = _weekday_range(last, end_d)
    known = set(_known_trading_days_from_ohlcv(last, end_d))
    days = _filter_trading_days_with_pdf(session, sample_isin, candidates, known, end_d)
    days = sorted({d for d in days if last < d <= end_d})
    out = [d.strftime('%Y%m%d') for d in days]
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
# 실행
# ─────────────────────────────────────────────────────────────
def main():
    session = requests.Session()
    if not krx_login(session):
        return

    isin_map = build_isin_map(session)
    frames, errors = [], []

    # 영업일 판별용 샘플 ISIN (딕셔너리 순서상 첫 유효 종목)
    sample_isin = None
    for code in ISU_CODES:
        sample_isin = isin_map.get(code)
        if sample_isin:
            break
    if not sample_isin:
        print('⚠️ ISIN 변환 가능한 종목이 없어 수집을 중단합니다.')
        return

    trd_dates = resolve_trd_dates(session, sample_isin)
    if not trd_dates:
        print('\n수집할 신규 영업일이 없습니다.')
        return

    for code, etf_name in ISU_CODES.items():
        isin = isin_map.get(code)
        if not isin:
            errors.append((code, "-", f"ISIN 변환 실패({etf_name})"))
            print(f"[실패] {code} ({etf_name}) : ISIN 변환 실패")
            continue

        for date in trd_dates:
            try:
                df = fetch_pdf(session, isin, date)
                if df.empty:
                    errors.append((code, date, f"빈 데이터({etf_name})"))
                    print(f"[빈값] {code} ({etf_name}) / {date}")
                else:
                    df.insert(0, "수집일자", date)
                    df.insert(0, "ETF명", etf_name)
                    df.insert(0, "ETF코드", code)
                    frames.append(df)
                    print(f"[OK]  {code} ({etf_name}) / {date} : {len(df)}개 구성종목")
            except Exception as e:
                errors.append((code, date, f"{etf_name}: {e}"))
                print(f"[실패] {code} ({etf_name}) / {date} : {e}")
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


if __name__ == "__main__":
    main()