# -*- coding: utf-8 -*-
"""
ETF OHLCV 데이터 수집 스크립트

서버에서 ETF 기본 정보를 가져오고, KRX 정보데이터시스템(MDCSTAT04301)
일자 CSV로 전 ETF OHLCV를 수집·적재한다. (종목별 크롤링 금지)
"""


import os
import sys
from pathlib import Path

os.environ["REPO_ROOT"] = r"C:\Users\hachi\OneDrive\02. Project\tradingKRX"

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
from krx_naver_ohlcv import (
    get_index_ohlcv_from_naver,
    index_close_series_from_naver,
)
load_project_env()


import pymysql
import pandas as pd
from sqlalchemy import create_engine
import requests as rq
from bs4 import BeautifulSoup
import re
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import traceback
import time
import os
import sys
import webbrowser
import threading
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
import numpy as np
import math

# Windows(cp949) 콘솔에서 유니코드(✓, ⚠️ 등) 출력 시 깨짐/예외 방지
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

_FETCH_RESULT_TIMEOUT_SEC = 300  # (레거시) 폴백 워커용
_FETCH_BATCH_SIZE = 48
_FETCH_MAX_WORKERS = 6


# 서버 설정 (.env)
DB_CONFIG = db_connect_kwargs()

# ---------------------------------------------------------------------------
# KRX OTP CSV (MDCSTAT04301) — naverPub/krx_client 로직 이식 (import 불가)
# ---------------------------------------------------------------------------
_KRX_BASE = 'https://data.krx.co.kr'
KRX_ETF_OHLCV_HEADERS = {
    'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050201',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
}
OTP_URL = f'{_KRX_BASE}/comm/fileDn/GenerateOTP/generate.cmd'
DOWN_URL = f'{_KRX_BASE}/comm/fileDn/download_csv/download.cmd'
LOGIN_PAGE = f'{_KRX_BASE}/contents/MDC/COMS/client/MDCCOMS001.cmd'
LOGIN_JSP = f'{_KRX_BASE}/contents/MDC/COMS/client/view/login.jsp?site=mdc'
LOGIN_URL = f'{_KRX_BASE}/contents/MDC/COMS/client/MDCCOMS001D1.cmd'
_LOGIN_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
)
BLD_ETF_OHLCV = 'dbms/MDC/STAT/standard/MDCSTAT04301'


def krx_login(session):
    """KRX_ID / KRX_PW 환경변수로 로그인."""
    uid, upw = require_env('KRX_ID'), require_env('KRX_PW')
    session.get(LOGIN_PAGE, headers={'User-Agent': _LOGIN_UA}, timeout=15)
    session.get(LOGIN_JSP, headers={'User-Agent': _LOGIN_UA, 'Referer': LOGIN_PAGE}, timeout=15)
    payload = {
        'mbrNm': '', 'telNo': '', 'di': '', 'certType': '',
        'mbrId': uid, 'pw': upw,
    }
    h = {'User-Agent': _LOGIN_UA, 'Referer': LOGIN_PAGE}
    data = session.post(LOGIN_URL, data=payload, headers=h, timeout=15).json()
    code = data.get('_error_code', '')
    if code == 'CD010':
        print('⚠️ 비밀번호 변경이 필요합니다. krx.co.kr 에서 변경 후 재시도하세요.')
        return False
    if code == 'CD011':
        payload['skipDup'] = 'Y'
        data = session.post(LOGIN_URL, data=payload, headers=h, timeout=15).json()
        code = data.get('_error_code', '')
    if code == 'CD001':
        print('· KRX 로그인 성공')
        return True
    print(f"⚠️ 로그인 실패: {code} / {data.get('_error_message', '')}")
    return False


def get_krx_csv(session, bld, params, retries=3):
    """OTP 발급 후 CSV bytes. csvxls_isNo=false 고정."""
    otp_params = {
        'locale': 'ko_KR',
        'name': 'fileDown',
        'csvxls_isNo': 'false',
        'url': bld,
    }
    otp_params.update(params)
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            otp = session.post(OTP_URL, data=otp_params, timeout=30).text
            if 'LOGOUT' in str(otp).upper():
                raise RuntimeError('OTP=LOGOUT — 로그인 세션이 없습니다.')
            res = session.post(DOWN_URL, data={'code': otp}, timeout=30)
            res.raise_for_status()
            if len(res.content) < 100:
                raise ValueError(f'응답이 비정상적으로 짧음 ({len(res.content)} bytes)')
            time.sleep(1)
            return res.content
        except Exception as e:
            last_err = e
            print(f'KRX 다운로드 실패 ({bld}, {attempt}/{retries}): {e}')
            time.sleep(1 if attempt < retries else 0)
    raise RuntimeError(f'KRX CSV 다운로드 실패 (bld={bld}): {last_err}')


def _etf_num_series(s):
    t = s.astype(str).str.replace(',', '', regex=False).str.strip()
    t = t.mask(t.isin(['-', '', 'nan', 'None', 'NaN', '<NA>']))
    return pd.to_numeric(t, errors='coerce')


def _etf_pad_ticker(s):
    t = s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    num = t.str.fullmatch(r'\d+', na=False)
    return t.where(~num, t.str.zfill(6))


def parse_krx_etf_ohlcv_day_csv(df, day_str):
    """KRX MDCSTAT04301 CSV → ETF OHLCV 적재용 DF."""
    d = df.copy()
    d.columns = d.columns.str.replace(' ', '')
    rename = {
        '종목코드': 'ticker',
        '종목명': 'name',
        '시가': 'open',
        '고가': 'high',
        '저가': 'low',
        '종가': 'close',
        '거래량': 'volume',
        '거래대금': 'trading_value',
        '순자산가치': 'nav',
        '순자산가치(NAV)': 'nav',
        '시가총액': 'mcap',
        '등락률': 'chg_pct',
    }
    d = d.rename(columns={k: v for k, v in rename.items() if k in d.columns})
    if 'ticker' not in d.columns or 'close' not in d.columns:
        raise ValueError(f"ETF CSV에 ticker/close 없음: {list(d.columns)}")
    d['ticker'] = _etf_pad_ticker(d['ticker'])
    for c in ('open', 'high', 'low', 'close', 'volume', 'trading_value', 'nav', 'mcap', 'chg_pct'):
        if c in d.columns:
            d[c] = _etf_num_series(d[c])
        else:
            d[c] = np.nan
    if 'name' not in d.columns:
        d['name'] = None
    d['date'] = datetime.strptime(day_str, '%Y%m%d').date()
    cols = [
        'ticker', 'date', 'name', 'open', 'high', 'low', 'close', 'volume',
        'trading_value', 'nav', 'mcap', 'chg_pct',
    ]
    return d[cols].dropna(subset=['ticker', 'close'])


def download_krx_etf_ohlcv_day(session, day_str):
    """전 ETF 시세 CSV (MDCSTAT04301)."""
    content = get_krx_csv(
        session,
        BLD_ETF_OHLCV,
        {'trdDd': day_str, 'share': '1', 'money': '1'},
    )
    try:
        raw = pd.read_csv(BytesIO(content), encoding='EUC-KR')
    except Exception:
        try:
            raw = pd.read_csv(BytesIO(content), encoding='cp949')
        except Exception:
            return pd.DataFrame(), content
    if raw is None or len(raw) == 0:
        return pd.DataFrame(), content
    return parse_krx_etf_ohlcv_day_csv(raw, day_str), content


ETF_OHLCV_INITIAL_TRADING_DAYS = 250
ETF_OHLCV_INITIAL_START = None  # 예: '20240101'


def _calendar_days_for_trading_days(n_trading):
    n = max(1, int(n_trading))
    return (n * 7) // 5 + 40


def _calendar_ymd_range(start_d, end_d):
    if start_d is None or end_d is None or start_d > end_d:
        return []
    return [d.strftime('%Y%m%d') for d in pd.date_range(start_d, end_d, freq='D')]


def resolve_etf_ohlcv_collect_plan(
    biz_day,
    initial_trading_days=ETF_OHLCV_INITIAL_TRADING_DAYS,
    initial_start=ETF_OHLCV_INITIAL_START,
):
    """
    DB MAX(date) 기준 자동 증분.
    - 비어 있음: 최근 N거래일(또는 지정 시작일) ~ biz_day
    - MAX == biz_day: 스킵
    - MAX < biz_day: (MAX+1) ~ biz_day
    """
    end = _as_date(biz_day)
    db_max = None
    try:
        con = pymysql.connect(**DB_CONFIG)
        cur = con.cursor()
        cur.execute('SELECT MAX(`date`) FROM krx_etf_ohlcv')
        row = cur.fetchone()
        con.close()
        db_max = _as_date(row[0]) if row else None
    except Exception:
        db_max = None

    plan = {
        'mode': 'skip',
        'db_max': db_max,
        'biz_day': end,
        'from_date': None,
        'to_date': end,
        'dates': [],
        'message': '',
    }
    if end is None:
        plan['message'] = '기준영업일 없음 — 스킵'
        return plan

    if db_max is None:
        start = _as_date(initial_start) if initial_start else (
            end - timedelta(days=_calendar_days_for_trading_days(initial_trading_days))
        )
        plan['mode'] = 'initial'
        plan['from_date'] = start
        plan['dates'] = _calendar_ymd_range(start, end)
        plan['message'] = (
            f'DB 비어 있음 → 초기 백필 {start}~{end} '
            f'(캘린더 {len(plan["dates"])}일, 목표≈{initial_trading_days}거래일)'
        )
        return plan

    if db_max >= end:
        plan['mode'] = 'skip'
        plan['from_date'] = db_max
        plan['message'] = f'이미 적재됨 (DB 최신={db_max}, 기준일={end}) — 수집 스킵'
        return plan

    start = db_max + timedelta(days=1)
    plan['mode'] = 'incremental'
    plan['from_date'] = start
    plan['dates'] = _calendar_ymd_range(start, end)
    plan['message'] = (
        f'증분 수집 {start}~{end} (DB 최신={db_max}, 캘린더 {len(plan["dates"])}일)'
    )
    return plan


def _etf_day_insert_update_counts(day, tickers):
    """해당일 기존 티커 수로 신규/갱신 추정."""
    if not tickers:
        return 0, 0
    existing = set()
    try:
        con = pymysql.connect(**DB_CONFIG)
        cur = con.cursor()
        tickers = [str(t) for t in tickers]
        for i in range(0, len(tickers), 500):
            chunk = tickers[i:i + 500]
            ph = ','.join(['%s'] * len(chunk))
            cur.execute(
                f'SELECT ticker FROM krx_etf_ohlcv WHERE `date`=%s AND ticker IN ({ph})',
                (day, *chunk),
            )
            existing.update(str(r[0]) for r in cur.fetchall())
        con.close()
    except Exception:
        return len(tickers), 0
    inserted = sum(1 for t in tickers if t not in existing)
    return inserted, len(tickers) - inserted

# OHLCV 기본 조회·모멘텀: 최소 거래일 수 및 캘린더 룩백(주말·공휴일 여유)
MIN_OHLCV_TRADING_DAYS = 125
# 모멘텀 대시보드(analyze_etf_momentum): 120일·125일 전량이 아니라 20거래일 이상이면 표에 포함
MOMENTUM_DASHBOARD_MIN_TRADING_DAYS = 20
# 지연 종가 모멘텀: (컬럼 접미사, T-0에서 뺄 거래일 수). 예: `_T5` → 5거래일 전 봉 종가를 구간 끝으로.
# 컬럼·지연 모멘텀 정의 순서(큰 lag 먼저: T-5 → T-3). 요약 HTML·섹터맵 등 **표시 순서**는 T-0 → T-3 → T-5.
MOMENTUM_DEFERRED_LAGS = (('_T5', 5), ('_T3', 3))
MOMENTUM_END_BARS_OFFSET = 5  # 하위 호환: T-5 lag와 동일
OHLCV_LOOKBACK_CALENDAR_DAYS = (MIN_OHLCV_TRADING_DAYS * 365) // 252 + 60

# SQLAlchemy 엔진 생성
engine = create_engine(db_url())


def get_etf_info_all_columns(criteria_date=None):
    """
    krx_etf_info 테이블의 모든 컬럼 정보를 가져옵니다.
    
    Args:
        criteria_date (str, optional): 기준일 (YYYY-MM-DD 형식). None이면 최신 기준일 사용.
    
    Returns:
        pandas.DataFrame: ETF 전체 정보 (모든 컬럼 포함)
    """
    if criteria_date is None:
        # 최신 기준일 조회
        query_latest = """
        SELECT MAX(기준일) as latest_date
        FROM krx_etf_info;
        """
        try:
            latest_df = pd.read_sql(query_latest, con=engine)
            if not latest_df.empty and latest_df['latest_date'].iloc[0] is not None:
                criteria_date = str(latest_df['latest_date'].iloc[0])
                print(f"최신 기준일: {criteria_date}")
            else:
                print("⚠️ 기준일을 찾을 수 없습니다.")
                return pd.DataFrame()
        except Exception as e:
            print(f"⚠️ 최신 기준일 조회 중 오류: {e}")
            return pd.DataFrame()
    
    query = """
    SELECT 
        종목코드,
        종목명,
        종가,
        대비,
        등락률,
        순자산가치,
        시가,
        고가,
        저가,
        거래량,
        거래대금,
        시가총액,
        순자산총액,
        상장좌수,
        기초지수_지수명,
        기초지수_종가,
        기초지수_대비,
        기초지수_등락률,
        기준일
    FROM krx_etf_info
    WHERE 기준일 = %s
    ORDER BY 종목코드;
    """
    
    try:
        etf_df = pd.read_sql(query, con=engine, params=(criteria_date,))
        print(f"✓ {criteria_date} 기준 ETF 전체 정보 {len(etf_df)}개 조회 완료")
        print(f"  컬럼 수: {len(etf_df.columns)}개")
        print(f"  컬럼명: {', '.join(etf_df.columns.tolist())}")
        return etf_df
    except Exception as e:
        print(f"⚠️ ETF 전체 정보 조회 중 오류 발생: {e}")
        print(traceback.format_exc())
        return pd.DataFrame()


def get_etf_list_from_db():
    """
    서버에서 ETF 기본 정보를 가져옵니다.
    최신 기준일의 데이터만 가져옵니다.
    
    Returns:
        pandas.DataFrame: ETF 기본 정보 (종목코드, 종목명, 종가, 대비, 등락률, 거래량, 거래대금, 기준일)
    """
    query = """
    SELECT 
        종목코드,
        종목명,
        종가,
        대비,
        등락률,
        거래량,
        거래대금,
        기준일
    FROM krx_etf_info
    WHERE 기준일 = (SELECT MAX(기준일) FROM krx_etf_info)
    ORDER BY 종목코드;
    """
    
    try:
        etf_df = pd.read_sql(query, con=engine)
        # 컬럼명을 기존 코드와 호환되도록 변경
        if not etf_df.empty:
            etf_df = etf_df.rename(columns={
                '종목코드': 'ticker',
                '종목명': 'etf_name',
                '종가': '기준가',
                '대비': '전일대비',
                '기준일': 'update_date'
            })
        print(f"✓ ETF 기본 정보 {len(etf_df)}개 조회 완료")
        return etf_df
    except Exception as e:
        print(f"⚠️ ETF 기본 정보 조회 중 오류 발생: {e}")
        return pd.DataFrame()


def get_etf_list_by_update_date(update_date=None):
    """
    특정 기준일의 ETF 리스트를 가져옵니다.
    
    Args:
        update_date (str, optional): 기준일 (YYYY-MM-DD 형식). None이면 최신 날짜 사용.
    
    Returns:
        pandas.DataFrame: ETF 기본 정보
    """
    if update_date is None:
        # 최신 기준일 조회
        query_latest = """
        SELECT MAX(기준일) as latest_date
        FROM krx_etf_info;
        """
        try:
            latest_df = pd.read_sql(query_latest, con=engine)
            if not latest_df.empty and latest_df['latest_date'].iloc[0] is not None:
                update_date = str(latest_df['latest_date'].iloc[0])
                print(f"최신 기준일: {update_date}")
            else:
                print("⚠️ 기준일을 찾을 수 없습니다.")
                return pd.DataFrame()
        except Exception as e:
            print(f"⚠️ 최신 기준일 조회 중 오류: {e}")
            return pd.DataFrame()
    
    query = """
    SELECT 
        종목코드,
        종목명,
        종가,
        대비,
        등락률,
        거래량,
        거래대금,
        기준일
    FROM krx_etf_info
    WHERE 기준일 = %s
    ORDER BY 종목코드;
    """
    
    try:
        etf_df = pd.read_sql(query, con=engine, params=(update_date,))
        # 컬럼명을 기존 코드와 호환되도록 변경
        if not etf_df.empty:
            etf_df = etf_df.rename(columns={
                '종목코드': 'ticker',
                '종목명': 'etf_name',
                '종가': '기준가',
                '대비': '전일대비',
                '기준일': 'update_date'
            })
        print(f"✓ {update_date} 기준 ETF 기본 정보 {len(etf_df)}개 조회 완료")
        return etf_df
    except Exception as e:
        print(f"⚠️ ETF 기본 정보 조회 중 오류 발생: {e}")
        return pd.DataFrame()


def get_etf_info_by_ticker(ticker):
    """
    특정 티커의 ETF 기본 정보를 가져옵니다.
    최신 기준일의 데이터를 가져옵니다.
    
    Args:
        ticker (str): ETF 티커 코드 (6자리)
    
    Returns:
        pandas.DataFrame: ETF 기본 정보 (단일 행)
    """
    query = """
    SELECT 
        종목코드,
        종목명,
        종가,
        대비,
        등락률,
        거래량,
        거래대금,
        기준일
    FROM krx_etf_info
    WHERE 종목코드 = %s
    ORDER BY 기준일 DESC
    LIMIT 1;
    """
    
    try:
        etf_df = pd.read_sql(query, con=engine, params=(ticker,))
        # 컬럼명을 기존 코드와 호환되도록 변경
        if not etf_df.empty:
            etf_df = etf_df.rename(columns={
                '종목코드': 'ticker',
                '종목명': 'etf_name',
                '종가': '기준가',
                '대비': '전일대비',
                '기준일': 'update_date'
            })
            print(f"✓ ETF {ticker} 정보 조회 완료: {etf_df['etf_name'].iloc[0]}")
        else:
            print(f"⚠️ ETF {ticker} 정보를 찾을 수 없습니다.")
        return etf_df
    except Exception as e:
        print(f"⚠️ ETF 정보 조회 중 오류 발생: {e}")
        return pd.DataFrame()


def ensure_etf_ohlcv_table(verbose=False):
    """
    ETF OHLCV 테이블이 존재하는지 확인하고, 없으면 생성합니다.
    
    Args:
        verbose (bool): 메시지 출력 여부
    """
    con = pymysql.connect(**DB_CONFIG)
    mycursor = con.cursor()
    
    create_etf_ohlcv_table = """
    CREATE TABLE IF NOT EXISTS krx_etf_ohlcv (
        ticker VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open DECIMAL(15, 2),
        high DECIMAL(15, 2),
        low DECIMAL(15, 2),
        close DECIMAL(15, 2),
        volume BIGINT,
        PRIMARY KEY (ticker, date),
        INDEX idx_date (date),
        INDEX idx_ticker (ticker)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    
    try:
        mycursor.execute(create_etf_ohlcv_table)
        con.commit()
        if verbose:
            print("✓ ETF OHLCV 테이블 확인 완료")
        return True
    except Exception as e:
        if verbose:
            print(f"⚠️ ETF OHLCV 테이블 생성 확인 중 오류: {e}")
        con.rollback()
        return False
    finally:
        con.close()


def save_etf_ohlcv_to_db(ohlcv_df, batch_size=50):
    """
    ETF OHLCV 데이터를 DB에 저장합니다.
    
    Args:
        ohlcv_df (pandas.DataFrame): OHLCV 데이터 (ticker, date, open, high, low, close, volume 컬럼 필요)
        batch_size (int): 배치 커밋 크기
    
    Returns:
        dict: 저장 결과 {'success': bool, 'records_saved': int, 'error': str}
    """
    if ohlcv_df.empty:
        return {'success': False, 'records_saved': 0, 'error': '데이터프레임이 비어있습니다.'}
    
    # 필요한 컬럼 확인
    required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in ohlcv_df.columns]
    if missing_columns:
        return {'success': False, 'records_saved': 0, 'error': f'필수 컬럼 누락: {missing_columns}'}
    
    # 테이블 확인 (조용히 수행)
    ensure_etf_ohlcv_table(verbose=False)
    
    # DB 연결 (락 타임아웃 설정)
    db_config_with_timeout = DB_CONFIG.copy()
    con = pymysql.connect(**db_config_with_timeout)
    mycursor = con.cursor()
    
    # 락 대기 시간 증가 (기본 50초 -> 120초)
    try:
        mycursor.execute("SET innodb_lock_wait_timeout = 120")
    except:
        pass  # 설정 실패해도 계속 진행
    
    # ETF OHLCV 저장 쿼리
    query_etf_ohlcv = """
    INSERT INTO krx_etf_ohlcv (ticker, date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
    open=new.open, high=new.high, low=new.low, close=new.close, volume=new.volume;
    """
    
    # 재시도 로직
    max_retries = 3
    retry_delay = 1  # 초
    
    for attempt in range(max_retries):
        try:
            # 데이터 준비
            data_to_save = ohlcv_df[required_columns].copy()
            
            # 데이터 타입 확인 및 변환
            data_to_save['ticker'] = data_to_save['ticker'].astype(str)
            if data_to_save['date'].dtype != 'object':
                data_to_save['date'] = pd.to_datetime(data_to_save['date']).dt.date
            else:
                data_to_save['date'] = pd.to_datetime(data_to_save['date']).dt.date
            
            # 숫자 컬럼 변환
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                data_to_save[col] = pd.to_numeric(data_to_save[col], errors='coerce')
            
            # NaN 값 제거
            data_to_save = data_to_save.dropna()
            
            if data_to_save.empty:
                con.close()
                return {'success': False, 'records_saved': 0, 'error': '유효한 데이터가 없습니다.'}
            
            # 작은 배치로 나누어 처리 (락 경합 감소)
            total_records = len(data_to_save)
            batch_size = 100  # 배치 크기 축소
            saved_count = 0
            
            args = data_to_save.values.tolist()
            
            # 배치 단위로 처리
            for i in range(0, len(args), batch_size):
                batch = args[i:i+batch_size]
                try:
                    mycursor.executemany(query_etf_ohlcv, batch)
                    con.commit()  # 각 배치마다 커밋
                    saved_count += len(batch)
                except Exception as batch_error:
                    # 배치 오류 시 롤백하고 재시도
                    con.rollback()
                    if 'Lock wait timeout' in str(batch_error) and attempt < max_retries - 1:
                        # 락 타임아웃이면 재시도
                        time.sleep(retry_delay * (attempt + 1))
                        break  # 외부 재시도 루프로
                    else:
                        raise batch_error
            
            # 모든 배치가 성공적으로 처리된 경우
            if saved_count == total_records:
                con.close()
                return {
                    'success': True,
                    'records_saved': saved_count,
                    'error': None
                }
            else:
                # 일부만 저장된 경우 (재시도 필요)
                con.rollback()
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    con.close()
                    return {
                        'success': False,
                        'records_saved': saved_count,
                        'error': f'일부 데이터만 저장됨: {saved_count}/{total_records}'
                    }
            
        except Exception as e:
            con.rollback()
            error_msg = str(e)
            
            # 락 타임아웃 오류인 경우 재시도
            if 'Lock wait timeout' in error_msg and attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            else:
                con.close()
                return {
                    'success': False,
                    'records_saved': 0,
                    'error': f"DB 저장 오류: {error_msg}"
                }
    
    # 모든 재시도 실패
    con.close()
    return {
        'success': False,
        'records_saved': 0,
        'error': 'DB 저장 실패: 최대 재시도 횟수 초과'
    }


def save_etf_ohlcv_by_ticker(ticker, ohlcv_df, batch_size=50):
    """
    특정 티커의 ETF OHLCV 데이터를 DB에 저장합니다.
    
    Args:
        ticker (str): ETF 티커 코드
        ohlcv_df (pandas.DataFrame): OHLCV 데이터
        batch_size (int): 배치 커밋 크기
    
    Returns:
        dict: 저장 결과
    """
    if ohlcv_df.empty:
        return {'success': False, 'records_saved': 0, 'error': '데이터프레임이 비어있습니다.'}
    
    # ticker 컬럼이 없으면 추가
    if 'ticker' not in ohlcv_df.columns:
        ohlcv_df = ohlcv_df.copy()
        ohlcv_df['ticker'] = ticker
    
    return save_etf_ohlcv_to_db(ohlcv_df, batch_size=batch_size)




def _as_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip().replace("-", "").replace(".", "")
    if len(s) >= 8 and s[:8].isdigit():
        return datetime.strptime(s[:8], "%Y%m%d").date()
    return pd.to_datetime(v, errors="coerce").date()


def resolve_etf_biz_day():
    """
    기준 영업일: krx_index_ohlcv(1001) 최신일 → 없으면 krx_etf_ohlcv 최신일 → 오늘.
    """
    try:
        q = "SELECT MAX(date) AS d FROM krx_index_ohlcv WHERE ticker = '1001'"
        df = pd.read_sql(q, con=engine)
        if df is not None and len(df) and pd.notna(df.iloc[0]["d"]):
            return _as_date(df.iloc[0]["d"])
    except Exception:
        pass
    try:
        q = "SELECT MAX(date) AS d FROM krx_etf_ohlcv"
        df = pd.read_sql(q, con=engine)
        if df is not None and len(df) and pd.notna(df.iloc[0]["d"]):
            return _as_date(df.iloc[0]["d"])
    except Exception:
        pass
    return date.today()


def get_etf_ohlcv_latest_dates(tickers=None):
    """ticker -> DB 최신 저장일(date). tickers 없으면 전체."""
    try:
        if tickers:
            tickers = [str(t) for t in tickers]
            ph = ",".join(["%s"] * len(tickers))
            q = (
                f"SELECT ticker, MAX(date) AS max_date FROM krx_etf_ohlcv "
                f"WHERE ticker IN ({ph}) GROUP BY ticker"
            )
            df = pd.read_sql(q, con=engine, params=tuple(tickers))
        else:
            q = "SELECT ticker, MAX(date) AS max_date FROM krx_etf_ohlcv GROUP BY ticker"
            df = pd.read_sql(q, con=engine)
        if df is None or df.empty:
            return {}
        out = {}
        for _, row in df.iterrows():
            d = _as_date(row["max_date"])
            if d is not None:
                out[str(row["ticker"])] = d
        return out
    except Exception:
        return {}


def collect_etf_ohlcv_data(
    max_etf=None,
    max_pages_per_etf=None,
    full_backfill=False,
    biz_day=None,
    backfill_years=3,
    initial_trading_days=ETF_OHLCV_INITIAL_TRADING_DAYS,
    initial_start=ETF_OHLCV_INITIAL_START,
):
    """
    ETF OHLCV 수집·DB 저장 (KRX MDCSTAT04301 일자 CSV).

    수집 범위는 DB MAX(date) 자동 증분:
      - 비어 있음 → 최근 initial_trading_days(또는 initial_start)부터
      - MAX == biz_day → 스킵
      - MAX < biz_day → 밀린 일만
    full_backfill=True(레거시): DB 무시하고 초기 백필과 동일하게 강제 재수집.
    """
    _ = max_pages_per_etf
    _ = backfill_years
    ensure_etf_ohlcv_table(verbose=True)

    if biz_day is None:
        biz_day = resolve_etf_biz_day()
    else:
        biz_day = _as_date(biz_day)
    print(f"기준 영업일(biz_day): {biz_day}")
    print("원천: KRX CSV MDCSTAT04301 (DB 자동 증분)")

    print("ETF 리스트 조회 중...")
    etf_list = get_etf_list_from_db()
    if etf_list.empty:
        print("⚠️ ETF 리스트가 비어있습니다.")
        return {
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "total_records": 0,
            "inserted": 0,
            "updated": 0,
            "error_list": [],
            "biz_day": biz_day,
            "mode": "empty_list",
        }

    if max_etf:
        etf_list = etf_list.head(max_etf)

    tickers = set()
    for _, r in etf_list.iterrows():
        t = str(r["ticker"]).strip()
        tickers.add(t.zfill(6) if t.isdigit() else t)

    if full_backfill:
        # 레거시 강제 백필: DB 최신을 무시하고 초기 구간부터
        end = _as_date(biz_day)
        start = _as_date(initial_start) if initial_start else (
            end - timedelta(days=_calendar_days_for_trading_days(initial_trading_days))
        )
        plan = {
            "mode": "initial",
            "db_max": None,
            "biz_day": end,
            "from_date": start,
            "to_date": end,
            "dates": _calendar_ymd_range(start, end),
            "message": (
                f"강제 백필(full_backfill) {start}~{end} "
                f"(캘린더 {len(_calendar_ymd_range(start, end))}일)"
            ),
        }
    else:
        plan = resolve_etf_ohlcv_collect_plan(
            biz_day,
            initial_trading_days=initial_trading_days,
            initial_start=initial_start,
        )

    print(f"  {plan['message']}")
    print(
        f"  DB 최신={plan['db_max']}, 기준일={plan['biz_day']}, 모드={plan['mode']}, "
        f"ETF 필터={len(tickers)}"
    )
    dates = plan["dates"]

    if plan["mode"] == "skip" or not dates:
        print("  (이미 적재됨 — ETF OHLCV CSV 스킵)")
        return {
            "success": 0,
            "failed": 0,
            "skipped": len(tickers),
            "total_records": 0,
            "inserted": 0,
            "updated": 0,
            "error_list": [],
            "biz_day": biz_day,
            "mode": "skip",
        }

    print(
        f"  대상 구간: {dates[0]}~{dates[-1]} "
        f"(캘린더 {len(dates)}일, 휴장은 응답으로 스킵)"
    )

    session = rq.Session()
    session.headers.update(KRX_ETF_OHLCV_HEADERS)
    if not krx_login(session):
        session.close()
        raise RuntimeError("KRX 로그인 실패. 환경변수 KRX_ID / KRX_PW 를 확인하세요.")

    error_days = []
    ok_days = 0
    empty_days = 0
    total_records = 0
    inserted_rows = 0
    updated_rows = 0
    try:
        for day in tqdm(dates, desc="ETF OHLCV KRX CSV"):
            try:
                day_df, _raw = download_krx_etf_ohlcv_day(session, day)
                if day_df is None or day_df.empty:
                    empty_days += 1
                    continue
                day_df = day_df[day_df["ticker"].isin(tickers)]
                if day_df.empty:
                    empty_days += 1
                    continue
                save_df = day_df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
                day_date = _as_date(save_df["date"].iloc[0])
                ins, upd = _etf_day_insert_update_counts(day_date, save_df["ticker"].tolist())
                result = save_etf_ohlcv_to_db(save_df, batch_size=200)
                if result.get("success"):
                    ok_days += 1
                    n = int(result.get("records_saved") or 0)
                    total_records += n
                    inserted_rows += ins
                    updated_rows += upd
                else:
                    error_days.append(day)
                    print(f"  ⚠️ {day} 저장 실패: {result.get('error')}")
            except Exception as e:
                error_days.append(day)
                print(f"  ⚠️ {day} ETF OHLCV 수집 실패: {e}")
                print(traceback.format_exc())
    finally:
        session.close()

    print(f"\n✓ ETF OHLCV 데이터 수집 완료 (biz_day={biz_day})")
    print(f"  - 대상 구간: {dates[0]}~{dates[-1]}")
    print(f"  - 수집 거래일: {ok_days} (휴장/빈응답={empty_days})")
    print(f"  - 실패 일수: {len(error_days)}")
    print(f"  - 행수: {total_records} (신규={inserted_rows}, 갱신={updated_rows})")
    if error_days:
        print(f"  실패일: {error_days[:10]}{'...' if len(error_days) > 10 else ''}")

    return {
        "success": ok_days,
        "failed": len(error_days),
        "skipped": empty_days,
        "total_records": total_records,
        "inserted": inserted_rows,
        "updated": updated_rows,
        "error_list": error_days,
        "biz_day": biz_day,
        "mode": plan["mode"],
    }


def retry_failed_etf_ohlcv(
    error_list,
    max_pages_per_etf=None,
    full_backfill=False,
    biz_day=None,
    backfill_years=3,
):
    """
    실패한 '일자'(YYYYMMDD) 재수집. 레거시 티커 리스트가 오면 DB 자동 증분 재실행.
    """
    _ = max_pages_per_etf
    _ = backfill_years
    if not error_list:
        print("재수집할 실패 항목이 없습니다.")
        return {"success": 0, "failed": 0, "skipped": 0, "total_records": 0, "error_list": []}

    if biz_day is None:
        biz_day = resolve_etf_biz_day()
    else:
        biz_day = _as_date(biz_day)

    day_like = [
        str(x).replace("-", "")
        for x in error_list
        if str(x).replace("-", "").isdigit() and len(str(x).replace("-", "")) == 8
    ]
    if not day_like:
        print("레거시 티커 실패 목록 → DB 자동 증분 재수집으로 대체")
        return collect_etf_ohlcv_data(full_backfill=full_backfill, biz_day=biz_day)

    print(f"\n실패한 {len(day_like)}개 일자 ETF OHLCV 재수집... (biz_day={biz_day})")
    ensure_etf_ohlcv_table(verbose=False)
    etf_list = get_etf_list_from_db()
    tickers = set()
    for _, r in etf_list.iterrows():
        t = str(r["ticker"]).strip()
        tickers.add(t.zfill(6) if t.isdigit() else t)

    session = rq.Session()
    session.headers.update(KRX_ETF_OHLCV_HEADERS)
    if not krx_login(session):
        session.close()
        raise RuntimeError("KRX 로그인 실패(재수집).")

    error_list_retry = []
    success_count = 0
    total_records = 0
    try:
        for day in tqdm(day_like, desc="실패일 ETF 재수집"):
            try:
                day_df, _raw = download_krx_etf_ohlcv_day(session, day)
                if day_df is None or day_df.empty:
                    error_list_retry.append(day)
                    continue
                day_df = day_df[day_df["ticker"].isin(tickers)]
                save_df = day_df[["ticker", "date", "open", "high", "low", "close", "volume"]].copy()
                result = save_etf_ohlcv_to_db(save_df, batch_size=200)
                if result.get("success"):
                    success_count += 1
                    total_records += int(result.get("records_saved") or 0)
                else:
                    error_list_retry.append(day)
            except Exception as e:
                error_list_retry.append(day)
                print(f"  ⚠️ {day} 재수집 오류: {e}")
    finally:
        session.close()

    print(f"\n✓ 실패일 ETF 재수집 완료")
    print(f"  - 성공: {success_count}일")
    print(f"  - 실패: {len(error_list_retry)}일")
    print(f"  - 총 레코드 수: {total_records}개")

    return {
        "success": success_count,
        "failed": len(error_list_retry),
        "skipped": 0,
        "total_records": total_records,
        "error_list": error_list_retry,
        "biz_day": biz_day,
    }


def get_etf_ohlcv_from_db(ticker=None, start_date=None, end_date=None):
    """
    DB에서 ETF OHLCV 데이터를 가져옵니다.
    종목명에 '레버리지' 또는 2배 인버스('인버스 x2', '인버스2X', '인버스 2X')가 포함된 ETF는 제외합니다.
    
    Args:
        ticker (str, optional): 특정 티커 코드. None이면 전체 ETF
        start_date (str, optional): 시작일 (YYYY-MM-DD 형식). None이면 최소 125거래일 확보용 캘린더 룩백
        end_date (str, optional): 종료일 (YYYY-MM-DD 형식). None이면 오늘
    
    Returns:
        pandas.DataFrame: OHLCV 데이터
    """
    if end_date is None:
        end_date = date.today()
    else:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    if start_date is None:
        start_date = end_date - timedelta(days=OHLCV_LOOKBACK_CALENDAR_DAYS)
    else:
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    
    # pymysql 직접 연결 사용 (SQLAlchemy 엔진의 파라미터 전달 문제 회피)
    try:
        con = pymysql.connect(**DB_CONFIG)
        cursor = con.cursor()
        
        if ticker:
            query = """
            SELECT o.ticker, o.date, o.open, o.high, o.low, o.close, o.volume
            FROM krx_etf_ohlcv o
            INNER JOIN (
                SELECT 종목코드
                FROM krx_etf_info
                WHERE 기준일 = (SELECT MAX(기준일) FROM krx_etf_info)
                  AND 종목명 NOT LIKE '%%레버리지%%'
                  AND 종목명 NOT LIKE '%%인버스 x2%%'
                  AND 종목명 NOT LIKE '%%인버스2X%%'
                  AND 종목명 NOT LIKE '%%인버스 2X%%'
            ) i ON o.ticker = i.종목코드
            WHERE o.date >= %s AND o.date <= %s AND o.ticker = %s
            ORDER BY o.ticker, o.date
            """
            cursor.execute(query, (start_date, end_date, ticker))
        else:
            query = """
            SELECT o.ticker, o.date, o.open, o.high, o.low, o.close, o.volume
            FROM krx_etf_ohlcv o
            INNER JOIN (
                SELECT 종목코드
                FROM krx_etf_info
                WHERE 기준일 = (SELECT MAX(기준일) FROM krx_etf_info)
                  AND 종목명 NOT LIKE '%%레버리지%%'
                  AND 종목명 NOT LIKE '%%인버스 x2%%'
                  AND 종목명 NOT LIKE '%%인버스2X%%'
                  AND 종목명 NOT LIKE '%%인버스 2X%%'
            ) i ON o.ticker = i.종목코드
            WHERE o.date >= %s AND o.date <= %s
            ORDER BY o.ticker, o.date
            """
            cursor.execute(query, (start_date, end_date))
        
        # 결과 가져오기
        columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
        results = cursor.fetchall()
        
        # DataFrame 생성
        if results:
            df = pd.DataFrame(results, columns=columns)
        else:
            df = pd.DataFrame(columns=columns)
        
        cursor.close()
        con.close()
        return df
    except Exception as e:
        print(f"⚠️ ETF OHLCV 데이터 조회 중 오류: {e}")
        print(traceback.format_exc())
        return pd.DataFrame()


def calculate_momentum_returns(ohlcv_df, periods=[10, 20, 50, 120], min_trading_days=None):
    """
    각 ETF의 기간별 상승률을 계산합니다.
    거래일 기준으로 계산합니다.

    - 기본 `N일_상승률`: 최신 봉 T-0 종가 vs N거래일 전 종가 (T-0 ~ T-N).
    - `N일_상승률_T5`, `N일_상승률_T3`: `MOMENTUM_DEFERRED_LAGS`에 정의된 만큼 T-0에서 거슬러 올라간 봉 종가를
      구간 끝으로 두고, 그보다 N거래일 전 종가와의 수익률(%).

    Args:
        ohlcv_df (pandas.DataFrame): OHLCV 데이터 (ticker, date, close 컬럼 필요)
        periods (list): 계산할 기간 리스트 (거래일 기준)
        min_trading_days (int, optional): 티커당 최소 봉 수. None이면 MIN_OHLCV_TRADING_DAYS와
            min(periods) 중 큰 값(기존 동작). 지정 시 max(min(periods), min_trading_days)로 완화·강화 가능.
    
    Returns:
        pandas.DataFrame: 각 ETF별 기간별 상승률 및 지연 종가 기준 `*_T5`, `*_T3` 등
    """
    if ohlcv_df.empty:
        return pd.DataFrame()
    
    # 날짜 정렬 및 close 값이 유효한 데이터만 필터링
    ohlcv_df = ohlcv_df.sort_values(['ticker', 'date']).copy()
    ohlcv_df = ohlcv_df[ohlcv_df['close'].notna() & (ohlcv_df['close'] > 0)].copy()
    
    if ohlcv_df.empty:
        return pd.DataFrame()
    
    # 각 티커별로 그룹화하여 계산
    results = []
    skipped_count = 0
    processed_count = 0
    
    for ticker in ohlcv_df['ticker'].unique():
        ticker_data = ohlcv_df[ohlcv_df['ticker'] == ticker].copy()
        ticker_data = ticker_data.sort_values('date').reset_index(drop=True)
        
        floor = MIN_OHLCV_TRADING_DAYS if min_trading_days is None else int(min_trading_days)
        min_bars = max(min(periods), floor)
        if len(ticker_data) < min_bars:
            skipped_count += 1
            continue
        
        # 최신 날짜와 종가 (마지막 행)
        latest_idx = len(ticker_data) - 1
        latest_date = ticker_data.iloc[latest_idx]['date']
        latest_close_raw = ticker_data.iloc[latest_idx]['close']
        
        # Decimal 타입을 float로 변환
        try:
            latest_close = float(latest_close_raw)
        except (ValueError, TypeError):
            latest_close = np.nan
        
        # close 값이 유효한지 확인
        if pd.isna(latest_close) or latest_close <= 0:
            skipped_count += 1
            continue
        
        result_row = {
            'ticker': ticker,
            'latest_date': latest_date,
            'latest_close': latest_close
        }
        
        # 일일 수익률 계산 (표준편차용)
        ticker_data = ticker_data.copy()
        ticker_data['prev_close'] = ticker_data['close'].shift(1)
        ticker_data['daily_return_pct'] = np.where(
            (ticker_data['prev_close'].notna()) & (ticker_data['prev_close'] > 0),
            (ticker_data['close'].astype(float) - ticker_data['prev_close'].astype(float)) / ticker_data['prev_close'].astype(float) * 100,
            np.nan
        )
        
        # 각 기간별 상승률(모멘텀)만 계산 (거래일 기준, 변동성 모멘텀 미산출)
        for period in periods:
            # period 거래일 전의 인덱스 계산
            period_idx = latest_idx - period
            
            if period_idx >= 0 and period_idx < len(ticker_data):
                # 해당 인덱스의 종가
                period_close_raw = ticker_data.iloc[period_idx]['close']
                
                # Decimal 타입을 float로 변환
                try:
                    period_close = float(period_close_raw)
                except (ValueError, TypeError):
                    period_close = np.nan
                
                if pd.notna(period_close) and period_close > 0:
                    return_pct = ((latest_close - period_close) / period_close) * 100
                    result_row[f'{period}일_상승률'] = return_pct
                else:
                    result_row[f'{period}일_상승률'] = np.nan
            else:
                result_row[f'{period}일_상승률'] = np.nan

        for suffix, lag in MOMENTUM_DEFERRED_LAGS:
            lag = int(lag)
            end_idx = latest_idx - lag
            end_close = np.nan
            if end_idx >= 0:
                raw_ec = ticker_data.iloc[end_idx]['close']
                try:
                    end_close = float(raw_ec)
                except (ValueError, TypeError):
                    end_close = np.nan
            for period in periods:
                col_deferred = f'{period}일_상승률{suffix}'
                if end_idx < 0 or pd.isna(end_close) or end_close <= 0:
                    result_row[col_deferred] = np.nan
                    continue
                period_idx_deferred = end_idx - period
                if period_idx_deferred >= 0 and period_idx_deferred < len(ticker_data):
                    pc_raw = ticker_data.iloc[period_idx_deferred]['close']
                    try:
                        period_close_d = float(pc_raw)
                    except (ValueError, TypeError):
                        period_close_d = np.nan
                    if pd.notna(period_close_d) and period_close_d > 0:
                        result_row[col_deferred] = ((end_close - period_close_d) / period_close_d) * 100
                    else:
                        result_row[col_deferred] = np.nan
                else:
                    result_row[col_deferred] = np.nan
        
        # 최소한 하나의 모멘텀 값이 있어야 결과에 포함
        has_momentum = any(
            pd.notna(result_row.get(f'{p}일_상승률', np.nan)) 
            for p in periods
        )
        
        if has_momentum:
            results.append(result_row)
            processed_count += 1
    
    if not results:
        print(f"   ⚠️ 모멘텀 계산 실패: 처리된 ETF {processed_count}개, 스킵된 ETF {skipped_count}개")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(results)
    
    # 각 기간별 데이터 통계 출력
    if not result_df.empty:
        period_stats = []
        for period in periods:
            col = f'{period}일_상승률'
            if col in result_df.columns:
                valid_count = result_df[col].notna().sum()
                period_stats.append(f"{period}일: {valid_count}개")
            for suffix, _lag in MOMENTUM_DEFERRED_LAGS:
                col_d = f'{period}일_상승률{suffix}'
                if col_d in result_df.columns:
                    vn = result_df[col_d].notna().sum()
                    lag_disp = {'_T5': 'T-5', '_T3': 'T-3'}.get(suffix, suffix)
                    period_stats.append(f"{period}일({lag_disp}): {vn}개")
        if period_stats:
            print(f"   - 기간별 유효 데이터: {', '.join(period_stats)}")
    
    print(f"   ✓ 모멘텀 계산 완료: {processed_count}개 ETF 처리, {skipped_count}개 ETF 스킵")
    return result_df


def filter_momentum_above_sma(ohlcv_df, momentum_df, window):
    """
    최신 종가가 window일 단순이동평균 이상인 종목만 남깁니다.
    종가 < SMA인 경우를 '이동평균선 이탈'로 보고 제외합니다.
    해당 티커의 OHLCV가 window일 미만이면 SMA를 적용하지 않고 포함합니다.

    Args:
        ohlcv_df: ticker, date, close 컬럼 (해당 기준일까지의 일봉)
        momentum_df: 필터할 모멘텀 데이터프레임
        window: 10 또는 20 등 이동평균 기간(거래일)

    Returns:
        momentum_df의 부분집합 복사본
    """
    if momentum_df is None or momentum_df.empty:
        return momentum_df
    if ohlcv_df is None or ohlcv_df.empty:
        return momentum_df.copy()
    ohlcv = ohlcv_df.sort_values(['ticker', 'date']).copy()
    ohlcv = ohlcv[ohlcv['close'].notna()].copy()
    ohlcv['close'] = pd.to_numeric(ohlcv['close'], errors='coerce')
    ohlcv = ohlcv[ohlcv['close'] > 0]
    tickers_ok = set()
    for ticker, g in ohlcv.groupby('ticker'):
        arr = g['close'].dropna().to_numpy()
        if len(arr) < window:
            tickers_ok.add(ticker)
            continue
        last = float(arr[-1])
        ma = float(arr[-window:].mean())
        if last >= ma:
            tickers_ok.add(ticker)
    return momentum_df[momentum_df['ticker'].isin(tickers_ok)].copy()


def calculate_weighted_average_momentum(momentum_df):
    """
    가중 평균 모멘텀을 계산합니다.
    - 평균_모멘텀: 5일·10일·20일·50일 각 25% 동일 가중치
    - 평균_모멘텀_2: 5일(40%), 10일(30%), 20일(20%), 50일(10%)
    - 동일 가중치로 `N일_상승률_T5`·`N일_상승률_T3` 등 지연 열이 모두 있으면 `평균_모멘텀_T5`·`평균_모멘텀_T3`·`평균_모멘텀_2_*`도 산출합니다.
    
    Args:
        momentum_df (pandas.DataFrame): 모멘텀 데이터프레임
    
    Returns:
        pandas.DataFrame: 평균_모멘텀(, 평균_모멘텀_2[, 지연 `평균_모멘텀_*`])이 추가된 데이터프레임
    """
    df = momentum_df.copy()

    def _vector_weighted_avg(frame: pd.DataFrame, weight_map: dict) -> pd.Series:
        """NaN 항목은 가중치에서 제외한 뒤 정규화 (기존 apply 규칙과 동일)."""
        cols = [c for c in weight_map if c in frame.columns]
        if not cols:
            return pd.Series(np.nan, index=frame.index)
        vals = frame[cols].apply(pd.to_numeric, errors='coerce')
        w = pd.Series({c: float(weight_map[c]) for c in cols}, dtype=float)
        weighted_sum = vals.mul(w, axis=1).sum(axis=1, min_count=1)
        total_weight = vals.notna().astype(float).mul(w, axis=1).sum(axis=1)
        out = weighted_sum / total_weight
        return out.where(total_weight > 0)

    # 평균_모멘텀: 5일·10일·20일·50일 각 25%
    weights = {
        '5일_상승률': 0.25,
        '10일_상승률': 0.25,
        '20일_상승률': 0.25,
        '50일_상승률': 0.25,
    }
    df['평균_모멘텀'] = _vector_weighted_avg(df, weights)

    # 평균_모멘텀_2: 5일(40%), 10일(30%), 20일(20%), 50일(10%)
    weights_2 = {
        '5일_상승률': 0.40,
        '10일_상승률': 0.30,
        '20일_상승률': 0.20,
        '50일_상승률': 0.10,
    }
    df['평균_모멘텀_2'] = _vector_weighted_avg(df, weights_2)

    for suffix, _lag in MOMENTUM_DEFERRED_LAGS:
        weights_def = {f'{k}{suffix}': v for k, v in weights.items()}
        if all(c in df.columns for c in weights_def):
            df[f'평균_모멘텀{suffix}'] = _vector_weighted_avg(df, weights_def)
        weights_2_def = {f'{k}{suffix}': v for k, v in weights_2.items()}
        if all(c in df.columns for c in weights_2_def):
            df[f'평균_모멘텀_2{suffix}'] = _vector_weighted_avg(df, weights_2_def)

    return df


def get_momentum_rankings(momentum_df, period_col, top_n=20):
    """
    특정 기간의 모멘텀 순위를 반환합니다.
    
    Args:
        momentum_df (pandas.DataFrame): 모멘텀 데이터프레임
        period_col (str): 순위를 매길 컬럼명
        top_n (int): 상위 N개
    
    Returns:
        pandas.DataFrame: 순위가 매겨진 데이터프레임
    """
    if period_col not in momentum_df.columns:
        return pd.DataFrame()
    
    # 복사본 생성
    df = momentum_df.copy()
    
    # ETF 이름이 없으면 추가
    if 'etf_name' not in df.columns:
        etf_info = get_etf_list_from_db()
        if not etf_info.empty:
            df = df.merge(
                etf_info[['ticker', 'etf_name']],
                on='ticker',
                how='left'
            )
            df['etf_name'] = df['etf_name'].fillna(df['ticker'])
        else:
            df['etf_name'] = df['ticker']
    
    # 해당 기간의 데이터가 있는 것만 필터링
    ranked_df = df[df[period_col].notna()].copy()
    
    if ranked_df.empty:
        return pd.DataFrame()
    
    # 내림차순 정렬 (상승률 높은 순)
    ranked_df = ranked_df.sort_values(period_col, ascending=False)
    
    # 순위 추가
    ranked_df['순위'] = range(1, len(ranked_df) + 1)
    
    # 상위 N개만 반환
    return ranked_df.head(top_n)


def get_previous_week_momentum(end_date=None):
    """
    전주(7일 전)의 모멘텀 데이터를 가져옵니다.
    
    Args:
        end_date (str, optional): 기준일 (YYYY-MM-DD 형식). None이면 오늘
    
    Returns:
        pandas.DataFrame: 전주 모멘텀 데이터프레임
    """
    if end_date is None:
        end_date = date.today()
    else:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # 전주(5거래일 전) 날짜 계산
    prev_date = _prev_trading_date(end_date, n_trading_days=5)
    
    # OHLCV 데이터 가져오기
    ohlcv_df = get_etf_ohlcv_from_db(end_date=prev_date.strftime('%Y-%m-%d'))
    
    if ohlcv_df.empty:
        return pd.DataFrame()
    
    # 모멘텀 계산
    momentum_df = calculate_momentum_returns(ohlcv_df, periods=[10, 20, 50, 120])
    
    if momentum_df.empty:
        return pd.DataFrame()
    
    # 평균 모멘텀 계산
    momentum_df = calculate_weighted_average_momentum(momentum_df)
    
    return momentum_df


def calculate_rank_change(current_rankings, previous_momentum_df, period_col):
    """
    현재 순위와 전주 순위를 비교하여 순위 변화를 계산합니다.
    
    Args:
        current_rankings (pandas.DataFrame): 현재 순위 데이터프레임
        previous_momentum_df (pandas.DataFrame): 전주 모멘텀 데이터프레임
        period_col (str): 비교할 컬럼명
    
    Returns:
        pandas.DataFrame: 순위 변화가 추가된 데이터프레임
    """
    if previous_momentum_df.empty or period_col not in previous_momentum_df.columns:
        current_rankings['순위변화'] = 0
        current_rankings['순위변화_표시'] = 'NEW'
        return current_rankings
    
    # 전주 순위 계산
    prev_ranked = previous_momentum_df[previous_momentum_df[period_col].notna()].copy()
    prev_ranked = prev_ranked.sort_values(period_col, ascending=False)
    prev_ranked['전주순위'] = range(1, len(prev_ranked) + 1)
    
    # 현재 순위와 병합
    result = current_rankings.merge(
        prev_ranked[['ticker', '전주순위']],
        on='ticker',
        how='left'
    )
    
    # 순위 변화 계산
    def calc_rank_change(row):
        if pd.isna(row['전주순위']):
            return 'NEW'
        change = row['전주순위'] - row['순위']
        if change > 0:
            return f'↑{int(change)}'
        elif change < 0:
            return f'↓{int(abs(change))}'
        else:
            return '→'
    
    result['순위변화'] = result.apply(
        lambda row: row['전주순위'] - row['순위'] if pd.notna(row['전주순위']) else None,
        axis=1
    )
    result['순위변화_표시'] = result.apply(calc_rank_change, axis=1)
    
    return result


def _df_to_html_table_rows(df, include_ticker=True):
    """DataFrame(순위, ticker?, etf명, 수익률)을 HTML tbody 행 문자열로. include_ticker이면 ticker 컬럼 포함."""
    if df is None or df.empty:
        return '<p class="no-data">데이터 없음</p>'
    rows = []
    for _, row in df.iterrows():
        try:
            rate = row['수익률']
            if isinstance(rate, str) and rate.endswith('%'):
                num_str = rate.replace('%', '').strip()
                try:
                    num = float(num_str)
                    cls = 'positive' if num > 0 else ('negative' if num < 0 else '')
                except ValueError:
                    cls = ''
            else:
                cls = ''
            rate_esc = str(rate).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
            name_esc = str(row['etf명']).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
            rank_cell = f'<td class="col-rank">{int(row["순위"])}</td>'
            if include_ticker:
                ticker_esc = str(row.get('ticker', '')).replace('&', '&amp;').replace('<', '&lt;')
                rows.append(f'<tr>{rank_cell}<td class="col-ticker">{ticker_esc}</td><td>{name_esc}</td><td class="col-return 수익률 {cls}">{rate_esc}</td></tr>')
            else:
                rows.append(f'<tr>{rank_cell}<td>{name_esc}</td><td class="col-return 수익률 {cls}">{rate_esc}</td></tr>')
        except Exception:
            pass
    if not rows:
        return '<p class="no-data">데이터 없음</p>'
    header = '<tr><th class="col-rank">순위</th><th class="col-ticker">ticker</th><th>etf명</th><th class="col-return 수익률">수익률</th></tr>' if include_ticker else '<tr><th class="col-rank">순위</th><th>etf명</th><th class="col-return 수익률">수익률</th></tr>'
    return '<table class="momentum-table sortable-tbl"><thead>' + header + '</thead><tbody>' + ''.join(rows) + '</tbody></table>'


def _momentum_tables_to_html(sections_list, period_info=None):
    """
    sections_list: 리스트 of (period_title, [(table_title, df), (table_title, df), ...])
    각 period당 표들을 나란히 렌더링. period_info가 있으면 맨 앞에 기간 정보 출력.
    DataFrame 컬럼: 순위, ticker, etf명, 수익률
    """
    html_parts = []
    html_parts.append("""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETF 기간별 모멘텀 순위</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; margin: 24px; background: #f5f6fa; color: #2c3e50; }
        h1 { font-size: 1.5rem; margin-bottom: 24px; color: #1a202c; }
        .period-info { font-size: 1rem; margin-bottom: 20px; padding: 12px 16px; background: #e6f2ff; border-left: 4px solid #4299e1; color: #2d3748; }
        section { margin-bottom: 32px; }
        h2 { font-size: 1.1rem; margin-bottom: 12px; color: #2d3748; border-bottom: 2px solid #4299e1; padding-bottom: 6px; }
        .table-row { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 16px; align-items: flex-start; }
        .table-cell { flex: 1; min-width: 280px; max-width: 450px; }
        .table-cell h3 { font-size: 0.95rem; margin-bottom: 8px; color: #4a5568; }
        table { border-collapse: collapse; width: 100%; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-radius: 8px; overflow: hidden; }
        table.momentum-table { table-layout: fixed; font-size: 0.82rem; }
        table.momentum-table th, table.momentum-table td { padding: 6px 8px; }
        th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #e2e8f0; font-size: 0.9rem; }
        th.col-rank, td.col-rank { width: 44px; min-width: 44px; }
        th.col-ticker, td.col-ticker { width: 54px; min-width: 54px; white-space: nowrap; }
        th.col-return, td.col-return { width: 68px; min-width: 68px; white-space: nowrap; }
        th { background: #4299e1; color: #fff; font-weight: 600; }
        tr:nth-child(even) { background: #f7fafc; }
        tr:hover { background: #edf2f7; }
        .수익률 { text-align: right; font-variant-numeric: tabular-nums; }
        .수익률.positive { color: #2f855a; font-weight: 600; }
        .수익률.negative { color: #c53030; font-weight: 600; }
        .no-data { color: #718096; font-style: italic; }
""" + _HTML_TABLE_SORT_CSS + """
    </style>
</head>
<body>
    <h1>ETF 기간별 모멘텀 순위 (현재 / 일주일 전 / 5% 이내 하락·상승 / 10일·20일선 이탈 제외)</h1>
""")
    if period_info:
        html_parts.append(f'    <p class="period-info"><strong>기간 정보</strong> {period_info}</p>')
    for period_title, tables in sections_list:
        html_parts.append(f'    <section><h2>{period_title}</h2>')
        html_parts.append('    <div class="table-row">')
        for table_title, df in tables:
            html_parts.append('      <div class="table-cell">')
            html_parts.append(f'        <h3>{table_title}</h3>')
            html_parts.append('        ' + _df_to_html_table_rows(df, include_ticker=True))
            html_parts.append('      </div>')
        html_parts.append('    </div></section>')
    html_parts.append(_html_table_sort_script())
    html_parts.append('</body></html>')
    return '\n'.join(html_parts)


def _safe_html(s):
    try:
        s = '' if s is None else str(s)
    except Exception:
        s = ''
    return (s.replace('&', '&amp;')
             .replace('<', '&lt;')
             .replace('>', '&gt;')
             .replace('"', '&quot;'))


# 헤더 클릭 시 열별 정렬(처음 내림차순, 같은 열 재클릭 시 오름차순 토글). `table.sortable-tbl`에만 적용.
_HTML_TABLE_SORT_CSS = """
    table.sortable-tbl thead th.sortable-col { cursor: pointer; user-select: none; position: relative; padding-right: 1.35em; }
    table.sortable-tbl thead th.sortable-col:hover { filter: brightness(1.1); }
    table.sortable-tbl thead th.sort-asc::after { content: ' \\25b2'; font-size: 0.68em; margin-left: 0.35em; opacity: 0.92; vertical-align: middle; }
    table.sortable-tbl thead th.sort-desc::after { content: ' \\25bc'; font-size: 0.68em; margin-left: 0.35em; opacity: 0.92; vertical-align: middle; }
"""


def _html_table_sort_script() -> str:
    """`table.sortable-tbl`: 헤더 클릭 → 동일 열 내림차순/오름차순 토글 (첫 클릭은 내림차순)."""
    return """<script>(function(){
function cellKey(cell){var raw=(cell?cell.textContent:'').replace(/\\u00a0/g,' ');
var lines=raw.split(/\\s*\\r?\\n\\s*/).map(function(x){return x.trim();}).filter(Boolean);
var t=(lines.length?lines[lines.length-1]:raw.trim());
if(!t||t==='\\u2014'||t==='-'||t==='N/A')return{type:'null'};
if(/^\\d{4}-\\d{2}-\\d{2}$/.test(t)){var ds=Date.parse(t+'T12:00:00');return{type:'num',v:isFinite(ds)?ds:NaN};}
var canon=t.replace(/,/g,'').replace(/\\s/g,'');if(/^-?\\d+(\\.\\d+)?%?$/.test(canon)){var pn=parseFloat(canon.replace('%',''));
if(!isNaN(pn))return{type:'num',v:pn};}return{type:'str',v:t};}
function cmpKey(a,b){if(a.type==='null'&&b.type==='null')return 0;if(a.type==='null')return 1;if(b.type==='null')return-1;
if(a.type==='num'&&b.type==='num'&&!isNaN(a.v)&&!isNaN(b.v))return a.v-b.v;if(a.type==='num')return-1;if(b.type==='num')return 1;
return String(a.v).localeCompare(String(b.v),'ko');
}
function wire(table){
var tbody=table.querySelector('tbody');if(!tbody)return;
var thead=table.querySelector('thead');if(!thead)return;
var hrows=thead.querySelectorAll('tr');if(!hrows.length)return;
var hdr=hrows[hrows.length-1];
var thArr=[].slice.call(hdr.cells);
thArr.forEach(function(th,colIdx){
th.classList.add('sortable-col');
th.addEventListener('click',function(ev){ev.preventDefault();
var prev=th.getAttribute('data-sort-dir');
var next=(!prev||prev==='')?'desc':(prev==='desc'?'asc':'desc');
[].forEach.call(table.querySelectorAll('thead th'),function(h){h.classList.remove('sort-asc','sort-desc');h.removeAttribute('data-sort-dir');});
th.classList.add(next==='desc'?'sort-desc':'sort-asc');th.setAttribute('data-sort-dir',next);
var desc=(next==='desc');
var ix=colIdx;var rows=[].slice.call(tbody.rows);
rows.sort(function(rA,rB){var ca=rA.cells[ix],cb=rB.cells[ix];var d=cmpKey(cellKey(ca),cellKey(cb));return desc?-d:d;});
rows.forEach(function(row){tbody.appendChild(row);});
});
});
}
document.querySelectorAll('table.sortable-tbl').forEach(wire);
})();</script>"""


def _fmt_pct(x, digits=2):
    try:
        if pd.isna(x):
            return ''
    except Exception:
        pass
    try:
        return f"{float(x):.{digits}f}%"
    except Exception:
        return str(x)


def _fmt_index_level(x, digits=2):
    """지수·가격 수준 표시(천 단위 구분)."""
    try:
        if pd.isna(x):
            return 'N/A'
    except Exception:
        pass
    try:
        return f"{float(x):,.{digits}f}"
    except Exception:
        return 'N/A'


def _momentum_color_class(x):
    try:
        if pd.isna(x):
            return 'm-na'
        v = float(x)
    except Exception:
        return 'm-na'
    if v >= 10:
        return 'm-strong-pos'
    if v >= 0:
        return 'm-pos'
    if v <= -10:
        return 'm-strong-neg'
    return 'm-neg'


def _get_kospi_daily_return_pct_for_ref_date(ref_date):
    """
    ref_date( date ) 당일 종가 대비 직전 거래일 종가 수익률(%).
    DB krx_index_ohlcv(ticker=1001) 우선, 실패 시 네이버 보조.
    """
    if ref_date is None:
        return np.nan
    try:
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        elif isinstance(ref_date, pd.Timestamp):
            ref_date = ref_date.date()
    except Exception:
        return np.nan

    ref_s = ref_date.strftime('%Y-%m-%d')
    try:
        q = """
        SELECT date, close FROM krx_index_ohlcv
        WHERE ticker = '1001' AND date <= %s
        ORDER BY date DESC
        LIMIT 2
        """
        df = pd.read_sql(q, con=engine, params=(ref_s,))
        if df is not None and len(df) >= 2:
            c0 = float(pd.to_numeric(df['close'].iloc[1], errors='coerce'))
            c1 = float(pd.to_numeric(df['close'].iloc[0], errors='coerce'))
            if pd.notna(c0) and pd.notna(c1) and c0 > 0:
                return (c1 / c0 - 1.0) * 100.0
    except Exception:
        pass

    try:
        s = index_close_series_from_naver('1001', ref_date, lookback_calendar_days=40)
        if s is None or len(s) < 2:
            return np.nan
        c0 = float(s.iloc[-2]); c1 = float(s.iloc[-1])
        if c0 > 0:
            return (c1 / c0 - 1.0) * 100.0
    except Exception:
        pass
    return np.nan


def _get_kospi_3d_return_pct_for_ref_date(ref_date):
    """
    ref_date( date ) 당일 종가 대비, ref_date 기준 거래일 역순 3번째 행의 종가 수익률(%).
    (역순 1=당일, 2=직전 거래일, 3=그 직전 거래일 종가를 분모로 사용)
    DB krx_index_ohlcv(ticker=1001) 우선, 실패 시 네이버 보조.
    """
    if ref_date is None:
        return np.nan
    try:
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        elif isinstance(ref_date, pd.Timestamp):
            ref_date = ref_date.date()
    except Exception:
        return np.nan

    ref_s = ref_date.strftime('%Y-%m-%d')
    try:
        q = """
        SELECT date, close FROM krx_index_ohlcv
        WHERE ticker = '1001' AND date <= %s
        ORDER BY date DESC
        LIMIT 3
        """
        df = pd.read_sql(q, con=engine, params=(ref_s,))
        if df is not None and len(df) >= 3:
            c_base = float(pd.to_numeric(df['close'].iloc[2], errors='coerce'))
            c0 = float(pd.to_numeric(df['close'].iloc[0], errors='coerce'))
            if pd.notna(c_base) and pd.notna(c0) and c_base > 0:
                return (c0 / c_base - 1.0) * 100.0
    except Exception:
        pass

    try:
        s = index_close_series_from_naver('1001', ref_date, lookback_calendar_days=90)
        if s is None or len(s) < 3:
            return np.nan
        c_base = float(s.iloc[-3]); c0 = float(s.iloc[-1])
        if c_base > 0:
            return (c0 / c_base - 1.0) * 100.0
    except Exception:
        pass
    return np.nan


def _get_kospi_nd_trading_return_pct_for_ref_date(ref_date, n_trading_days: int):
    """
    ref_date( date ) 당일 종가 대비 n_trading_days 거래일 전 종가 수익률(%).
    (ETF `N일_상승률`과 동일: 최신 종가 vs N거래일 전 종가)
    """
    if ref_date is None:
        return np.nan
    n = int(n_trading_days)
    if n < 1:
        return np.nan
    try:
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        elif isinstance(ref_date, pd.Timestamp):
            ref_date = ref_date.date()
    except Exception:
        return np.nan

    ref_s = ref_date.strftime('%Y-%m-%d')
    limit = n + 1
    try:
        q = """
        SELECT date, close FROM krx_index_ohlcv
        WHERE ticker = '1001' AND date <= %s
        ORDER BY date DESC
        LIMIT %s
        """
        df = pd.read_sql(q, con=engine, params=(ref_s, int(limit)))
        if df is not None and len(df) >= limit:
            c_n = float(pd.to_numeric(df['close'].iloc[n], errors='coerce'))
            c0 = float(pd.to_numeric(df['close'].iloc[0], errors='coerce'))
            if pd.notna(c_n) and pd.notna(c0) and c_n > 0:
                return (c0 / c_n - 1.0) * 100.0
    except Exception:
        pass

    try:
        s = index_close_series_from_naver('1001', ref_date, lookback_calendar_days=max(400, n * 5))
        if s is None or len(s) < limit:
            return np.nan
        c_n = float(s.iloc[-(n + 1)]); c0 = float(s.iloc[-1])
        if c_n > 0:
            return (c0 / c_n - 1.0) * 100.0
    except Exception:
        pass
    return np.nan


def _get_kospi_close_and_sma_for_ref_date(ref_date, windows=(5, 10, 20)):
    """
    ref_date 이하 코스피(1001) 종가로 기준일(마지막 행) 종가 및 단순이동평균(SMA) 값.
    DB krx_index_ohlcv 우선, 실패 시 네이버 보조.

    Returns:
        dict: 'close', 'ma5', 'ma10', 'ma20' 키 — 값은 float 또는 np.nan
    """
    empty = {'close': np.nan, 'ma5': np.nan, 'ma10': np.nan, 'ma20': np.nan}
    if ref_date is None:
        return dict(empty)
    try:
        if isinstance(ref_date, datetime):
            ref_date = ref_date.date()
        elif isinstance(ref_date, pd.Timestamp):
            ref_date = ref_date.date()
    except Exception:
        return dict(empty)

    windows = tuple(int(w) for w in windows)
    max_w = max(windows)
    limit_n = max(320, max_w * 16)
    ref_s = ref_date.strftime('%Y-%m-%d')

    def _series_from_db():
        try:
            q = """
            SELECT date, close FROM krx_index_ohlcv
            WHERE ticker = '1001' AND date <= %s
            ORDER BY date DESC
            LIMIT %s
            """
            df = pd.read_sql(q, con=engine, params=(ref_s, int(limit_n)))
            if df is None or df.empty or len(df) < max_w:
                return None
            df = df.sort_values('date', ascending=True)
            s = pd.to_numeric(df['close'], errors='coerce').dropna()
            return s if len(s) >= max_w else None
        except Exception:
            return None

    def _series_from_naver():
        try:
            s = index_close_series_from_naver('1001', ref_date, lookback_calendar_days=max(400, max_w * 5))
            if s is None or s.empty:
                return None
            return s if len(s) >= max_w else None
        except Exception:
            return None

    s_closes = _series_from_db()
    if s_closes is None:
        s_closes = _series_from_naver()
    if s_closes is None or s_closes.empty:
        return dict(empty)

    out = dict(empty)
    out['close'] = float(s_closes.iloc[-1])
    for w in windows:
        if len(s_closes) >= w:
            v = s_closes.rolling(w, min_periods=w).mean().iloc[-1]
            if pd.notna(v):
                out[f'ma{w}'] = float(v)
    return out


def _get_kospi_weighted_avg_momentum_for_ref_date(ref_date):
    """ETF `평균_모멘텀`과 동일 가중치(5·10·20·50일 각 25%)로 코스피 가중 평균 수익률(%)."""
    weights = {
        '5일_상승률': 0.25,
        '10일_상승률': 0.25,
        '20일_상승률': 0.25,
        '50일_상승률': 0.25,
    }
    weighted_sum = 0.0
    total_w = 0.0
    nd_map = {'5일_상승률': 5, '10일_상승률': 10, '20일_상승률': 20, '50일_상승률': 50}
    for col, w in weights.items():
        nd = nd_map.get(col)
        if nd is None:
            continue
        r = _get_kospi_nd_trading_return_pct_for_ref_date(ref_date, nd)
        if pd.notna(r):
            weighted_sum += float(r) * w
            total_w += w
    if total_w <= 0:
        return np.nan
    return weighted_sum / total_w


def _get_kospi_weekly_return_pct_for_ref_ts(ref_ts):
    """
    ref_ts(데이터 최종일)와 같은 ISO 주(월~일)에서 코스피(1001) 첫 거래일(=해당 주 첫 거래일)
    시가 대비 ref_ts 이하 마지막 거래일 종가 수익률(%).
    ETF weekly_return_pct와 동일한 주간 정의(ISO 주·거래일).
    """
    if ref_ts is None:
        return np.nan
    try:
        ref_ts = pd.Timestamp(ref_ts).normalize()
    except Exception:
        return np.nan
    if pd.isna(ref_ts):
        return np.nan

    iso = ref_ts.isocalendar()
    iso_y, iso_w = int(iso.year), int(iso.week)
    week_monday = date.fromisocalendar(iso_y, iso_w, 1)
    ref_d = ref_ts.date()
    start_s = week_monday.strftime('%Y-%m-%d')
    end_s = ref_d.strftime('%Y-%m-%d')

    try:
        q = """
        SELECT date, open, close FROM krx_index_ohlcv
        WHERE ticker = '1001' AND date >= %s AND date <= %s
        ORDER BY date
        """
        df = pd.read_sql(q, con=engine, params=(start_s, end_s))
        if df is None or df.empty:
            raise ValueError('empty kospi week')
        df['_d'] = pd.to_datetime(df['date'], errors='coerce').dt.normalize()
        df = df[df['_d'].notna()].copy()
        ic = df['_d'].dt.isocalendar()
        wk_mask = (ic.year.astype(int) == iso_y) & (ic.week.astype(int) == iso_w)
        wk = df.loc[wk_mask].sort_values('_d')
        wk = wk[wk['_d'] <= ref_ts].sort_values('_d')
        if len(wk) < 1:
            raise ValueError('kospi week < 1 row')
        o_first = float(pd.to_numeric(wk.iloc[0].get('open', np.nan), errors='coerce'))
        c_last = float(pd.to_numeric(wk.iloc[-1].get('close', np.nan), errors='coerce'))
        # open이 비어있으면(혹은 0) 종가로 fallback (구형 스키마/데이터 대응)
        if not (pd.notna(o_first) and o_first > 0):
            o_first = float(pd.to_numeric(wk.iloc[0].get('close', np.nan), errors='coerce'))
        if pd.notna(o_first) and o_first > 0 and pd.notna(c_last):
            return (c_last / o_first - 1.0) * 100.0
    except Exception:
        pass

    try:
        df = get_index_ohlcv_from_naver(
            '1001',
            (week_monday - timedelta(days=7)).strftime('%Y%m%d'),
            ref_d.strftime('%Y%m%d'),
        )
        if df is None or df.empty:
            return np.nan
        tmp = df.copy()
        tmp['_d'] = pd.to_datetime(tmp['date'], errors='coerce').dt.normalize()
        tmp['open'] = pd.to_numeric(tmp['open'], errors='coerce')
        tmp['close'] = pd.to_numeric(tmp['close'], errors='coerce')
        tmp = tmp[tmp['_d'].notna() & tmp['close'].notna()].copy()
        ic = tmp['_d'].dt.isocalendar()
        wk_mask = (ic.year.astype(int) == iso_y) & (ic.week.astype(int) == iso_w)
        wk = tmp.loc[wk_mask].sort_values('_d')
        wk = wk[wk['_d'] <= ref_ts].sort_values('_d')
        if len(wk) < 1:
            return np.nan
        o_first = float(wk.iloc[0]['open']) if pd.notna(wk.iloc[0]['open']) else np.nan
        c_last = float(wk.iloc[-1]['close'])
        if not (pd.notna(o_first) and o_first > 0):
            o_first = float(wk.iloc[0]['close'])
        if pd.notna(o_first) and o_first > 0 and pd.notna(c_last):
            return (c_last / o_first - 1.0) * 100.0
    except Exception:
        pass
    return np.nan


def _compute_last_close_and_sma(ohlcv_df, window=20):
    """
    Returns DataFrame columns:
      - ticker, last_close, weekly_return_pct(float)
      - 단일 window(int) 입력 시: sma, above_sma(bool), above_sma_label(str) 포함 (호환)
      - 복수 windows(iterable) 입력 시: sma_{w}, above_sma_{w}, above_sma_label_{w} 포함
    weekly_return_pct: 데이터 최종일이 속한 ISO 주(월~일) 안에서 첫 거래일(해당 주 첫 거래일) 시가 대비
                      최종일(≤최종일) 종가 수익률(%).
    당일 수익률: 기준일 ETF 전일 대비 수익률(%).
    3일 수익률: 기준일 종가 대비 거래일 역순 3번째 행(당일·직전·그전) 종가 수익률(%).
    MA10연속추세: 당일·전일 각각 직전 거래일 대비 MA10이 모두 상승이면 '상승', 모두 하락이면 '하락', 그 외 '-'.
    ATR14_일변동률(%): ATR14(원본 float) / 당일 종가 × 100.
    """
    _empty_cols = [
        'ticker', 'last_close', 'sma', 'above_sma', 'above_sma_label', 'weekly_return_pct',
        '당일 수익률', '3일 수익률', 'ATR14', 'MA10연속추세', 'ATR14_일변동률(%)',
    ]
    if ohlcv_df is None or ohlcv_df.empty:
        return pd.DataFrame(columns=_empty_cols)

    o = ohlcv_df.copy()
    o = o.sort_values(['ticker', 'date'])
    o = o[o['close'].notna()].copy()
    if o.empty:
        return pd.DataFrame(columns=_empty_cols)

    for _c in ['open', 'high', 'low', 'close']:
        if _c in o.columns:
            o[_c] = pd.to_numeric(o[_c], errors='coerce')
    o = o[o['close'].notna()].copy()

    o['_d'] = pd.to_datetime(o['date'], errors='coerce').dt.normalize()
    o = o[o['_d'].notna()].copy()
    if o.empty:
        return pd.DataFrame(columns=_empty_cols)

    ref_ts = o['_d'].max()
    ref_date = ref_ts.date()
    iso = ref_ts.isocalendar()
    iso_y, iso_w = int(iso.year), int(iso.week)

    weekly = []
    for t, g in o.groupby('ticker', sort=False):
        g = g.sort_values('_d')
        wk_mask = (g['_d'].dt.isocalendar().year.astype(int) == iso_y) & (
            g['_d'].dt.isocalendar().week.astype(int) == iso_w)
        wk = g.loc[wk_mask].sort_values('_d')
        if len(wk) >= 1:
            o_first = float(pd.to_numeric(wk.iloc[0].get('open', np.nan), errors='coerce'))
            c_last = float(pd.to_numeric(wk.iloc[-1].get('close', np.nan), errors='coerce'))
            if not (pd.notna(o_first) and o_first > 0):
                # open이 비어있으면 종가로 fallback (구형 데이터 대응)
                o_first = float(pd.to_numeric(wk.iloc[0].get('close', np.nan), errors='coerce'))
            weekly_ret = (c_last / o_first - 1.0) * 100.0 if (pd.notna(o_first) and o_first > 0 and pd.notna(c_last)) else np.nan
        else:
            weekly_ret = np.nan

        sub = g[g['_d'] <= ref_ts].sort_values('_d')
        etf_daily = np.nan
        etf_3d = np.nan
        atr14 = np.nan
        atr14_raw = np.nan
        ma10_trend = '-'
        atr14_day_vol_pct = np.nan
        if not sub.empty and len(sub) >= 2:
            last_d = sub['_d'].iloc[-1]
            if last_d.normalize() == ref_ts.normalize():
                c0 = float(sub.iloc[-2]['close'])
                c1 = float(sub.iloc[-1]['close'])
                if c0 and pd.notna(c0):
                    etf_daily = (c1 / c0 - 1.0) * 100.0

        # 3일 수익률: ref_ts 당일 종가 vs 거래일 역순 3번째 행 종가(당일·직전일·그전일 중 마지막)
        try:
            if not sub.empty and len(sub) >= 3:
                last_d = sub['_d'].iloc[-1]
                if last_d.normalize() == ref_ts.normalize():
                    c_base = float(sub.iloc[-3]['close'])
                    c1 = float(sub.iloc[-1]['close'])
                    if pd.notna(c_base) and c_base > 0 and pd.notna(c1):
                        etf_3d = (c1 / c_base - 1.0) * 100.0
        except Exception:
            pass

        # ATR14: True Range(14) 단순이동평균 (ref_ts 기준)
        try:
            if all(x in sub.columns for x in ['high', 'low', 'close']) and len(sub) >= 15:
                h = pd.to_numeric(sub['high'], errors='coerce')
                l = pd.to_numeric(sub['low'], errors='coerce')
                c = pd.to_numeric(sub['close'], errors='coerce')
                pc = c.shift(1)
                tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
                atr = tr.rolling(14, min_periods=14).mean()
                v = atr.iloc[-1]
                if pd.notna(v):
                    atr14_raw = float(v)
                    atr14 = float(math.trunc(atr14_raw))
                else:
                    atr14 = np.nan
        except Exception:
            pass

        # MA10: 당일·전일 각각 직전일 대비 MA10이 연속 상승이면 상승, 연속 하락이면 하락
        try:
            if len(sub) >= 12:
                c10 = pd.to_numeric(sub['close'], errors='coerce')
                sma10 = c10.rolling(10, min_periods=10).mean()
                tail10 = sma10.dropna()
                if len(tail10) >= 3:
                    m0, m1, m2 = float(tail10.iloc[-1]), float(tail10.iloc[-2]), float(tail10.iloc[-3])
                    if m0 > m1 and m1 > m2:
                        ma10_trend = '상승'
                    elif m0 < m1 and m1 < m2:
                        ma10_trend = '하락'
                    else:
                        ma10_trend = '-'
        except Exception:
            pass

        # ATR14 / 당일 종가 × 100 (%)
        try:
            if not sub.empty and pd.notna(atr14_raw):
                lc_atr = float(pd.to_numeric(sub.iloc[-1]['close'], errors='coerce'))
                if lc_atr > 0:
                    atr14_day_vol_pct = (atr14_raw / lc_atr) * 100.0
        except Exception:
            pass

        weekly.append((t, weekly_ret, etf_daily, etf_3d, atr14, ma10_trend, atr14_day_vol_pct))
    weekly_df = pd.DataFrame(
        weekly,
        columns=['ticker', 'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14', 'MA10연속추세', 'ATR14_일변동률(%)'],
    )

    last_close = o.groupby('ticker').tail(1)[['ticker', 'close']].copy()
    last_close = last_close.rename(columns={'close': 'last_close'})
    base = last_close.merge(weekly_df, on='ticker', how='left')

    def _merge_sma(base_df, w):
        w = int(w)
        sma_col = f'sma_{w}'
        above_col = f'above_sma_{w}'
        label_col = f'above_sma_label_{w}'
        sma_series = o.groupby('ticker')['close'].transform(lambda s: s.rolling(w, min_periods=w).mean())
        tmp = o[['ticker']].copy()
        tmp[sma_col] = sma_series
        last_sma = tmp.groupby('ticker').tail(1)[['ticker', sma_col]].copy()
        out_df = base_df.merge(last_sma, on='ticker', how='left')
        out_df[above_col] = (out_df['last_close'] >= out_df[sma_col]) & out_df[sma_col].notna()
        out_df[label_col] = out_df[above_col].map(lambda v: '위' if bool(v) else '아래')
        return out_df

    if isinstance(window, (list, tuple, set)):
        out_df = base
        for w in window:
            out_df = _merge_sma(out_df, w)
        return out_df

    # 단일 window(int) 호환 출력
    w = int(window)
    out_df = _merge_sma(base, w)
    out_df = out_df.rename(columns={f'sma_{w}': 'sma', f'above_sma_{w}': 'above_sma', f'above_sma_label_{w}': 'above_sma_label'})
    return out_df


def _build_cross_momentum_table(momentum_df, periods=(5, 10, 20, 50, 120), sort_col='평균_모멘텀', top_n=30):
    if momentum_df is None or momentum_df.empty:
        return pd.DataFrame()
    df = momentum_df.copy()
    if 'etf_name' not in df.columns and 'etf명' not in df.columns:
        df['etf_name'] = df.get('ticker', '')

    name_col = 'etf_name' if 'etf_name' in df.columns else 'etf명'
    cols = ['ticker', name_col]
    for p in periods:
        c = f'{p}일_상승률'
        if c in df.columns:
            cols.append(c)
        for suf, _lag in MOMENTUM_DEFERRED_LAGS:
            cd = f'{p}일_상승률{suf}'
            if cd in df.columns:
                cols.append(cd)
    for c in ['평균_모멘텀', '평균_모멘텀_2']:
        if c in df.columns and c not in cols:
            cols.append(c)
    for suf, _lag in MOMENTUM_DEFERRED_LAGS:
        for stem in (f'평균_모멘텀{suf}', f'평균_모멘텀_2{suf}'):
            if stem in df.columns and stem not in cols:
                cols.append(stem)
    if sort_col not in df.columns:
        sort_col = '평균_모멘텀' if '평균_모멘텀' in df.columns else cols[-1]

    df2 = df[cols].copy()
    df2 = df2[df2[sort_col].notna()].sort_values(sort_col, ascending=False).head(int(top_n)).copy()
    df2.insert(0, '순위', range(1, len(df2) + 1))
    df2 = df2.rename(columns={name_col: '종목명'})
    return df2


def _prev_trading_date(end_date, n_trading_days=5):
    """
    end_date( date ) 기준으로 DB OHLCV의 거래일을 사용해 n_trading_days 이전 거래일을 추정합니다.
    (캘린더 -7일이 아니라 "거래일 -5" 같은 기준을 맞추기 위함)
    """
    try:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    except Exception:
        return end_date - timedelta(days=7)

    try:
        ohlcv = get_etf_ohlcv_from_db(end_date=end_date.strftime('%Y-%m-%d'))
        if ohlcv is None or ohlcv.empty or 'date' not in ohlcv.columns:
            return end_date - timedelta(days=7)
        dts = pd.to_datetime(ohlcv['date'], errors='coerce').dropna().dt.date.unique().tolist()
        dts = sorted(dts)
        if not dts:
            return end_date - timedelta(days=7)
        # end_date 이하 마지막 거래일을 기준으로 n일 전
        dts_le = [d for d in dts if d <= end_date]
        if not dts_le:
            return end_date - timedelta(days=7)
        idx = max(0, len(dts_le) - 1 - int(n_trading_days))
        return dts_le[idx]
    except Exception:
        return end_date - timedelta(days=7)


def _etf_last_close_on_or_before(ohlcv_df, ticker, as_of_date):
    """티커 OHLCV에서 as_of_date(포함) 이전 가장 최근 종가."""
    if ohlcv_df is None or ohlcv_df.empty or 'date' not in ohlcv_df.columns or 'close' not in ohlcv_df.columns:
        return np.nan
    t = str(ticker).strip()
    g = ohlcv_df[ohlcv_df['ticker'].astype(str).str.strip() == t].copy()
    if g.empty:
        return np.nan
    g['date'] = pd.to_datetime(g['date'], errors='coerce')
    g = g[g['date'].notna()]
    if g.empty:
        return np.nan
    as_ts = pd.Timestamp(as_of_date).normalize()
    g = g[g['date'].dt.normalize() <= as_ts]
    if g.empty:
        return np.nan
    g = g.sort_values('date')
    return float(pd.to_numeric(g.iloc[-1]['close'], errors='coerce'))


def _ohlcv_universe_on_or_before(ohlcv_df, as_of_date):
    """섹터 등락률 등에서 넘긴 유니버스 OHLCV를 as_of_date(포함) 이하만 남깁니다."""
    if ohlcv_df is None or ohlcv_df.empty or 'date' not in ohlcv_df.columns or 'close' not in ohlcv_df.columns:
        return pd.DataFrame()
    out = ohlcv_df.copy()
    out['date'] = pd.to_datetime(out['date'], errors='coerce')
    end_ts = pd.Timestamp(as_of_date).normalize()
    out = out[out['date'].notna() & (out['date'].dt.normalize() <= end_ts)]
    return out if not out.empty else pd.DataFrame()


def _compute_watchlist_5d_hold_returns_df(ohlcv_df, eval_date, watchlist: dict) -> pd.DataFrame:
    """
    관심종목(watchlist dict 키 순서) 각각에 대해:
    eval_date 기준 5거래일 전 종가 매수 → eval_date 종가까지(5거래일 보유) 단순 수익률(%)을 산출합니다.
    OHLCV는 호출부 유니버스(예: code_name 조회분)만 사용합니다.
    """
    out_cols = ['순위', 'ticker', '종목명', '매수가', '평가가', '수익률(%)']
    if not watchlist or ohlcv_df is None or ohlcv_df.empty:
        return pd.DataFrame(columns=out_cols)
    if isinstance(eval_date, str):
        eval_date = datetime.strptime(eval_date, '%Y-%m-%d').date()
    purchase_date = _prev_trading_date(eval_date, n_trading_days=5)
    ohlcv_past = _ohlcv_universe_on_or_before(ohlcv_df, purchase_date)
    ohlcv_cur = _ohlcv_universe_on_or_before(ohlcv_df, eval_date)
    if ohlcv_past.empty or ohlcv_cur.empty:
        return pd.DataFrame(columns=out_cols)

    rows: list[dict] = []
    for rank_i, tk_raw in enumerate(watchlist.keys(), start=1):
        disp_tk = str(tk_raw).strip()
        nm = str(watchlist.get(tk_raw, '') or '').strip() or disp_tk
        tk_key = disp_tk
        bp = _etf_last_close_on_or_before(ohlcv_past, tk_key, purchase_date)
        cp = _etf_last_close_on_or_before(ohlcv_cur, tk_key, eval_date)
        nk = _norm_etf_ticker_key(tk_key)
        if nk != tk_key:
            if pd.isna(bp):
                bp = _etf_last_close_on_or_before(ohlcv_past, nk, purchase_date)
            if pd.isna(cp):
                cp = _etf_last_close_on_or_before(ohlcv_cur, nk, eval_date)
        ret_pct = np.nan
        if pd.notna(bp) and pd.notna(cp) and bp > 0:
            ret_pct = (cp / bp - 1.0) * 100.0
        rows.append({
            '순위': rank_i,
            'ticker': disp_tk,
            '종목명': nm,
            '매수가': bp,
            '평가가': cp,
            '수익률(%)': ret_pct,
        })
    return pd.DataFrame(rows, columns=out_cols)


def _compute_top7_momentum_5d_portfolio_returns(
    eval_date,
    min_trading_days=None,
    ohlcv_universe=None,
    name_by_ticker=None,
    momentum_basis=None,
    top_n: int = 7,
):
    """
    매수일(= eval_date 기준 5거래일 전) 시점 OHLCV로 모멘텀을 산출한 뒤, 지표별 상위 N종목을 고릅니다.

    - **선정용 모멘텀**: T-0·`MOMENTUM_DEFERRED_LAGS`(예: T-5, T-3) 종가 기준 `N일_상승률_*`·`평균_모멘텀_*`.
    - **수익률(모든 행 공통)**: 매수일 종가(purchase_date) → **평가일(eval_date) 종가(당일)** 동일 비중·동일 식.

    Args:
        eval_date: 평가 기준일(거래일, 평가가·수익률 분자에 사용).
        min_trading_days: `calculate_momentum_returns`에 그대로 전달.
        ohlcv_universe: 지정 시 DB 전체가 아니라 이 OHLCV(티커 집합)만으로 모멘텀·가격 산출.
        name_by_ticker: 지정 시 종목명으로 사용(ticker→이름 dict). 미지정이면 DB etf_info 병합 시도.
        momentum_basis: None이면 T-0·지연(T-5·T-3 등) 행 모두 포함. 't0'|'t3'|'t5'이면 해당 정의만.
        top_n: 매수기준별로 모멘텀 상위 몇 종목을 고를지(기본 7, 대시보드와 동일).

    Returns:
        tuple: (요약 DataFrame, dict[매수기준 라벨] -> 종목별 상세 DataFrame)
    """
    if isinstance(eval_date, str):
        eval_date = datetime.strptime(eval_date, '%Y-%m-%d').date()
    purchase_date = _prev_trading_date(eval_date, n_trading_days=5)
    if ohlcv_universe is not None and isinstance(ohlcv_universe, pd.DataFrame) and not ohlcv_universe.empty:
        ohlcv_past = _ohlcv_universe_on_or_before(ohlcv_universe, purchase_date)
        ohlcv_cur = _ohlcv_universe_on_or_before(ohlcv_universe, eval_date)
    else:
        ohlcv_past = get_etf_ohlcv_from_db(end_date=purchase_date.strftime('%Y-%m-%d'))
        ohlcv_cur = get_etf_ohlcv_from_db(end_date=eval_date.strftime('%Y-%m-%d'))
    if ohlcv_past.empty or ohlcv_cur.empty:
        return pd.DataFrame(), {}

    momentum_df = calculate_momentum_returns(
        ohlcv_past, periods=[5, 10, 20, 50, 120], min_trading_days=min_trading_days
    )
    if momentum_df.empty:
        return pd.DataFrame(), {}
    momentum_df = calculate_weighted_average_momentum(momentum_df)
    if name_by_ticker is not None and isinstance(name_by_ticker, dict) and len(name_by_ticker) > 0:

        def _nm_top7(tk):
            tk = str(tk).strip()
            return name_by_ticker.get(tk) or name_by_ticker.get(_norm_etf_ticker_key(tk), tk)

        momentum_df['etf_name'] = momentum_df['ticker'].astype(str).map(_nm_top7)
    elif 'etf_name' not in momentum_df.columns:
        etf_info = get_etf_list_from_db()
        if etf_info is not None and not etf_info.empty:
            momentum_df = momentum_df.merge(
                etf_info[['ticker', 'etf_name']],
                on='ticker',
                how='left',
            )
            momentum_df['etf_name'] = momentum_df['etf_name'].fillna(momentum_df['ticker'])
        else:
            momentum_df['etf_name'] = momentum_df['ticker']

    tn = max(1, int(top_n))
    sort_specs_all = [
        ('평균_모멘텀', f'평균 모멘텀(T-0) 상위 {tn}'),
        ('5일_상승률', f'5일 모멘텀(T-0) 상위 {tn}'),
        ('10일_상승률', f'10일 모멘텀(T-0) 상위 {tn}'),
        ('20일_상승률', f'20일 모멘텀(T-0) 상위 {tn}'),
        ('50일_상승률', f'50일 모멘텀(T-0) 상위 {tn}'),
        ('120일_상승률', f'120일 모멘텀(T-0) 상위 {tn}'),
    ]
    _lag_disp = {'_T5': 'T-5', '_T3': 'T-3'}
    for suf, _lag in MOMENTUM_DEFERRED_LAGS:
        lab = _lag_disp.get(suf, suf)
        sort_specs_all.append((f'평균_모멘텀{suf}', f'평균 모멘텀({lab}) 상위 {tn}'))
        for p in (5, 10, 20, 50, 120):
            sort_specs_all.append((f'{p}일_상승률{suf}', f'{p}일 모멘텀({lab}) 상위 {tn}'))
    _mb = str(momentum_basis).lower() if momentum_basis is not None else None
    if _mb == 't5':
        sort_specs = [sp for sp in sort_specs_all if str(sp[0]).endswith('_T5')]
    elif _mb == 't3':
        sort_specs = [sp for sp in sort_specs_all if str(sp[0]).endswith('_T3')]
    elif _mb == 't0':
        sort_specs = [
            sp for sp in sort_specs_all
            if not (str(sp[0]).endswith('_T3') or str(sp[0]).endswith('_T5'))
        ]
    else:
        sort_specs = sort_specs_all
    rows = []
    details_by_label: dict[str, pd.DataFrame] = {}
    for col, label in sort_specs:
        if col not in momentum_df.columns:
            rows.append({'매수기준': label, '5거래일평균수익률(%)': np.nan, '유효': f'0/{tn}'})
            details_by_label[label] = pd.DataFrame(
                columns=['순위', 'ticker', '종목명', '매수가', '평가가', '수익률(%)']
            )
            continue
        ranked = momentum_df[momentum_df[col].notna()].sort_values(col, ascending=False).head(tn)
        tickers = ranked['ticker'].astype(str).tolist()
        rets = []
        detail_rows = []
        for rank_i, (_, rrow) in enumerate(ranked.iterrows(), start=1):
            tk = str(rrow['ticker']).strip()
            nm = str(rrow.get('etf_name', tk) or tk)
            bp = _etf_last_close_on_or_before(ohlcv_past, tk, purchase_date)
            cp = _etf_last_close_on_or_before(ohlcv_cur, tk, eval_date)
            ret_pct = np.nan
            if pd.notna(bp) and pd.notna(cp) and bp > 0:
                ret_pct = (cp / bp - 1.0) * 100.0
                rets.append(ret_pct)
            detail_rows.append({
                '순위': rank_i,
                'ticker': tk,
                '종목명': nm,
                '매수가': bp,
                '평가가': cp,
                '수익률(%)': ret_pct,
            })
        avg_r = float(np.mean(rets)) if rets else np.nan
        rows.append({
            '매수기준': label,
            '5거래일평균수익률(%)': avg_r,
            '유효': f'{len(rets)}/{tn}',
        })
        details_by_label[label] = pd.DataFrame(detail_rows)
    return pd.DataFrame(rows), details_by_label


def _df_html(df, cols=None, pct_cols=None, heatmap_cols=None, compare_kospi_map=None):
    """DataFrame → 대시보드용 sortable HTML 테이블 문자열."""
    if df is None or df.empty:
        return '<p class="no-data">데이터 없음</p>'
    d = df.copy()
    if cols:
        keep = [c for c in cols if c in d.columns]
        d = d[keep].copy()
    pct_cols = set(pct_cols or [])
    heatmap_cols = set(heatmap_cols or [])
    kospi_cmp = compare_kospi_map or {}
    align_right = pct_cols | heatmap_cols | {
        '현재가', 'ATR14', 'ATR14_일변동률(%)', '1주수익률', '5거래일평균수익률(%)',
        '20일(5거래일)수익률', '순위', '전주순위', '순위_상승', '유효', '매수가', '평가가',
        '구성순위',
    }
    _tbl_classes = ['tbl', 'sortable-tbl']
    if '종목명' in d.columns:
        _tbl_classes.append('tbl-name-wide')
    _tbl_class_s = ' '.join(_tbl_classes)
    th = ''.join([
        (
            f'<th class="t-num">{_safe_html(c)}</th>'
            if c in align_right
            else (f'<th class="col-name">{_safe_html(c)}</th>' if c == '종목명' else f'<th>{_safe_html(c)}</th>')
        )
        for c in d.columns
    ])
    trs = []
    for _, r in d.iterrows():
        tds = []
        for c in d.columns:
            v = r.get(c, '')
            cls = ''
            num_align = 't-num' if c in align_right else ''
            disp = v
            if c in pct_cols:
                disp = _fmt_pct(v)
            if c in heatmap_cols:
                cls = _momentum_color_class(v)
                disp = _fmt_pct(v) if isinstance(v, (int, float, np.floating)) else disp
            kref = kospi_cmp.get(c, None)
            has_k = kref is not None and pd.notna(kref)
            if has_k:
                if c in pct_cols or isinstance(v, (int, float, np.floating)):
                    disp = _fmt_pct(v)
                try:
                    ev = float(v)
                    kv = float(kref)
                    if pd.notna(ev) and pd.notna(kv):
                        if ev > kv:
                            cls = 'kospi-beat'
                        elif ev < kv:
                            cls = 'kospi-miss'
                except (TypeError, ValueError):
                    pass
            elif c == 'weekly_return_pct':
                cls = cls or _momentum_color_class(v)
                disp = _fmt_pct(v)
            elif c in ('당일 수익률', '3일 수익률'):
                disp = _fmt_pct(v)
                cls = cls or _momentum_color_class(v)
            elif c == '현재가':
                try:
                    fv = float(v)
                    disp = f'{fv:,.0f}' if pd.notna(fv) else ''
                except (TypeError, ValueError):
                    disp = str(v) if v != '' else ''
            elif c in ('매수가', '평가가'):
                try:
                    fv = float(v)
                    disp = f'{fv:,.0f}' if pd.notna(fv) else ''
                except (TypeError, ValueError):
                    disp = str(v) if v != '' else ''
            elif c == 'ATR14':
                try:
                    fv = float(v)
                    disp = f'{int(round(fv)):,}' if pd.notna(fv) else ''
                except (TypeError, ValueError):
                    disp = str(v) if v != '' else ''
            _name_cls = ' col-name' if c == '종목명' else ''
            td_cls = f'{num_align}{_name_cls} {cls}'.strip()
            tds.append(f'<td class="{td_cls}">{_safe_html(disp)}</td>')
        trs.append('<tr>' + ''.join(tds) + '</tr>')
    return f'<table class="{_tbl_class_s}"><thead><tr>{th}</tr></thead><tbody>' + ''.join(trs) + '</tbody></table>'


def _html_momentum7_scorecard(summary_df, det_map):
    """매수일 기준 모멘텀 상위 N 요약·종목별 아코디언 HTML(T-0·지연 T-5/T-3 등 선정, 수익률은 평가일 종가 공통)."""
    if summary_df is None or summary_df.empty:
        return '<p class="no-data">데이터 없음</p>'
    parts = ['<div class="m7-scorecard">']
    for _, row in summary_df.iterrows():
        lbl = str(row.get('매수기준', '') or '')
        ar = row.get('5거래일평균수익률(%)', np.nan)
        pct_disp = _fmt_pct(ar)
        valid_s = str(row.get('유효', ''))
        lbl_esc = _safe_html(lbl)
        pct_esc = _safe_html(pct_disp)
        valid_esc = _safe_html(valid_s)
        ddf = det_map.get(lbl) if isinstance(det_map, dict) else None
        if ddf is None or ddf.empty:
            inner = '<p class="no-data">선택 종목 없음</p>'
        else:
            inner = _df_html(
                ddf,
                cols=['순위', 'ticker', '종목명', '매수가', '평가가', '수익률(%)'],
                pct_cols=['수익률(%)'],
            )
        parts.append(
            '<details class="m7-acc">'
            '<summary>'
            f'<span class="m7-sum-name">{lbl_esc}</span>'
            f'<span class="m7-sum-mid">{pct_esc}</span>'
            f'<span class="m7-sum-valid">{valid_esc}</span>'
            '</summary>'
            f'<div class="m7-acc-body">{inner}</div>'
            '</details>'
        )
    parts.append('</div>')
    return ''.join(parts)


def _compute_rank_change_section(current_momentum_df, prev_momentum_df, period_col='평균_모멘텀', top_n=20):
    if current_momentum_df is None or current_momentum_df.empty or period_col not in current_momentum_df.columns:
        return {'new_entries': pd.DataFrame(), 'rank_up': pd.DataFrame(), 'dropped': pd.DataFrame(), 'cur_ranked': pd.DataFrame()}

    cur_ranked = get_momentum_rankings(current_momentum_df, period_col, top_n=top_n)
    cur_ranked = calculate_rank_change(cur_ranked, prev_momentum_df if prev_momentum_df is not None else pd.DataFrame(), period_col)

    new_entries = cur_ranked[cur_ranked.get('순위변화_표시', '') == 'NEW'].copy()
    rank_up = cur_ranked[pd.to_numeric(cur_ranked.get('순위변화', 0), errors='coerce').fillna(0) >= 5].copy()

    dropped = pd.DataFrame()
    if prev_momentum_df is not None and not prev_momentum_df.empty and period_col in prev_momentum_df.columns:
        prev_ranked = get_momentum_rankings(prev_momentum_df, period_col, top_n=top_n)
        prev_top = set(prev_ranked['ticker'].astype(str).tolist()) if not prev_ranked.empty else set()
        cur_top = set(cur_ranked['ticker'].astype(str).tolist()) if not cur_ranked.empty else set()
        dropped_t = sorted(list(prev_top - cur_top))
        if dropped_t:
            tmp = prev_ranked[prev_ranked['ticker'].astype(str).isin(dropped_t)].copy()
            tmp = tmp.rename(columns={'순위': '전주순위'})
            if 'etf_name' in tmp.columns:
                dropped = tmp[['ticker', 'etf_name', '전주순위', period_col]].copy()
            else:
                dropped = tmp[['ticker', '전주순위', period_col]].copy()

    return {'new_entries': new_entries, 'rank_up': rank_up, 'dropped': dropped, 'cur_ranked': cur_ranked}


def _insert_current_price_after_name(df, ohlcv_df=None):
    """종목명 또는 etf_name 바로 오른쪽에 `현재가`(최근 종가) 컬럼을 둡니다."""
    if df is None or df.empty:
        return df
    out = df.copy()
    name_col = '종목명' if '종목명' in out.columns else ('etf_name' if 'etf_name' in out.columns else None)
    if name_col is None or 'ticker' not in out.columns:
        return out
    if '현재가' not in out.columns:
        if 'last_close' in out.columns:
            out['현재가'] = pd.to_numeric(out['last_close'], errors='coerce')
            out = out.drop(columns=['last_close'], errors='ignore')
        elif ohlcv_df is not None and not ohlcv_df.empty and 'close' in ohlcv_df.columns:
            o = ohlcv_df.sort_values(['ticker', 'date']).copy()
            o['close'] = pd.to_numeric(o['close'], errors='coerce')
            px = o.groupby('ticker', sort=False)['close'].last()
            out['현재가'] = out['ticker'].astype(str).map(px)
        else:
            out['현재가'] = np.nan
    else:
        out['현재가'] = pd.to_numeric(out['현재가'], errors='coerce')
    if 'last_close' in out.columns:
        out = out.drop(columns=['last_close'], errors='ignore')
    cols = [c for c in out.columns if c != '현재가']
    try:
        ni = cols.index(name_col) + 1
    except ValueError:
        return out
    new_order = cols[:ni] + ['현재가'] + cols[ni:]
    return out[[c for c in new_order if c in out.columns]]


def _build_full_momentum_ma_merge(current_momentum_df, ohlcv_df, rank_col='평균_모멘텀'):
    """
    유니버스 전체 티커에 대해 모멘텀 + MA/ATR/주간 부가열을 병합합니다(순위·매수후보용).
    `above_sma_50`은 매수후보(상위 N·MA50) 필터에 사용됩니다.
    `rank_col` 기준 모멘텀이 없는 종목도 행에 남깁니다(정렬 시 맨 뒤). 매수 후보 상위 N은 `_build_action_list`에서 유효 모멘텀만으로 순위를 다시 매깁니다.
    """
    if current_momentum_df is None or current_momentum_df.empty:
        return pd.DataFrame()
    if rank_col not in current_momentum_df.columns:
        return pd.DataFrame()
    if ohlcv_df is None or ohlcv_df.empty:
        return pd.DataFrame()
    df = current_momentum_df.copy()
    df['ticker'] = df['ticker'].astype(str)
    if '종목명' not in df.columns:
        if 'etf_name' in df.columns:
            df = df.rename(columns={'etf_name': '종목명'})
        else:
            df['종목명'] = df['ticker']
    ma = _compute_last_close_and_sma(ohlcv_df, window=[5, 10, 20, 50])
    merged = df.merge(ma, on='ticker', how='left')
    merged = merged.sort_values(rank_col, ascending=False, na_position='last').reset_index(drop=True)
    merged.insert(0, '순위', np.arange(1, len(merged) + 1, dtype=int))

    def _ma_y_col(src):
        if isinstance(src, pd.Series):
            return (src == True).map(lambda v: 'Y' if v else '')
        return ''

    merged['MA5위'] = _ma_y_col(merged['above_sma_5']) if 'above_sma_5' in merged.columns else ''
    merged['MA10위'] = _ma_y_col(merged['above_sma_10']) if 'above_sma_10' in merged.columns else ''
    _s20 = merged['above_sma_20'] if 'above_sma_20' in merged.columns else merged.get('above_sma')
    merged['MA20위'] = _ma_y_col(_s20) if isinstance(_s20, pd.Series) else ''
    _s50 = merged['above_sma_50'] if 'above_sma_50' in merged.columns else None
    merged['MA50위'] = _ma_y_col(_s50) if isinstance(_s50, pd.Series) else ''

    if 'last_close' in merged.columns:
        merged['현재가'] = pd.to_numeric(merged['last_close'], errors='coerce')
        merged = merged.drop(columns=['last_close'], errors='ignore')
    return merged


def _build_action_list(current_momentum_df, ohlcv_df, top_n=20, momentum_basis='t0'):
    """
    momentum_basis: 't0' → `평균_모멘텀`·`N일_상승률`로 순위·표시.
                    't3' → `평균_모멘텀_T3`·`N일_상승률_T3`(동일 MA50 필터).
                    't5' → `평균_모멘텀_T5`·`N일_상승률_T5`(동일 MA50 필터).
    """
    mb = str(momentum_basis).lower()
    if mb == 't5':
        rank_col = '평균_모멘텀_T5'
    elif mb == 't3':
        rank_col = '평균_모멘텀_T3'
    else:
        rank_col = '평균_모멘텀'
    merged = _build_full_momentum_ma_merge(current_momentum_df, ohlcv_df, rank_col=rank_col)
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    m_elig = merged[merged[rank_col].notna()].copy()
    m_elig = m_elig.drop(columns=['순위'], errors='ignore')
    m_elig = m_elig.sort_values(rank_col, ascending=False).reset_index(drop=True)
    m_elig.insert(0, '순위', np.arange(1, len(m_elig) + 1, dtype=int))

    cond_top = m_elig['순위'] <= int(top_n)
    cond_ma50 = m_elig.get('above_sma_50', False) == True

    strong = m_elig[cond_top & cond_ma50].copy()

    cols = _action_list_column_order(m_elig, momentum_basis=momentum_basis)
    strong = strong[[c for c in cols if c in strong.columns]].copy() if not strong.empty else pd.DataFrame(columns=cols)
    watch = pd.DataFrame(columns=cols)
    return strong, watch


def _save_etf_dashboard_html(current_momentum_df, prev_momentum_df=None, ohlcv_df=None, backtest_result=None,
                            period_info=None, open_web=True, out_filename='etf_dashboard.html'):
    """
    대시보드형 1페이지 HTML 생성.
    - 요약: T-0·T-3·T-5 각각 포트폴리오 상위7·매수 후보(평균 모멘텀 기준 상위·MA50 필터)
    - 교차표 + 히트맵: 종목×기간(5/10/20/50/120) + MA5·10·20/주간변동
    - 전주 변화: 신규/급상승/이탈
    - 상세: (필요시) 기존 상위표를 간략 아코디언으로 제공
    - 백테스트: 이번 주 결과 요약표 포함
    """
    # Breadth
    total_etf = int(current_momentum_df['ticker'].nunique()) if current_momentum_df is not None and not current_momentum_df.empty else 0
    breadth = np.nan
    if current_momentum_df is not None and not current_momentum_df.empty and '20일_상승률' in current_momentum_df.columns:
        breadth = (current_momentum_df['20일_상승률'].gt(0).sum() / len(current_momentum_df) * 100.0) if len(current_momentum_df) else np.nan

    kospi_daily_ref = np.nan
    kospi_3d_ref = np.nan
    kospi_weekly_ref = np.nan
    kospi_avg_ref = np.nan
    kospi_period_refs: dict[str, float] = {}
    kospi_sma_info: dict = {'close': np.nan, 'ma5': np.nan, 'ma10': np.nan, 'ma20': np.nan}
    eval_d_tab = date.today()
    purchase_d_tab = _prev_trading_date(eval_d_tab, n_trading_days=5)
    if ohlcv_df is not None and not ohlcv_df.empty and 'date' in ohlcv_df.columns:
        _dts = pd.to_datetime(ohlcv_df['date'], errors='coerce').dropna()
        if len(_dts):
            _ref_ts = pd.Timestamp(_dts.max()).normalize()
            _ref_d = _ref_ts.date()
            eval_d_tab = _ref_d
            purchase_d_tab = _prev_trading_date(eval_d_tab, n_trading_days=5)
            kospi_daily_ref = _get_kospi_daily_return_pct_for_ref_date(_ref_d)
            kospi_3d_ref = _get_kospi_3d_return_pct_for_ref_date(_ref_d)
            kospi_weekly_ref = _get_kospi_weekly_return_pct_for_ref_ts(_ref_ts)
            kospi_avg_ref = _get_kospi_weighted_avg_momentum_for_ref_date(_ref_d)
            for _n in (5, 10, 20, 50, 120):
                kospi_period_refs[f'{_n}일_상승률'] = _get_kospi_nd_trading_return_pct_for_ref_date(_ref_d, _n)
            kospi_sma_info = _get_kospi_close_and_sma_for_ref_date(_ref_d)
    kospi_daily_card = _fmt_pct(kospi_daily_ref) if pd.notna(kospi_daily_ref) else 'N/A'
    kospi_3d_card = _fmt_pct(kospi_3d_ref) if pd.notna(kospi_3d_ref) else 'N/A'
    kospi_weekly_card = _fmt_pct(kospi_weekly_ref) if pd.notna(kospi_weekly_ref) else 'N/A'
    kospi_close_lv_card = _fmt_index_level(kospi_sma_info.get('close'))
    kospi_ma5_card = _fmt_index_level(kospi_sma_info.get('ma5'))
    kospi_ma10_card = _fmt_index_level(kospi_sma_info.get('ma10'))
    kospi_ma20_card = _fmt_index_level(kospi_sma_info.get('ma20'))

    try:
        momentum7_t5_df, momentum7_t5_details = _compute_top7_momentum_5d_portfolio_returns(
            eval_d_tab, min_trading_days=None, momentum_basis='t5'
        )
        momentum7_t3_df, momentum7_t3_details = _compute_top7_momentum_5d_portfolio_returns(
            eval_d_tab, min_trading_days=None, momentum_basis='t3'
        )
        momentum7_t0_df, momentum7_t0_details = _compute_top7_momentum_5d_portfolio_returns(
            eval_d_tab, min_trading_days=None, momentum_basis='t0'
        )
    except Exception:
        momentum7_t5_df, momentum7_t5_details = pd.DataFrame(), {}
        momentum7_t3_df, momentum7_t3_details = pd.DataFrame(), {}
        momentum7_t0_df, momentum7_t0_details = pd.DataFrame(), {}
    purchase_5d_str = purchase_d_tab.strftime('%Y-%m-%d')
    eval_date_str = eval_d_tab.strftime('%Y-%m-%d')

    cross = _build_cross_momentum_table(current_momentum_df, periods=(5, 10, 20, 50, 120), sort_col='평균_모멘텀', top_n=30)
    ma_multi = _compute_last_close_and_sma(ohlcv_df, window=[5, 10, 20])
    merge_cols = ['ticker', 'last_close', 'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14', 'MA10연속추세', 'ATR14_일변동률(%)']
    for w in [5, 10, 20]:
        c = f'above_sma_label_{w}'
        if c in ma_multi.columns:
            merge_cols.append(c)
    cross2 = cross.merge(ma_multi[merge_cols], on='ticker', how='left') if not cross.empty else cross
    # 보기 좋은 컬럼명
    rename_map = {}
    for w in [5, 10, 20]:
        k = f'above_sma_label_{w}'
        if k in cross2.columns:
            rename_map[k] = f'MA{w}위/아래'
    if rename_map:
        cross2 = cross2.rename(columns=rename_map)
    cross2 = _insert_current_price_after_name(cross2, ohlcv_df) if not cross2.empty else cross2

    change = _compute_rank_change_section(current_momentum_df, prev_momentum_df, period_col='평균_모멘텀', top_n=20)
    new_entries = _insert_current_price_after_name(change.get('new_entries', pd.DataFrame()), ohlcv_df)
    rank_up = _insert_current_price_after_name(change.get('rank_up', pd.DataFrame()), ohlcv_df)
    dropped = _insert_current_price_after_name(change.get('dropped', pd.DataFrame()), ohlcv_df)

    strong_t5, _ = _build_action_list(current_momentum_df, ohlcv_df, top_n=20, momentum_basis='t5')
    strong_t3, _ = _build_action_list(current_momentum_df, ohlcv_df, top_n=20, momentum_basis='t3')
    strong_t0, _ = _build_action_list(current_momentum_df, ohlcv_df, top_n=20, momentum_basis='t0')
    strong_t5 = _insert_current_price_after_name(strong_t5, ohlcv_df) if strong_t5 is not None and not strong_t5.empty else strong_t5
    strong_t3 = _insert_current_price_after_name(strong_t3, ohlcv_df) if strong_t3 is not None and not strong_t3.empty else strong_t3
    strong_t0 = _insert_current_price_after_name(strong_t0, ohlcv_df) if strong_t0 is not None and not strong_t0.empty else strong_t0

    # 백테스트 상세(bt_df)만 유지 — 요약 스코어카드는 상위7(T-0·T-3·T-5 분리)으로 대체
    bt_df = pd.DataFrame()
    if backtest_result:
        bt_rows = []
        for strat_label, key in [
            ('일주일 전 모멘텀 상위 N', 'results'),
            ('5% 하락 ETF 모멘텀 상위 N', 'results_5pct'),
            ('5% 상승 ETF 모멘텀 상위 N', 'results_5pct_rise'),
            ('MA10 이탈 제외 모멘텀 상위 N', 'results_ma10'),
            ('MA20 이탈 제외 모멘텀 상위 N', 'results_ma20'),
            ('MA10 제외·5% 하락 모멘텀 상위 N', 'results_ma10_5pct'),
            ('MA20 제외·5% 하락 모멘텀 상위 N', 'results_ma20_5pct'),
            ('MA10 제외·5% 상승 모멘텀 상위 N', 'results_ma10_5pct_rise'),
            ('MA20 제외·5% 상승 모멘텀 상위 N', 'results_ma20_5pct_rise'),
        ]:
            res = backtest_result.get(key, {}) or {}
            for p in [10, 20, 50, 120]:
                if p not in res:
                    continue
                r = res[p]
                bt_rows.append({
                    '전략': strat_label,
                    '모멘텀기간': f'{p}일',
                    '1주수익률': r.get('avg_return', np.nan),
                    '유효': f"{r.get('valid_count','')}/{r.get('total_count','')}"
                })
        bt_df = pd.DataFrame(bt_rows)

    action_kospi_cmp = {
        '평균_모멘텀': kospi_avg_ref,
        '5일_상승률': kospi_period_refs.get('5일_상승률', np.nan),
        '10일_상승률': kospi_period_refs.get('10일_상승률', np.nan),
        '20일_상승률': kospi_period_refs.get('20일_상승률', np.nan),
        '50일_상승률': kospi_period_refs.get('50일_상승률', np.nan),
        '120일_상승률': kospi_period_refs.get('120일_상승률', np.nan),
        'weekly_return_pct': kospi_weekly_ref,
        '당일 수익률': kospi_daily_ref,
        '3일 수익률': kospi_3d_ref,
    }
    cross_kospi_cmp = {'당일 수익률': kospi_daily_ref, '3일 수익률': kospi_3d_ref}

    # Minimal legacy detailed rankings (accordion)
    legacy_bits = []
    if current_momentum_df is not None and not current_momentum_df.empty:
        for p in [10, 20, 50, 120]:
            col = f'{p}일_상승률'
            if col not in current_momentum_df.columns:
                continue
            ranked = get_momentum_rankings(current_momentum_df, col, top_n=20)
            if ranked.empty:
                continue
            d = ranked[['순위', 'ticker'] + (['etf_name'] if 'etf_name' in ranked.columns else []) + [col]].copy()
            if 'etf_name' in d.columns:
                d = d.rename(columns={'etf_name': '종목명'})
            d = d.rename(columns={col: f'{p}일'})
            d = _insert_current_price_after_name(d, ohlcv_df)
            legacy_bits.append(f"<details><summary>{p}일 모멘텀 상위 20</summary>{_df_html(d, pct_cols=[f'{p}일'])}</details>")
        if '평균_모멘텀' in current_momentum_df.columns:
            ranked = get_momentum_rankings(current_momentum_df, '평균_모멘텀', top_n=20)
            if not ranked.empty:
                d = ranked[['순위', 'ticker'] + (['etf_name'] if 'etf_name' in ranked.columns else []) + ['평균_모멘텀']].copy()
                if 'etf_name' in d.columns:
                    d = d.rename(columns={'etf_name': '종목명'})
                d = _insert_current_price_after_name(d, ohlcv_df)
                legacy_bits.append(f"<details><summary>평균 모멘텀 상위 20</summary>{_df_html(d, pct_cols=['평균_모멘텀'])}</details>")
    legacy_html = '\n'.join(legacy_bits) if legacy_bits else '<p class="no-data">데이터 없음</p>'

    # Cross heatmap columns
    _heat_candidates = [
        '5일_상승률', '10일_상승률', '20일_상승률', '50일_상승률', '120일_상승률',
        '평균_모멘텀', '평균_모멘텀_2',
    ]
    for _p in (5, 10, 20, 50, 120):
        for _suf, _ in MOMENTUM_DEFERRED_LAGS:
            _heat_candidates.append(f'{_p}일_상승률{_suf}')
    for _suf, _ in MOMENTUM_DEFERRED_LAGS:
        _heat_candidates.extend([f'평균_모멘텀{_suf}', f'평균_모멘텀_2{_suf}'])
    heat_cols = [c for c in _heat_candidates if c in cross2.columns]
    if not cross2.empty:
        _hm_rest = [c for c in heat_cols if c in cross2.columns]
        _hm_base = [c for c in ['순위', 'ticker', '종목명', '현재가'] if c in cross2.columns]
        cross2_hm = cross2[_hm_base + _hm_rest]
    else:
        cross2_hm = cross2

    momentum7_scorecard_t5_html = _html_momentum7_scorecard(momentum7_t5_df, momentum7_t5_details)
    momentum7_scorecard_t3_html = _html_momentum7_scorecard(momentum7_t3_df, momentum7_t3_details)
    momentum7_scorecard_t0_html = _html_momentum7_scorecard(momentum7_t0_df, momentum7_t0_details)

    _n_t5 = len(strong_t5) if strong_t5 is not None and not strong_t5.empty else 0
    _n_t3 = len(strong_t3) if strong_t3 is not None and not strong_t3.empty else 0
    _n_t0 = len(strong_t0) if strong_t0 is not None and not strong_t0.empty else 0
    _pct_action_t0 = ['평균_모멘텀', '5일_상승률', '10일_상승률', '20일_상승률', '50일_상승률', '120일_상승률',
                      'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)']
    _pct_action_t5 = ['평균_모멘텀_T5', '5일_상승률_T5', '10일_상승률_T5', '20일_상승률_T5', '50일_상승률_T5', '120일_상승률_T5',
                        'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)']
    _pct_action_t3 = ['평균_모멘텀_T3', '5일_상승률_T3', '10일_상승률_T3', '20일_상승률_T3', '50일_상승률_T3', '120일_상승률_T3',
                        'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)']

    title_info = _safe_html(period_info) if period_info else ''
    breadth_str = f"{breadth:.1f}%" if pd.notna(breadth) else "N/A"

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ETF 모멘텀 대시보드</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; margin: 20px; background: #f5f6fa; color: #1a202c; }}
    h1 {{ font-size: 1.4rem; margin: 0 0 8px 0; }}
    .sub {{ color: #4a5568; margin-bottom: 14px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 12px 0 18px 0; }}
    .card {{ background: #fff; border-radius: 12px; padding: 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
    .card .k {{ color:#4a5568; font-size: 0.85rem; }}
    .card .v {{ font-size: 1.25rem; font-weight: 700; margin-top: 6px; }}
    .card-kospi-inner .k {{ margin-top: 0; }}
    .card-kospi-inner .k + .v {{ margin-top: 4px; }}
    .card-kospi-inner .k:not(:first-child) {{ margin-top: 10px; }}
    .card .v.v-sm {{ font-size: 1.05rem; }}
    .tabs {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }}
    .tabbtn {{ background:#e2e8f0; border:0; padding: 8px 10px; border-radius: 10px; cursor:pointer; }}
    .tabbtn.active {{ background:#2b6cb0; color:#fff; }}
    .tab {{ display:none; margin-top: 14px; }}
    .tab.active {{ display:block; }}
    .section {{ background:#fff; border-radius: 12px; padding: 14px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
    .section h2 {{ font-size: 1.05rem; margin: 0 0 10px 0; }}
    .section h3 {{ font-size: 0.98rem; margin: 0 0 8px 0; color: #2d3748; }}
    .section-part {{ font-size: 1.12rem; font-weight: 700; margin: 8px 0 14px 0; color: #1a365d; padding-bottom: 8px; border-bottom: 2px solid #cbd5e0; }}
    .meta {{ color:#4a5568; margin: 6px 0 10px 0; }}
    .no-data {{ color:#718096; font-style: italic; }}
    table.tbl {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 10px; overflow: hidden; }}
    table.tbl th, table.tbl td {{ border-bottom: 1px solid #e2e8f0; padding: 8px 10px; font-size: 0.88rem; }}
    table.tbl th {{ background: #2b6cb0; color: #fff; text-align: left; }}
    table.tbl th.t-num, table.tbl td.t-num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    table.tbl tr:nth-child(even) {{ background: #f7fafc; }}
    td.m-pos {{ background: rgba(72, 187, 120, 0.12); }}
    td.m-strong-pos {{ background: rgba(72, 187, 120, 0.28); font-weight: 700; }}
    td.m-neg {{ background: rgba(245, 101, 101, 0.12); }}
    td.m-strong-neg {{ background: rgba(245, 101, 101, 0.26); font-weight: 700; }}
    td.m-na {{ color:#a0aec0; }}
    td.kospi-beat {{ background: rgba(72, 187, 120, 0.35); font-weight: 600; }}
    td.kospi-miss {{ background: rgba(245, 101, 101, 0.32); font-weight: 600; }}
    .m7-scorecard {{ display: flex; flex-direction: column; gap: 10px; }}
    details.m7-acc {{ background:#f7fafc; border:1px solid #e2e8f0; border-radius:10px; padding:10px 14px; }}
    details.m7-acc > summary {{ list-style:none; cursor:pointer; display:flex; flex-wrap:wrap; align-items:center; gap:14px; padding:2px 0; font-weight:600; color:#2d3748; }}
    details.m7-acc > summary::-webkit-details-marker {{ display:none; }}
    .m7-sum-name {{ flex:1 1 220px; min-width:160px; }}
    .m7-sum-mid, .m7-sum-valid {{ font-variant-numeric:tabular-nums; text-align:right; }}
    .m7-sum-mid {{ min-width:76px; color:#2b6cb0; }}
    .m7-sum-valid {{ min-width:42px; color:#4a5568; font-weight:600; font-size:0.92em; }}
    .m7-acc-body {{ margin-top:10px; padding-top:10px; border-top:1px solid #e2e8f0; }}
    table.tbl.tbl-name-wide {{ width: max-content; max-width: min(100%, 1680px); }}
    table.tbl.tbl-name-wide th.col-name, table.tbl.tbl-name-wide td.col-name {{ min-width: 300px; white-space: nowrap; vertical-align: middle; }}
    details summary {{ cursor:pointer; padding: 6px 0; color:#2d3748; font-weight: 600; }}
{_HTML_TABLE_SORT_CSS}
  </style>
</head>
<body>
  <h1>ETF 모멘텀 대시보드</h1>
  <div class="sub">{title_info}</div>

  <div class="cards">
    <div class="card"><div class="k">분석 ETF 수</div><div class="v">{total_etf}</div></div>
    <div class="card"><div class="k">시장 브레스드 (20일 모멘텀&gt;0 비율)</div><div class="v">{breadth_str}</div></div>
    <div class="card"><div class="k">코스피 수익률</div><div class="card-kospi-inner"><div class="k">당일 (전일대비)</div><div class="v v-sm">{_safe_html(kospi_daily_card)}</div><div class="k">3일 (역순 3번째 거래일 종가 대비)</div><div class="v v-sm">{_safe_html(kospi_3d_card)}</div><div class="k">주간 (ISO주·거래일, 월요일 시가 대비)</div><div class="v v-sm">{_safe_html(kospi_weekly_card)}</div><div class="k">기준일 종가 (1001)</div><div class="v v-sm">{_safe_html(kospi_close_lv_card)}</div><div class="k">5일 이평선 (SMA)</div><div class="v v-sm">{_safe_html(kospi_ma5_card)}</div><div class="k">10일 이평선 (SMA)</div><div class="v v-sm">{_safe_html(kospi_ma10_card)}</div><div class="k">20일 이평선 (SMA)</div><div class="v v-sm">{_safe_html(kospi_ma20_card)}</div></div></div>
    <div class="card"><div class="k">매수 후보 수 (T-0 / T-3 / T-5)</div><div class="v">{_n_t0} / {_n_t3} / {_n_t5}</div></div>
  </div>

  <div class="tabs">
    <button class="tabbtn active" data-tab="t1">요약</button>
    <button class="tabbtn" data-tab="t2">교차표·히트맵</button>
    <button class="tabbtn" data-tab="t3">전주 대비 변화</button>
    <button class="tabbtn" data-tab="t4">상세 순위</button>
    <button class="tabbtn" data-tab="t5">백테스트</button>
  </div>

    <div id="t1" class="tab active">
    <div class="section-part">T-0 기준 모멘텀</div>
    <div class="section">
      <h3>포트폴리오 수익률 (5거래일 전 매수 → 평가일, 상위 7)</h3>
      <div class="meta">매수일(모멘텀·매수가 기준일): {purchase_5d_str} · 평가일(당일 종가): {eval_date_str} · <strong>선정만</strong> 매수일 시점 <strong>최신 종가(T-0) 기준</strong> `N일_상승률`·`평균_모멘텀`으로 상위 7. <strong>수익률</strong>: 매수일 종가→평가일 종가·동일 비중 평균(%). 코스피 비교 색은 주간·당일·3일 등에만 동일 적용.</div>
      {momentum7_scorecard_t0_html}
    </div>
    <div class="section">
      <h3>매수 후보 (Action List)</h3>
      <div class="meta"><strong>평균_모멘텀</strong> 상위 20 중 종가≥MA50. 당일·3일·주간·N일 등락률·가중평균 모멘텀 열은 코스피(1001)와 비교해 색 표시.</div>
      {_df_html(strong_t0, pct_cols=_pct_action_t0, compare_kospi_map=action_kospi_cmp)}
    </div>

    <div class="section-part">T-3 기준 모멘텀</div>
    <div class="section">
      <h3>포트폴리오 수익률 (5거래일 전 매수 → 평가일, 상위 7)</h3>
      <div class="meta">매수일·평가일·수익률 식은 위와 동일. <strong>선정</strong>: 매수일 시점 OHLCV에서 <strong>T-3 종가 기준</strong> `N일_상승률_T3`·`평균_모멘텀_T3`로 상위 7.</div>
      {momentum7_scorecard_t3_html}
    </div>
    <div class="section">
      <h3>매수 후보 (Action List)</h3>
      <div class="meta"><strong>평균_모멘텀_T3</strong> 상위 20 중 종가≥MA50. T-3 모멘텀 열은 코스피와 정의가 달라 비교 색은 주간·당일·3일 등에만 적용됩니다.</div>
      {_df_html(strong_t3, pct_cols=_pct_action_t3, compare_kospi_map=action_kospi_cmp)}
    </div>

    <div class="section-part">T-5 기준 모멘텀</div>
    <div class="section">
      <h3>포트폴리오 수익률 (5거래일 전 매수 → 평가일, 상위 7)</h3>
      <div class="meta">매수일(모멘텀·매수가 기준일): {purchase_5d_str} · 평가일(당일 종가): {eval_date_str} · <strong>선정</strong>: 매수일 시점 OHLCV에서 <strong>T-5 종가 기준</strong> `N일_상승률_T5`·`평균_모멘텀_T5`로 상위 7. <strong>수익률</strong>: 매수일 종가→평가일 종가·동일 비중 평균(%). 코스피 비교 색은 주간·당일·3일 등에만 동일 적용.</div>
      {momentum7_scorecard_t5_html}
    </div>
    <div class="section">
      <h3>매수 후보 (Action List)</h3>
      <div class="meta"><strong>평균_모멘텀_T5</strong> 상위 20 중 종가≥MA50. 당일·3일·주간·ATR은 기준일 기준. T-5 모멘텀 열은 코스피와 정의가 달라 비교 색은 해당 열에 적용되지 않을 수 있습니다.</div>
      {_df_html(strong_t5, pct_cols=_pct_action_t5, compare_kospi_map=action_kospi_cmp)}
    </div>
  </div>

  <div id="t2" class="tab">
    <div class="section">
      <h2>종목 중심 교차 분석표 (상위 30)</h2>
      {_df_html(cross2, pct_cols=heat_cols + ['weekly_return_pct', '당일 수익률','3일 수익률', 'ATR14_일변동률(%)'], heatmap_cols=heat_cols, compare_kospi_map=cross_kospi_cmp)}
    </div>
    <div class="section">
      <h2>히트맵 (색: 모멘텀 강도)</h2>
      <div class="meta">상위 30개 ETF 행 × 기간(5/10/20/50/120/평균) 열</div>
      {_df_html(cross2_hm, pct_cols=heat_cols, heatmap_cols=heat_cols)}
    </div>
  </div>

  <div id="t3" class="tab">
    <div class="section">
      <h2>신규 진입 (평균 모멘텀 Top20 기준)</h2>
      {_df_html(new_entries, cols=['순위','ticker','etf_name','현재가','평균_모멘텀','전주순위','순위변화_표시'], pct_cols=['평균_모멘텀'])}
    </div>
    <div class="section">
      <h2>순위 급상승 (전주 대비 +5 이상)</h2>
      {_df_html(rank_up, cols=['순위','ticker','etf_name','현재가','평균_모멘텀','전주순위','순위변화'], pct_cols=['평균_모멘텀'])}
    </div>
    <div class="section">
      <h2>이탈 (전주 Top20 → 이번주 제외)</h2>
      {_df_html(dropped, cols=['ticker','etf_name','현재가','전주순위','평균_모멘텀'], pct_cols=['평균_모멘텀'])}
    </div>
  </div>

  <div id="t4" class="tab">
    <div class="section">
      <h2>기간별 상세 순위 (필요할 때만 펼쳐보기)</h2>
      {legacy_html}
    </div>
  </div>

  <div id="t5" class="tab">
    <div class="section">
      <h2>백테스트 결과 (이번 주)</h2>
      {_df_html(bt_df, cols=['전략','모멘텀기간','1주수익률','유효'], pct_cols=['1주수익률'])}
    </div>
  </div>

  <script>
    const btns = Array.from(document.querySelectorAll('.tabbtn'));
    const tabs = Array.from(document.querySelectorAll('.tab'));
    btns.forEach(b => b.addEventListener('click', () => {{
      btns.forEach(x => x.classList.remove('active'));
      tabs.forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      document.getElementById(b.dataset.tab).classList.add('active');
    }}));
  </script>
{_html_table_sort_script()}
</body>
</html>"""

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
    html_path = os.path.join(script_dir, out_filename)
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✓ 대시보드 HTML 저장: {html_path}")
    if open_web:
        try:
            if sys.platform == 'win32':
                os.startfile(html_path)
            else:
                from pathlib import Path
                webbrowser.open(Path(html_path).as_uri())
        except Exception as e:
            print(f"   ⚠️ 브라우저 자동 열기 실패: {e}. 수동으로 열어주세요: {html_path}")
    return html_path


def visualize_momentum_rankings(momentum_df, momentum_df_1w_ago=None, momentum_df_5pct_drop=None,
                                momentum_df_5pct_rise=None, save_path=None, show_rank_change=True, open_web=True,
                                period_info=None, ma10_variants=None, ma20_variants=None, ohlcv_df=None,
                                backtest_result=None, dashboard_only=True):
    """
    ETF 모멘텀 순위를 표로 출력하고, 기간별 모멘텀 표를 웹(HTML)으로 출력합니다.
    맨 앞에 기간 정보와 '현재 기준 상위 20' 4개(10/20/50/120일)를 먼저 출력한 뒤, 기존 순서대로 출력합니다.
    표 컬럼: 순위, ticker, etf명, 수익률.

    출력 순서 (터미널/HTML 동일):
      1. 기간 정보 (기준일, 비거래일 시 안내)
      2. 현재 기준 상위 20 — 10일 모멘텀
      3. 현재 기준 상위 20 — 20일 모멘텀
      4. 현재 기준 상위 20 — 50일 모멘텀
      5. 현재 기준 상위 20 — 120일 모멘텀
      6. 10일 모멘텀 섹션 (현재 / 일주일 전 / 5% 하락 / 5% 상승 4표)
      7. 20일 모멘텀 섹션 (동일 4표)
      8. 50일 모멘텀 섹션 (동일 4표)
      9. 120일 모멘텀 섹션 (동일 4표)
     10. 평균 모멘텀 섹션 (동일 4표)

    Args:
        momentum_df (pandas.DataFrame): 현재 기준 모멘텀 데이터프레임
        momentum_df_1w_ago (pandas.DataFrame, optional): 일주일 전 기준 모멘텀
        momentum_df_5pct_drop (pandas.DataFrame, optional): 일주일 5% 이내 하락 ETF만 필터한 모멘텀
        momentum_df_5pct_rise (pandas.DataFrame, optional): 일주일 5% 이내 상승 ETF만 필터한 모멘텀
        ma10_variants (dict, optional): {'current','1w','5drop','5rise'} 키에 10일선 이탈 제외 모멘텀 DF
        ma20_variants (dict, optional): 동일 구조, 20일선 이탈 제외
        save_path (str, optional): 표 저장 경로 (CSV 등)
        show_rank_change (bool): 호환용
        open_web (bool): True이면 HTML 저장 후 브라우저로 열기
        period_info (str, optional): 기준일 등 기간 정보 (맨 앞에 출력)
    """
    # ETF 이름 추가
    etf_info = get_etf_list_from_db()
    if not etf_info.empty:
        momentum_df = momentum_df.merge(
            etf_info[['ticker', 'etf_name']],
            on='ticker',
            how='left'
        )
        momentum_df['etf_name'] = momentum_df['etf_name'].fillna(momentum_df['ticker'])
    else:
        momentum_df['etf_name'] = momentum_df['ticker']

    periods = [10, 20, 50, 120]
    period_cols = [f'{p}일_상승률' for p in periods]

    def to_display_table(ranked_df, value_col):
        """순위, ticker, etf명, 수익률 컬럼의 DataFrame 반환 (수익률은 문자열 % 포맷)"""
        if ranked_df.empty:
            return None
        t = ranked_df[['순위', 'ticker', 'etf_name', value_col]].copy()
        t.columns = ['순위', 'ticker', 'etf명', '수익률']
        t['수익률'] = t['수익률'].apply(lambda x: f'{x:.2f}%')
        return t

    # 일주일 전 / 5% 하락·상승 표용 데이터는 인자로 받음 (호출측에서 계산해 전달)
    momentum_df_1w = momentum_df_1w_ago if momentum_df_1w_ago is not None else None
    momentum_df_5pct = momentum_df_5pct_drop if momentum_df_5pct_drop is not None else None
    momentum_df_5pct_r = momentum_df_5pct_rise if momentum_df_5pct_rise is not None else None

    # ----- 1) 맨 처음: 기간 정보 출력 -----
    if period_info:
        print(f"\n{'='*60}\n  [기간 정보] {period_info}\n{'='*60}")

    # ----- 2) '현재 기준 상위 20'만 10일·20일·50일·120일 모멘텀 4개를 먼저 출력 -----
    section_current_only = []
    for period, col in zip(periods, period_cols):
        period_label = f'{period}일 모멘텀'
        label_cur = '현재 기준 상위 20'
        if col in momentum_df.columns:
            ranked_df = get_momentum_rankings(momentum_df, col, top_n=20)
            table_cur = to_display_table(ranked_df, col)
            section_current_only.append((f'{period_label} - {label_cur}', table_cur))
            if table_cur is not None:
                print(f"\n{'='*60}\n  {period_label} - {label_cur}\n{'='*60}")
                print(table_cur.to_string(index=False))
                print()
        else:
            section_current_only.append((f'{period_label} - {label_cur}', None))
            print(f"\n[{period_label}] 데이터 없음\n")
    sections_for_web = [('현재 기준 상위 20 (10일 / 20일 / 50일 / 120일 모멘텀)', section_current_only)]

    # ----- 2-2) '현재 기준 상위 20' — 5일·20일·50일·120일 모멘텀 4개 -----
    periods_5 = [5, 20, 50, 120]
    period_cols_5 = [f'{p}일_상승률' for p in periods_5]
    section_current_5 = []
    for period, col in zip(periods_5, period_cols_5):
        period_label = f'{period}일 모멘텀'
        label_cur = '현재 기준 상위 20'
        if col in momentum_df.columns:
            ranked_df = get_momentum_rankings(momentum_df, col, top_n=20)
            table_cur = to_display_table(ranked_df, col)
            section_current_5.append((f'{period_label} - {label_cur}', table_cur))
            if table_cur is not None:
                print(f"\n{'='*60}\n  {period_label} - {label_cur}\n{'='*60}")
                print(table_cur.to_string(index=False))
                print()
        else:
            section_current_5.append((f'{period_label} - {label_cur}', None))
            print(f"\n[{period_label}] 데이터 없음\n")
    sections_for_web.append(('현재 기준 상위 20 (5일 / 20일 / 50일 / 120일 모멘텀)', section_current_5))

    # ----- 3) 기존 순서: 각 기간별 [현재 / 일주일 전 / 5% 하락 / 5% 상승] 4개 표 -----
    for period, col in zip(periods, period_cols):
        period_label = f'{period}일 모멘텀'
        tables_in_row = []
        label_cur = '현재 기준 상위 20'
        if col in momentum_df.columns:
            ranked_df = get_momentum_rankings(momentum_df, col, top_n=20)
            table_cur = to_display_table(ranked_df, col)
            tables_in_row.append((label_cur, table_cur))
            if table_cur is not None:
                print(f"\n{'='*60}\n  {period_label} - {label_cur}\n{'='*60}")
                print(table_cur.to_string(index=False))
                print()
        else:
            tables_in_row.append((label_cur, None))
            print(f"\n[{period_label}] 데이터 없음\n")
        label_1w = '일주일 전 기준 상위 20'
        if momentum_df_1w is not None and col in momentum_df_1w.columns:
            ranked_1w = get_momentum_rankings(momentum_df_1w, col, top_n=20)
            table_1w = to_display_table(ranked_1w, col)
            tables_in_row.append((label_1w, table_1w))
        else:
            tables_in_row.append((label_1w, None))
        label_5pct = '일주일 5% 이내 하락 ETF 상위 20'
        if momentum_df_5pct is not None and col in momentum_df_5pct.columns:
            ranked_5pct = get_momentum_rankings(momentum_df_5pct, col, top_n=20)
            table_5pct = to_display_table(ranked_5pct, col)
            tables_in_row.append((label_5pct, table_5pct))
        else:
            tables_in_row.append((label_5pct, None))
        label_5pct_r = '일주일 5% 이내 상승 ETF 상위 20'
        if momentum_df_5pct_r is not None and col in momentum_df_5pct_r.columns:
            ranked_5pct_r = get_momentum_rankings(momentum_df_5pct_r, col, top_n=20)
            table_5pct_r = to_display_table(ranked_5pct_r, col)
            tables_in_row.append((label_5pct_r, table_5pct_r))
        else:
            tables_in_row.append((label_5pct_r, None))
        sections_for_web.append((period_label, tables_in_row))

    # 평균 모멘텀
    avg_title = '평균 모멘텀 (5일·10일·20일·50일 각 25%)'
    tables_avg = []
    for label, mdf in [('현재 기준 상위 20', momentum_df), ('일주일 전 기준 상위 20', momentum_df_1w),
                       ('일주일 5% 이내 하락 ETF 상위 20', momentum_df_5pct), ('일주일 5% 이내 상승 ETF 상위 20', momentum_df_5pct_r)]:
        if mdf is not None and '평균_모멘텀' in mdf.columns:
            ranked_avg = get_momentum_rankings(mdf, '평균_모멘텀', top_n=20)
            tables_avg.append((label, to_display_table(ranked_avg, '평균_모멘텀')))
        else:
            tables_avg.append((label, None))
    sections_for_web.append((avg_title, tables_avg))
    if momentum_df is not None and '평균_모멘텀' in momentum_df.columns:
        ranked_avg = get_momentum_rankings(momentum_df, '평균_모멘텀', top_n=20)
        table_avg = to_display_table(ranked_avg, '평균_모멘텀')
        if table_avg is not None:
            print(f"\n{'='*60}\n  {avg_title} - 현재 기준\n{'='*60}")
            print(table_avg.to_string(index=False))
            print()
    else:
        print("\n[평균 모멘텀] 데이터 없음\n")

    # 평균 모멘텀_2 (5일 40%, 10일 30%, 20일 20%, 50일 10%)
    avg_title_2 = '평균 모멘텀_2 (5일 40%, 10일 30%, 20일 20%, 50일 10%)'
    tables_avg_2 = []
    for label, mdf in [('현재 기준 상위 20', momentum_df), ('일주일 전 기준 상위 20', momentum_df_1w),
                      ('일주일 5% 이내 하락 ETF 상위 20', momentum_df_5pct), ('일주일 5% 이내 상승 ETF 상위 20', momentum_df_5pct_r)]:
        if mdf is not None and '평균_모멘텀_2' in mdf.columns:
            ranked_avg = get_momentum_rankings(mdf, '평균_모멘텀_2', top_n=20)
            tables_avg_2.append((label, to_display_table(ranked_avg, '평균_모멘텀_2')))
        else:
            tables_avg_2.append((label, None))
    sections_for_web.append((avg_title_2, tables_avg_2))
    if momentum_df is not None and '평균_모멘텀_2' in momentum_df.columns:
        ranked_avg_2 = get_momentum_rankings(momentum_df, '평균_모멘텀_2', top_n=20)
        table_avg_2 = to_display_table(ranked_avg_2, '평균_모멘텀_2')
        if table_avg_2 is not None:
            print(f"\n{'='*60}\n  {avg_title_2} - 현재 기준\n{'='*60}")
            print(table_avg_2.to_string(index=False))
            print()
    else:
        print("\n[평균 모멘텀_2 (5일 40%, 10일 30%, 20일 20%, 50일 10%)] 데이터 없음\n")

    def _add_ma_filtered_output(variant_dict, banner_title):
        """종가 ≥ N일 SMA인 종목만 남긴 모멘텀: 콘솔 출력 및 sections_for_web에 동일 구조로 추가."""
        if variant_dict is None:
            return
        m_cur = variant_dict.get('current')
        if m_cur is None or m_cur.empty:
            return
        m_1w = variant_dict.get('1w')
        m_5d = variant_dict.get('5drop')
        m_5r = variant_dict.get('5rise')
        prefix = banner_title + ' — '
        print(f"\n{'#'*60}\n  {banner_title}\n{'#'*60}")

        section_ma_cur = []
        for period, col in zip(periods, period_cols):
            period_label = f'{period}일 모멘텀'
            label_cur = '현재 기준 상위 20'
            if col in m_cur.columns:
                ranked_df = get_momentum_rankings(m_cur, col, top_n=20)
                table_cur = to_display_table(ranked_df, col)
            else:
                table_cur = None
            section_ma_cur.append((f'{period_label} - {label_cur}', table_cur))
            if table_cur is not None:
                print(f"\n{'='*60}\n  [{banner_title}] {period_label} - {label_cur}\n{'='*60}")
                print(table_cur.to_string(index=False))
                print()
        sections_for_web.append((f'{prefix}현재 기준 상위 20 (10일 / 20일 / 50일 / 120일 모멘텀)', section_ma_cur))

        section_ma_5 = []
        for period, col in zip(periods_5, period_cols_5):
            period_label = f'{period}일 모멘텀'
            label_cur = '현재 기준 상위 20'
            if col in m_cur.columns:
                ranked_df = get_momentum_rankings(m_cur, col, top_n=20)
                table_cur = to_display_table(ranked_df, col)
            else:
                table_cur = None
            section_ma_5.append((f'{period_label} - {label_cur}', table_cur))
            if table_cur is not None:
                print(f"\n{'='*60}\n  [{banner_title}] {period_label} - {label_cur}\n{'='*60}")
                print(table_cur.to_string(index=False))
                print()
        sections_for_web.append((f'{prefix}현재 기준 상위 20 (5일 / 20일 / 50일 / 120일 모멘텀)', section_ma_5))

        for period, col in zip(periods, period_cols):
            period_label = f'{period}일 모멘텀'
            tables_in_row = []
            label_cur = '현재 기준 상위 20'
            if col in m_cur.columns:
                ranked_df = get_momentum_rankings(m_cur, col, top_n=20)
                table_cur = to_display_table(ranked_df, col)
                tables_in_row.append((label_cur, table_cur))
                if table_cur is not None:
                    print(f"\n{'='*60}\n  [{banner_title}] {period_label} - {label_cur}\n{'='*60}")
                    print(table_cur.to_string(index=False))
                    print()
            else:
                tables_in_row.append((label_cur, None))
            label_1w = '일주일 전 기준 상위 20'
            if m_1w is not None and col in m_1w.columns:
                ranked_1w = get_momentum_rankings(m_1w, col, top_n=20)
                table_1w = to_display_table(ranked_1w, col)
                tables_in_row.append((label_1w, table_1w))
            else:
                tables_in_row.append((label_1w, None))
            label_5pct = '일주일 5% 이내 하락 ETF 상위 20'
            if m_5d is not None and col in m_5d.columns:
                ranked_5pct = get_momentum_rankings(m_5d, col, top_n=20)
                table_5pct = to_display_table(ranked_5pct, col)
                tables_in_row.append((label_5pct, table_5pct))
            else:
                tables_in_row.append((label_5pct, None))
            label_5pct_r = '일주일 5% 이내 상승 ETF 상위 20'
            if m_5r is not None and col in m_5r.columns:
                ranked_5pct_r = get_momentum_rankings(m_5r, col, top_n=20)
                table_5pct_r = to_display_table(ranked_5pct_r, col)
                tables_in_row.append((label_5pct_r, table_5pct_r))
            else:
                tables_in_row.append((label_5pct_r, None))
            sections_for_web.append((f'{prefix}{period_label}', tables_in_row))

        tables_avg_m = []
        for label, mdf in [('현재 기준 상위 20', m_cur), ('일주일 전 기준 상위 20', m_1w),
                           ('일주일 5% 이내 하락 ETF 상위 20', m_5d), ('일주일 5% 이내 상승 ETF 상위 20', m_5r)]:
            if mdf is not None and '평균_모멘텀' in mdf.columns:
                ranked_avg = get_momentum_rankings(mdf, '평균_모멘텀', top_n=20)
                tables_avg_m.append((label, to_display_table(ranked_avg, '평균_모멘텀')))
            else:
                tables_avg_m.append((label, None))
        sections_for_web.append((f'{prefix}{avg_title}', tables_avg_m))
        if m_cur is not None and '평균_모멘텀' in m_cur.columns:
            ranked_avg = get_momentum_rankings(m_cur, '평균_모멘텀', top_n=20)
            table_avg = to_display_table(ranked_avg, '평균_모멘텀')
            if table_avg is not None:
                print(f"\n{'='*60}\n  [{banner_title}] {avg_title} - 현재 기준\n{'='*60}")
                print(table_avg.to_string(index=False))
                print()

        tables_avg_m2 = []
        for label, mdf in [('현재 기준 상위 20', m_cur), ('일주일 전 기준 상위 20', m_1w),
                           ('일주일 5% 이내 하락 ETF 상위 20', m_5d), ('일주일 5% 이내 상승 ETF 상위 20', m_5r)]:
            if mdf is not None and '평균_모멘텀_2' in mdf.columns:
                ranked_avg = get_momentum_rankings(mdf, '평균_모멘텀_2', top_n=20)
                tables_avg_m2.append((label, to_display_table(ranked_avg, '평균_모멘텀_2')))
            else:
                tables_avg_m2.append((label, None))
        sections_for_web.append((f'{prefix}{avg_title_2}', tables_avg_m2))
        if m_cur is not None and '평균_모멘텀_2' in m_cur.columns:
            ranked_avg_2 = get_momentum_rankings(m_cur, '평균_모멘텀_2', top_n=20)
            table_avg_2 = to_display_table(ranked_avg_2, '평균_모멘텀_2')
            if table_avg_2 is not None:
                print(f"\n{'='*60}\n  [{banner_title}] {avg_title_2} - 현재 기준\n{'='*60}")
                print(table_avg_2.to_string(index=False))
                print()

    _add_ma_filtered_output(ma10_variants, '10일선 이탈 종목 제외')
    _add_ma_filtered_output(ma20_variants, '20일선 이탈 종목 제외')

    # 대시보드 1페이지 HTML
    _save_etf_dashboard_html(
        current_momentum_df=momentum_df,
        prev_momentum_df=momentum_df_1w,
        ohlcv_df=ohlcv_df,
        backtest_result=backtest_result,
        period_info=period_info,
        open_web=open_web,
        out_filename='etf_dashboard.html',
    )

    # 필요 시 기존의 상세 표 HTML도 유지(호환)
    if not dashboard_only:
        html_content = _momentum_tables_to_html(sections_for_web, period_info=period_info)
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            script_dir = os.getcwd()
        html_path = os.path.join(script_dir, 'etf_momentum_rankings.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"✓ 웹 표 저장: {html_path}")
        if open_web:
            try:
                if sys.platform == 'win32':
                    os.startfile(html_path)
                else:
                    from pathlib import Path
                    webbrowser.open(Path(html_path).as_uri())
            except Exception as e:
                print(f"   ⚠️ 브라우저 자동 열기 실패: {e}. 수동으로 열어주세요: {html_path}")

    # save_path가 있으면 평균 모멘텀 표를 CSV로 저장
    if save_path:
        base, ext = os.path.splitext(save_path)
        if ext.lower() != '.csv':
            save_path = base + '.csv'
        if '평균_모멘텀' in momentum_df.columns:
            ranked_avg = get_momentum_rankings(momentum_df, '평균_모멘텀', top_n=20)
            if not ranked_avg.empty:
                out = ranked_avg[['순위', 'etf_name', '평균_모멘텀']].copy()
                out.columns = ['순위', 'etf명', '수익률']
                out.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"✓ 표 저장 완료: {save_path}")


def print_momentum_rankings_table(momentum_df, ohlcv_df=None):
    """
    ETF 모멘텀 순위를 표로 출력합니다.
    
    Args:
        momentum_df (pandas.DataFrame): 모멘텀 데이터프레임
        ohlcv_df (pandas.DataFrame, optional): 있으면 10일·20일선 이탈(종가 < SMA) 종목을 뺀 순위도 추가 출력
    """
    # 복사본 생성 (원본 수정 방지)
    df = momentum_df.copy()
    
    # ETF 이름이 없으면 추가
    if 'etf_name' not in df.columns:
        etf_info = get_etf_list_from_db()
        if not etf_info.empty:
            df = df.merge(
                etf_info[['ticker', 'etf_name']],
                on='ticker',
                how='left'
            )
            df['etf_name'] = df['etf_name'].fillna(df['ticker'])
        else:
            df['etf_name'] = df['ticker']
    
    periods = [10, 20, 50, 120]
    period_cols = [f'{p}일_상승률' for p in periods]
    
    print("\n" + "=" * 100)
    print("ETF 모멘텀 순위 분석")
    print("=" * 100)
    
    # 각 기간별 상위 20개 표 출력
    for period, col in zip(periods, period_cols):
        if col not in df.columns:
            continue
        
        ranked_df = get_momentum_rankings(df, col, top_n=20)
        
        if ranked_df.empty:
            print(f"\n{period}일 모멘텀 상위 20개: 데이터 없음")
            continue
        
        print(f"\n{period}일 모멘텀 상위 20개:")
        print("-" * 100)
        print(f"{'순위':<6} {'종목코드':<10} {'ETF명':<30} {'상승률(%)':<15}")
        print("-" * 100)
        
        for idx, row in ranked_df.head(20).iterrows():
            # etf_name이 없을 경우 ticker 사용
            etf_name = row.get('etf_name', row.get('ticker', ''))
            print(f"{row['순위']:<6} {row['ticker']:<10} {etf_name:<30} {row[col]:>10.2f}%")
    
    # 평균 모멘텀 상위 20개 표 출력
    if '평균_모멘텀' in df.columns:
        ranked_avg = get_momentum_rankings(df, '평균_모멘텀', top_n=20)
        
        if not ranked_avg.empty:
            print(f"\n평균 모멘텀 상위 20개 (5일·10일·20일·50일 각 25%):")
            print("-" * 100)
            print(f"{'순위':<6} {'종목코드':<10} {'ETF명':<30} {'평균모멘텀(%)':<15} {'5일':<10} {'10일':<10} {'20일':<10} {'50일':<10}")
            print("-" * 100)
            
            for idx, row in ranked_avg.head(20).iterrows():
                # etf_name이 없을 경우 ticker 사용
                etf_name = row.get('etf_name', row.get('ticker', ''))
                print(f"{row['순위']:<6} {row['ticker']:<10} {etf_name:<30} "
                      f"{row['평균_모멘텀']:>10.2f}% "
                      f"{row.get('5일_상승률', 0):>8.2f}% "
                      f"{row.get('10일_상승률', 0):>8.2f}% "
                      f"{row.get('20일_상승률', 0):>8.2f}% "
                      f"{row.get('50일_상승률', 0):>8.2f}%")

    if ohlcv_df is not None and not ohlcv_df.empty:
        for ma_win, subtitle in [(10, '10일선 이탈 종목 제외 (종가 ≥ 10일 SMA)'), (20, '20일선 이탈 종목 제외 (종가 ≥ 20일 SMA)')]:
            df_ma = filter_momentum_above_sma(ohlcv_df, df, ma_win)
            print("\n" + "=" * 100)
            print(subtitle)
            print("=" * 100)
            for period, col in zip(periods, period_cols):
                if col not in df_ma.columns:
                    continue
                ranked_df = get_momentum_rankings(df_ma, col, top_n=20)
                if ranked_df.empty:
                    print(f"\n{period}일 모멘텀 상위 20개: 데이터 없음")
                    continue
                print(f"\n{period}일 모멘텀 상위 20개:")
                print("-" * 100)
                print(f"{'순위':<6} {'종목코드':<10} {'ETF명':<30} {'상승률(%)':<15}")
                print("-" * 100)
                for idx, row in ranked_df.head(20).iterrows():
                    etf_name = row.get('etf_name', row.get('ticker', ''))
                    print(f"{row['순위']:<6} {row['ticker']:<10} {etf_name:<30} {row[col]:>10.2f}%")
            if '평균_모멘텀' in df_ma.columns:
                ranked_avg = get_momentum_rankings(df_ma, '평균_모멘텀', top_n=20)
                if not ranked_avg.empty:
                    print(f"\n평균 모멘텀 상위 20개 (5일·10일·20일·50일 각 25%):")
                    print("-" * 100)
                    print(f"{'순위':<6} {'종목코드':<10} {'ETF명':<30} {'평균모멘텀(%)':<15} {'5일':<10} {'10일':<10} {'20일':<10} {'50일':<10}")
                    print("-" * 100)
                    for idx, row in ranked_avg.head(20).iterrows():
                        etf_name = row.get('etf_name', row.get('ticker', ''))
                        print(f"{row['순위']:<6} {row['ticker']:<10} {etf_name:<30} "
                              f"{row['평균_모멘텀']:>10.2f}% "
                              f"{row.get('5일_상승률', 0):>8.2f}% "
                              f"{row.get('10일_상승률', 0):>8.2f}% "
                              f"{row.get('20일_상승률', 0):>8.2f}% "
                              f"{row.get('50일_상승률', 0):>8.2f}%")
    
    print("\n" + "=" * 100)


def analyze_etf_momentum(end_date=None, visualize=True, save_path=None):
    """
    모든 ETF의 모멘텀을 분석하고 순위를 출력합니다.
    
    Args:
        end_date (str, optional): 기준일 (YYYY-MM-DD 형식). None이면 오늘
        visualize (bool): 그래프 표시 여부
        save_path (str, optional): 그래프 저장 경로
    """
    print("ETF 모멘텀 분석 시작...")
    
    # OHLCV 데이터 가져오기
    print("1. ETF OHLCV 데이터 조회 중...")
    ohlcv_df = get_etf_ohlcv_from_db(end_date=end_date)
    
    if ohlcv_df.empty:
        print("⚠️ OHLCV 데이터가 없습니다.")
        return

    # 기준일(거래일) 보정: 요청일이 비거래일이면 데이터 상 마지막 거래일 기준으로 산출
    ohlcv_dates = pd.to_datetime(ohlcv_df['date'])
    actual_end_date = ohlcv_dates.max()
    if hasattr(actual_end_date, 'date'):
        actual_end_date = actual_end_date.date()
    elif isinstance(actual_end_date, pd.Timestamp):
        actual_end_date = actual_end_date.date()
    if end_date is not None:
        requested_end = datetime.strptime(str(end_date), '%Y-%m-%d').date() if isinstance(end_date, str) else (end_date if isinstance(end_date, date) else date.today())
    else:
        requested_end = date.today()
    period_info = f"기준일(거래일): {actual_end_date}"
    if requested_end != actual_end_date:
        period_info += f" (요청일 {requested_end}은 비거래일이므로 해당일 기준으로 산출)"

    unique_tickers = ohlcv_df['ticker'].unique()
    print(f"   ✓ {len(unique_tickers)}개 ETF의 데이터 조회 완료")
    print(f"   - 총 레코드 수: {len(ohlcv_df)}개")
    
    # 데이터 샘플 확인
    if len(ohlcv_df) > 0:
        sample_ticker = unique_tickers[0]
        sample_data = ohlcv_df[ohlcv_df['ticker'] == sample_ticker]
        print(f"   - 샘플 ETF ({sample_ticker}) 데이터 개수: {len(sample_data)}개")
        if len(sample_data) > 0:
            print(f"   - 샘플 날짜 범위: {sample_data['date'].min()} ~ {sample_data['date'].max()}")
    
    # 모멘텀 계산
    print("2. 모멘텀 계산 중...")
    momentum_df = calculate_momentum_returns(
        ohlcv_df,
        periods=[5, 10, 20, 50, 120],
        min_trading_days=MOMENTUM_DASHBOARD_MIN_TRADING_DAYS,
    )
    
    if momentum_df.empty:
        print("⚠️ 모멘텀 계산 결과가 없습니다.")
        # 추가 디버깅 정보
        print(f"   - 입력 데이터: {len(ohlcv_df)}개 레코드, {len(unique_tickers)}개 ETF")
        if len(ohlcv_df) > 0:
            # 각 ETF별 데이터 개수 확인
            ticker_counts = ohlcv_df.groupby('ticker').size()
            print(f"   - ETF별 데이터 개수 통계:")
            print(f"     * 최소: {ticker_counts.min()}개")
            print(f"     * 최대: {ticker_counts.max()}개")
            print(f"     * 평균: {ticker_counts.mean():.1f}개")
            print(f"     * {MOMENTUM_DASHBOARD_MIN_TRADING_DAYS}거래일 이상 데이터를 가진 ETF: {(ticker_counts >= MOMENTUM_DASHBOARD_MIN_TRADING_DAYS).sum()}개")
            print(f"     * 120일 이상 데이터를 가진 ETF: {(ticker_counts >= 120).sum()}개")
            print(f"     * 50일 이상 데이터를 가진 ETF: {(ticker_counts >= 50).sum()}개")
            print(f"     * 10일 이상 데이터를 가진 ETF: {(ticker_counts >= 10).sum()}개")
        return
    
    print(f"   ✓ {len(momentum_df)}개 ETF의 모멘텀 계산 완료")
    
    # 각 기간별 데이터 확인
    if not momentum_df.empty:
        periods = [5, 10, 20, 50, 120]
        print(f"   - 모멘텀 데이터 통계:")
        for period in periods:
            col = f'{period}일_상승률'
            if col in momentum_df.columns:
                valid_count = momentum_df[col].notna().sum()
                print(f"     * {period}일 모멘텀: {valid_count}개 ETF ({valid_count/len(momentum_df)*100:.1f}%)")
            for _suf, _lag_n in MOMENTUM_DEFERRED_LAGS:
                col_d = f'{period}일_상승률{_suf}'
                if col_d in momentum_df.columns:
                    vcd = momentum_df[col_d].notna().sum()
                    _lag_disp = {'_T5': 'T-5', '_T3': 'T-3'}.get(_suf, _suf)
                    print(f"     * {period}일 모멘텀({_lag_disp}): {vcd}개 ETF ({vcd/len(momentum_df)*100:.1f}%)")
    
    # 평균 모멘텀 계산
    print("3. 가중 평균 모멘텀 계산 중...")
    momentum_df = calculate_weighted_average_momentum(momentum_df)

    # 전주(5거래일 전) 기준 모멘텀 (비교용)
    end_date_1w = _prev_trading_date(actual_end_date, n_trading_days=5)
    end_date_1w_str = end_date_1w.strftime('%Y-%m-%d')
    momentum_df_1w_ago = None
    ohlcv_1w = get_etf_ohlcv_from_db(end_date=end_date_1w_str)
    if not ohlcv_1w.empty:
        momentum_df_1w_ago = calculate_momentum_returns(
            ohlcv_1w,
            periods=[5, 10, 20, 50, 120],
            min_trading_days=MOMENTUM_DASHBOARD_MIN_TRADING_DAYS,
        )
        if not momentum_df_1w_ago.empty:
            momentum_df_1w_ago = calculate_weighted_average_momentum(momentum_df_1w_ago)
            etf_info = get_etf_list_from_db()
            if not etf_info.empty:
                momentum_df_1w_ago = momentum_df_1w_ago.merge(
                    etf_info[['ticker', 'etf_name']], on='ticker', how='left'
                )
                momentum_df_1w_ago['etf_name'] = momentum_df_1w_ago['etf_name'].fillna(momentum_df_1w_ago['ticker'])
            else:
                momentum_df_1w_ago['etf_name'] = momentum_df_1w_ago['ticker']
            print(f"   ✓ 일주일 전({end_date_1w_str}) 기준 모멘텀 계산 완료")

    # 일주일간 5% 이내 하락 / 5% 이내 상승 ETF만 필터한 모멘텀 (현재 기준)
    momentum_df_5pct_drop = None
    momentum_df_5pct_rise = None
    if not momentum_df.empty and 'latest_close' in momentum_df.columns and not ohlcv_1w.empty:
        current_close = momentum_df.set_index('ticker')['latest_close']
        ohlcv_1w_sorted = ohlcv_1w.sort_values(['ticker', 'date'])
        last_1w = ohlcv_1w_sorted.groupby('ticker').last()['close'].astype(float)
        week_ret = (current_close.reindex(last_1w.index) - last_1w) / last_1w * 100
        tickers_5pct = week_ret[(week_ret >= -5) & (week_ret <= 0)].index.tolist()
        tickers_5pct_rise = week_ret[(week_ret >= 0) & (week_ret <= 5)].index.tolist()
        if tickers_5pct:
            momentum_df_5pct_drop = momentum_df[momentum_df['ticker'].isin(tickers_5pct)].copy()
            if not momentum_df_5pct_drop.empty:
                momentum_df_5pct_drop = calculate_weighted_average_momentum(momentum_df_5pct_drop)
                if 'etf_name' not in momentum_df_5pct_drop.columns:
                    etf_info = get_etf_list_from_db()
                    if not etf_info.empty:
                        momentum_df_5pct_drop = momentum_df_5pct_drop.merge(
                            etf_info[['ticker', 'etf_name']], on='ticker', how='left'
                        )
                        momentum_df_5pct_drop['etf_name'] = momentum_df_5pct_drop['etf_name'].fillna(momentum_df_5pct_drop['ticker'])
                    else:
                        momentum_df_5pct_drop['etf_name'] = momentum_df_5pct_drop['ticker']
                print(f"   ✓ 일주일 5% 이내 하락 ETF {len(tickers_5pct)}개 대상 모멘텀 계산 완료")
        if tickers_5pct_rise:
            momentum_df_5pct_rise = momentum_df[momentum_df['ticker'].isin(tickers_5pct_rise)].copy()
            if not momentum_df_5pct_rise.empty:
                momentum_df_5pct_rise = calculate_weighted_average_momentum(momentum_df_5pct_rise)
                if 'etf_name' not in momentum_df_5pct_rise.columns:
                    etf_info = get_etf_list_from_db()
                    if not etf_info.empty:
                        momentum_df_5pct_rise = momentum_df_5pct_rise.merge(
                            etf_info[['ticker', 'etf_name']], on='ticker', how='left'
                        )
                        momentum_df_5pct_rise['etf_name'] = momentum_df_5pct_rise['etf_name'].fillna(momentum_df_5pct_rise['ticker'])
                    else:
                        momentum_df_5pct_rise['etf_name'] = momentum_df_5pct_rise['ticker']
                print(f"   ✓ 일주일 5% 이내 상승 ETF {len(tickers_5pct_rise)}개 대상 모멘텀 계산 완료")

    ma10_variants = {
        'current': filter_momentum_above_sma(ohlcv_df, momentum_df, 10),
        '1w': filter_momentum_above_sma(ohlcv_1w, momentum_df_1w_ago, 10) if momentum_df_1w_ago is not None else None,
        '5drop': filter_momentum_above_sma(ohlcv_df, momentum_df_5pct_drop, 10) if momentum_df_5pct_drop is not None else None,
        '5rise': filter_momentum_above_sma(ohlcv_df, momentum_df_5pct_rise, 10) if momentum_df_5pct_rise is not None else None,
    }
    ma20_variants = {
        'current': filter_momentum_above_sma(ohlcv_df, momentum_df, 20),
        '1w': filter_momentum_above_sma(ohlcv_1w, momentum_df_1w_ago, 20) if momentum_df_1w_ago is not None else None,
        '5drop': filter_momentum_above_sma(ohlcv_df, momentum_df_5pct_drop, 20) if momentum_df_5pct_drop is not None else None,
        '5rise': filter_momentum_above_sma(ohlcv_df, momentum_df_5pct_rise, 20) if momentum_df_5pct_rise is not None else None,
    }
    
    # 순위 표 출력 (터미널)
    print("4. 모멘텀 순위 표 생성 중...")
    print_momentum_rankings_table(momentum_df, ohlcv_df=ohlcv_df)

    # 백테스트 실행 후, 대시보드 1페이지로 통합 출력
    print("\n" + "=" * 60)
    print("5. 모멘텀 포트폴리오 백테스트 실행...")
    backtest_result = backtest_momentum_portfolio(
        end_date=end_date,
        top_n=5,
        min_trading_days=MOMENTUM_DASHBOARD_MIN_TRADING_DAYS,
    )

    if visualize:
        visualize_momentum_rankings(
            momentum_df,
            momentum_df_1w_ago=momentum_df_1w_ago,
            momentum_df_5pct_drop=momentum_df_5pct_drop,
            momentum_df_5pct_rise=momentum_df_5pct_rise,
            open_web=True,
            period_info=period_info,
            ma10_variants=ma10_variants,
            ma20_variants=ma20_variants,
            ohlcv_df=ohlcv_df,
            backtest_result=backtest_result,
            dashboard_only=True,
        )
        print("6. 대시보드(etf_dashboard.html)로 통합 출력했습니다.")

    # (옵션) 기존 백테스트 단독 HTML은 기본 비활성화
    # if backtest_result and visualize:
    #     _save_backtest_result_html(backtest_result, open_web=True)
    
    return momentum_df


def _save_backtest_result_html(backtest_result, open_web=True):
    """백테스트 결과를 HTML 파일로 저장하고 optionally 브라우저로 연다. 각 포트폴리오 종목(티커, 종목명, 수익률) 포함."""
    if not backtest_result:
        return
    purchase_date = backtest_result.get('purchase_date')
    evaluation_date = backtest_result.get('evaluation_date')
    results = backtest_result.get('results', {})
    results_5pct = backtest_result.get('results_5pct', {})
    results_5pct_rise = backtest_result.get('results_5pct_rise', {})
    results_ma10 = backtest_result.get('results_ma10', {})
    results_ma20 = backtest_result.get('results_ma20', {})
    results_ma10_5pct = backtest_result.get('results_ma10_5pct', {})
    results_ma20_5pct = backtest_result.get('results_ma20_5pct', {})
    results_ma10_5pct_rise = backtest_result.get('results_ma10_5pct_rise', {})
    results_ma20_5pct_rise = backtest_result.get('results_ma20_5pct_rise', {})
    purchase_str = purchase_date.strftime('%Y-%m-%d') if hasattr(purchase_date, 'strftime') else str(purchase_date)
    eval_str = evaluation_date.strftime('%Y-%m-%d') if hasattr(evaluation_date, 'strftime') else str(evaluation_date)

    def portfolio_table(portfolio_list, strategy_name, period):
        if not portfolio_list:
            return ""
        rows = []
        for p in portfolio_list:
            ticker = p.get('ticker', '')
            name = (p.get('etf_name') or ticker).replace('<', '&lt;').replace('>', '&gt;').replace('&', '&amp;')
            ret = p.get('return_pct', np.nan)
            ret_str = f"{ret:.2f}%" if pd.notna(ret) else "N/A"
            rows.append(f"<tr><td>{ticker}</td><td>{name}</td><td class=\"num\">{ret_str}</td></tr>")
        return "<table class=\"port-table\"><thead><tr><th>ticker</th><th>종목명</th><th>수익률</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"

    strategies = [
        ('일주일 전 모멘텀 상위 N', results),
        ('5% 하락 ETF 모멘텀 상위 N', results_5pct),
        ('5% 상승 ETF 모멘텀 상위 N', results_5pct_rise),
        ('MA10 이탈 제외 모멘텀 상위 N', results_ma10),
        ('MA20 이탈 제외 모멘텀 상위 N', results_ma20),
        ('MA10 제외·5% 하락 모멘텀 상위 N', results_ma10_5pct),
        ('MA20 제외·5% 하락 모멘텀 상위 N', results_ma20_5pct),
        ('MA10 제외·5% 상승 모멘텀 상위 N', results_ma10_5pct_rise),
        ('MA20 제외·5% 상승 모멘텀 상위 N', results_ma20_5pct_rise),
    ]
    summary_rows = []
    for strat_label, res in strategies:
        for period in [10, 20, 50, 120]:
            if period not in res:
                continue
            r = res[period]
            avg = f"{r['avg_return']:.2f}%" if pd.notna(r['avg_return']) else 'N/A'
            summary_rows.append(
                f"<tr><td>{strat_label}</td><td>{period}일</td><td>{avg}</td><td>{r['valid_count']}/{r['total_count']}</td></tr>")

    table_body = '\n'.join(summary_rows)

    def _overall_line(key, label):
        v = backtest_result.get(key, np.nan)
        s = f"{v:.2f}%" if pd.notna(v) else "N/A"
        return f"{label}: {s}"

    footer_bits = [
        _overall_line('overall_avg_return', '일주일 전 모멘텀 전체 평균'),
        _overall_line('overall_avg_return_5pct', '5% 하락 ETF 전체 평균'),
        _overall_line('overall_avg_return_5pct_rise', '5% 상승 ETF 전체 평균'),
        _overall_line('overall_avg_return_ma10', 'MA10 이탈 제외 전체 평균'),
        _overall_line('overall_avg_return_ma20', 'MA20 이탈 제외 전체 평균'),
        _overall_line('overall_avg_return_ma10_5pct', 'MA10 제외·5% 하락 전체 평균'),
        _overall_line('overall_avg_return_ma20_5pct', 'MA20 제외·5% 하락 전체 평균'),
        _overall_line('overall_avg_return_ma10_5pct_rise', 'MA10 제외·5% 상승 전체 평균'),
        _overall_line('overall_avg_return_ma20_5pct_rise', 'MA20 제외·5% 상승 전체 평균'),
    ]
    footer_html = " · ".join(footer_bits)

    detail_parts = []
    for strategy_name, res in strategies:
        for period in [10, 20, 50, 120]:
            if period not in res:
                continue
            r = res[period]
            pl = r.get('portfolio', [])
            tbl = portfolio_table(pl, strategy_name, period)
            if tbl:
                detail_parts.append(f"<div class=\"port-section\"><h3>{strategy_name} · {period}일</h3>{tbl}</div>")
    detail_html = "\n".join(detail_parts)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETF 모멘텀 백테스트 결과</title>
    <style>
        body {{ font-family: 'Malgun Gothic', sans-serif; margin: 24px; background: #f5f6fa; color: #2c3e50; }}
        h1 {{ font-size: 1.4rem; color: #1a202c; }}
        h3 {{ font-size: 1rem; color: #2d3748; margin: 20px 0 8px 0; }}
        .meta {{ margin: 16px 0; color: #4a5568; }}
        table {{ border-collapse: collapse; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-radius: 8px; overflow: hidden; margin-bottom: 8px; }}
        th, td {{ padding: 10px 14px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
        th {{ background: #4299e1; color: #fff; }}
        tr:nth-child(even) {{ background: #f7fafc; }}
        td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .port-table {{ max-width: 480px; }}
        .port-section {{ margin-bottom: 24px; }}
        .footer {{ margin-top: 20px; font-weight: 600; }}
    </style>
</head>
<body>
    <h1>ETF 모멘텀 포트폴리오 백테스트 결과</h1>
    <div class="meta">매수일: {purchase_str} · 평가일: {eval_str} · 보유기간: 5거래일</div>
    <table>
        <thead><tr><th>구분</th><th>모멘텀 기간</th><th>포트폴리오 수익률</th><th>유효 종목 수</th></tr></thead>
        <tbody>
            {table_body}
        </tbody>
    </table>
    <div class="footer">{footer_html}</div>
    <h2 style="margin-top:32px; font-size:1.1rem;">포트폴리오 종목 상세 (티커 · 종목명 · 수익률)</h2>
    {detail_html}
</body>
</html>"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
    html_path = os.path.join(script_dir, 'etf_backtest_result.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✓ 백테스트 결과 웹 저장: {html_path}")
    if open_web:
        try:
            if sys.platform == 'win32':
                os.startfile(html_path)
            else:
                from pathlib import Path
                webbrowser.open(Path(html_path).as_uri())
        except Exception as e:
            print(f"   ⚠️ 브라우저 자동 열기 실패: {e}. 수동으로 열어주세요: {html_path}")


def backtest_momentum_portfolio(end_date=None, top_n=5, min_trading_days=None):
    """
    일주일 전의 모멘텀 상위 N개 포트폴리오를 동일 비중으로 매수했을 때
    현재까지의 수익률을 백테스트합니다. 추가로 일주일 5% 이내 하락 ETF만 대상으로 한 모멘텀 상위 N도 백테스트합니다.
    
    Args:
        end_date (str, optional): 기준일 (YYYY-MM-DD 형식). None이면 오늘
        top_n (int): 각 모멘텀 기간별 상위 N개 (기본값: 5)
        min_trading_days (int, optional): calculate_momentum_returns에 전달( None이면 전역 MIN_OHLCV 기준 )
    
    Returns:
        dict: 백테스트 결과
    """
    print("=" * 60)
    print("모멘텀 포트폴리오 백테스트 시작")
    print("=" * 60)
    
    # 날짜 설정
    if end_date is None:
        end_date = date.today()
    else:
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    # 5거래일 전 매수일 계산
    purchase_date = _prev_trading_date(end_date, n_trading_days=5)
    
    print(f"\n📅 백테스트 기간:")
    print(f"   - 매수일: {purchase_date.strftime('%Y-%m-%d')}")
    print(f"   - 평가일: {end_date.strftime('%Y-%m-%d')}")
    print(f"   - 보유기간: 5거래일")
    
    # 1. 일주일 전의 모멘텀 데이터 가져오기
    print(f"\n1. {purchase_date.strftime('%Y-%m-%d')} 기준 모멘텀 데이터 조회 중...")
    ohlcv_df_past = get_etf_ohlcv_from_db(end_date=purchase_date.strftime('%Y-%m-%d'))
    
    if ohlcv_df_past.empty:
        print("⚠️ 일주일 전 OHLCV 데이터가 없습니다.")
        return None
    
    # 모멘텀 계산
    momentum_df_past = calculate_momentum_returns(
        ohlcv_df_past, periods=[10, 20, 50, 120], min_trading_days=min_trading_days
    )
    
    if momentum_df_past.empty:
        print("⚠️ 일주일 전 모멘텀 데이터가 없습니다.")
        return None
    
    print(f"   ✓ {len(momentum_df_past)}개 ETF의 모멘텀 데이터 조회 완료")
    
    # ETF 이름 추가
    if 'etf_name' not in momentum_df_past.columns:
        etf_info = get_etf_list_from_db()
        if not etf_info.empty:
            momentum_df_past = momentum_df_past.merge(
                etf_info[['ticker', 'etf_name']],
                on='ticker',
                how='left'
            )
            momentum_df_past['etf_name'] = momentum_df_past['etf_name'].fillna(momentum_df_past['ticker'])
        else:
            momentum_df_past['etf_name'] = momentum_df_past['ticker']
    
    # 2. 현재 가격 데이터 가져오기
    print(f"\n2. {end_date.strftime('%Y-%m-%d')} 기준 현재가 데이터 조회 중...")
    ohlcv_df_current = get_etf_ohlcv_from_db(end_date=end_date.strftime('%Y-%m-%d'))
    
    if ohlcv_df_current.empty:
        print("⚠️ 현재 OHLCV 데이터가 없습니다.")
        return None
    
    # 각 티커별 최신 종가 가져오기 (현재 날짜)
    current_prices = {}
    for ticker in ohlcv_df_current['ticker'].unique():
        ticker_data = ohlcv_df_current[ohlcv_df_current['ticker'] == ticker].sort_values('date')
        if not ticker_data.empty:
            latest_close = ticker_data.iloc[-1]['close']
            try:
                current_prices[ticker] = float(latest_close)
            except (ValueError, TypeError):
                current_prices[ticker] = np.nan
    
    # 각 티커별 매수일 종가 가져오기 (일주일 전 날짜)
    purchase_prices = {}
    for ticker in ohlcv_df_past['ticker'].unique():
        ticker_data = ohlcv_df_past[ohlcv_df_past['ticker'] == ticker].sort_values('date')
        if not ticker_data.empty:
            # 매수일(purchase_date)에 가장 가까운 날짜의 종가 사용
            ticker_data['date'] = pd.to_datetime(ticker_data['date'])
            purchase_date_dt = pd.to_datetime(purchase_date)
            
            # 매수일 이하의 가장 최근 날짜 찾기
            valid_data = ticker_data[ticker_data['date'] <= purchase_date_dt]
            if not valid_data.empty:
                latest_close = valid_data.iloc[-1]['close']
                try:
                    purchase_prices[ticker] = float(latest_close)
                except (ValueError, TypeError):
                    purchase_prices[ticker] = np.nan
            else:
                purchase_prices[ticker] = np.nan
    
    print(f"   ✓ {len(current_prices)}개 ETF의 현재가 조회 완료")
    print(f"   ✓ {len([p for p in purchase_prices.values() if pd.notna(p)])}개 ETF의 매수가 조회 완료")

    # 전전주(매수일 기준 5거래일 전) 종가 조회 → 5거래일 수익률 기반 5% 이내 하락/상승 필터용
    two_weeks_ago = _prev_trading_date(purchase_date, n_trading_days=5)
    ohlcv_2w = get_etf_ohlcv_from_db(end_date=two_weeks_ago.strftime('%Y-%m-%d'))
    week_ret_pct = {}
    if not ohlcv_2w.empty and not ohlcv_df_past.empty:
        ohlcv_2w_sorted = ohlcv_2w.sort_values(['ticker', 'date'])
        close_2w = ohlcv_2w_sorted.groupby('ticker').last()['close'].astype(float)
        purchase_date_dt = pd.to_datetime(purchase_date)
        ohlcv_past_sorted = ohlcv_df_past.sort_values(['ticker', 'date'])
        ohlcv_past_sorted['date'] = pd.to_datetime(ohlcv_past_sorted['date'])
        valid = ohlcv_past_sorted[ohlcv_past_sorted['date'] <= purchase_date_dt]
        close_1w = valid.groupby('ticker').last()['close'].astype(float)
        for t in close_1w.index:
            if t in close_2w.index and close_2w[t] > 0:
                week_ret_pct[t] = (float(close_1w[t]) - float(close_2w[t])) / float(close_2w[t]) * 100
    tickers_5pct_drop = [t for t, r in week_ret_pct.items() if -5 <= r <= 0]
    tickers_5pct_rise = [t for t, r in week_ret_pct.items() if 0 <= r <= 5]
    
    # 3. 각 모멘텀 기간별 상위 N개 선택 및 수익률 계산 (모멘텀 랭크만)
    periods = [10, 20, 50, 120]
    results = {}
    results_5pct = {}
    results_5pct_rise = {}
    results_ma10 = {}
    results_ma10_5pct = {}
    results_ma10_5pct_rise = {}
    results_ma20 = {}
    results_ma20_5pct = {}
    results_ma20_5pct_rise = {}
    tickers_ma10_ok = set(filter_momentum_above_sma(ohlcv_df_past, momentum_df_past, 10)['ticker'].tolist())
    tickers_ma20_ok = set(filter_momentum_above_sma(ohlcv_df_past, momentum_df_past, 20)['ticker'].tolist())
    momentum_ma10 = momentum_df_past[momentum_df_past['ticker'].isin(tickers_ma10_ok)].copy()
    momentum_ma20 = momentum_df_past[momentum_df_past['ticker'].isin(tickers_ma20_ok)].copy()

    def run_portfolio(period_df, top_etfs, label_suffix=""):
        portfolio = []
        total_return = 0
        valid_count = 0
        for rank, (idx, row) in enumerate(top_etfs.iterrows(), 1):
            ticker = row['ticker']
            etf_name = row.get('etf_name', ticker)
            purchase_price = purchase_prices.get(ticker, np.nan)
            current_price = current_prices.get(ticker, np.nan)
            if pd.notna(current_price) and pd.notna(purchase_price) and purchase_price > 0:
                return_pct = ((current_price - purchase_price) / purchase_price) * 100
                total_return += return_pct
                valid_count += 1
                portfolio.append({
                    'ticker': ticker, 'etf_name': etf_name,
                    'purchase_price': purchase_price, 'current_price': current_price,
                    'return_pct': return_pct
                })
                print(f"{rank:<6} {ticker:<10} {etf_name:<30} {purchase_price:>10.2f} {current_price:>10.2f} {return_pct:>9.2f}%")
            else:
                print(f"{rank:<6} {ticker:<10} {etf_name:<30} {purchase_price:>10.2f} {'N/A':>10} {'N/A':>10}")
        avg_return = (total_return / valid_count) if valid_count > 0 else np.nan
        return portfolio, avg_return, valid_count, len(top_etfs)

    def fill_backtest_periods(m_src, out_dict, header_line_fn):
        if m_src is None or m_src.empty:
            return
        for period in periods:
            period_col = f'{period}일_상승률'
            if period_col not in m_src.columns:
                continue
            period_df = m_src[m_src[period_col].notna()].copy()
            if period_df.empty:
                continue
            period_df = period_df.sort_values(period_col, ascending=False)
            top_etfs = period_df.head(top_n)
            if top_etfs.empty:
                continue
            print(header_line_fn(period))
            print(f"{'순위':<6} {'티커':<10} {'종목명':<30} {'매수가':>10} {'현재가':>10} {'수익률':>10}")
            print("-" * 80)
            portfolio, avg_return, valid_count, total_count = run_portfolio(period_df, top_etfs)
            if valid_count > 0:
                print("-" * 80)
                print(f"{'포트폴리오 평균 수익률':<50} {avg_return:>9.2f}%")
            out_dict[period] = {'portfolio': portfolio, 'avg_return': avg_return, 'valid_count': valid_count, 'total_count': total_count}
    
    print(f"\n3. 각 모멘텀 기간별 상위 {top_n}개 포트폴리오 구성 및 수익률 계산...")
    print("-" * 60)
    
    for period in periods:
        period_col = f'{period}일_상승률'
        if period_col not in momentum_df_past.columns:
            continue
        period_df = momentum_df_past[momentum_df_past[period_col].notna()].copy()
        if period_df.empty:
            continue
        period_df = period_df.sort_values(period_col, ascending=False)
        top_etfs = period_df.head(top_n)
        if top_etfs.empty:
            continue
        
        print(f"\n📊 {period}일 모멘텀 상위 {top_n}개 포트폴리오:")
        print(f"{'순위':<6} {'티커':<10} {'종목명':<30} {'매수가':>10} {'현재가':>10} {'수익률':>10}")
        print("-" * 80)
        portfolio, avg_return, valid_count, total_count = run_portfolio(period_df, top_etfs)
        if valid_count > 0:
            print("-" * 80)
            print(f"{'포트폴리오 평균 수익률':<50} {avg_return:>9.2f}%")
        results[period] = {'portfolio': portfolio, 'avg_return': avg_return, 'valid_count': valid_count, 'total_count': total_count}

    # 3-2. 일주일 5% 이내 하락 ETF만 대상 모멘텀 상위 N 매수 백테스트
    if tickers_5pct_drop:
        print(f"\n3-2. 일주일 5% 이내 하락 ETF 모멘텀 상위 {top_n}개 포트폴리오...")
        print("-" * 60)
        momentum_5pct = momentum_df_past[momentum_df_past['ticker'].isin(tickers_5pct_drop)].copy()
        if not momentum_5pct.empty:
            for period in periods:
                period_col = f'{period}일_상승률'
                if period_col not in momentum_5pct.columns:
                    continue
                period_df = momentum_5pct[momentum_5pct[period_col].notna()].copy()
                if period_df.empty:
                    continue
                period_df = period_df.sort_values(period_col, ascending=False)
                top_etfs = period_df.head(top_n)
                if top_etfs.empty:
                    continue
                print(f"\n📊 {period}일 (5% 하락 ETF) 모멘텀 상위 {top_n}개:")
                print(f"{'순위':<6} {'티커':<10} {'종목명':<30} {'매수가':>10} {'현재가':>10} {'수익률':>10}")
                print("-" * 80)
                portfolio, avg_return, valid_count, total_count = run_portfolio(period_df, top_etfs)
                if valid_count > 0:
                    print("-" * 80)
                    print(f"{'포트폴리오 평균 수익률':<50} {avg_return:>9.2f}%")
                results_5pct[period] = {'portfolio': portfolio, 'avg_return': avg_return, 'valid_count': valid_count, 'total_count': total_count}

    # 3-3. 일주일 5% 이내 상승 ETF만 대상 모멘텀 상위 N 매수 백테스트
    if tickers_5pct_rise:
        print(f"\n3-3. 일주일 5% 이내 상승 ETF 모멘텀 상위 {top_n}개 포트폴리오...")
        print("-" * 60)
        momentum_5pct_r = momentum_df_past[momentum_df_past['ticker'].isin(tickers_5pct_rise)].copy()
        if not momentum_5pct_r.empty:
            for period in periods:
                period_col = f'{period}일_상승률'
                if period_col not in momentum_5pct_r.columns:
                    continue
                period_df = momentum_5pct_r[momentum_5pct_r[period_col].notna()].copy()
                if period_df.empty:
                    continue
                period_df = period_df.sort_values(period_col, ascending=False)
                top_etfs = period_df.head(top_n)
                if top_etfs.empty:
                    continue
                print(f"\n📊 {period}일 (5% 상승 ETF) 모멘텀 상위 {top_n}개:")
                print(f"{'순위':<6} {'티커':<10} {'종목명':<30} {'매수가':>10} {'현재가':>10} {'수익률':>10}")
                print("-" * 80)
                portfolio, avg_return, valid_count, total_count = run_portfolio(period_df, top_etfs)
                if valid_count > 0:
                    print("-" * 80)
                    print(f"{'포트폴리오 평균 수익률':<50} {avg_return:>9.2f}%")
                results_5pct_rise[period] = {'portfolio': portfolio, 'avg_return': avg_return, 'valid_count': valid_count, 'total_count': total_count}

    print(f"\n3-4. 매수일 기준 10일선 이탈 제외(종가 ≥ MA10) — 모멘텀 상위 {top_n}개...")
    print("-" * 60)
    fill_backtest_periods(
        momentum_ma10, results_ma10,
        lambda p: f"\n📊 {p}일 (MA10 이탈 제외) 모멘텀 상위 {top_n}개")

    print(f"\n3-5. 매수일 기준 20일선 이탈 제외(종가 ≥ MA20) — 모멘텀 상위 {top_n}개...")
    print("-" * 60)
    fill_backtest_periods(
        momentum_ma20, results_ma20,
        lambda p: f"\n📊 {p}일 (MA20 이탈 제외) 모멘텀 상위 {top_n}개")

    if tickers_5pct_drop:
        set5d = set(tickers_5pct_drop)
        m10_5d = momentum_ma10[momentum_ma10['ticker'].isin(set5d)].copy()
        m20_5d = momentum_ma20[momentum_ma20['ticker'].isin(set5d)].copy()
        print(f"\n3-6. MA10 이탈 제외 + 5% 이내 하락 ETF 모멘텀 상위 {top_n}개...")
        print("-" * 60)
        fill_backtest_periods(
            m10_5d, results_ma10_5pct,
            lambda p: f"\n📊 {p}일 (MA10 제외·5% 하락) 모멘텀 상위 {top_n}개")
        print(f"\n3-7. MA20 이탈 제외 + 5% 이내 하락 ETF 모멘텀 상위 {top_n}개...")
        print("-" * 60)
        fill_backtest_periods(
            m20_5d, results_ma20_5pct,
            lambda p: f"\n📊 {p}일 (MA20 제외·5% 하락) 모멘텀 상위 {top_n}개")

    if tickers_5pct_rise:
        set5r = set(tickers_5pct_rise)
        m10_5r = momentum_ma10[momentum_ma10['ticker'].isin(set5r)].copy()
        m20_5r = momentum_ma20[momentum_ma20['ticker'].isin(set5r)].copy()
        print(f"\n3-8. MA10 이탈 제외 + 5% 이내 상승 ETF 모멘텀 상위 {top_n}개...")
        print("-" * 60)
        fill_backtest_periods(
            m10_5r, results_ma10_5pct_rise,
            lambda p: f"\n📊 {p}일 (MA10 제외·5% 상승) 모멘텀 상위 {top_n}개")
        print(f"\n3-9. MA20 이탈 제외 + 5% 이내 상승 ETF 모멘텀 상위 {top_n}개...")
        print("-" * 60)
        fill_backtest_periods(
            m20_5r, results_ma20_5pct_rise,
            lambda p: f"\n📊 {p}일 (MA20 제외·5% 상승) 모멘텀 상위 {top_n}개")

    # 4. 전체 결과 요약
    w = 34
    print("\n" + "=" * 60)
    print("📈 백테스트 결과 요약")
    print("=" * 60)
    print(f"{'구분':<{w}} {'모멘텀 기간':<12} {'포트폴리오 수익률':<18} {'유효 종목 수':<12}")
    print("-" * 60)

    def _print_summary_rows(res_dict, label):
        for period in periods:
            if period in res_dict:
                r = res_dict[period]
                avg = r['avg_return']
                v = r['valid_count']
                print(f"{label:<{w}} {period}일{'':<6} {(f'{avg:.2f}%' if pd.notna(avg) else 'N/A'):<18} {v}/{top_n}")

    _print_summary_rows(results, '일주일 전 모멘텀 상위 N')
    _print_summary_rows(results_5pct, '5% 하락 ETF 모멘텀 상위 N')
    _print_summary_rows(results_5pct_rise, '5% 상승 ETF 모멘텀 상위 N')
    _print_summary_rows(results_ma10, 'MA10 이탈 제외 모멘텀 상위 N')
    _print_summary_rows(results_ma20, 'MA20 이탈 제외 모멘텀 상위 N')
    _print_summary_rows(results_ma10_5pct, 'MA10 제외·5% 하락 모멘텀 상위 N')
    _print_summary_rows(results_ma20_5pct, 'MA20 제외·5% 하락 모멘텀 상위 N')
    _print_summary_rows(results_ma10_5pct_rise, 'MA10 제외·5% 상승 모멘텀 상위 N')
    _print_summary_rows(results_ma20_5pct_rise, 'MA20 제외·5% 상승 모멘텀 상위 N')

    valid_returns = [r['avg_return'] for r in results.values() if pd.notna(r['avg_return'])]
    valid_returns_5pct = [r['avg_return'] for r in results_5pct.values() if pd.notna(r['avg_return'])]
    valid_returns_5pct_rise = [r['avg_return'] for r in results_5pct_rise.values() if pd.notna(r['avg_return'])]
    valid_ma10 = [r['avg_return'] for r in results_ma10.values() if pd.notna(r['avg_return'])]
    valid_ma20 = [r['avg_return'] for r in results_ma20.values() if pd.notna(r['avg_return'])]
    valid_ma10_5 = [r['avg_return'] for r in results_ma10_5pct.values() if pd.notna(r['avg_return'])]
    valid_ma20_5 = [r['avg_return'] for r in results_ma20_5pct.values() if pd.notna(r['avg_return'])]
    valid_ma10_5r = [r['avg_return'] for r in results_ma10_5pct_rise.values() if pd.notna(r['avg_return'])]
    valid_ma20_5r = [r['avg_return'] for r in results_ma20_5pct_rise.values() if pd.notna(r['avg_return'])]
    any_avg = any([valid_returns, valid_returns_5pct, valid_returns_5pct_rise, valid_ma10, valid_ma20,
                   valid_ma10_5, valid_ma20_5, valid_ma10_5r, valid_ma20_5r])
    if any_avg:
        print("-" * 60)
        if valid_returns:
            print(f"{'일주일 전 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_returns):>10.2f}%")
        if valid_returns_5pct:
            print(f"{'5% 하락 ETF 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_returns_5pct):>10.2f}%")
        if valid_returns_5pct_rise:
            print(f"{'5% 상승 ETF 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_returns_5pct_rise):>10.2f}%")
        if valid_ma10:
            print(f"{'MA10 이탈 제외 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_ma10):>10.2f}%")
        if valid_ma20:
            print(f"{'MA20 이탈 제외 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_ma20):>10.2f}%")
        if valid_ma10_5:
            print(f"{'MA10 제외·5%하락 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_ma10_5):>10.2f}%")
        if valid_ma20_5:
            print(f"{'MA20 제외·5%하락 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_ma20_5):>10.2f}%")
        if valid_ma10_5r:
            print(f"{'MA10 제외·5%상승 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_ma10_5r):>10.2f}%")
        if valid_ma20_5r:
            print(f"{'MA20 제외·5%상승 모멘텀 전체 평균':<{w}} {'':<12} {np.mean(valid_ma20_5r):>10.2f}%")

    print("=" * 60)

    return {
        'purchase_date': purchase_date,
        'evaluation_date': end_date,
        'results': results,
        'results_5pct': results_5pct,
        'results_5pct_rise': results_5pct_rise,
        'results_ma10': results_ma10,
        'results_ma20': results_ma20,
        'results_ma10_5pct': results_ma10_5pct,
        'results_ma20_5pct': results_ma20_5pct,
        'results_ma10_5pct_rise': results_ma10_5pct_rise,
        'results_ma20_5pct_rise': results_ma20_5pct_rise,
        'overall_avg_return': np.mean(valid_returns) if valid_returns else np.nan,
        'overall_avg_return_5pct': np.mean(valid_returns_5pct) if valid_returns_5pct else np.nan,
        'overall_avg_return_5pct_rise': np.mean(valid_returns_5pct_rise) if valid_returns_5pct_rise else np.nan,
        'overall_avg_return_ma10': np.mean(valid_ma10) if valid_ma10 else np.nan,
        'overall_avg_return_ma20': np.mean(valid_ma20) if valid_ma20 else np.nan,
        'overall_avg_return_ma10_5pct': np.mean(valid_ma10_5) if valid_ma10_5 else np.nan,
        'overall_avg_return_ma20_5pct': np.mean(valid_ma20_5) if valid_ma20_5 else np.nan,
        'overall_avg_return_ma10_5pct_rise': np.mean(valid_ma10_5r) if valid_ma10_5r else np.nan,
        'overall_avg_return_ma20_5pct_rise': np.mean(valid_ma20_5r) if valid_ma20_5r else np.nan,
    }


# etf_list_extracted.csv에서 생성: 종목코드 -> 종목명.
ETF_CODE_NAME: dict[str, str] = {
    '396500': 'TIGER 반도체TOP10',
    '487240': 'KODEX AI전력핵심설비',
    '091160': 'KODEX 반도체',
    '305720': 'KODEX 2차전지산업',
    '395270': 'HANARO Fn K-반도체',
    '0101N0': 'RISE AI전력인프라',
    '0091P0': 'TIGER 코리아원자력',
    '367760': 'RISE 네트워크인프라',
    '466920': 'SOL 조선TOP3플러스',
    '0148J0': 'TIGER 코리아휴머노이드로봇산업',
    '364980': 'TIGER 2차전지TOP10',
    '0115D0': 'KODEX 조선TOP10',
    '0080G0': 'KODEX 방산TOP10',
    '455850': 'SOL AI반도체소부장',
    '494670': 'TIGER 조선TOP10',
    '0098F0': 'KODEX 원자력SMR',
    '091230': 'TIGER 반도체',
    '457990': 'PLUS 태양광&ESS',
    '434730': 'HANARO 원자력iSelect',
    '102970': 'KODEX 증권',
    '395160': 'KODEX AI반도체',
    '469150': 'ACE AI반도체TOP3+',
    '449450': 'PLUS K방산',
    '471990': 'KODEX AI반도체핵심장비',
    '445290': 'KODEX 로봇액티브',
    '138540': 'TIGER 현대차그룹플러스',
    '385510': 'KODEX 신재생에너지액티브',
    '117700': 'KODEX 건설',
    '463250': 'TIGER K방산&우주',
    '462010': 'TIGER 2차전지소재Fn',
    '444200': 'SOL 코리아메가테크액티브',
    '0005D0': 'SOL 전고체배터리&실리콘음극재',
    '102780': 'KODEX 삼성그룹',
    '091180': 'KODEX 자동차',
    '305540': 'TIGER 2차전지테마',
    '139220': 'TIGER 200 건설',
    '474590': 'WON 반도체밸류체인액티브',
    '471760': 'TIGER AI반도체핵심공정',
    '0092B0': 'SOL 한국원자력SMR',
    '462900': 'KoAct 바이오헬스케어액티브',
    '157500': 'TIGER 증권',
    '469070': 'RISE AI&로봇',
    '491820': 'HANARO 전력설비투자',
    '433500': 'ACE 원자력TOP10',
    '422420': 'RISE 2차전지액티브',
    '228790': 'TIGER 화장품',
    '377990': 'TIGER Fn신재생에너지',
    '475050': 'ACE KPOP포커스',
    '307520': 'TIGER 지주회사',
    '0168K0': 'TIGER 기술이전바이오액티브',
    '466930': 'SOL 자동차TOP3플러스',
    '0093A0': 'RISE AI반도체TOP10',
    '139230': 'TIGER 200 중공업',
    '421320': 'PLUS 우주항공&UAM',
    '0177X0': 'ACE K휴머노이드로봇산업TOP2+',
    '364970': 'TIGER 바이오TOP10',
    '461950': 'KODEX 2차전지핵심소재10',
    '475310': 'SOL 반도체후공정',
    '0141S0': 'SOL 조선기자재',
    '469170': 'ACE 포스코그룹포커스',
    '091170': 'KODEX 은행',
    '494220': 'UNICORN SK하이닉스밸류체인액티브',
    '0182R0': '1Q K반도체TOP2+',
    '388420': 'RISE 비메모리반도체액티브',
    '463050': 'TIME K바이오액티브',
    '244580': 'KODEX 바이오',
    '0008T0': 'SOL 화장품TOP3플러스',
    '0000J0': 'PLUS 한화그룹주',
    '475300': 'SOL 반도체전공정',
    '465330': 'RISE 2차전지TOP10',
    '139250': 'TIGER 200 에너지화학',
    '0150K0': 'KoAct 수소전력ESS인프라액티브',
    '0000Z0': 'RISE 바이오TOP10액티브',
    '117680': 'KODEX 철강',
    '117460': 'KODEX 에너지화학',
    '0177A0': 'WON 두산그룹포커스',
    '455860': 'SOL 2차전지소부장Fn',
    '367770': 'RISE 수소경제테마',
    '143860': 'TIGER 헬스케어',
    '0005G0': 'ITF K-AI반도체코어테크',
    '476260': 'HANARO 반도체핵심공정주도주',
    '490480': 'SOL K방산',
    '0155N0': 'HANARO K휴머노이드테마TOP10',
    '0074K0': 'KoAct K수출핵심기업TOP30액티브',
    '487130': 'KoAct AI인프라액티브',
    '364990': 'TIGER 게임TOP10',
    '138520': 'TIGER 삼성그룹',
    '140700': 'KODEX 보험',
    '228810': 'TIGER 미디어컨텐츠',
    '157490': 'TIGER 소프트웨어',
    '300950': 'KODEX 게임산업',
    '138530': 'TIGER LG그룹플러스',
    '0090B0': 'PLUS K방산소부장',
    '445150': 'KODEX 친환경조선해운액티브',
    '0154F0': 'WON 초대형IB&금융지주',
    '441540': 'HANARO Fn조선해운',
    '228800': 'TIGER 여행레저',
    '266420': 'KODEX 헬스케어',
    '479850': 'HANARO K-뷰티',
    '395290': 'HANARO Fn K-POP&미디어',
    '381570': 'HANARO Fn친환경에너지',
    '381560': 'HANARO Fn전기&수소차',
    '365000': 'TIGER 인터넷TOP10',
    '488200': 'KIWOOM K-2차전지북미공급망',
    '401470': 'KODEX 메타버스액티브',
    '139270': 'TIGER 200 금융',
    '213610': 'KODEX 삼성그룹밸류',
    '0103T0': '1Q K소버린AI',
    '482030': 'KoAct 반도체&2차전지핵심소재액티브',
    '102960': 'KODEX 기계장비',
    '385600': 'ACE 2차전지&친환경차액티브',
    '498050': 'HANARO 바이오코리아액티브',
    '466810': 'BNK 2차전지양극재',
    '438900': 'HANARO Fn K-푸드',
    '401170': 'RISE 메타버스',
    '266360': 'KODEX K콘텐츠',
    '367740': 'HANARO Fn5G산업',
    '108450': 'ACE 삼성그룹섹터가중',
    '091220': 'TIGER 은행',
    '464600': 'SOL 자동차소부장Fn',
    '139240': 'TIGER 200 철강소재',
    '404120': 'TIME K신재생에너지액티브',
    '464610': 'SOL 의료기기소부장Fn',
    '385520': 'KODEX 자율주행액티브',
    '227540': 'TIGER 200 헬스케어',
    '0105D0': 'SOL 한국AI소프트웨어',
    '266410': 'KODEX 필수소비재',
    '140710': 'KODEX 운송',
    '139290': 'TIGER 200 경기소비재',
    '300610': 'TIGER K게임',
    '487750': 'BNK 온디바이스AI',
    '284980': 'RISE 200금융',
    '388280': 'RISE K엔터&여행레저',
    '486240': 'DAISHIN343 AI반도체&인프라액티브',
    '446700': 'RISE 배터리 리사이클링',
    '131890': 'ACE 삼성그룹동일가중',
    '395280': 'HANARO Fn K-게임',
    '400970': 'TIGER Fn메타버스',
    '410870': 'TIME K컬처액티브',
    '0120J0': 'BNK 카카오그룹포커스',
    '266390': 'KODEX 경기소비재',
    '300640': 'RISE 게임테마',
    '483020': 'KIWOOM 의료AI',
    '315270': 'TIGER 200커뮤니케이션서비스',
    '387280': 'TIGER 퓨처모빌리티액티브',
    '139280': 'TIGER 경기방어',
    '307510': 'TIGER 의료기기',
    '227560': 'TIGER 200 생활소비재',
    '253280': 'RISE 헬스케어',
    '404650': 'SOL KRX기후변화솔루션',
    '404260': 'KODEX 기후변화솔루션',
    '376410': 'TIGER 탄소효율그린뉴딜',
    '442090': '에셋플러스 코리아대장장이액티브',
    '0053M0': '더제이 중소형포커스액티브',
    '322400': 'HANARO e커머스',
    '227550': 'TIGER 200 산업재',
    '488210': 'KIWOOM K-반도체북미공급망',
    '427120': 'RISE AI플랫폼',
    '375760': 'HANARO 탄소효율그린뉴딜',
    '326230': 'RISE 내수주플러스',
    '314700': 'HANARO 농업융복합산업',
    '140570': 'RISE 수출주',
    '368680': 'KODEX K-뉴딜디지털플러스',
    '226380': 'ACE Fn성장소비주도주',
    '402460': 'HANARO Fn K-메타버스MZ',
    '280920': 'PLUS 주도업종',
    '375770': 'KODEX 탄소효율그린뉴딜',
    '438740': 'MIDAS 중소형액티브',
    '404540': 'TIGER KRX기후변화솔루션',
    '422260': 'VITA MZ소비액티브',
    '407300': 'HANARO Fn골프테마',
    '407820': '에셋플러스 코리아플랫폼액티브',
}


# 섹터 등락률 HTML 관심 탭: 아래 종목만 대상으로 거래일별 전일 종가 대비 당일 등락률(%) 순위표.
# 티커는 문자열로 두고, 종목명만 수정·추가하면 됩니다.
# 기본 HTML에서는 [관심 섹터]·[관심 그룹]·[관심 액티브ETF]·[섹터 ETF 맵 순위(sector_etf_dict)]·[관심 패시브ETF] 일별 등락률 순위 탭으로 각각 표시됩니다.
SECTOR_MOMENTUM_DAILY_RANK_SECTOR_ETFS: dict[str, str] = {
    '091160': 'KODEX 반도체',
    '487240': 'KODEX AI전력핵심설비',
    '0148J0': 'TIGER 코리아휴머노이드로봇산업',
    '367760': 'RISE 네트워크인프라',
    '305720': 'KODEX 2차전지산업',
    '102970': 'KODEX 증권',
    '091180': 'KODEX 자동차',
    '455850': 'SOL AI반도체소부장',
    '0091P0': 'TIGER 코리아원자력',
    '494670': 'TIGER 조선TOP10',
    '0080G0': 'KODEX 방산TOP10',
    '449450': 'PLUS K방산',
    '471990': 'KODEX AI반도체핵심장비',
    '463250': 'TIGER K방산&우주',
    '462010': 'TIGER 2차전지소재Fn',
    '469070': 'RISE AI&로봇',
    '471760': 'TIGER AI반도체핵심공정',
    '091170': 'KODEX 은행',
    '421320': 'PLUS 우주항공',
    '117700': 'KODEX 건설',
    '475300': 'SOL 반도체전공정',
    '457990': 'PLUS 태양광&ESS',
    '307520': 'TIGER 지주회사',
    '364970': 'TIGER 바이오TOP10',
    '228790': 'TIGER 화장품',
    '157490': 'TIGER 소프트웨어',
    '377990': 'TIGER Fn신재생에너지',
    '475310': 'SOL 반도체후공정',
    '0141S0': 'SOL 조선기자재',
    '475050': 'ACE KPOP포커스',
    '140700': 'KODEX 보험',
    '464600': 'SOL 자동차소부장Fn',
    '143860': 'TIGER 헬스케어',
    '365000': 'TIGER 인터넷TOP10',
    '266390': 'KODEX 경기소비재',
    '364990': 'TIGER 게임TOP10',
    '381560': 'HANARO Fn전기&수소차',
    '228800': 'TIGER 여행레저',
    '228810': 'TIGER 미디어컨텐츠',
    '117680': 'KODEX 철강',
    '266360': 'KODEX K콘텐츠',
    '117460': 'KODEX 에너지화학',
    '367740': 'HANARO Fn5G산업',
    '102960': 'KODEX 기계장비',
    '140710': 'KODEX 운송',
    '438900': 'HANARO Fn K-푸드',
    '266410': 'KODEX 필수소비재',
    '322400': 'HANARO e커머스',
}
SECTOR_MOMENTUM_DAILY_RANK_GROUP_ETFS: dict[str, str] = {
    '105780': 'RISE 5대그룹주',
    '138520': 'TIGER 삼성그룹',
    '138540': 'TIGER 현대차그룹플러스',
    '0000J0': 'PLUS 한화그룹주',
    '138530': 'TIGER LG그룹플러스',
    '0177A0': 'WON 두산그룹포커스',
    '469170': 'ACE 포스코그룹포커스',
    '307520': 'TIGER 지주회사',
    '0120J0': 'BNK 카카오그룹포커스',
}
SECTOR_MOMENTUM_DAILY_RANK_Actives_ETFS: dict[str, str] = {
    '474590': 'WON 반도체밸류체인액티브',
    '471780': 'TIGER 코리아테크액티브',
    '391670': 'HK 베스트일레븐액티브',
    '388420': 'RISE 비메모리반도체액티브',
    '495060': 'TIME 코리아밸류업액티브',
    '364690': 'KODEX 혁신기술테마액티브',
    '495230': 'KoAct 코리아밸류업액티브',
    '494220': 'UNICORN SK하이닉스밸류체인액티브',
    '442260': '마이티 다이나믹퀀트액티브',
    '395750': 'PLUS ESG가치주액티브',
    '433250': 'UNICORN R&D 액티브',
    '401470': 'KODEX 메타버스액티브',
    '448570': 'FOCUS AI코리아액티브',
    '365040': 'TIGER AI코리아그로스액티브',
    '385600': 'ACE 2차전지&친환경차액티브',
    '445290': 'KODEX 로봇액티브',
    '395760': 'PLUS ESG성장주액티브',
    '385520': 'KODEX 자율주행액티브',
    '413930': 'WON AI ESG액티브',
    '385590': 'ACE ESG액티브',
    '476850': 'KoAct 배당성장액티브',
    '487130': 'KoAct AI인프라액티브',
    '494330': 'ACE 라이프자산주주가치액티브',
    '442090': '에셋플러스 코리아대장장이액티브',
    '0151P0': 'RISE 코리아전략산업액티브',
    '486240': 'DAISHIN343 AI반도체&인프라액티브',
    '496130': 'TRUSTON 코리아밸류업액티브',
    '0166S0': 'PLUS K제조업핵심기업액티브',
    '0074K0': 'KoAct K수출핵심기업TOP30액티브',
    '445690': 'BNK 주주가치액티브',
    '470310': 'UNICORN 생성형AI강소기업액티브',
    '444200': 'SOL 코리아메가테크액티브',
    '441800': 'TIME Korea플러스배당액티브',
    '387280': 'TIGER 퓨처모빌리티액티브',
    '422260': 'VITA MZ소비액티브',
    '373490': 'KODEX 코리아혁신성장액티브',
    '472720': 'TRUSTON 주주가치액티브',
    '404120': 'TIME K신재생에너지액티브',
    '447430': 'ACE 주주환원가치주액티브',
    '457930': 'BNK 미래전략기술액티브',
    '410870': 'TIME K컬처액티브',
    '438740': 'MIDAS 중소형액티브',
    '491510': '파워 K-주주가치액티브',
    '0172Y0': 'ACE K수출핵심TOP10산업액티브',
    '0053M0': '더제이 중소형포커스액티브',
    '445150': 'KODEX 친환경조선해운액티브',
    '482030': 'KoAct 반도체&2차전지핵심소재액티브',
    '0000Z0': 'RISE 바이오TOP10액티브',
    '385510': 'KODEX 신재생에너지액티브',
    '0150K0': 'KoAct 수소전력ESS인프라액티브',
    '422420': 'RISE 2차전지액티브',
    '476000': 'UNICORN 포스트IPO액티브',
}
SECTOR_MOMENTUM_DAILY_RANK_Passives_ETFS: dict[str, str] = {
    '367760': 'RISE 네트워크인프라',
    '266390': 'KODEX 경기소비재',
    '402460': 'HANARO Fn K-메타버스MZ',
    '466930': 'SOL 자동차TOP3플러스',
    '464600': 'SOL 자동차소부장Fn',
    '401170': 'RISE 메타버스',
    '0167A0': 'SOL AI반도체TOP2플러스',
    '395270': 'HANARO Fn K-반도체',
    '395160': 'KODEX AI반도체TOP2플러스',
    '487750': 'BNK 온디바이스AI',
    '140700': 'KODEX 보험',
    '367740': 'HANARO Fn5G산업',
    '0105D0': 'SOL 한국AI소프트웨어',
    '475300': 'SOL 반도체전공정',
    '381560': 'HANARO Fn전기&수소차',
    '091160': 'KODEX 반도체',
    '400970': 'TIGER Fn메타버스',
    '469150': 'ACE AI반도체TOP3+',
    '091230': 'TIGER 반도체',
    '091180': 'KODEX 자동차',
    '0182R0': '1Q K반도체TOP2+',
    '157490': 'TIGER 소프트웨어',
    '396500': 'TIGER 반도체TOP10',
    '476260': 'HANARO 반도체핵심공정주도주',
    '388280': 'RISE K엔터&여행레저',
    '0148J0': 'TIGER 코리아휴머노이드로봇산업',
    '0155N0': 'HANARO K휴머노이드테마TOP10',
    '0093A0': 'RISE AI반도체TOP10',
    '365000': 'TIGER 인터넷TOP10',
    '091220': 'TIGER 은행',
    '091170': 'KODEX 은행',
    '322400': 'HANARO e커머스',
    '0177X0': 'ACE K휴머노이드로봇산업TOP2+',
    '266410': 'KODEX 필수소비재',
    '407300': 'HANARO Fn골프테마',
    '266360': 'KODEX K콘텐츠',
    '438900': 'HANARO Fn K-푸드',
    '300950': 'KODEX 게임산업',
    '395280': 'HANARO Fn K-게임',
    '364990': 'TIGER 게임TOP10',
    '479850': 'HANARO K-뷰티',
    '469070': 'RISE AI&로봇',
    '300640': 'RISE 게임테마',
    '464610': 'SOL 의료기기소부장Fn',
    '0008T0': 'SOL 화장품TOP3플러스',
    '307520': 'TIGER 지주회사',
    '228790': 'TIGER 화장품',
    '300610': 'TIGER K게임',
    '140710': 'KODEX 운송',
    '228800': 'TIGER 여행레저',
    '364970': 'TIGER 바이오TOP10',
    '455850': 'SOL AI반도체소부장',
    '475310': 'SOL 반도체후공정',
    '143860': 'TIGER 헬스케어',
    '314700': 'HANARO 농업융복합산업',
    '266420': 'KODEX 헬스케어',
    '490480': 'SOL K방산',
    '253280': 'RISE 헬스케어',
    '0154F0': 'WON 초대형IB&금융지주',
    '494670': 'TIGER 조선TOP10',
    '471990': 'KODEX AI반도체핵심장비',
    '466920': 'SOL 조선TOP3플러스',
    '475050': 'ACE KPOP포커스',
    '0080G0': 'KODEX 방산TOP10',
    '427120': 'RISE AI플랫폼',
    '244580': 'KODEX 바이오',
    '441540': 'HANARO Fn조선해운',
    '471760': 'TIGER AI반도체핵심공정',
    '0115D0': 'KODEX 조선TOP10',
    '449450': 'PLUS K방산',
    '307510': 'TIGER 의료기기',
    '395290': 'HANARO Fn K-POP&미디어',
    '463250': 'TIGER K방산&우주',
    '421320': 'PLUS 우주항공',
    '395150': 'KODEX 웹툰&드라마',
    '381570': 'HANARO Fn친환경에너지',
    '117460': 'KODEX 에너지화학',
    '228810': 'TIGER 미디어컨텐츠',
    '457990': 'PLUS 태양광&ESS',
    '377990': 'TIGER Fn신재생에너지',
    '483020': 'KIWOOM 의료AI',
    '102960': 'KODEX 기계장비',
    '117680': 'KODEX 철강',
    '117700': 'KODEX 건설',
    '157500': 'TIGER 증권',
    '102970': 'KODEX 증권',
    '446700': 'RISE 배터리 리사이클링',
    '305720': 'KODEX 2차전지산업',
    '364980': 'TIGER 2차전지TOP10',
    '305540': 'TIGER 2차전지테마',
    '433500': 'ACE 원자력TOP10',
    '434730': 'HANARO 원자력iSelect',
    '0101N0': 'RISE AI전력인프라',
    '0098F0': 'KODEX 원자력SMR',
    '465330': 'RISE 2차전지TOP10',
    '0117V0': 'TIGER 코리아AI전력기기TOP3플러스',
    '491820': 'HANARO 전력설비투자',
    '487240': 'KODEX AI전력핵심설비',
    '0005D0': 'SOL 전고체배터리&실리콘음극재',
    '466810': 'BNK 2차전지양극재',
    '0092B0': 'SOL 한국원자력SMR',
    '455860': 'SOL 2차전지소부장Fn',
    '0091P0': 'TIGER 코리아원자력',
    '462010': 'TIGER 2차전지소재Fn',
    '0090B0': 'PLUS K방산소부장',
    '0141S0': 'SOL 조선기자재',
    '461950': 'KODEX 2차전지핵심소재10',
}
SECTOR_MOMENTUM_DAILY_RANK_ETFS: dict[str, str] = {
    **SECTOR_MOMENTUM_DAILY_RANK_SECTOR_ETFS,
    **SECTOR_MOMENTUM_DAILY_RANK_GROUP_ETFS,
    **SECTOR_MOMENTUM_DAILY_RANK_Actives_ETFS,
    **SECTOR_MOMENTUM_DAILY_RANK_Passives_ETFS,
}

sector_etf_dict = {
    "2차전지": {
        "466810": "BNK 2차전지양극재",
        "305720": "KODEX 2차전지산업",
        "461950": "KODEX 2차전지핵심소재10",
        "465330": "RISE 2차전지TOP10",
        "0005D0": "SOL 전고체배터리&실리콘음극재",
        "364980": "TIGER 2차전지TOP10",
        "462010": "TIGER 2차전지소재Fn",
        "305540": "TIGER 2차전지테마",
        "455860": "SOL 2차전지소부장Fn",
        "446700": "RISE 배터리 리사이클링",
    },
    "건설": {
        "117700": "KODEX 건설",
    },
    "기계장비": {
        "102960": "KODEX 기계장비",
    },
    "로봇": {
        "0177X0": "ACE K휴머노이드로봇산업TOP2+",
        "0155N0": "HANARO K휴머노이드테마TOP10",
        "0204D0": "KODEX 현대차로보틱스밸류체인TOP3플러스",
        "469070": "RISE AI&로봇",
        "0190C0": "RISE 현대차고정피지컬AI",
        "0148J0": "TIGER 코리아휴머노이드로봇산업",
    },
    "바이오": {
        "483020": "KIWOOM 의료AI",
        "244580": "KODEX 바이오",
        "364970": "TIGER 바이오TOP10",
        "266420": "KODEX 헬스케어",
        "253280": "RISE 헬스케어",
        "464610": "SOL 의료기기소부장Fn",
        "307510": "TIGER 의료기기",
        "143860": "TIGER 헬스케어",
    },
    "반도체": {
        "0182R0": "1Q K반도체TOP2+",
        "469150": "ACE AI반도체TOP3+",
        "395270": "HANARO Fn K-반도체",
        "476260": "HANARO 반도체핵심공정주도주",
        "0005G0": "IBK K-AI반도체코어테크",
        "395160": "KODEX AI반도체TOP2플러스",
        "471990": "KODEX AI반도체핵심장비",
        "091160": "KODEX 반도체",
        "0093A0": "RISE AI반도체TOP10",
        "0167A0": "SOL AI반도체TOP2플러스",
        "455850": "SOL AI반도체소부장",
        "475300": "SOL 반도체전공정",
        "475310": "SOL 반도체후공정",
        "471760": "TIGER AI반도체핵심공정",
        "091230": "TIGER 반도체",
        "396500": "TIGER 반도체TOP10",
    },
    "방산": {
        "0080G0": "KODEX 방산TOP10",
        "449450": "PLUS K방산",
        "0090B0": "PLUS K방산소부장",
        "490480": "SOL K방산",
        "463250": "TIGER K방산&우주",
    },
    "보험": {
        "140700": "KODEX 보험",
    },
    "소비": {
        "438900": "HANARO Fn K-푸드",
        "322400": "HANARO e커머스",
        "266390": "KODEX 경기소비재",
        "266410": "KODEX 필수소비재",
        "326230": "RISE 내수주플러스",
        "139280": "TIGER 경기방어",
        "228800": "TIGER 여행레저",
    },
    "소프트웨어": {
        "0105D0": "SOL 한국AI소프트웨어",
        "157490": "TIGER 소프트웨어",
        "365000": "TIGER 인터넷TOP10",
    },
    "에너지": {
        "381560": "HANARO Fn전기&수소차",
        "381570": "HANARO Fn친환경에너지",
        "375760": "HANARO 탄소효율그린뉴딜",
        "117460": "KODEX 에너지화학",
        "375770": "KODEX 탄소효율그린뉴딜",
        "457990": "PLUS 태양광&ESS",
        "367770": "RISE 수소경제테마",
        "377990": "TIGER Fn신재생에너지",
        "376410": "TIGER 탄소효율그린뉴딜",
    },
    "우주항공": {
        "421320": "PLUS 우주항공",
        "0207G0": "SOL 우주항공밸류체인",
    },
    "운송": {
        "140710": "KODEX 운송",
    },
    "원자력": {
        "433500": "ACE 원자력TOP10",
        "434730": "HANARO 원자력iSelect",
        "0098F0": "KODEX 원자력SMR",
        "0092B0": "SOL 한국원자력SMR",
        "0091P0": "TIGER 코리아원자력",
    },
    "은행": {
        "091170": "KODEX 은행",
        "091220": "TIGER 은행",
        "466940": "TIGER 은행고배당플러스TOP10",
    },
    "자동차": {
        "091180": "KODEX 자동차",
        "466930": "SOL 자동차TOP3플러스",
        "464600": "SOL 자동차소부장Fn",
        "138540": "TIGER 현대차그룹플러스",
    },
    "전력": {
        "491820": "HANARO 전력설비투자",
        "487240": "KODEX AI전력핵심설비",
        "0101N0": "RISE AI전력인프라",
        "0117V0": "TIGER 코리아AI전력기기TOP3플러스",
    },
    "조선": {
        "441540": "HANARO Fn조선해운",
        "0115D0": "KODEX 조선TOP10",
        "466920": "SOL 조선TOP3플러스",
        "0141S0": "SOL 조선기자재",
        "494670": "TIGER 조선TOP10",
    },
    "증권": {
        "0111J0": "HANARO 증권고배당TOP3플러스",
        "102970": "KODEX 증권",
        "157500": "TIGER 증권",
    },
    "철강": {
        "117680": "KODEX 철강",
    },
    "콘텐츠": {
        "475050": "ACE KPOP포커스",
        "395290": "HANARO Fn K-POP&미디어",
        "395280": "HANARO Fn K-게임",
        "266360": "KODEX K콘텐츠",
        "300950": "KODEX 게임산업",
        "395150": "KODEX 웹툰&드라마",
        "388280": "RISE K엔터&여행레저",
        "300640": "RISE 게임테마",
        "300610": "TIGER K게임",
        "364990": "TIGER 게임TOP10",
        "228810": "TIGER 미디어컨텐츠",
    },
    "통신": {
        "367740": "HANARO Fn5G산업",
        "367760": "RISE 네트워크인프라",
    },
    "화장품": {
        "479850": "HANARO K-뷰티",
        "0008T0": "SOL 화장품TOP3플러스",
        "228790": "TIGER 화장품",
    },
}


def _norm_etf_ticker_key(t) -> str:
    """DB·피벗에서 티커가 숫자만인 경우 선행 0 정렬용."""
    s = str(t).strip()
    if s.isdigit():
        return s.zfill(6)
    return s


def _nday_daily_top20_section_bits(
    px: pd.DataFrame,
    periods: list[int],
    cell_name_ticker: dict[str, tuple[str, str]],
    last_trading_days: int = 20,
) -> list[str]:
    """
    px: 거래일 인덱스 × 티커 컬럼 종가 피벗(정렬됨).
    periods: N거래일 등락률 창(일).
    cell_name_ticker: 피벗 컬럼 티커 문자열 -> (종목명, 표시용 ticker).
    반환: 유니버스/관심종목 탭 공통 — 일별 N일 등락률 Top20 HTML 조각 리스트.
    """
    section_bits: list[str] = []
    n_tail = int(last_trading_days)
    for n_win in periods:
        label = f'{int(n_win)}일'
        ret_w = px / px.shift(int(n_win)) - 1.0
        ret_w = ret_w.dropna(how='all')
        last_dates = ret_w.index[-n_tail:] if len(ret_w.index) >= 1 else []
        rows: list[dict] = []
        for d in last_dates:
            s = ret_w.loc[d].dropna()
            if s.empty:
                continue
            s = s.sort_values(ascending=False)
            top = s.head(20)
            row: dict = {'date': d.strftime('%Y-%m-%d')}
            for i in range(1, 21):
                if i <= len(top):
                    tkr = str(top.index[i - 1])
                    nm, tkr_disp = cell_name_ticker.get(tkr, ('', tkr))
                    v = float(top.iloc[i - 1])
                    row[f'Top{i}'] = (
                        f"{_safe_html(nm)}({_safe_html(tkr_disp)})<br>"
                        f"<span class='small'>{_fmt_pct(v * 100.0)}</span>"
                    )
                else:
                    row[f'Top{i}'] = ''
            rows.append(row)
        if rows:
            daily_top20_df = pd.DataFrame(rows).set_index('date')
            daily_top20_html = daily_top20_df.to_html(classes='tbl sortable-tbl', escape=False)
            section_bits.append(
                f'<h2 class="sec-title">최근 {n_tail}거래일 — 일별 {label} 등락률 Top20</h2>'
                f'<div class="sec-sub">행: 최근 {n_tail}거래일 · 열: Top1~Top20 · '
                f'각 행 날짜 종가 기준 <strong>{label} 전 거래일 종가 대비 등락률</strong></div>'
                f'{daily_top20_html}<div style="height:18px;"></div>'
            )
    return section_bits


def _build_sector_watchlist_nday_daily_top20_html(
    ohlcv_df: pd.DataFrame,
    watchlist: dict[str, str],
    periods: list[int],
    last_trading_days: int = 20,
) -> str:
    """
    관심종목 OHLCV만으로, 유니버스 탭과 동일한 **일별 5·10·20·50일(periods) 등락률 Top20** 섹션 HTML.
    행=거래일 · 열=Top1~Top20, 피벗 N일 수익률 = 유니버스와 동일 (종가/ N거래일 전 종가 - 1).
    """
    if watchlist is None or len(watchlist) == 0:
        return ''
    if ohlcv_df is None or ohlcv_df.empty:
        return '<p class="meta">OHLCV가 없어 일별 표를 만들 수 없습니다.</p>'

    wl_norm = {_norm_etf_ticker_key(k): str(k).strip() for k in watchlist.keys()}
    wl_names = {_norm_etf_ticker_key(k): watchlist[k] for k in watchlist.keys()}
    allow = set(wl_norm.keys())

    tmp = ohlcv_df[['ticker', 'date', 'close']].copy()
    tmp['ticker'] = tmp['ticker'].astype(str)
    tmp['_tkn'] = tmp['ticker'].map(_norm_etf_ticker_key)
    tmp = tmp[tmp['_tkn'].isin(allow)]
    if tmp.empty:
        return '<p class="meta">관심종목 OHLCV가 유니버스에 없습니다. ETF_CODE_NAME(또는 조회 범위)에 티커를 포함하세요.</p>'

    tmp['date'] = pd.to_datetime(tmp['date'], errors='coerce')
    tmp['close'] = pd.to_numeric(tmp['close'], errors='coerce')
    tmp = tmp.dropna(subset=['ticker', 'date', 'close'])
    if tmp.empty:
        return '<p class="meta">관심종목 OHLCV가 유니버스에 없습니다. ETF_CODE_NAME(또는 조회 범위)에 티커를 포함하세요.</p>'

    px = tmp.pivot_table(index='date', columns='ticker', values='close', aggfunc='last').sort_index()
    if px.empty:
        return '<p class="meta">관심종목 피벗 데이터가 없습니다.</p>'

    cell: dict[str, tuple[str, str]] = {}
    for tcol in px.columns:
        tcol_s = str(tcol)
        nk = _norm_etf_ticker_key(tcol_s)
        cell[tcol_s] = (wl_names.get(nk, ''), wl_norm.get(nk, tcol_s))

    try:
        bits = _nday_daily_top20_section_bits(px, list(periods), cell, last_trading_days=int(last_trading_days))
    except Exception:
        bits = []

    if not bits:
        return '<p class="meta">최근 구간에 유효한 일별 N거래일 등락률 데이터가 없습니다.</p>'

    n_tail = int(last_trading_days)
    per_s = '·'.join(str(int(p)) for p in periods)
    meta = _safe_html(
        f'관심종목 {len(watchlist)}개 · 최근 {n_tail}거래일 · '
        f'유니버스 탭과 동일 지표(각 거래일 종가 기준 {per_s}거래일 전 종가 대비 등락률, 관심종목 범위 내 Top20)'
    )
    return f'<p class="meta">{meta}</p>' + ''.join(bits)


def _compute_ma_above_flags(ohlcv_df: pd.DataFrame, window: int) -> pd.Series:
    """
    티커별 최신 종가가 최근 `window`거래일 종가 단순이동평균 이상인지 (종가 >= MA).
    거래일 window 미만이면 NaN.
    """
    col_name = f'{window}일선_상회'
    if ohlcv_df is None or ohlcv_df.empty or window < 1:
        return pd.Series(dtype=float, name=col_name)
    flags: dict[str, float] = {}
    for ticker, g in ohlcv_df.groupby('ticker', sort=False):
        g = g.sort_values('date')
        if len(g) < window:
            flags[str(ticker)] = np.nan
            continue
        c = pd.to_numeric(g['close'], errors='coerce')
        ma = float(c.tail(window).mean())
        last = float(c.iloc[-1])
        if pd.isna(ma) or pd.isna(last) or ma <= 0:
            flags[str(ticker)] = np.nan
        else:
            flags[str(ticker)] = 1.0 if last >= ma else 0.0
    return pd.Series(flags, name=col_name)


def _ohlcv_drop_last_trading_rows(ohlcv_df: pd.DataFrame, n: int) -> pd.DataFrame:
    """티커별 마지막 n개 거래일 행을 제거한 OHLCV (n거래일 전 시점 스냅샷용)."""
    if ohlcv_df is None or ohlcv_df.empty or n <= 0:
        return ohlcv_df.copy() if ohlcv_df is not None else pd.DataFrame()
    parts: list[pd.DataFrame] = []
    for ticker, g in ohlcv_df.groupby('ticker', sort=False):
        g = g.sort_values('date')
        if len(g) <= n:
            continue
        parts.append(g.iloc[:-n])
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _rank_map_by_sort_col(df: pd.DataFrame, sort_col: str, ticker_col: str = 'ticker') -> dict[str, float]:
    """sort_col 기준 내림차순(동률은 행 순서)으로 1..N 순위 맵."""
    if df is None or df.empty or sort_col not in df.columns:
        return {}
    d = df.sort_values(sort_col, ascending=False, na_position='last').reset_index(drop=True)
    return {str(d.iloc[i][ticker_col]): float(i + 1) for i in range(len(d))}


def _html_universe_avg_nday_returns_table(px: pd.DataFrame, periods: list[int], last_trading_days: int = 20) -> str:
    """
    유니버스 전 종목 피벗(px) 기준, 최근 `last_trading_days` 거래일 각각에 대해
    N일 등락률(%)의 횡단면 산술평균 + 그날의 창별 평균의 산술평균(평균등락률) 표 HTML.
    """
    px = px.sort_index()
    n_tail = max(1, int(last_trading_days))
    if px.shape[0] < 2 or not periods:
        return ''
    rets = {int(n): px / px.shift(int(n)) - 1.0 for n in periods}
    last_dates = px.index[-n_tail:] if len(px.index) >= n_tail else px.index
    rows: list[dict] = []
    for d in last_dates:
        row: dict = {'거래일': d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10]}
        acc: list[float] = []
        for n_win in periods:
            key = int(n_win)
            if d not in rets[key].index:
                row[f'{key}일평균(%)'] = np.nan
                continue
            s = rets[key].loc[d].dropna()
            m = float(s.mean()) * 100.0 if not s.empty else np.nan
            row[f'{key}일평균(%)'] = m
            if pd.notna(m):
                acc.append(m)
        row['평균등락률(%)'] = float(np.mean(acc)) if acc else np.nan
        rows.append(row)
    out = pd.DataFrame(rows)
    cols = ['거래일'] + [f'{int(n)}일평균(%)' for n in periods] + ['평균등락률(%)']
    cols = [c for c in cols if c in out.columns]
    out = out[cols]
    th_list = []
    for c in out.columns:
        if c == '거래일':
            th_list.append(f'<th>{_safe_html(c)}</th>')
        else:
            th_list.append(f'<th class="col-return">{_safe_html(c)}</th>')
    th = ''.join(th_list)
    trs: list[str] = []
    for _, r in out.iterrows():
        tds: list[str] = []
        for c in out.columns:
            v = r[c]
            if c == '거래일':
                tds.append(f'<td>{_safe_html(str(v))}</td>')
            else:
                try:
                    fv = float(v)
                    cls = _momentum_color_class(fv) if pd.notna(fv) else 'm-na'
                    disp = _fmt_pct(fv) if pd.notna(fv) else ''
                except (TypeError, ValueError):
                    cls = ''
                    disp = ''
                tds.append(f'<td class="num {cls}">{_safe_html(disp)}</td>')
        trs.append('<tr>' + ''.join(tds) + '</tr>')
    per_s = '·'.join(str(int(p)) for p in periods)
    cap = f'<h2 class="sec-title">최근 {n_tail}거래일 — 유니버스 일별 평균 등락률</h2>'
    sub = (
        f'<div class="sec-sub">행: 최근 {n_tail}거래일 · 각 거래일마다 전 종목(값 있는 종목)의 '
        f'<strong>{per_s}거래일 등락률</strong> 산술평균(%) · 마지막 열은 창별 평균의 산술평균입니다.</div>'
    )
    return cap + sub + f'<table class="tbl sortable-tbl"><thead><tr>{th}</tr></thead><tbody>' + ''.join(trs) + '</tbody></table><div style="height:18px;"></div>'


def _html_sector_buy_candidate_like(df: pd.DataFrame) -> str:
    """대시보드 매수후보 표와 동일한 tbl·%·히트맵 규칙(코스피 비교 없음)."""
    if df is None or df.empty:
        return '<p class="meta">표시할 데이터가 없습니다.</p>'
    d = df.copy()
    _heat_extra = {
        '평균_모멘텀', '평균_모멘텀_2', 'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
    }
    for _suf, _ in MOMENTUM_DEFERRED_LAGS:
        _heat_extra.add(f'평균_모멘텀{_suf}')
        _heat_extra.add(f'평균_모멘텀_2{_suf}')
    heat_cols_set = {
        c for c in d.columns
        if c in _heat_extra
        or c.endswith('_상승률')
        or any(c.endswith(f'_상승률{_suf}') for _suf, _ in MOMENTUM_DEFERRED_LAGS)
    }
    pct_cols_set = set(heat_cols_set)
    align_right = pct_cols_set | {'현재가', 'ATR14', '순위'}
    th_list = []
    for c in d.columns:
        if c == '종목명':
            th_list.append(f'<th class="col-name">{_safe_html(c)}</th>')
        elif c in align_right:
            th_list.append(f'<th class="col-return">{_safe_html(c)}</th>')
        else:
            th_list.append(f'<th>{_safe_html(c)}</th>')
    th = ''.join(th_list)
    trs: list[str] = []
    for _, r in d.iterrows():
        tds: list[str] = []
        for c in d.columns:
            v = r.get(c, '')
            cls = ''
            disp = v
            if c in pct_cols_set:
                disp = _fmt_pct(v)
                if c in heat_cols_set:
                    try:
                        cls = _momentum_color_class(float(v)) if pd.notna(v) else 'm-na'
                    except (TypeError, ValueError):
                        cls = ''
            elif c == '현재가':
                try:
                    fv = float(v)
                    disp = f'{fv:,.0f}' if pd.notna(fv) else ''
                except (TypeError, ValueError):
                    disp = str(v) if v != '' else ''
            elif c == 'ATR14':
                try:
                    fv = float(v)
                    disp = f'{int(round(fv)):,}' if pd.notna(fv) else ''
                except (TypeError, ValueError):
                    disp = str(v) if v != '' else ''
            elif c == '순위':
                try:
                    disp = str(int(v)) if pd.notna(v) else ''
                except (TypeError, ValueError):
                    disp = str(v) if v != '' else ''
            td_cls_parts = []
            if c == '종목명':
                td_cls_parts.append('col-name')
            if c in align_right:
                td_cls_parts.append('num')
            if cls:
                td_cls_parts.append(cls)
            td_cls = ' '.join(td_cls_parts)
            disp_s = disp if isinstance(disp, str) else str(disp)
            tds.append(f'<td class="{td_cls}">{_safe_html(disp_s)}</td>')
        trs.append('<tr>' + ''.join(tds) + '</tr>')
    _tbl_ex = ' tbl-name-wide' if '종목명' in d.columns else ''
    return f'<table class="tbl sortable-tbl{_tbl_ex}"><thead><tr>{th}</tr></thead><tbody>' + ''.join(trs) + '</tbody></table>'


def _action_list_column_order(merged: pd.DataFrame, momentum_basis: str = 't0') -> list[str]:
    """`_build_action_list` 출력과 동일한 열 순서. momentum_basis 't3'|'t5'이면 해당 접미사 모멘텀 열."""
    mb = str(momentum_basis).lower()
    if mb == 't5':
        suf = '_T5'
    elif mb == 't3':
        suf = '_T3'
    else:
        suf = None
    avg_col = (f'평균_모멘텀{suf}' if suf else '평균_모멘텀')
    cols = ['순위', 'ticker', '종목명', '현재가', avg_col]
    for p in [5, 10, 20, 50, 120]:
        c = (f'{p}일_상승률{suf}' if suf else f'{p}일_상승률')
        if c in merged.columns:
            cols.append(c)
    cols += [
        'weekly_return_pct',
        '당일 수익률',
        '3일 수익률',
        'MA5위', 'MA10위', 'MA20위', 'MA50위',
        'MA10연속추세',
        'ATR14',
        'ATR14_일변동률(%)',
    ]
    return [c for c in cols if c in merged.columns]


def _sector_watchlist_display_cols_fallback(momentum_basis: str = 't0') -> list[str]:
    """`mf_merge`가 비었을 때 관심종목 대시보드 표에 쓸 기본 열 순서."""
    mb = str(momentum_basis).lower()
    if mb == 't5':
        suf = '_T5'
    elif mb == 't3':
        suf = '_T3'
    else:
        suf = None
    avg_col = (f'평균_모멘텀{suf}' if suf else '평균_모멘텀')
    cols = ['순위', 'ticker', '종목명', '현재가', avg_col]
    for p in [5, 10, 20, 50, 120]:
        cols.append(f'{p}일_상승률{suf}' if suf else f'{p}일_상승률')
    cols += [
        'weekly_return_pct',
        '당일 수익률',
        '3일 수익률',
        'MA5위', 'MA10위', 'MA20위', 'MA50위',
        'MA10연속추세',
        'ATR14',
        'ATR14_일변동률(%)',
    ]
    return cols


def _watchlist_dashboard_df_enriched(
    merged_full: pd.DataFrame,
    ohlcv_df: pd.DataFrame,
    watchlist: dict[str, str],
    display_cols: list[str],
) -> pd.DataFrame:
    """
    관심종목 dict 키 순서로 행을 나열합니다. `merged_full`에 없으면(모멘텀 미산출 등)
    OHLCV·MA 열만 채운 스텁 행을 넣어 누락을 막습니다.

    `순위` 열: `merged_full`이 있으면 **전체 유니버스 기준** `merged_full`의 정렬 열(평균_모멘텀·`평균_모멘텀_T5`·`평균_모멘텀_T3` 등)에 따른 `순위` 값,
    매칭되지 않으면 NaN(표에서는 빈 칸에 가깝게 표시).
    """
    if not watchlist:
        return pd.DataFrame(columns=display_cols)

    ma_full = pd.DataFrame()
    if ohlcv_df is not None and not ohlcv_df.empty:
        try:
            ma_full = _compute_last_close_and_sma(ohlcv_df, window=[5, 10, 20, 50])
        except Exception:
            ma_full = pd.DataFrame()

    def _ma_y_from_row(mr, col):
        if mr is None or col not in mr.index:
            return ''
        v = mr[col]
        return 'Y' if v == True else ''

    rows: list[dict] = []
    for tk_raw in watchlist.keys():
        disp_tk = str(tk_raw).strip()
        nm = str(watchlist.get(tk_raw, '') or '').strip() or disp_tk
        nk = _norm_etf_ticker_key(disp_tk)

        base: dict = {c: np.nan for c in display_cols}
        base['ticker'] = disp_tk
        base['종목명'] = nm

        matched = pd.DataFrame()
        if merged_full is not None and not merged_full.empty and 'ticker' in merged_full.columns:
            msk = merged_full['ticker'].astype(str).map(_norm_etf_ticker_key) == nk
            matched = merged_full.loc[msk]
        if not matched.empty:
            r0 = matched.iloc[0]
            for c in display_cols:
                if c in r0.index:
                    base[c] = r0[c]
            base['ticker'] = disp_tk
            if '종목명' in display_cols:
                base['종목명'] = nm
        else:
            mr = None
            if not ma_full.empty and 'ticker' in ma_full.columns:
                mma = ma_full[ma_full['ticker'].astype(str).map(_norm_etf_ticker_key) == nk]
                if not mma.empty:
                    mr = mma.iloc[0]
            if mr is not None:
                if '현재가' in display_cols and 'last_close' in mr.index:
                    base['현재가'] = pd.to_numeric(mr.get('last_close'), errors='coerce')
                for k in ('weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14', 'MA10연속추세', 'ATR14_일변동률(%)'):
                    if k in display_cols and k in mr.index:
                        base[k] = mr[k]
                if 'MA5위' in display_cols:
                    base['MA5위'] = _ma_y_from_row(mr, 'above_sma_5')
                if 'MA10위' in display_cols:
                    base['MA10위'] = _ma_y_from_row(mr, 'above_sma_10')
                if 'MA20위' in display_cols:
                    base['MA20위'] = _ma_y_from_row(mr, 'above_sma_20')
                if 'MA50위' in display_cols:
                    base['MA50위'] = _ma_y_from_row(mr, 'above_sma_50')
        rows.append({c: base.get(c, np.nan) for c in display_cols})

    return pd.DataFrame(rows, columns=display_cols)


def _flatten_sector_etf_dict_for_ohlcv(sector_map: dict) -> dict[str, str]:
    """`sector_etf_dict`(섹터명→{티커:종목명})를 OHLCV 조회용 단일 {티커:종목명}으로 펼칩니다."""
    out: dict[str, str] = {}
    if not isinstance(sector_map, dict):
        return out
    for _sec, tickers in sector_map.items():
        if not isinstance(tickers, dict):
            continue
        for tk, nm in tickers.items():
            k = str(tk).strip()
            if not k:
                continue
            out[k] = str(nm).strip() or k
    return out


def _norm_ticker_to_sector_from_sector_etf_dict(sector_map: dict) -> dict[str, str]:
    """티커(`_norm_etf_ticker_key`) → 첫 등장 섹터명."""
    m: dict[str, str] = {}
    if not isinstance(sector_map, dict):
        return m
    for sec, tickers in sector_map.items():
        if not isinstance(tickers, dict):
            continue
        sn = str(sec).strip()
        for tk in tickers.keys():
            nk = _norm_etf_ticker_key(tk)
            if nk and nk not in m:
                m[nk] = sn
    return m


SECTOR_ETF_MAP_ACCORDION_CSS = """
<style>
details.sector-map-acc { margin-bottom: 10px; border: 1px solid #e2e8f0; border-radius: 10px; background: #fff; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
details.sector-map-acc > summary.sector-map-acc-sum { list-style: none; cursor: pointer; display: grid; grid-template-columns: 52px minmax(120px,1.2fr) 120px 80px; gap: 10px; align-items: center; padding: 10px 14px; font-size: 0.88rem; color: #2d3748; background: #f7fafc; }
details.sector-map-acc > summary.sector-map-acc-sum::-webkit-details-marker { display: none; }
.sector-map-acc-body { padding: 8px 12px 14px; border-top: 1px solid #edf2f7; background: #fcfdff; }
.sector-map-acc-body table.tbl { margin-top: 4px; }
</style>
"""


def _sector_map_constituent_subframe(
    sector_name: str,
    sector_map: dict,
    enriched_ranked: pd.DataFrame,
    rank_col: str,
) -> pd.DataFrame:
    """섹터명에 해당하는 구성 종목 표(모멘텀 열·현재가 등)."""
    if not sector_name or not isinstance(sector_map, dict):
        return pd.DataFrame()
    tdict = sector_map.get(sector_name)
    if not isinstance(tdict, dict):
        return pd.DataFrame()
    norms = {_norm_etf_ticker_key(t) for t in tdict}
    sub = enriched_ranked[
        enriched_ranked['ticker'].astype(str).map(_norm_etf_ticker_key).isin(norms)
    ].copy()
    if sub.empty:
        return pd.DataFrame()
    sub = sub.sort_values(rank_col, ascending=False, na_position='last').reset_index(drop=True)
    sub.insert(0, '구성순위', np.arange(1, len(sub) + 1, dtype=int))
    cols = ['구성순위', 'ticker', '종목명', rank_col]
    if '현재가' in sub.columns:
        cols.append('현재가')
    return sub[[c for c in cols if c in sub.columns]].copy()


def _html_sector_rank_accordion_block(
    sec_df: pd.DataFrame,
    sector_map: dict,
    enriched_ranked: pd.DataFrame,
    rank_col: str,
    compare_kospi_map: dict | None,
    mb_tag: str,
) -> str:
    """섹터 순위를 `<details>` 행으로 — 요약 행 클릭 시 구성 종목 표."""
    kmap = compare_kospi_map if isinstance(compare_kospi_map, dict) else {}
    if sec_df is None or sec_df.empty:
        return _df_html(sec_df, compare_kospi_map=kmap)

    parts: list[str] = []
    for i, (_, srow) in enumerate(sec_df.iterrows()):
        sn = str(srow.get('섹터', '') or '').strip()
        rk = srow.get('순위')
        try:
            rk_s = str(int(rk)) if pd.notna(rk) else '—'
        except (TypeError, ValueError):
            rk_s = str(rk) if rk != '' else '—'
        mom_v = srow.get(rank_col)
        mom_disp = _fmt_pct(mom_v) if pd.notna(mom_v) else '—'
        mom_cls = ''
        try:
            if pd.notna(mom_v):
                mom_cls = _momentum_color_class(float(mom_v))
                kref = kmap.get(rank_col)
                if kref is not None and pd.notna(kref):
                    ev, kv = float(mom_v), float(kref)
                    if pd.notna(ev) and pd.notna(kv):
                        if ev > kv:
                            mom_cls = 'kospi-beat'
                        elif ev < kv:
                            mom_cls = 'kospi-miss'
        except (TypeError, ValueError):
            mom_cls = ''
        n_ok = srow.get('유효종목수', '')
        try:
            n_s = str(int(n_ok)) if pd.notna(n_ok) else '—'
        except (TypeError, ValueError):
            n_s = str(n_ok)

        sub_df = _sector_map_constituent_subframe(sn, sector_map, enriched_ranked, rank_col)
        if sub_df is None or sub_df.empty:
            inner = '<p class="meta">구성 종목 표시할 데이터가 없습니다.</p>'
        else:
            inner = _df_html(
                sub_df,
                pct_cols=[c for c in (rank_col,) if c in sub_df.columns],
                heatmap_cols=[rank_col] if rank_col in sub_df.columns else [],
                compare_kospi_map=kmap,
            )

        _id_attr = f'sector-map-acc-{mb_tag}-{i}'
        parts.append(
            f'<details class="sector-map-acc" id="{_id_attr}">'
            '<summary class="sector-map-acc-sum">'
            f'<span class="t-num">{_safe_html(rk_s)}</span>'
            f'<span>{_safe_html(sn)}</span>'
            f'<span class="t-num {mom_cls}">{_safe_html(mom_disp)}</span>'
            f'<span class="t-num">{_safe_html(n_s)}</span>'
            '</summary>'
            f'<div class="sector-map-acc-body">{inner}</div>'
            '</details>'
        )
    return ''.join(parts)


def _html_sector_etf_dictionary_rankings_panel(
    mom_ap: pd.DataFrame,
    ohlcv_df: pd.DataFrame,
    sector_map: dict,
    compare_kospi_map: dict | None,
) -> str:
    """
    `sector_etf_dict` 유니버스에 대해 T-0·T-3·T-5 각각
    (1) 섹터 평균 모멘텀 순위(행 클릭 시 구성 종목 펼침) (2) 포트폴리오 수익률 상위 10
    (3) 섹터별 최고 종목 순위 (4) 전체 종목 순위 표를 생성합니다.
    열 구성·포맷은 관심종목 탭의 매수 후보 표(`_df_html`·`_action_list_column_order`)와 동일 계열입니다.
    """
    flat = _flatten_sector_etf_dict_for_ohlcv(sector_map)
    if not flat:
        return '<p class="no-data">sector_etf_dict 가 비어 있습니다.</p>'
    if mom_ap is None or mom_ap.empty or 'ticker' not in mom_ap.columns:
        return '<p class="meta">모멘텀 데이터가 없어 섹터 ETF 맵 분석을 표시할 수 없습니다.</p>'
    if ohlcv_df is None or ohlcv_df.empty:
        return '<p class="meta">OHLCV가 없어 섹터 ETF 맵 분석을 표시할 수 없습니다.</p>'

    tsec = _norm_ticker_to_sector_from_sector_etf_dict(sector_map)
    keys_norm = {_norm_etf_ticker_key(t) for t in flat}
    mom_s = mom_ap[mom_ap['ticker'].astype(str).map(_norm_etf_ticker_key).isin(keys_norm)].copy()
    ohlcv_s = ohlcv_df[ohlcv_df['ticker'].astype(str).map(_norm_etf_ticker_key).isin(keys_norm)].copy()
    if mom_s.empty:
        return '<p class="meta">sector_etf_dict 티커에 해당하는 모멘텀 행이 없습니다.</p>'

    kmap = compare_kospi_map if isinstance(compare_kospi_map, dict) else {}
    bits: list[str] = [
        '<div class="watch-dash-summary">',
        SECTOR_ETF_MAP_ACCORDION_CSS,
        '<p class="meta">',
        _safe_html(
            '아래 `sector_etf_dict`에 정의된 섹터·종목만 대상입니다. '
            '섹터 수치는 해당 섹터에 속한 종목의 모멘텀 열 산술평균(유효 종목만)입니다. '
            '섹터별 대표 종목은 동일 열 기준 최고값 1종목입니다. '
            '전체 순위는 딕셔너리에 포함된 전 종목을 같은 기준으로 정렬한 결과입니다. '
            '각 T 구간에서 섹터 순위 다음에 「포트폴리오 수익률 … 상위 10」이 이어집니다.'
        ),
        '</p>',
    ]

    blocks: list[tuple[str, str, str, list[str]]] = [
        (
            't0',
            'T-0 기준',
            '평균_모멘텀',
            [
                '평균_모멘텀', '5일_상승률', '10일_상승률', '20일_상승률', '50일_상승률', '120일_상승률',
                'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
            ],
        ),
        (
            't3',
            'T-3 기준',
            '평균_모멘텀_T3',
            [
                '평균_모멘텀_T3', '5일_상승률_T3', '10일_상승률_T3', '20일_상승률_T3', '50일_상승률_T3', '120일_상승률_T3',
                'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
            ],
        ),
        (
            't5',
            'T-5 기준',
            '평균_모멘텀_T5',
            [
                '평균_모멘텀_T5', '5일_상승률_T5', '10일_상승률_T5', '20일_상승률_T5', '50일_상승률_T5', '120일_상승률_T5',
                'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
            ],
        ),
    ]

    for mb, title_h, rank_col, pct_cols in blocks:
        if rank_col not in mom_s.columns:
            bits.append('<div class="section-part">' + _safe_html(title_h) + '</div>')
            bits.append(
                f'<p class="meta">{_safe_html(rank_col)} 열이 없어 이 구간을 건너뜁니다.</p>'
            )
            continue

        m_full = _build_full_momentum_ma_merge(mom_s, ohlcv_s, rank_col=rank_col)
        if m_full is None or m_full.empty:
            display_cols = _sector_watchlist_display_cols_fallback(mb)
        else:
            display_cols = _action_list_column_order(m_full, momentum_basis=mb)

        enriched = _watchlist_dashboard_df_enriched(m_full, ohlcv_s, flat, display_cols)
        if enriched is None or enriched.empty:
            bits.append('<div class="section-part">' + _safe_html(title_h) + '</div>')
            bits.append('<p class="meta">표시할 행이 없습니다.</p>')
            continue

        enriched = enriched.copy()
        enriched['섹터'] = enriched['ticker'].astype(str).map(lambda t: tsec.get(_norm_etf_ticker_key(t), ''))
        rest_cols = [c for c in display_cols if c != '순위']
        full_cols = ['순위', '섹터'] + rest_cols
        full_cols = [c for c in full_cols if c in enriched.columns]

        enriched_ranked = enriched.sort_values(rank_col, ascending=False, na_position='last').reset_index(drop=True)
        enriched_ranked['순위'] = np.arange(1, len(enriched_ranked) + 1, dtype=int)
        full_tbl = enriched_ranked[[c for c in full_cols if c in enriched_ranked.columns]].copy()

        sec_rows: list[dict] = []
        if isinstance(sector_map, dict):
            for sec, tdict in sector_map.items():
                if not isinstance(tdict, dict):
                    continue
                norms = {_norm_etf_ticker_key(t) for t in tdict}
                sub_e = enriched[enriched['ticker'].astype(str).map(_norm_etf_ticker_key).isin(norms)]
                vals = pd.to_numeric(sub_e[rank_col], errors='coerce')
                sec_rows.append(
                    {
                        '섹터': str(sec).strip(),
                        rank_col: float(vals.mean()) if vals.notna().any() else np.nan,
                        '유효종목수': int(vals.notna().sum()),
                    }
                )
        sec_df = pd.DataFrame(sec_rows)
        if not sec_df.empty:
            sec_df = sec_df.sort_values(rank_col, ascending=False, na_position='last').reset_index(drop=True)
            sec_df.insert(0, '순위', np.arange(1, len(sec_df) + 1, dtype=int))
            sec_tbl_cols = ['순위', '섹터', rank_col, '유효종목수']
            sec_tbl = sec_df[[c for c in sec_tbl_cols if c in sec_df.columns]].copy()
        else:
            sec_tbl = pd.DataFrame()

        winners: list[pd.Series] = []
        if isinstance(sector_map, dict):
            for sec, tdict in sector_map.items():
                if not isinstance(tdict, dict):
                    continue
                norms = {_norm_etf_ticker_key(t) for t in tdict}
                sub_r = enriched_ranked[enriched_ranked['ticker'].astype(str).map(_norm_etf_ticker_key).isin(norms)]
                if sub_r.empty:
                    continue
                sub_ok = sub_r[sub_r[rank_col].notna()]
                if not sub_ok.empty:
                    pick = sub_ok.iloc[0]
                else:
                    pick = sub_r.iloc[0]
                winners.append(pick)
        if winners:
            win_df = pd.DataFrame(winners)
            win_df = win_df.sort_values(rank_col, ascending=False, na_position='last').reset_index(drop=True)
            win_df['순위'] = np.arange(1, len(win_df) + 1, dtype=int)
            wcols = ['순위', '섹터'] + [c for c in rest_cols if c in win_df.columns]
            wcols = [c for c in wcols if c in win_df.columns]
            best_tbl = win_df[wcols].copy()
        else:
            best_tbl = pd.DataFrame()

        heat_extra = {rank_col, 'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)'}
        for _p in (5, 10, 20, 50, 120):
            c = f'{_p}일_상승률' if mb == 't0' else f'{_p}일_상승률_{"T5" if mb == "t5" else "T3"}'
            heat_extra.add(c)
        heat_full = {c for c in full_tbl.columns if c in heat_extra or c.endswith('_상승률') or '_상승률_' in str(c)}
        heat_best = {c for c in best_tbl.columns if c in heat_extra or c.endswith('_상승률') or '_상승률_' in str(c)}

        bits.append('<div class="section-part">' + _safe_html(title_h) + '</div>')
        bits.append('<div class="section">')
        bits.append('<h3>섹터 순위 (섹터 내 종목 모멘텀 평균)</h3>')
        bits.append(
            '<p class="meta">순위 행(헤더: 순위·섹터·모멘텀·유효종목수)을 클릭하면 해당 섹터 구성 종목이 펼쳐집니다.</p>'
        )
        bits.append(
            _html_sector_rank_accordion_block(sec_tbl, sector_map, enriched_ranked, rank_col, kmap, mb)
        )
        bits.append('</div>')

        bits.append('<div class="section">')
        bits.append('<h3>포트폴리오 수익률 (5거래일 전 매수 → 평가일, 상위 10)</h3>')
        try:
            _dts_ev = pd.to_datetime(ohlcv_s['date'], errors='coerce').dropna()
            eval_d_pf = (
                pd.Timestamp(_dts_ev.max()).normalize().date()
                if len(_dts_ev)
                else date.today()
            )
            purchase_pf = _prev_trading_date(eval_d_pf, n_trading_days=5).strftime('%Y-%m-%d')
            eval_pf = eval_d_pf.strftime('%Y-%m-%d')
            m10_df, m10_det = _compute_top7_momentum_5d_portfolio_returns(
                eval_d_pf,
                min_trading_days=None,
                ohlcv_universe=ohlcv_s,
                name_by_ticker=flat,
                momentum_basis=mb,
                top_n=10,
            )
            port_inner = _html_momentum7_scorecard(m10_df, m10_det)
            scope_pf = (
                '매수·평가·선정 규칙은 관심 탭의 「포트폴리오 수익률 (5거래일 전 매수 → 평가일, 상위 7)」과 동일하며, '
                '여기서만 구성 종목 수를 10으로 늘렸습니다.'
            )
            bits.append(
                '<div class="meta">'
                + _safe_html(f'매수일: {purchase_pf} · 평가일: {eval_pf} · ')
                + _safe_html(scope_pf)
                + '</div>'
            )
            bits.append(port_inner)
        except Exception:
            bits.append('<p class="meta">포트폴리오 수익률(상위 10)을 계산하지 못했습니다.</p>')
        bits.append('</div>')

        bits.append('<div class="section">')
        bits.append('<h3>섹터별 최고 수익률(모멘텀) 종목 순위</h3>')
        bits.append(
            _df_html(
                best_tbl,
                pct_cols=[c for c in pct_cols if c in best_tbl.columns],
                heatmap_cols=list(heat_best),
                compare_kospi_map=kmap,
            )
        )
        bits.append('</div>')
        bits.append('<div class="section">')
        bits.append('<h3>전체 종목 순위 (sector_etf_dict)</h3>')
        bits.append(
            _df_html(
                full_tbl,
                pct_cols=[c for c in pct_cols if c in full_tbl.columns],
                heatmap_cols=list(heat_full),
                compare_kospi_map=kmap,
            )
        )
        bits.append('</div>')

    bits.append('</div>')
    return ''.join(bits)


def build_etf_code_name_returns_dataframe(
    code_name: dict[str, str] | None = None,
    end_date=None,
    periods=(5, 10, 20, 50),
    extra_ohlcv_universe: dict[str, str] | None = None,
):
    """
    ETF_CODE_NAME(또는 지정 dict)에 대해 DB OHLCV 기준 거래일 N일 등락률(%)을 계산합니다.
    최신 종가 대비 N거래일 전 종가 변화율이며, calculate_momentum_returns와 동일 로직입니다.
    추가: 5·10일선(종가≥MA) 상회, 5거래일 전 시점 동일 기준 순위(전주순위) 및 순위 개선 여부(순위_상승).

    `extra_ohlcv_universe`: `code_name`에 없어도 OHLCV를 함께 조회할 티커(이름) dict.
        관심종목 전용 일별 등락률 등 HTML에서 `attrs['ohlcv_df']`에 포함시키기 위해 사용합니다.
        반환 DataFrame의 행 구성·정렬 기준은 여전히 `code_name`만 사용합니다.
    모멘텀·전주순위 계산은 ETF 모멘텀 대시보드와 동일하게 `MOMENTUM_DASHBOARD_MIN_TRADING_DAYS`(20거래일) 최소 봉을 씁니다.
    """
    if code_name is None:
        code_name = ETF_CODE_NAME
    period_list = list(periods)
    if end_date is None:
        end_d = date.today()
    elif isinstance(end_date, str):
        end_d = datetime.strptime(end_date, '%Y-%m-%d').date()
    elif isinstance(end_date, datetime):
        end_d = end_date.date()
    elif isinstance(end_date, date):
        end_d = end_date
    else:
        end_d = date.today()
    start_d = end_d - timedelta(days=OHLCV_LOOKBACK_CALENDAR_DAYS)
    start_s = start_d.strftime('%Y-%m-%d')
    end_s = end_d.strftime('%Y-%m-%d')

    fetch_keys: list[str] = []
    seen: set[str] = set()
    for tk in list(code_name.keys()):
        k = str(tk).strip()
        if k and k not in seen:
            seen.add(k)
            fetch_keys.append(k)
    if extra_ohlcv_universe:
        for tk in list(extra_ohlcv_universe.keys()):
            k = str(tk).strip()
            if k and k not in seen:
                seen.add(k)
                fetch_keys.append(k)

    parts: list[pd.DataFrame] = []
    for ticker in tqdm(fetch_keys, desc='ETF OHLCV 조회'):
        df = get_etf_ohlcv_from_db(ticker=ticker, start_date=start_s, end_date=end_s)
        if df is not None and not df.empty:
            parts.append(df)
    ohlcv_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    mom = calculate_momentum_returns(
        ohlcv_df, periods=period_list, min_trading_days=MOMENTUM_DASHBOARD_MIN_TRADING_DAYS
    )
    ohlcv_5ago = _ohlcv_drop_last_trading_rows(ohlcv_df, 5)
    mom_prev = (
        calculate_momentum_returns(
            ohlcv_5ago, periods=period_list, min_trading_days=MOMENTUM_DASHBOARD_MIN_TRADING_DAYS
        )
        if not ohlcv_5ago.empty
        else pd.DataFrame()
    )

    base = pd.DataFrame({'ticker': list(code_name.keys())})
    if mom is None or mom.empty:
        out = base.copy()
        for p in period_list:
            out[f'{p}일_상승률'] = np.nan
            for _suf, _ in MOMENTUM_DEFERRED_LAGS:
                out[f'{p}일_상승률{_suf}'] = np.nan
        out['latest_date'] = pd.NaT
        out['latest_close'] = np.nan
    else:
        merge_cols = ['ticker', 'latest_date', 'latest_close'] + [f'{p}일_상승률' for p in period_list]
        for _suf, _ in MOMENTUM_DEFERRED_LAGS:
            merge_cols += [f'{p}일_상승률{_suf}' for p in period_list]
        merge_cols = [c for c in merge_cols if c in mom.columns]
        out = base.merge(mom[merge_cols], on='ticker', how='left')

    sort_col = 'ticker'
    for preferred in ('20일_상승률', '50일_상승률', '10일_상승률', '5일_상승률'):
        if not mom.empty and preferred in mom.columns:
            sort_col = preferred
            break
        if preferred in out.columns:
            sort_col = preferred
            break

    prev_rank_map: dict[str, float] = {}
    if not mom_prev.empty and sort_col in mom_prev.columns and sort_col != 'ticker':
        prev_rank_map = _rank_map_by_sort_col(mom_prev, sort_col)

    out['ticker'] = out['ticker'].astype(str)
    out['전주순위'] = out['ticker'].map(prev_rank_map)
    ma5 = _compute_ma_above_flags(ohlcv_df, 5)
    ma10 = _compute_ma_above_flags(ohlcv_df, 10)
    out['5일선_상회'] = out['ticker'].map(ma5)
    out['10일선_상회'] = out['ticker'].map(ma10)
    out['종목명'] = out['ticker'].map(code_name)

    out = out.sort_values(sort_col, ascending=False, na_position='last').reset_index(drop=True)
    out.insert(0, '순위', np.arange(1, len(out) + 1, dtype=int))
    pv = pd.to_numeric(out['전주순위'], errors='coerce')
    cv = pd.to_numeric(out['순위'], errors='coerce')
    out['순위_상승'] = np.where(
        pv.notna() & cv.notna(),
        np.where(pv > cv, 1.0, 0.0),
        np.nan,
    )
    # (HTML 확장용) 동일 호출 내에서 이미 조회한 OHLCV를 재활용하기 위해 attrs로 보관
    try:
        out.attrs['ohlcv_df'] = ohlcv_df.copy()
    except Exception:
        pass
    return out


def save_etf_code_name_returns_html(
    code_name: dict[str, str] | None = None,
    end_date=None,
    periods=(5, 10, 20, 50),
    html_path=None,
    open_browser=True,
    print_table=True,
    daily_rank_watchlist: dict[str, str] | None = None,
    daily_rank_last_days: int = 20,
):
    """
    ETF_CODE_NAME 종목 관련 HTML을 저장합니다.
    유니버스 탭: 최근 N거래일 **일별 평균 등락률**(5·10·20·50일 횡단면 평균 + 평균등락률),
    **5거래일 전 모멘텀 상위 7 보유수익**(T-0·T-3·T-5 등 선정, 수익률은 평가일 종가 공통; `code_name` 유니버스 OHLCV만 사용),
    **매수후보**(대시보드와 동일 규칙·열, 유니버스는 `code_name`),
    이후 일별 N일 등락률 Top20 표.
    `daily_rank_watchlist`가 None(기본)일 때: **관심 섹터**·**관심 그룹**·**관심 액티브ETF**·**섹터 ETF 맵 순위**·**관심 패시브ETF** 일별 등락률 순위
    탭으로 나누어, 각각 **대시보드(T-0·T-3·T-5 모멘텀 기준 각 1표)**·**5거래일 전 매수→5거래일 보유 수익률**·**일별 등락률 순위**를
    (`SECTOR_MOMENTUM_DAILY_RANK_SECTOR_ETFS` / `SECTOR_MOMENTUM_DAILY_RANK_GROUP_ETFS` /
    `SECTOR_MOMENTUM_DAILY_RANK_Actives_ETFS` / `SECTOR_MOMENTUM_DAILY_RANK_Passives_ETFS` 및 `sector_etf_dict` OHLCV) 기준으로 표시합니다.
    `daily_rank_watchlist`에 dict를 넘기면 기존처럼 단일 **관심종목 일별 등락률 순위** 탭으로 표시합니다.
    `daily_rank_watchlist`가 빈 dict면 유니버스만 단일 페이지로 표시합니다.
    관심 목록 dict에만 있고 `code_name`에 없는 티커도 **OHLCV는 함께 조회**하여 일별 등락률·관심 대시보드에 반영합니다.
    `sector_etf_dict`에 정의된 티커는 **추가 OHLCV 유니버스**로 항상 병합 조회되며, 기본 분할 탭 모드일 때
    **[섹터 ETF 맵 순위]** 탭에서 해당 딕셔너리만 대상으로 한 섹터 평균·섹터별 최강 종목·전체 종목 순위(T-0·T-3·T-5)를 표시합니다.
    유니버스 메인 탭의 전체 종목 순위표 생략 규칙은 그대로입니다.

    `daily_rank_last_days`: 일별 Top20·평균등락률 표에 쓰는 **최근 거래일 행 수**(기본 20).

    Returns:
        str | None: 저장된 파일 경로, 실패 시 None
    """
    if code_name is None:
        code_name = ETF_CODE_NAME
    period_list = list(periods)
    _ltd = max(1, int(daily_rank_last_days))
    use_default_split_watchlists = daily_rank_watchlist is None
    if use_default_split_watchlists:
        wl_combined: dict[str, str] = {
            **SECTOR_MOMENTUM_DAILY_RANK_SECTOR_ETFS,
            **SECTOR_MOMENTUM_DAILY_RANK_GROUP_ETFS,
            **SECTOR_MOMENTUM_DAILY_RANK_Actives_ETFS,
            **SECTOR_MOMENTUM_DAILY_RANK_Passives_ETFS,
        }
    else:
        _wl_in = daily_rank_watchlist
        if not isinstance(_wl_in, dict):
            _wl_in = {}
        wl_combined = dict(_wl_in)
    _sector_etf_dict_flat_for_fetch = _flatten_sector_etf_dict_for_ohlcv(sector_etf_dict)
    if _sector_etf_dict_flat_for_fetch:
        wl_combined = {**wl_combined, **_sector_etf_dict_flat_for_fetch}
    df = build_etf_code_name_returns_dataframe(
        code_name=code_name,
        end_date=end_date,
        periods=period_list,
        extra_ohlcv_universe=wl_combined if len(wl_combined) > 0 else None,
    )
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except Exception:
        script_dir = os.getcwd()
    if html_path is None:
        html_path = os.path.join(script_dir, 'etf_code_name_returns.html')
    else:
        html_path = os.path.abspath(html_path)

    # left merge 후 NaN(float)과 datetime.date가 섞이면 .max()에서 타입 비교 오류가 난다 → datetime으로 통일
    if 'latest_date' in df.columns:
        _ld = pd.to_datetime(df['latest_date'], errors='coerce')
        as_of = _ld.max() if _ld.notna().any() else None
    else:
        as_of = None
    if as_of is not None:
        try:
            as_of_str = pd.Timestamp(as_of).strftime('%Y-%m-%d')
        except Exception:
            as_of_str = str(as_of)
    else:
        as_of_str = 'N/A'

    pct_cols = [f'{p}일_상승률' for p in period_list]

    meta = (
        f'대상 {len(code_name)}개 종목 · OHLCV 최신 기준일(데이터 있는 종목 중): {as_of_str} · '
        f'본 페이지는 최근 {_ltd}거래일에 대해, 각 거래일 종가 기준 N거래일 등락률 상위 20종목을 '
        f'5일 → 10일 → 20일 → 50일 순으로 표시합니다. (전체 종목 순위표는 HTML에서 생략)'
    )
    meta = _safe_html(meta)

    # 최근 _ltd 거래일 × periods 일 등락률 일별 Top20 표 + 유니버스 평균등락률 + 매수후보형 표
    top20_section_html = ''
    universe_avg_html = ''
    universe_buy_html = ''
    universe_momentum7_html = ''
    mf_merge = pd.DataFrame()
    mf_merge_t5 = pd.DataFrame()
    mf_merge_t3 = pd.DataFrame()
    mom_ap = pd.DataFrame()
    try:
        ohlcv_df = df.attrs.get('ohlcv_df', None)
    except Exception:
        ohlcv_df = None

    if ohlcv_df is not None and isinstance(ohlcv_df, pd.DataFrame) and not ohlcv_df.empty:
        try:
            try:
                _dts_m7 = pd.to_datetime(ohlcv_df['date'], errors='coerce').dropna()
                if as_of is not None:
                    eval_d_m7 = pd.Timestamp(as_of).normalize().date()
                elif len(_dts_m7):
                    eval_d_m7 = pd.Timestamp(_dts_m7.max()).normalize().date()
                else:
                    eval_d_m7 = date.today()
                momentum7_t5_m, momentum7_det_t5_m = _compute_top7_momentum_5d_portfolio_returns(
                    eval_d_m7,
                    min_trading_days=None,
                    ohlcv_universe=ohlcv_df,
                    name_by_ticker=code_name,
                    momentum_basis='t5',
                )
                momentum7_t3_m, momentum7_det_t3_m = _compute_top7_momentum_5d_portfolio_returns(
                    eval_d_m7,
                    min_trading_days=None,
                    ohlcv_universe=ohlcv_df,
                    name_by_ticker=code_name,
                    momentum_basis='t3',
                )
                momentum7_t0_m, momentum7_det_t0_m = _compute_top7_momentum_5d_portfolio_returns(
                    eval_d_m7,
                    min_trading_days=None,
                    ohlcv_universe=ohlcv_df,
                    name_by_ticker=code_name,
                    momentum_basis='t0',
                )
                purchase_5d_s = _prev_trading_date(eval_d_m7, n_trading_days=5).strftime('%Y-%m-%d')
                eval_date_s = eval_d_m7.strftime('%Y-%m-%d')
                m7_t5_inner = _html_momentum7_scorecard(momentum7_t5_m, momentum7_det_t5_m)
                m7_t3_inner = _html_momentum7_scorecard(momentum7_t3_m, momentum7_det_t3_m)
                m7_t0_inner = _html_momentum7_scorecard(momentum7_t0_m, momentum7_det_t0_m)
                universe_momentum7_html = (
                    '<div class="section-part" style="font-size:1.1rem;font-weight:700;margin:12px 0 10px;color:#1a365d;border-bottom:2px solid #cbd5e0;padding-bottom:6px;">T-0 기준 모멘텀</div>'
                    '<h2 class="sec-title">포트폴리오 수익률 (상위 7)</h2>'
                    '<div class="sec-sub">매수일·평가일·수익률 식 동일. 선정: T-0 모멘텀.</div>'
                    + m7_t0_inner
                    + '<div style="height:14px;"></div>'
                    + '<div class="section-part" style="font-size:1.1rem;font-weight:700;margin:18px 0 10px;color:#1a365d;border-bottom:2px solid #cbd5e0;padding-bottom:6px;">T-3 기준 모멘텀</div>'
                    + '<h2 class="sec-title">포트폴리오 수익률 (상위 7)</h2>'
                    + '<div class="sec-sub">매수일: '
                    + _safe_html(purchase_5d_s)
                    + ' · 평가일: '
                    + _safe_html(eval_date_s)
                    + ' · 선정: T-3 모멘텀(`_T3`). 수익률: 매수일→평가일 종가·동일 비중.</div>'
                    + m7_t3_inner
                    + '<div style="height:14px;"></div>'
                    + '<div class="section-part" style="font-size:1.1rem;font-weight:700;margin:18px 0 10px;color:#1a365d;border-bottom:2px solid #cbd5e0;padding-bottom:6px;">T-5 기준 모멘텀</div>'
                    + '<h2 class="sec-title">포트폴리오 수익률 (상위 7)</h2>'
                    + '<div class="sec-sub">매수일: '
                    + _safe_html(purchase_5d_s)
                    + ' · 평가일: '
                    + _safe_html(eval_date_s)
                    + ' · 선정: T-5 모멘텀(`_T5`). 수익률: 매수일→평가일 종가·동일 비중.</div>'
                    + m7_t5_inner
                    + '<div style="height:18px;"></div>'
                )
            except Exception:
                universe_momentum7_html = ''

            tmp = ohlcv_df[['ticker', 'date', 'close']].copy()
            tmp['ticker'] = tmp['ticker'].astype(str)
            tmp['date'] = pd.to_datetime(tmp['date'])
            tmp['close'] = pd.to_numeric(tmp['close'], errors='coerce')
            tmp = tmp.dropna(subset=['ticker', 'date', 'close'])
            if not tmp.empty:
                px = tmp.pivot_table(index='date', columns='ticker', values='close', aggfunc='last').sort_index()
                cell_univ = {str(c): (code_name.get(str(c), ''), str(c)) for c in px.columns}
                universe_avg_html = _html_universe_avg_nday_returns_table(px, period_list, last_trading_days=_ltd)
                top20_section_html = ''.join(
                    _nday_daily_top20_section_bits(px, period_list, cell_univ, last_trading_days=_ltd)
                )
                mom_ap = calculate_momentum_returns(
                    ohlcv_df,
                    periods=sorted(set(list(period_list) + [120])),
                    min_trading_days=MOMENTUM_DASHBOARD_MIN_TRADING_DAYS,
                )
                if mom_ap is not None and not mom_ap.empty:
                    mom_ap = calculate_weighted_average_momentum(mom_ap)

                    _nm_all = dict(code_name)
                    for _wk, _wv in (wl_combined or {}).items():
                        _ks = str(_wk).strip()
                        if _ks and _ks not in _nm_all:
                            _nm_all[_ks] = _wv

                    def _cnm_row(tk):
                        tk = str(tk).strip()
                        return _nm_all.get(tk) or _nm_all.get(_norm_etf_ticker_key(tk), tk)

                    mom_ap['etf_name'] = mom_ap['ticker'].astype(str).map(_cnm_row)
                    strong_u_t5, _ = _build_action_list(mom_ap, ohlcv_df, top_n=20, momentum_basis='t5')
                    strong_u_t3, _ = _build_action_list(mom_ap, ohlcv_df, top_n=20, momentum_basis='t3')
                    strong_u_t0, _ = _build_action_list(mom_ap, ohlcv_df, top_n=20, momentum_basis='t0')
                    universe_buy_html = (
                        '<div class="section-part" style="font-size:1.1rem;font-weight:700;margin:4px 0 10px;color:#1a365d;border-bottom:2px solid #cbd5e0;padding-bottom:6px;">T-0 기준 모멘텀</div>'
                        '<h2 class="sec-title">매수 후보</h2>'
                        '<div class="sec-sub">평균_모멘텀 상위 20 중 종가≥MA50.</div>'
                        + _html_sector_buy_candidate_like(strong_u_t0)
                        + '<div class="section-part" style="font-size:1.1rem;font-weight:700;margin:18px 0 10px;color:#1a365d;border-bottom:2px solid #cbd5e0;padding-bottom:6px;">T-3 기준 모멘텀</div>'
                        '<h2 class="sec-title">매수 후보</h2>'
                        '<div class="sec-sub">평균_모멘텀_T3 상위 20 중 종가≥MA50.</div>'
                        + _html_sector_buy_candidate_like(strong_u_t3)
                        + '<div class="section-part" style="font-size:1.1rem;font-weight:700;margin:18px 0 10px;color:#1a365d;border-bottom:2px solid #cbd5e0;padding-bottom:6px;">T-5 기준 모멘텀</div>'
                        '<h2 class="sec-title">매수 후보</h2>'
                        '<div class="sec-sub">평균_모멘텀_T5 상위 20 중 종가≥MA50.</div>'
                        + _html_sector_buy_candidate_like(strong_u_t5)
                        + '<div style="height:18px;"></div>'
                    )
                    mf_merge = _build_full_momentum_ma_merge(mom_ap, ohlcv_df)
                    mf_merge_t5 = pd.DataFrame()
                    mf_merge_t3 = pd.DataFrame()
                    if (
                        mf_merge is not None
                        and not mf_merge.empty
                        and '평균_모멘텀_T5' in mf_merge.columns
                    ):
                        mf_merge_t5 = _build_full_momentum_ma_merge(mom_ap, ohlcv_df, rank_col='평균_모멘텀_T5')
                    if (
                        mf_merge is not None
                        and not mf_merge.empty
                        and '평균_모멘텀_T3' in mf_merge.columns
                    ):
                        mf_merge_t3 = _build_full_momentum_ma_merge(mom_ap, ohlcv_df, rank_col='평균_모멘텀_T3')
        except Exception:
            top20_section_html = top20_section_html or ''
            universe_avg_html = universe_avg_html or ''
            universe_buy_html = universe_buy_html or ''

    sector_watch_action_kospi_cmp: dict = {}
    try:
        eval_d_sw = pd.Timestamp(as_of).normalize().date() if as_of is not None else date.today()
        ref_ts_sw = pd.Timestamp(eval_d_sw)
        kospi_daily_ref_sw = _get_kospi_daily_return_pct_for_ref_date(eval_d_sw)
        kospi_3d_ref_sw = _get_kospi_3d_return_pct_for_ref_date(eval_d_sw)
        kospi_weekly_ref_sw = _get_kospi_weekly_return_pct_for_ref_ts(ref_ts_sw)
        kospi_avg_ref_sw = _get_kospi_weighted_avg_momentum_for_ref_date(eval_d_sw)
        kospi_period_refs_sw: dict[str, float] = {}
        for _n in (5, 10, 20, 50, 120):
            kospi_period_refs_sw[f'{_n}일_상승률'] = _get_kospi_nd_trading_return_pct_for_ref_date(eval_d_sw, _n)
        sector_watch_action_kospi_cmp = {
            '평균_모멘텀': kospi_avg_ref_sw,
            '5일_상승률': kospi_period_refs_sw.get('5일_상승률', np.nan),
            '10일_상승률': kospi_period_refs_sw.get('10일_상승률', np.nan),
            '20일_상승률': kospi_period_refs_sw.get('20일_상승률', np.nan),
            '50일_상승률': kospi_period_refs_sw.get('50일_상승률', np.nan),
            '120일_상승률': kospi_period_refs_sw.get('120일_상승률', np.nan),
            'weekly_return_pct': kospi_weekly_ref_sw,
            '당일 수익률': kospi_daily_ref_sw,
            '3일 수익률': kospi_3d_ref_sw,
        }
    except Exception:
        sector_watch_action_kospi_cmp = {}

    watch_dash_css = """
    .watch-dash-summary .section { background:#fff; border-radius: 12px; padding: 14px; margin-bottom: 12px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); max-width: 1280px; }
    .watch-dash-summary .section h3 { font-size: 0.98rem; margin: 0 0 8px 0; color: #2d3748; }
    .watch-dash-summary .section-part { font-size: 1.12rem; font-weight: 700; margin: 8px 0 14px 0; color: #1a365d; padding-bottom: 8px; border-bottom: 2px solid #cbd5e0; }
    .watch-dash-summary .meta { color:#4a5568; margin: 6px 0 10px 0; font-size: 0.92rem; }
    .watch-dash-summary table.tbl { max-width: 1280px; }
    .watch-dash-summary table.tbl.tbl-name-wide { width: max-content; max-width: min(100%, 1680px); }
    table.tbl.tbl-name-wide th.col-name, table.tbl.tbl-name-wide td.col-name { min-width: 300px; white-space: nowrap; vertical-align: middle; }
    table.tbl td.kospi-beat { background: rgba(72, 187, 120, 0.35); font-weight: 600; }
    table.tbl td.kospi-miss { background: rgba(245, 101, 101, 0.32); font-weight: 600; }
"""

    def _html_watch_panel_for_wl(wl: dict[str, str], heading_label: str) -> str:
        """ETF 모멘텀 대시보드 요약과 동일: T-0·T-3·T-5 각 (포트폴리오 상위7 + 매수 후보) 후 일별 등락률 표."""
        if not isinstance(wl, dict) or len(wl) == 0:
            return f'<p class="no-data">{_safe_html(heading_label)} 목록이 비어 있습니다.</p>'
        try:
            _oh = df.attrs.get('ohlcv_df', None)
        except Exception:
            _oh = None
        watchlist_daily_html = _build_sector_watchlist_nday_daily_top20_html(
            _oh, wl, period_list, last_trading_days=_ltd
        )
        hl = _safe_html(heading_label)
        _pct_action_t0 = [
            '평균_모멘텀', '5일_상승률', '10일_상승률', '20일_상승률', '50일_상승률', '120일_상승률',
            'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
        ]
        _pct_action_t5 = [
            '평균_모멘텀_T5', '5일_상승률_T5', '10일_상승률_T5', '20일_상승률_T5', '50일_상승률_T5', '120일_상승률_T5',
            'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
        ]
        _pct_action_t3 = [
            '평균_모멘텀_T3', '5일_상승률_T3', '10일_상승률_T3', '20일_상승률_T3', '50일_상승률_T3', '120일_상승률_T3',
            'weekly_return_pct', '당일 수익률', '3일 수익률', 'ATR14_일변동률(%)',
        ]

        if _oh is None or not isinstance(_oh, pd.DataFrame) or _oh.empty:
            return (
                f'<p class="meta">OHLCV가 없어 요약을 표시할 수 없습니다. ({hl})</p>'
                + watchlist_daily_html
            )
        if mom_ap is None or mom_ap.empty or 'ticker' not in mom_ap.columns:
            return (
                f'<p class="meta">모멘텀 데이터가 없어 요약을 표시할 수 없습니다. ({hl})</p>'
                + watchlist_daily_html
            )

        wl_norm_set = {_norm_etf_ticker_key(k) for k in wl}
        ohlcv_wl = _oh[_oh['ticker'].astype(str).map(_norm_etf_ticker_key).isin(wl_norm_set)].copy()
        if ohlcv_wl.empty:
            return (
                f'<p class="meta">{hl} 티커에 해당하는 OHLCV가 유니버스 조회 결과에 없습니다.</p>'
                + watchlist_daily_html
            )

        mom_wl = mom_ap[mom_ap['ticker'].astype(str).map(_norm_etf_ticker_key).isin(wl_norm_set)].copy()
        name_map: dict[str, str] = dict(code_name)
        for _wk, _wv in wl.items():
            _ks = str(_wk).strip()
            if _ks:
                name_map[_ks] = str(_wv).strip() or _ks

        eval_w = pd.Timestamp(as_of).normalize().date() if as_of is not None else date.today()
        purchase_w = _prev_trading_date(eval_w, n_trading_days=5).strftime('%Y-%m-%d')
        eval_w_str = eval_w.strftime('%Y-%m-%d')
        kmap = sector_watch_action_kospi_cmp if isinstance(sector_watch_action_kospi_cmp, dict) else {}

        bits: list[str] = ['<div class="watch-dash-summary">']

        def _append_momentum_block(mb: str, title_suffix: str, meta_port: str, meta_action: str, pct_cols: list[str]):
            try:
                m7_df, m7_det = _compute_top7_momentum_5d_portfolio_returns(
                    eval_w,
                    min_trading_days=None,
                    ohlcv_universe=ohlcv_wl,
                    name_by_ticker=name_map,
                    momentum_basis=mb,
                )
            except Exception:
                m7_df, m7_det = pd.DataFrame(), {}
            try:
                strong_df, _ = _build_action_list(mom_wl, ohlcv_wl, top_n=20, momentum_basis=mb)
                if strong_df is not None and not strong_df.empty:
                    strong_df = _insert_current_price_after_name(strong_df, ohlcv_wl)
            except Exception:
                strong_df = pd.DataFrame()
            bits.append('<div class="section-part">' + _safe_html(title_suffix) + '</div>')
            bits.append('<div class="section">')
            bits.append('<h3>포트폴리오 수익률 (5거래일 전 매수 → 평가일, 상위 7)</h3>')
            bits.append(f'<div class="meta">{_safe_html(meta_port)}</div>')
            bits.append(_html_momentum7_scorecard(m7_df, m7_det))
            bits.append('</div>')
            bits.append('<div class="section">')
            bits.append('<h3>매수 후보 (Action List)</h3>')
            bits.append(f'<div class="meta">{_safe_html(meta_action)}</div>')
            bits.append(_df_html(strong_df, pct_cols=pct_cols, compare_kospi_map=kmap))
            bits.append('</div>')

        scope_note = (
            f'{heading_label} 목록에 포함된 종목만 대상으로 산출합니다. '
            '매수일·평가일·수익률·상위 7 선정 방식은 ETF 모멘텀 대시보드 요약과 동일하며, '
            '매수 후보는 해당 목록 내에서 평균 모멘텀 상위 20 중 종가≥MA50입니다.'
        )
        _append_momentum_block(
            't0',
            'T-0 기준 모멘텀',
            f'매수일: {purchase_w} · 평가일: {eval_w_str} · 선정: 매수일 시점 최신 종가(T-0) 기준 '
            f'`N일_상승률`·`평균_모멘텀`으로 상위 7. {scope_note}',
            f'평균_모멘텀 상위 20 중 종가≥MA50. {scope_note}',
            _pct_action_t0,
        )
        _append_momentum_block(
            't3',
            'T-3 기준 모멘텀',
            f'매수일: {purchase_w} · 평가일: {eval_w_str} · 선정: 매수일 시점 OHLCV에서 T-3 종가 기준 '
            f'`N일_상승률_T3`·`평균_모멘텀_T3`로 상위 7. {scope_note}',
            f'평균_모멘텀_T3 상위 20 중 종가≥MA50. {scope_note}',
            _pct_action_t3,
        )
        _append_momentum_block(
            't5',
            'T-5 기준 모멘텀',
            f'매수일: {purchase_w} · 평가일: {eval_w_str} · 선정: 매수일 시점 OHLCV에서 T-5 종가 기준 '
            f'`N일_상승률_T5`·`평균_모멘텀_T5`로 상위 7. {scope_note}',
            f'평균_모멘텀_T5 상위 20 중 종가≥MA50. {scope_note}',
            _pct_action_t5,
        )
        bits.append('</div>')
        return ''.join(bits) + watchlist_daily_html

    have_watch_tabs = isinstance(wl_combined, dict) and len(wl_combined) > 0
    if have_watch_tabs and use_default_split_watchlists:
        watch_sector_panel_html = _html_watch_panel_for_wl(SECTOR_MOMENTUM_DAILY_RANK_SECTOR_ETFS, '관심 섹터')
        watch_group_panel_html = _html_watch_panel_for_wl(SECTOR_MOMENTUM_DAILY_RANK_GROUP_ETFS, '관심 그룹')
        watch_active_panel_html = _html_watch_panel_for_wl(SECTOR_MOMENTUM_DAILY_RANK_Actives_ETFS, '관심 액티브ETF')
        watch_sector_map_panel_html = ''
        if _sector_etf_dict_flat_for_fetch:
            try:
                watch_sector_map_panel_html = _html_sector_etf_dictionary_rankings_panel(
                    mom_ap,
                    ohlcv_df,
                    sector_etf_dict,
                    sector_watch_action_kospi_cmp,
                )
            except Exception:
                watch_sector_map_panel_html = (
                    '<p class="meta">섹터 ETF 맵 순위 패널을 생성하지 못했습니다.</p>'
                )
        watch_passive_panel_html = _html_watch_panel_for_wl(SECTOR_MOMENTUM_DAILY_RANK_Passives_ETFS, '관심 패시브ETF')
        body_inner = f"""
  <div class="tabset sector-etf-tabs">
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_main" checked>
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_watch_sector">
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_watch_group">
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_watch_active">
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_sector_map">
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_watch_passive">
    <div class="tab-labels">
      <label for="sector_etf_tab_main">N거래일 등락률 Top20 (유니버스)</label>
      <label for="sector_etf_tab_watch_sector">관심 섹터 일별 등락률 순위</label>
      <label for="sector_etf_tab_watch_group">관심 그룹 일별 등락률 순위</label>
      <label for="sector_etf_tab_watch_active">관심 액티브ETF 일별 등락률 순위</label>
      <label for="sector_etf_tab_sector_map">섹터 ETF 맵 순위 (sector_etf_dict)</label>
      <label for="sector_etf_tab_watch_passive">관심 패시브ETF 일별 등락률 순위</label>
    </div>
    <section class="tab-panel main-panel">
      {universe_avg_html}
      <p class="meta">{meta}</p>
      {universe_momentum7_html}
      {universe_buy_html}
      {top20_section_html}
    </section>
    <section class="tab-panel watch-sector-panel">
      {watch_sector_panel_html}
    </section>
    <section class="tab-panel watch-group-panel">
      {watch_group_panel_html}
    </section>
    <section class="tab-panel watch-active-panel">
      {watch_active_panel_html}
    </section>
    <section class="tab-panel sector-map-panel">
      {watch_sector_map_panel_html}
    </section>
    <section class="tab-panel watch-passive-panel">
      {watch_passive_panel_html}
    </section>
  </div>"""
        tab_css = """
    .tabset.sector-etf-tabs { position: relative; }
    .tabset.sector-etf-tabs > input { position: absolute; opacity: 0; width: 0; height: 0; }
    .tabset.sector-etf-tabs .tab-labels { display: flex; flex-wrap: wrap; gap: 6px; margin: 12px 0 16px; }
    .tabset.sector-etf-tabs .tab-labels label {
      display: inline-block; padding: 9px 14px; cursor: pointer; background: #e2e8f0;
      border-radius: 8px; font-size: 0.9rem; color: #2d3748; user-select: none;
    }
    .tabset.sector-etf-tabs > input#sector_etf_tab_main:checked ~ .tab-labels label[for="sector_etf_tab_main"],
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_sector:checked ~ .tab-labels label[for="sector_etf_tab_watch_sector"],
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_group:checked ~ .tab-labels label[for="sector_etf_tab_watch_group"],
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_active:checked ~ .tab-labels label[for="sector_etf_tab_watch_active"],
    .tabset.sector-etf-tabs > input#sector_etf_tab_sector_map:checked ~ .tab-labels label[for="sector_etf_tab_sector_map"],
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_passive:checked ~ .tab-labels label[for="sector_etf_tab_watch_passive"] {
      background: #2b6cb0; color: #fff; font-weight: 600;
    }
    .tabset.sector-etf-tabs .tab-panel { display: none; }
    .tabset.sector-etf-tabs > input#sector_etf_tab_main:checked ~ .main-panel,
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_sector:checked ~ .watch-sector-panel,
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_group:checked ~ .watch-group-panel,
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_active:checked ~ .watch-active-panel,
    .tabset.sector-etf-tabs > input#sector_etf_tab_sector_map:checked ~ .sector-map-panel,
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch_passive:checked ~ .watch-passive-panel { display: block; }
"""
    elif have_watch_tabs:
        watch_single_panel_html = _html_watch_panel_for_wl(wl_combined, '관심종목')
        body_inner = f"""
  <div class="tabset sector-etf-tabs">
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_main" checked>
    <input type="radio" name="sector_etf_tab" id="sector_etf_tab_watch">
    <div class="tab-labels">
      <label for="sector_etf_tab_main">N거래일 등락률 Top20 (유니버스)</label>
      <label for="sector_etf_tab_watch">관심종목 일별 등락률 순위</label>
    </div>
    <section class="tab-panel main-panel">
      {universe_avg_html}
      <p class="meta">{meta}</p>
      {universe_momentum7_html}
      {universe_buy_html}
      {top20_section_html}
    </section>
    <section class="tab-panel watch-panel">
      {watch_single_panel_html}
    </section>
  </div>"""
        tab_css = """
    .tabset.sector-etf-tabs { position: relative; }
    .tabset.sector-etf-tabs > input { position: absolute; opacity: 0; width: 0; height: 0; }
    .tabset.sector-etf-tabs .tab-labels { display: flex; flex-wrap: wrap; gap: 6px; margin: 12px 0 16px; }
    .tabset.sector-etf-tabs .tab-labels label {
      display: inline-block; padding: 9px 14px; cursor: pointer; background: #e2e8f0;
      border-radius: 8px; font-size: 0.9rem; color: #2d3748; user-select: none;
    }
    .tabset.sector-etf-tabs > input#sector_etf_tab_main:checked ~ .tab-labels label[for="sector_etf_tab_main"],
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch:checked ~ .tab-labels label[for="sector_etf_tab_watch"] {
      background: #2b6cb0; color: #fff; font-weight: 600;
    }
    .tabset.sector-etf-tabs .tab-panel { display: none; }
    .tabset.sector-etf-tabs > input#sector_etf_tab_main:checked ~ .main-panel,
    .tabset.sector-etf-tabs > input#sector_etf_tab_watch:checked ~ .watch-panel { display: block; }
"""
    else:
        body_inner = f"""
  {universe_avg_html}
  <p class="meta">{meta}</p>
  {universe_momentum7_html}
  {universe_buy_html}
  {top20_section_html}"""
        tab_css = ""

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>섹터 등락률 BY ETF</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; margin: 20px; background: #f5f6fa; color: #1a202c; }}
    h1 {{ font-size: 1.35rem; margin: 0 0 8px 0; }}
    .sec-title {{ font-size: 1.05rem; margin: 16px 0 6px 0; }}
    .sec-sub {{ color: #4a5568; margin: 0 0 10px 0; font-size: 0.9rem; }}
    .small {{ font-size: 0.78rem; color: #4a5568; }}
    .meta {{ color: #4a5568; margin-bottom: 16px; font-size: 0.92rem; }}
    table.tbl {{ border-collapse: collapse; width: 100%; max-width: 1280px; background: #fff; border-radius: 10px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
    table.tbl th, table.tbl td {{ border-bottom: 1px solid #e2e8f0; padding: 8px 10px; font-size: 0.86rem; }}
    table.tbl th {{ background: #2b6cb0; color: #fff; text-align: left; }}
    table.tbl tr:nth-child(even) {{ background: #f7fafc; }}
    table.tbl tr:hover {{ background: #edf2f7; }}
    th.col-return, td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    th.col-name {{ min-width: 200px; }}
    table.tbl.tbl-name-wide {{ width: max-content; max-width: min(100%, 1680px); }}
    table.tbl.tbl-name-wide th.col-name, table.tbl.tbl-name-wide td.col-name {{ min-width: 300px; white-space: nowrap; vertical-align: middle; }}
    td.m-pos {{ background: rgba(72, 187, 120, 0.12); }}
    td.m-strong-pos {{ background: rgba(72, 187, 120, 0.28); font-weight: 700; }}
    td.m-neg {{ background: rgba(245, 101, 101, 0.12); }}
    td.m-strong-neg {{ background: rgba(245, 101, 101, 0.26); font-weight: 700; }}
    td.m-na {{ color: #a0aec0; }}
    th.col-flag, td.col-flag {{ text-align: center; white-space: nowrap; }}
    td.ma-yes {{ background: rgba(72, 187, 120, 0.2); font-weight: 600; color: #276749; }}
    td.ma-no {{ background: rgba(237, 242, 247, 1); color: #718096; }}
    .no-data {{ color: #718096; font-style: italic; }}
    table.tbl th.t-num, table.tbl td.t-num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .m7-scorecard {{ display: flex; flex-direction: column; gap: 10px; }}
    details.m7-acc {{ background:#f7fafc; border:1px solid #e2e8f0; border-radius:10px; padding:10px 14px; }}
    details.m7-acc > summary {{ list-style:none; cursor:pointer; display:flex; flex-wrap:wrap; align-items:center; gap:14px; padding:2px 0; font-weight:600; color:#2d3748; }}
    details.m7-acc > summary::-webkit-details-marker {{ display:none; }}
    .m7-sum-name {{ flex:1 1 220px; min-width:160px; }}
    .m7-sum-mid, .m7-sum-valid {{ font-variant-numeric:tabular-nums; text-align:right; }}
    .m7-sum-mid {{ min-width:76px; color:#2b6cb0; }}
    .m7-sum-valid {{ min-width:42px; color:#4a5568; font-weight:600; font-size:0.92em; }}
    .m7-acc-body {{ margin-top:10px; padding-top:10px; border-top:1px solid #e2e8f0; }}
{watch_dash_css}
{tab_css}
{_HTML_TABLE_SORT_CSS}
  </style>
</head>
<body>
  <h1>섹터 등락률 BY ETF</h1>
{body_inner}
{_html_table_sort_script()}
</body>
</html>
"""
    try:
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        if print_table:
            def _yn(x):
                return '예' if pd.notna(x) and float(x) >= 1 else ('아니오' if pd.notna(x) else '—')

            ma5s = df['5일선_상회'] if '5일선_상회' in df.columns else pd.Series(dtype=float)
            ma10s = df['10일선_상회'] if '10일선_상회' in df.columns else pd.Series(dtype=float)
            rups = df['순위_상승'] if '순위_상승' in df.columns else pd.Series(dtype=float)
            print(
                f"\n[ETF_CODE_NAME] 기준일(최대): {as_of_str} · "
                f"5일선 상회: 예 {int((ma5s == 1.0).sum())} / 아니오 {int((ma5s == 0.0).sum())} / — {int(ma5s.isna().sum())} · "
                f"10일선 상회: 예 {int((ma10s == 1.0).sum())} / 아니오 {int((ma10s == 0.0).sum())} / — {int(ma10s.isna().sum())} · "
                f"순위상승(5거래일 전 대비): 예 {int((rups == 1.0).sum())} / 아니오 {int((rups == 0.0).sum())} / — {int(rups.isna().sum())}"
            )
            disp_cols = ['순위', '전주순위', '순위_상승', 'ticker', '종목명', '5일선_상회', '10일선_상회'] + pct_cols
            disp_cols = [c for c in disp_cols if c in df.columns]
            disp = df[disp_cols].copy()
            for _c in ('5일선_상회', '10일선_상회', '순위_상승'):
                if _c in disp.columns:
                    disp[_c] = disp[_c].apply(_yn)
            rename_pct = {f'{p}일_상승률': f'{p}일%' for p in period_list}
            disp = disp.rename(columns=rename_pct)
            with pd.option_context('display.max_rows', 250, 'display.width', 200, 'display.unicode.east_asian_width', True):
                print(disp.to_string(index=False))
            print(f"\nHTML 저장: {html_path}")
        if open_browser:
            try:
                from pathlib import Path as _Path
                webbrowser.open(_Path(html_path).resolve().as_uri())
            except Exception:
                webbrowser.open('file:///' + html_path.replace(os.sep, '/'))
        return html_path
    except Exception as e:
        print(f"⚠️ HTML 저장 실패: {e}")
        print(traceback.format_exc())
        return None


if __name__ == "__main__":
    # 테스트 코드
    print("=" * 50)
    print("ETF 기본 정보 조회 테스트")
    print("=" * 50)
    
    # 전체 ETF 리스트 조회
    print("\n1. 전체 ETF 리스트 조회:")
    etf_list = get_etf_list_from_db()
    if not etf_list.empty:
        print(f"\n조회된 ETF 수: {len(etf_list)}")
        print("\n처음 5개 ETF:")
        print(etf_list.head())
    
    # 최신 업데이트 날짜 기준 조회
    print("\n2. 최신 업데이트 날짜 기준 ETF 리스트 조회:")
    etf_list_latest = get_etf_list_by_update_date()
    if not etf_list_latest.empty:
        print(f"\n조회된 ETF 수: {len(etf_list_latest)}")
        print("\n처음 5개 ETF:")
        print(etf_list_latest.head())
    
    # 특정 티커 조회 (예시)
    if not etf_list.empty:
        sample_ticker = etf_list['ticker'].iloc[0]
        print(f"\n3. 특정 티커 조회 (예시: {sample_ticker}):")
        etf_info = get_etf_info_by_ticker(sample_ticker)
        if not etf_info.empty:
            print(etf_info)
    
    # 전체 컬럼 정보 조회 테스트
    print("\n" + "=" * 50)
    print("ETF 전체 컬럼 정보 조회 테스트")
    print("=" * 50)
    etf_all = get_etf_info_all_columns()
    if not etf_all.empty:
        print(f"\n조회된 ETF 수: {len(etf_all)}")
        print(f"\n전체 컬럼 목록:")
        for idx, col in enumerate(etf_all.columns, 1):
            print(f"  {idx}. {col}")
        print("\n처음 3개 ETF의 전체 정보:")
        print(etf_all.head(3))
    
    # ETF OHLCV 데이터 수집 (DB MAX(date) 자동 증분)
    print("\n" + "=" * 50)
    print("ETF OHLCV 데이터 수집 시작 (KRX MDCSTAT04301, DB 자동 증분)")
    print("=" * 50)
    
    print("\nETF OHLCV 자동 증분 수집을 시작합니다...")
    result = collect_etf_ohlcv_data(max_etf=None)
    print(f"\n수집 결과: {result}")

    if result.get("error_list") and len(result["error_list"]) > 0:
        print("\n" + "=" * 50)
        print("실패한 일자 ETF 재수집 시작")
        print("=" * 50)
        retry_result = retry_failed_etf_ohlcv(
            result["error_list"],
            biz_day=result.get("biz_day"),
        )
        print(f"\n재수집 결과: {retry_result}")
    
    # ETF 모멘텀 분석
    print("\n" + "=" * 50)
    print("ETF 모멘텀 순위 분석 시작")
    print("=" * 50)
    momentum_df = analyze_etf_momentum(visualize=True, save_path='etf_momentum_rankings.png')
    if momentum_df is not None and not momentum_df.empty:
        print(f"\n✓ 모멘텀 분석 완료: {len(momentum_df)}개 ETF 분석됨")

    # ETF_CODE_NAME 딕셔너리 종목만 5·10·20·50거래일 등락률 표 → HTML
    print("\n" + "=" * 50)
    print("ETF_CODE_NAME 등락률 표 HTML 저장")
    print("=" * 50)
    try:
        html_out = save_etf_code_name_returns_html()
        if html_out:
            print(f"\n✓ 저장 완료: {html_out}")
    except Exception as _e:
        print(f"\n⚠️ ETF_CODE_NAME HTML 저장 중 오류: {_e}")
        print(traceback.format_exc())
