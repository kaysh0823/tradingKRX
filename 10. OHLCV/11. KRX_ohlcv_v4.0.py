# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 15:35:14 2024

@author: hachi

v4.0: OHLCV + KRX 종목/ETF 정보(krx_info_v3.0) 통합.
       본 파일만 실행하면 종목·ETF 적재 후 OHLCV 수집까지 수행.
       일봉 원천: KRX MDCSTAT01501 일자 CSV (전종목 1요청/일). 종목별 크롤링 금지.
       투자자별 매매: KRX 12010 MDCSTAT02401 (일자×투자자구분 CSV). frgn.naver 제거.
"""


import os
import sys
from pathlib import Path

os.environ["REPO_ROOT"] = r"C:\Users\hachi\OneDrive\02. Project\tradingKRX"
print(repr(os.getenv("DB_USER")), repr(os.getenv("DB_PASSWORD")))

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
    get_index_ohlcv_from_naver_api,
    get_index_ohlcv_from_naver_crawl,
)
from indicators_core import RS_AVG_COLS_D, rs_avg, talent_score, talent_up_count
load_project_env()



import requests as rq
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
import numpy as np
import pymysql
import json
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
#import indicators_stock
import traceback
import os
import inspect

import math


# 임시 해결: 스크립트 실행 전에 셀에서 직접 주입
def _script_dir():
    """Spyder F5 / Jupyter 셀 / IPython 모두에서 스크립트 경로 반환."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        for fi in inspect.stack():
            p = getattr(fi, "filename", "") or ""
            if "KRX_ohlcv" in p:
                return os.path.dirname(os.path.abspath(p))
        wd = os.getcwd()
        for name in ("01. KRX_ohlcv_v4.0.py", "KRX_ohlcv_v4.0.py"):
            if os.path.isfile(os.path.join(wd, name)):
                return wd
        ohlcv_dir = os.path.join(wd, "OHLCV")
        for name in ("01. KRX_ohlcv_v4.0.py", "KRX_ohlcv_v4.0.py"):
            if os.path.isfile(os.path.join(ohlcv_dir, name)):
                return ohlcv_dir
        return wd


def _ohlcv_values_differ(new_val, old_val):
    """수집값과 DB값이 의미 있게 다른지 판별 (NULL/NaN 동등, 숫자는 허용 오차)."""
    new_na = new_val is None or (isinstance(new_val, float) and math.isnan(new_val)) or pd.isna(new_val)
    old_na = old_val is None or (isinstance(old_val, float) and math.isnan(old_val)) or pd.isna(old_val)
    if new_na and old_na:
        return False
    if new_na or old_na:
        return True
    try:
        return not math.isclose(float(new_val), float(old_val), rel_tol=1e-9, abs_tol=1e-4)
    except (TypeError, ValueError):
        return new_val != old_val


def fetch_ohlcv_args_only_changed(mycursor, table, ticker, price_df, value_columns):
    """
    DB에 없거나 value_columns 중 하나라도 다른 행만 INSERT용 튜플로 반환.
    튜플 형식: (ticker, date, *value_columns 순서).
    """
    if price_df is None or len(price_df) == 0:
        return []
    dates = pd.to_datetime(price_df['date'], errors='coerce')
    if dates.isna().all():
        return []
    dmin = dates.min().date()
    dmax = dates.max().date()
    select_cols = ', '.join(f'`{c}`' for c in ['date'] + list(value_columns))
    sql = f"SELECT {select_cols} FROM `{table}` WHERE ticker = %s AND `date` >= %s AND `date` <= %s"
    mycursor.execute(sql, (ticker, dmin, dmax))
    rows = mycursor.fetchall()
    db_by_date = {}
    for row in rows:
        d = row[0]
        if isinstance(d, pd.Timestamp):
            d = d.date()
        elif isinstance(d, datetime):
            d = d.date()
        elif isinstance(d, date):
            pass
        db_by_date[d] = row[1:]

    result = []
    for _, row in price_df.iterrows():
        d = pd.Timestamp(row['date']).date()
        db_vals = db_by_date.get(d)
        vals = tuple(row[c] for c in value_columns)
        if db_vals is None:
            result.append((ticker, row['date']) + vals)
            continue
        for i, c in enumerate(value_columns):
            if _ohlcv_values_differ(row[c], db_vals[i]):
                result.append((ticker, row['date']) + vals)
                break
    return result


# 일봉 DB 정합용 참조 CSV (파일명 YYYYMMDD.csv = 해당 거래일 데이터)
KRX_OHLCV_REFERENCE_DIR = r'C:\Users\hachi\OneDrive\00. Code\KRX\KRX_Data\OHLCV'


def _read_csv_with_encoding(path):
    for enc in ('utf-8-sig', 'cp949', 'euc-kr', 'utf-8'):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
        except Exception:
            continue
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def _find_csv_column(df, candidates):
    cols = list(df.columns)
    for cand in candidates:
        for c in cols:
            sc = str(c).strip()
            if sc == cand or sc.lower() == cand.lower():
                return c
    cand_norm = [c.strip().lower().replace(' ', '') for c in candidates]
    for c in cols:
        key = str(c).strip().lower().replace(' ', '')
        if key in cand_norm:
            return c
    return None


def _standardize_ticker_cell(val):
    if val is None or (not isinstance(val, str) and pd.isna(val)):
        return None
    s = str(val).strip()
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    if s.replace('.', '', 1).isdigit():
        try:
            n = int(float(s))
            return str(n).zfill(6) if n >= 0 else None
        except ValueError:
            pass
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s if s else None


def load_reference_ohlcv_day_csv(path, file_date):
    """
    YYYYMMDD.csv 한 장을 ticker, date, open, high, low, close, volume 형태로 정규화.
    거래일은 파일명 기준(file_date)으로 통일.
    """
    df = _read_csv_with_encoding(path)
    if df is None or len(df) == 0:
        return None
    tcol = _find_csv_column(
        df,
        ('종목코드', '티커', 'ticker', 'TICKER', 'Symbol', 'symbol', 'CODE', 'code', '단축코드', '종목단축코드'),
    )
    ocol = _find_csv_column(df, ('시가', 'open', 'OPEN', 'Open'))
    hcol = _find_csv_column(df, ('고가', 'high', 'HIGH', 'High'))
    lcol = _find_csv_column(df, ('저가', 'low', 'LOW', 'Low'))
    ccol = _find_csv_column(df, ('종가', 'close', 'CLOSE', 'Close'))
    vcol = _find_csv_column(df, ('거래량', '거래량(주)', 'volume', 'VOLUME', 'Volume'))
    if not all([tcol, ocol, hcol, lcol, ccol, vcol]):
        return None
    out = pd.DataFrame({
        'ticker': df[tcol].map(_standardize_ticker_cell),
        'date': file_date,
        'open': pd.to_numeric(df[ocol], errors='coerce'),
        'high': pd.to_numeric(df[hcol], errors='coerce'),
        'low': pd.to_numeric(df[lcol], errors='coerce'),
        'close': pd.to_numeric(df[ccol], errors='coerce'),
        'volume': pd.to_numeric(df[vcol], errors='coerce'),
    })
    out = out.dropna(subset=['ticker']).drop_duplicates(subset=['ticker'], keep='first')
    out = out.dropna(subset=['open', 'high', 'low', 'close', 'volume'], how='any')
    return out if len(out) else None


def sync_krx_ohlcv_from_reference_csv_dir(
    mycursor, con, ref_dir, ymd_from_str, ymd_to_str, executemany_query, batch_size=400
):
    """
    ref_dir의 YYYYMMDD.csv가 있는 거래일만 krx_ohlcv와 비교해, 불일치(또는 DB 미존재) 시 파일 값으로 DB 반영.
    해당 날짜 CSV가 없으면 그 날짜는 비교·저장·업데이트를 하지 않음.
    """
    if not ref_dir or not os.path.isdir(ref_dir):
        print(f'  (참조 OHLCV 폴더가 없습니다: {ref_dir})')
        return
    d0 = datetime.strptime(ymd_from_str, '%Y%m%d').date()
    d1 = datetime.strptime(ymd_to_str, '%Y%m%d').date()
    dates_with_file = []
    cur_d = d0
    while cur_d <= d1:
        ymd = cur_d.strftime('%Y%m%d')
        fpath = os.path.join(ref_dir, f'{ymd}.csv')
        if os.path.isfile(fpath):
            dates_with_file.append((cur_d, fpath))
        cur_d += timedelta(days=1)

    if not dates_with_file:
        print('  (참조 CSV 정합 스킵: 해당 기간에 YYYYMMDD.csv 파일 없음)')
        return

    print('  참조 CSV가 있는 거래일만 DB와 비교 후, 불일치 시 파일 기준 반영')

    files_applied = 0
    rows_updated = 0
    ref_days_compared = 0
    for cur_d, fpath in dates_with_file:
        try:
            ref_df = load_reference_ohlcv_day_csv(fpath, cur_d)
            if ref_df is None:
                print(f'  ⚠ 참조 CSV 컬럼 매핑 실패 또는 빈 파일: {fpath}')
                continue
            ref_days_compared += 1
            mycursor.execute(
                'SELECT ticker, open, high, low, close, volume FROM krx_ohlcv WHERE `date` = %s',
                (cur_d,),
            )
            db_rows = mycursor.fetchall()
            db_map = {str(r[0]).strip().zfill(6) if str(r[0]).strip().isdigit() else str(r[0]).strip(): r[1:] for r in db_rows}
            args = []
            for _, row in ref_df.iterrows():
                tk = row['ticker']
                o, h, l, c, v = row['open'], row['high'], row['low'], row['close'], row['volume']
                db_vals = db_map.get(tk)
                if db_vals is None:
                    args.append((tk, cur_d, o, h, l, c, v))
                    continue
                if any(_ohlcv_values_differ(row[col], db_vals[i]) for i, col in enumerate(['open', 'high', 'low', 'close', 'volume'])):
                    args.append((tk, cur_d, o, h, l, c, v))
            if args:
                for i in range(0, len(args), batch_size):
                    chunk = args[i : i + batch_size]
                    mycursor.executemany(executemany_query, chunk)
                con.commit()
                files_applied += 1
                rows_updated += len(args)
        except Exception as e:
            print(f'  ⚠ 참조 CSV 정합 오류 ({fpath}): {e}')
            try:
                con.rollback()
            except Exception:
                pass
    if files_applied:
        print(f'  ✓ 참조 CSV 정합: {files_applied}개 일자, 총 {rows_updated}행 DB 반영(파일 기준)')
    elif ref_days_compared > 0:
        print(f'  ✓ 참조 CSV {ref_days_compared}개 일자: DB와 모두 일치')
    elif dates_with_file:
        print('  ⚠ 참조 CSV 파일은 있으나 유효하게 로드된 일자 없음')


# =============================================================================
# KRX 종목/ETF 정보 업데이트 (구 00. krx_info_v3.0.py)
# =============================================================================
# 다운로드한 원본 CSV 백업 (False면 저장 안 함)
SAVE_KRX_CSV_BACKUP = True
KRX_CSV_BACKUP_DIR = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data'

KRX_INFO_HEADERS = {
    'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050201',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}
OTP_URL = 'https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
DOWN_URL = 'https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

_KRX_BASE = 'https://data.krx.co.kr'
LOGIN_PAGE = f'{_KRX_BASE}/contents/MDC/COMS/client/MDCCOMS001.cmd'
LOGIN_JSP = f'{_KRX_BASE}/contents/MDC/COMS/client/view/login.jsp?site=mdc'
LOGIN_URL = f'{_KRX_BASE}/contents/MDC/COMS/client/MDCCOMS001D1.cmd'
_LOGIN_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
)


def krx_login(session: rq.Session) -> bool:
    """
    KRX_ID / KRX_PW 환경변수로 로그인.
    CD001=성공, CD011=중복로그인(skipDup=Y 재시도), CD010=비밀번호 변경 필요.
    """
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
    """
    KRX 정보데이터시스템에서 OTP 발급 후 CSV를 다운로드하여 bytes로 반환.
    실패 시 최대 retries회 재시도. 호출 간 1초 대기.
    """
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
                print(f'OTP 응답 앞 200자: {otp[:200]!r}')
                raise ValueError(f'응답이 비정상적으로 짧음 ({len(res.content)} bytes)')

            time.sleep(1)
            return res.content

        except Exception as e:
            last_err = e
            print(f'KRX 다운로드 실패 ({bld}, {attempt}/{retries}): {e}')
            time.sleep(1 if attempt < retries else 0)

    raise RuntimeError(f'KRX CSV 다운로드 실패 (bld={bld}): {last_err}')


# ---------------------------------------------------------------------------
# 전종목 일봉 OHLCV — KRX [12001] MDCSTAT01501 (일자 CSV 1회 = 전종목)
# ---------------------------------------------------------------------------
BLD_STOCK_OHLCV = 'dbms/MDC/STAT/standard/MDCSTAT01501'
# DB 비어 있을 때만: 최근 N거래일 분량(캘린더 여유 포함) 또는 지정 시작일
OHLCV_INITIAL_TRADING_DAYS = 250
OHLCV_INITIAL_START = None  # 예: '20240101' — 지정 시 초기 백필 시작일(YYYYMMDD)


def _as_plain_date(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip().replace('-', '').replace('.', '')
    if len(s) >= 8 and s[:8].isdigit():
        return datetime.strptime(s[:8], '%Y%m%d').date()
    try:
        return pd.to_datetime(v, errors='coerce').date()
    except Exception:
        return None


def _calendar_days_for_trading_days(n_trading):
    """거래일 N ≈ 캘린더일 (주말·공휴일 여유)."""
    n = max(1, int(n_trading))
    return (n * 7) // 5 + 40


def _calendar_ymd_range(start_d, end_d):
    if start_d is None or end_d is None or start_d > end_d:
        return []
    return [d.strftime('%Y%m%d') for d in pd.date_range(start_d, end_d, freq='D')]


def _krx_num_series(s):
    """숫자 파싱. 단독 '-' 만 결측, 부호(-1.23)는 유지."""
    t = s.astype(str).str.replace(',', '', regex=False).str.strip()
    t = t.mask(t.isin(['-', '', 'nan', 'None', 'NaN', '<NA>']))
    return pd.to_numeric(t, errors='coerce')


def _krx_pad_ticker(s):
    t = s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    num = t.str.fullmatch(r'\d+', na=False)
    return t.where(~num, t.str.zfill(6))


def parse_krx_stock_ohlcv_day_csv(df, day_str):
    """KRX MDCSTAT01501 CSV → krx_ohlcv 적재용 DataFrame."""
    d = df.copy()
    d.columns = d.columns.str.replace(' ', '')
    rename = {
        '종목코드': 'ticker',
        '종목명': 'name',
        '시장구분': 'market',
        '시가': 'open',
        '고가': 'high',
        '저가': 'low',
        '종가': 'close',
        '거래량': 'volume',
        '거래대금': 'trading_value',
        '시가총액': 'mcap',
        '등락률': 'chg_pct',
    }
    d = d.rename(columns={k: v for k, v in rename.items() if k in d.columns})
    if 'ticker' not in d.columns or 'close' not in d.columns:
        raise ValueError(f"OHLCV CSV에 ticker/close 없음: {list(d.columns)}")
    d['ticker'] = _krx_pad_ticker(d['ticker'])
    for c in ('open', 'high', 'low', 'close', 'volume', 'trading_value', 'mcap', 'chg_pct'):
        if c in d.columns:
            d[c] = _krx_num_series(d[c])
        else:
            d[c] = np.nan
    if 'name' not in d.columns:
        d['name'] = None
    if 'market' not in d.columns:
        d['market'] = None
    else:
        d['market'] = d['market'].astype(str).str.strip()
        d.loc[d['market'].str.contains('코스닥|KOSDAQ', case=False, na=False), 'market'] = 'KOSDAQ'
        d.loc[d['market'].str.contains('유가|KOSPI', case=False, na=False), 'market'] = 'KOSPI'
    d['date'] = datetime.strptime(day_str, '%Y%m%d').date()
    cols = [
        'ticker', 'date', 'name', 'market',
        'open', 'high', 'low', 'close', 'volume',
        'trading_value', 'mcap', 'chg_pct',
    ]
    return d[cols].dropna(subset=['ticker', 'close'])


def download_krx_stock_ohlcv_day(session, day_str):
    """전종목 시세 CSV (MDCSTAT01501). (DataFrame, raw_bytes) 반환. 휴장이면 빈 DF."""
    content = get_krx_csv(
        session,
        BLD_STOCK_OHLCV,
        {'mktId': 'ALL', 'trdDd': day_str, 'share': '1', 'money': '1'},
    )
    try:
        raw = pd.read_csv(BytesIO(content), encoding='EUC-KR')
    except Exception:
        return pd.DataFrame(), content
    if raw is None or len(raw) == 0:
        return pd.DataFrame(), content
    return parse_krx_stock_ohlcv_day_csv(raw, day_str), content


def ensure_krx_ohlcv_extra_columns(mycursor, con):
    """
    krx_ohlcv에 name/market/trading_value/mcap/chg_pct 가 없으면 ADD COLUMN.
    기존 OHLC·volume 스키마는 유지.
    """
    extras = [
        ('name', 'VARCHAR(100) NULL'),
        ('market', 'VARCHAR(20) NULL'),
        ('trading_value', 'BIGINT NULL'),
        ('mcap', 'BIGINT NULL'),
        ('chg_pct', 'DOUBLE NULL'),
    ]
    mycursor.execute(
        """
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'krx_ohlcv'
        """
    )
    have = {r[0] for r in mycursor.fetchall()}
    for col, typ in extras:
        if col not in have:
            try:
                mycursor.execute(f'ALTER TABLE krx_ohlcv ADD COLUMN `{col}` {typ}')
                con.commit()
                print(f'  · krx_ohlcv.{col} 컬럼 추가')
            except Exception as e:
                print(f'  ⚠️ krx_ohlcv.{col} 추가 실패(무시): {e}')
                con.rollback()
    mycursor.execute(
        """
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'krx_ohlcv'
        """
    )
    return {r[0] for r in mycursor.fetchall()}


def resolve_ohlcv_collect_plan(
    mycursor,
    biz_day,
    table='krx_ohlcv',
    initial_trading_days=OHLCV_INITIAL_TRADING_DAYS,
    initial_start=OHLCV_INITIAL_START,
):
    """
    DB MAX(date) 기준 자동 증분 수집 플랜.
    - DB 비어 있음: 최근 initial_trading_days(또는 initial_start) ~ biz_day
    - MAX == biz_day: 스킵
    - MAX < biz_day: (MAX+1일) ~ biz_day 캘린더 순회(휴장은 CSV 빈응답으로 스킵)
    """
    end = _as_plain_date(biz_day)
    mycursor.execute(f'SELECT MAX(`date`) FROM `{table}`')
    row = mycursor.fetchone()
    db_max = _as_plain_date(row[0]) if row else None

    plan = {
        'mode': 'skip',
        'table': table,
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
        if initial_start:
            start = _as_plain_date(initial_start)
        else:
            start = end - timedelta(days=_calendar_days_for_trading_days(initial_trading_days))
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


def upsert_krx_ohlcv_day_df(mycursor, con, day_df, table_cols, batch_size=1000):
    """하루치 전종목 DF를 krx_ohlcv에 upsert. 반환: {total, inserted, updated}."""
    empty = {'total': 0, 'inserted': 0, 'updated': 0}
    if day_df is None or day_df.empty:
        return empty
    value_cols = ['open', 'high', 'low', 'close', 'volume']
    opt_cols = [c for c in ('name', 'market', 'trading_value', 'mcap', 'chg_pct') if c in table_cols]
    cols = ['ticker', 'date'] + value_cols + opt_cols
    use = day_df[[c for c in cols if c in day_df.columns]].copy()
    for c in value_cols + [x for x in opt_cols if x in ('trading_value', 'mcap', 'chg_pct')]:
        if c in use.columns:
            use[c] = pd.to_numeric(use[c], errors='coerce')
    use = use.dropna(subset=['ticker', 'close'])
    if use.empty:
        return empty

    day = use['date'].iloc[0]
    if isinstance(day, pd.Timestamp):
        day = day.date()
    tickers = [str(t) for t in use['ticker'].tolist()]
    existing = set()
    for i in range(0, len(tickers), 500):
        chunk = tickers[i:i + 500]
        ph = ','.join(['%s'] * len(chunk))
        mycursor.execute(
            f'SELECT ticker FROM krx_ohlcv WHERE `date`=%s AND ticker IN ({ph})',
            (day, *chunk),
        )
        existing.update(str(r[0]) for r in mycursor.fetchall())
    inserted = sum(1 for t in tickers if t not in existing)
    updated = len(tickers) - inserted

    col_sql = ', '.join(f'`{c}`' for c in cols)
    ph = ', '.join(['%s'] * len(cols))
    upd = ', '.join(f'`{c}`=new.`{c}`' for c in cols if c not in ('ticker', 'date'))
    sql = f"""
    INSERT INTO krx_ohlcv ({col_sql})
    VALUES ({ph}) AS new
    ON DUPLICATE KEY UPDATE {upd}
    """
    rows = []
    for r in use.itertuples(index=False):
        tup = []
        for v in r:
            if isinstance(v, float) and (math.isnan(v) or pd.isna(v)):
                tup.append(None)
            elif not isinstance(v, (bytes, bytearray)) and pd.isna(v):
                tup.append(None)
            else:
                tup.append(v)
        rows.append(tuple(tup))
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        mycursor.executemany(sql, batch)
        con.commit()
    return {'total': len(rows), 'inserted': inserted, 'updated': updated}


def collect_krx_ohlcv_by_days(session, mycursor, con, dates, ticker_filter=None):
    """
    거래일(캘린더) 루프로 MDCSTAT01501 일자 CSV 수집·upsert.
    ticker_filter: 보통주 종목코드 set이면 해당 티커만 적재(None이면 CSV 전종목).
    """
    table_cols = ensure_krx_ohlcv_extra_columns(mycursor, con)
    total_rows = 0
    inserted_rows = 0
    updated_rows = 0
    ok_days = 0
    empty_days = 0
    error_days = []
    for day in tqdm(dates, desc='KRX 일봉 CSV'):
        try:
            day_df, raw = download_krx_stock_ohlcv_day(session, day)
            if day_df is None or day_df.empty:
                empty_days += 1
                continue
            if ticker_filter is not None:
                day_df = day_df[day_df['ticker'].isin(ticker_filter)]
            stats = upsert_krx_ohlcv_day_df(mycursor, con, day_df, table_cols)
            total_rows += stats['total']
            inserted_rows += stats['inserted']
            updated_rows += stats['updated']
            ok_days += 1
            _save_krx_csv_backup(raw, 'ohlcv_all', day)
        except Exception as e:
            error_days.append(day)
            print(f'  ⚠️ {day} OHLCV 수집 실패: {e}')
            print(traceback.format_exc())
    print(
        f'  · 일봉 CSV 완료: 거래일적재={ok_days}, 휴장/빈응답={empty_days}, '
        f'실패={len(error_days)}, 행={total_rows} (신규={inserted_rows}, 갱신={updated_rows})'
    )
    return {
        'ok_days': ok_days,
        'empty_days': empty_days,
        'error_days': error_days,
        'rows': total_rows,
        'inserted': inserted_rows,
        'updated': updated_rows,
    }


def build_weekly_ohlcv_from_daily(mycursor, con, ticker_codes, batch_size=50):
    """
    krx_ohlcv 일봉 → 주봉(금요일 기준 W-FRI) 리샘플 후 krx_ohlcv_week upsert.
    종목별 네이버 요청 없이 DB만 사용.
    """
    query = """
        INSERT INTO krx_ohlcv_week (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
        open=new.open, high=new.high, low=new.low, close=new.close, volume=new.volume
    """
    commit_counter = 0
    error_list = []
    for ticker in tqdm(ticker_codes, desc='주봉(일봉→리샘플)'):
        try:
            mycursor.execute(
                """
                SELECT `date`, open, high, low, close, volume
                FROM krx_ohlcv WHERE ticker=%s ORDER BY `date`
                """,
                (ticker,),
            )
            rows = mycursor.fetchall()
            if not rows:
                continue
            price = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            price['date'] = pd.to_datetime(price['date'])
            price = price.set_index('date').sort_index()
            for c in ('open', 'high', 'low', 'close', 'volume'):
                price[c] = pd.to_numeric(price[c], errors='coerce')
            week = price.resample('W-FRI').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
            }).dropna(subset=['close'])
            if week.empty:
                continue
            week = week.reset_index()
            week['ticker'] = ticker
            week = week[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
            args = fetch_ohlcv_args_only_changed(
                mycursor, 'krx_ohlcv_week', ticker, week, ['open', 'high', 'low', 'close', 'volume']
            )
            if args:
                mycursor.executemany(query, args)
                commit_counter += 1
                if commit_counter >= batch_size:
                    con.commit()
                    commit_counter = 0
        except Exception:
            error_list.append(ticker)
            print(ticker)
            print(traceback.format_exc())
    if commit_counter > 0:
        con.commit()
    return error_list


def _save_krx_csv_backup(content, name, day_str):
    """원본 CSV를 {name}_{biz_day}.csv 로 백업. SAVE_KRX_CSV_BACKUP=False면 스킵."""
    if not SAVE_KRX_CSV_BACKUP:
        return
    os.makedirs(KRX_CSV_BACKUP_DIR, exist_ok=True)
    path = os.path.join(KRX_CSV_BACKUP_DIR, f'{name}_{day_str}.csv')
    with open(path, 'wb') as f:
        f.write(content)
    print(f'  백업 저장: {path}')


def _is_valid_sector_csv(content):
    """업종분류 CSV: 데이터 행이 있고 종가가 전부 비어 있지 않으면 유효."""
    try:
        df = pd.read_csv(BytesIO(content), encoding='EUC-KR')
    except Exception:
        return False
    if df is None or len(df) == 0:
        return False
    if '종가' in df.columns:
        close = df['종가'].astype(str).str.strip().replace({'': np.nan, '-': np.nan, 'nan': np.nan})
        if close.isna().all():
            return False
    return True


def find_biz_day(session, max_lookback=10):
    """
    15시 이전이면 전일, 이후면 당일부터 시작해
    KOSPI 업종분류 다운로드가 성공할 때까지 하루씩 뒤로 이동 (최대 max_lookback일).
    """
    if datetime.now().hour < 15:
        candidate = date.today() - timedelta(days=1)
    else:
        candidate = date.today()

    for i in range(max_lookback):
        day_str = candidate.strftime('%Y%m%d')
        print(f'영업일 확인 중: {day_str} ({i + 1}/{max_lookback})')
        try:
            content = get_krx_csv(
                session,
                'dbms/MDC/STAT/standard/MDCSTAT03901',
                {'mktId': 'STK', 'trdDd': day_str, 'money': '1'},
            )
            if _is_valid_sector_csv(content):
                print(f'영업일자 확정: {day_str}')
                return day_str, content
            print(f'  → 데이터 없음(휴장 등), 하루 뒤로 이동')
        except Exception as e:
            print(f'  → 다운로드 실패: {e}, 하루 뒤로 이동')
        candidate = candidate - timedelta(days=1)

    raise RuntimeError(f'최근 {max_lookback}일 내 유효한 영업일을 찾지 못했습니다.')


def update_krx_info():
    """
    KRX에서 업종분류·PER/PBR·ETF 시세를 다운로드해 DB에 upsert.
    Returns:
        str: 확정된 영업일(YYYYMMDD). OHLCV 등 후속 작업의 biz_day로 사용.
    """
    print('KRX 종목/ETF 정보 업데이트 시작...')

    krx_session = rq.Session()
    krx_session.headers.update(KRX_INFO_HEADERS)
    if not krx_login(krx_session):
        raise RuntimeError('KRX 로그인 실패. 환경변수 KRX_ID / KRX_PW 를 확인하세요.')

    biz_day, kospi_csv = find_biz_day(krx_session)
    _save_krx_csv_backup(kospi_csv, 'sector_kospi', biz_day)

    kosdaq_csv = get_krx_csv(
        krx_session,
        'dbms/MDC/STAT/standard/MDCSTAT03901',
        {'mktId': 'KSQ', 'trdDd': biz_day, 'money': '1'},
    )
    _save_krx_csv_backup(kosdaq_csv, 'sector_kosdaq', biz_day)

    ratio_csv = get_krx_csv(
        krx_session,
        'dbms/MDC/STAT/standard/MDCSTAT03501',
        {'searchType': '1', 'mktId': 'ALL', 'trdDd': biz_day},
    )
    _save_krx_csv_backup(ratio_csv, 'ratio', biz_day)

    etf_csv = get_krx_csv(
        krx_session,
        'dbms/MDC/STAT/standard/MDCSTAT04301',
        {'trdDd': biz_day, 'share': '1', 'money': '1'},
    )
    _save_krx_csv_backup(etf_csv, 'etf', biz_day)

    ### 코스피/코스닥 업종 분류
    sector_stk = pd.read_csv(BytesIO(kospi_csv), encoding='EUC-KR')
    sector_ksq = pd.read_csv(BytesIO(kosdaq_csv), encoding='EUC-KR')

    krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)

    krx_sector['종목명'] = krx_sector['종목명'].str.strip()
    krx_sector['기준일'] = biz_day

    ### 개별종목 지표 PER/PBR/배당수익률
    krx_ratio = pd.read_csv(BytesIO(ratio_csv), encoding='EUC-KR')

    krx_ratio['종목명'] = krx_ratio['종목명'].str.strip()
    krx_ratio['기준일'] = biz_day

    ### 합치기
    krx_diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ratio['종목명'])))

    krx_ticker = pd.merge(krx_sector, krx_ratio,
                          on=krx_sector.columns.intersection(
                              krx_ratio.columns).tolist(),
                          how='outer')

    ### 종목 구분
    krx_ticker['종목구분'] = np.where(krx_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                                  np.where(krx_ticker['종목명'].str.endswith('리츠'), '리츠',
                                           np.where(krx_ticker['종목명'].isin(krx_diff), '기타',
                                                    '보통주')))

    krx_ticker = krx_ticker.reset_index(drop=True)
    krx_ticker.columns = krx_ticker.columns.str.replace(' ', '')
    krx_ticker = krx_ticker[['종목코드', '종목명', '시장구분', '업종명', '종가', '대비', '등락률',
                             '시가총액', '기준일', 'EPS', 'PER', 'BPS', 'PBR', '주당배당금', '배당수익률', '종목구분']]
    krx_ticker = krx_ticker.replace({np.nan: None})
    krx_ticker['기준일'] = pd.to_datetime(krx_ticker['기준일'])

    krx_ticker = krx_ticker.dropna(subset=['업종명'])

    ## DB 적재 (krx_ticker)
    con = pymysql.connect(
        user=require_env('DB_USER'),
        passwd=require_env('DB_PASSWORD'),
        host='127.0.0.1',
        db='kor_stock_db',
        charset='utf8'
    )
    mycursor = con.cursor()

    query = f"""
    insert into krx_ticker (종목코드, 종목명, 시장구분, 업종명, 종가, 대비, 등락률, 시가총액, 기준일, EPS, PER, BPS, PBR, 주당배당금, 배당수익률, 종목구분)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    종목명=new.종목명, 시장구분=new.시장구분, 업종명=new.업종명, 종가=new.종가, 대비=new.대비, 등락률=new.등락률, 시가총액=new.시가총액, EPS=new.EPS,
    PER=new.PER, BPS=new.BPS, PBR=new.PBR, 주당배당금=new.주당배당금, 배당수익률=new.배당수익률, 종목구분=new.종목구분;
    """

    args = krx_ticker.values.tolist()
    mycursor.executemany(query, args)
    con.commit()
    con.close()

    print('ticker 적재 완료')

    ##### 지수 정보 가져오기 (주석 유지 — 구 krx_info)

    # index_list = []
    # try:
    #     # KOSPI 지수 목록 가져오기 (market 파라미터 명시)
    #     for ticker in stock.get_index_ticker_list(biz_day, market='KOSPI'):
    #         data = [ticker, stock.get_index_ticker_name(ticker)]
    #         index_list.append(data)
    # except Exception as e:
    #     print(f"KOSPI 지수 목록 가져오기 실패: {e}")
    #     print(f"에러 타입: {type(e).__name__}")
    #     # 대안: market 파라미터 없이 시도
    #     try:
    #         for ticker in stock.get_index_ticker_list(biz_day):
    #             data = [ticker, stock.get_index_ticker_name(ticker)]
    #             index_list.append(data)
    #     except Exception as e2:
    #         print(f"대안 방법도 실패: {e2}")

    # try:
    #     # KOSDAQ 지수 목록 가져오기
    #     for ticker in stock.get_index_ticker_list(biz_day, market='KOSDAQ'):
    #         data = [ticker, stock.get_index_ticker_name(ticker)]
    #         index_list.append(data)
    # except Exception as e:
    #     print(f"KOSDAQ 지수 목록 가져오기 실패: {e}")


    # index_df = pd.DataFrame(index_list, columns = ['ticker', 'sector'])



    # sector_portf = {}

    # for idx, ser in tqdm(index_df.iterrows(), total = index_df.shape[0]):


    #     # a = stock.get_index_portfolio_deposit_file(ser.ticker)

    #     port = []
    #     for t in stock.get_index_portfolio_deposit_file(ser.ticker):


    #         port.append(t)


    #     sector_portf[ser.ticker] = port




    # engine = create_engine(db_url())

    # query = """
    # select * from krx_ticker
    # where 기준일 = (select max(기준일) from krx_ticker) and 종목구분 = '보통주';
    # """

    # ticker_list = pd.read_sql(query, con=engine)

    # ticker_list = ticker_list[['종목코드', '종목명', '업종명']]

    # ticker_list = ticker_list.set_index('종목코드')


    # stock_sector_list = []
    # for t in tqdm(ticker_list.index):

    #     for k, v in sector_portf.items():

    #         if t in v:
    #             stock_sector_list.append([t, k])

    # stock_sector_df = pd.DataFrame(stock_sector_list, columns = ['ticker', 'sector_cd'])


    # stock_sector_list2 = []
    # for idx, ser in tqdm(stock_sector_df.iterrows(), total = stock_sector_df.shape[0]):

    #     for i, s in ticker_list.iterrows():
    #         if i == ser.ticker:
    #             data = [i, s.종목명]

    #     for i, s in index_df.iterrows():
    #         if s.ticker == ser.sector_cd:
    #             data.append(s.ticker)
    #             data.append(s.sector)

    #     stock_sector_list2.append(data)
    # # for k, v in tqdm


    # ## DB 저장 쿼리

    # con = pymysql.connect(user='root',
    # passwd=require_env('DB_PASSWORD'),
    # host='127.0.0.1',
    # db='kor_stock_db',
    # charset='utf8')

    # mycursor = con.cursor()

    # query = """
    #     insert into krx_ticker_sector (ticker, cp_nm ,sector_cd, sector_nm)
    #     values (%s, %s, %s, %s) as new
    #     on duplicate key update
    #     ticker=new.ticker, cp_nm=new.cp_nm, sector_cd=new.sector_cd, sector_nm=new.sector_nm;
    # """






    # mycursor.executemany(query, stock_sector_list2)
    # con.commit()
    # con.close()

    ### ETF 저장
    etf_df = pd.read_csv(BytesIO(etf_csv), encoding='cp949', dtype=str)
    etf_df.columns = etf_df.columns.str.strip()
    etf_df = etf_df.rename(columns={'순자산가치(NAV)': '순자산가치'})

    etf_df['종목코드'] = etf_df['종목코드'].astype(str).str.zfill(6)
    etf_df['종목명'] = etf_df['종목명'].str.strip()

    numeric_cols = [
        '종가', '대비', '등락률', '순자산가치', '시가', '고가', '저가',
        '거래량', '거래대금', '시가총액', '순자산총액', '상장좌수',
        '기초지수_종가', '기초지수_대비', '기초지수_등락률'
    ]

    existing_numeric_cols = [col for col in numeric_cols if col in etf_df.columns]

    for col in existing_numeric_cols:
        etf_df[col] = etf_df[col].str.replace(',', '', regex=False).str.replace(r'^\-$', '', regex=True)
        etf_df[col] = pd.to_numeric(etf_df[col], errors='coerce')

    if '기초지수_지수명' in etf_df.columns:
        etf_df['기초지수_지수명'] = etf_df['기초지수_지수명'].astype(str).str.strip()
        etf_df['기초지수_지수명'] = etf_df['기초지수_지수명'].replace('nan', None)
    else:
        etf_df['기초지수_지수명'] = None

    etf_df['기준일'] = pd.to_datetime(biz_day)
    etf_df = etf_df.replace({np.nan: None, pd.NaT: None})

    etf_cols = ['종목코드', '종목명', '종가', '대비', '등락률', '순자산가치',
                '시가', '고가', '저가', '거래량', '거래대금', '시가총액',
                '순자산총액', '상장좌수', '기초지수_지수명', '기초지수_종가',
                '기초지수_대비', '기초지수_등락률', '기준일']

    etf_df = etf_df[etf_cols]

    con = pymysql.connect(
        user=require_env('DB_USER'),
        passwd=require_env('DB_PASSWORD'),
        host='127.0.0.1',
        db='kor_stock_db',
        charset='utf8'
    )
    mycursor = con.cursor()

    create_etf_info_table = """
    CREATE TABLE IF NOT EXISTS krx_etf_info (
        종목코드 VARCHAR(10) NOT NULL,
        종목명 VARCHAR(200) NOT NULL,
        종가 DECIMAL(15, 2),
        대비 DECIMAL(15, 2),
        등락률 DECIMAL(10, 4),
        순자산가치 DECIMAL(15, 2),
        시가 DECIMAL(15, 2),
        고가 DECIMAL(15, 2),
        저가 DECIMAL(15, 2),
        거래량 BIGINT,
        거래대금 BIGINT,
        시가총액 BIGINT,
        순자산총액 BIGINT,
        상장좌수 BIGINT,
        기초지수_지수명 VARCHAR(200),
        기초지수_종가 DECIMAL(15, 2),
        기초지수_대비 DECIMAL(15, 2),
        기초지수_등락률 DECIMAL(10, 4),
        기준일 DATE NOT NULL,
        PRIMARY KEY (종목코드, 기준일),
        INDEX idx_etf_info_date (기준일)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    mycursor.execute(create_etf_info_table)
    con.commit()

    query = """
    insert into krx_etf_info (
        종목코드, 종목명, 종가, 대비, 등락률, 순자산가치, 시가, 고가, 저가,
        거래량, 거래대금, 시가총액, 순자산총액, 상장좌수,
        기초지수_지수명, 기초지수_종가, 기초지수_대비, 기초지수_등락률, 기준일
    ) values (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    ) as new
    on duplicate key update
        종목명=new.종목명,
        종가=new.종가,
        대비=new.대비,
        등락률=new.등락률,
        순자산가치=new.순자산가치,
        시가=new.시가,
        고가=new.고가,
        저가=new.저가,
        거래량=new.거래량,
        거래대금=new.거래대금,
        시가총액=new.시가총액,
        순자산총액=new.순자산총액,
        상장좌수=new.상장좌수,
        기초지수_지수명=new.기초지수_지수명,
        기초지수_종가=new.기초지수_종가,
        기초지수_대비=new.기초지수_대비,
        기초지수_등락률=new.기초지수_등락률;
    """

    batch_size = 1000
    args = etf_df.values.tolist()

    total_batches = (len(args) + batch_size - 1) // batch_size
    for i in range(0, len(args), batch_size):
        batch = args[i:i + batch_size]
        mycursor.executemany(query, batch)
        con.commit()
        if total_batches > 1:
            print(f'ETF 배치 처리: {i // batch_size + 1}/{total_batches} 완료', end='\r')

    con.close()
    print('\nETF 정보 적재 완료')
    return biz_day


# =============================================================================
# 실행: 1) 종목/ETF 정보 → 2) OHLCV 수집
# =============================================================================

## 서버 설정
engine = create_engine(db_url())

print('=' * 60)
print('1. KRX 종목/ETF 정보 업데이트')
print('=' * 60)
biz_day = update_krx_info()
print(f'기준 영업일(biz_day): {biz_day}')

print('=' * 60)
print('2. OHLCV 가져오기 시작')
print('=' * 60)

con = pymysql.connect(user=require_env('DB_USER'),
passwd=require_env('DB_PASSWORD'),
host='127.0.0.1',
db='kor_stock_db',
charset='utf8')

mycursor = con.cursor()


query = """
select * from krx_ticker
where 기준일 = (select max(기준일) from krx_ticker) and 종목구분 = '보통주';
"""

ticker_list = pd.read_sql(query, con=engine)
ticker_codes = ticker_list['종목코드'].tolist()


### 일봉 — KRX MDCSTAT01501 일자 CSV (전종목 1요청/일). DB MAX(date) 자동 증분.

print(' - 일봉 데이터를 저장합니다. (KRX CSV MDCSTAT01501, DB 자동 증분)')

ohlcv_plan = resolve_ohlcv_collect_plan(mycursor, biz_day)
print(f'   {ohlcv_plan["message"]}')
print(
    f'   DB 최신={ohlcv_plan["db_max"]}, 기준일={ohlcv_plan["biz_day"]}, '
    f'모드={ohlcv_plan["mode"]}'
)

# 참조 CSV 정합 구간: 수집 대상 또는 최근 초기 룩백
_ref_from = ohlcv_plan['from_date'] or (
    _as_plain_date(biz_day) - timedelta(days=_calendar_days_for_trading_days(OHLCV_INITIAL_TRADING_DAYS))
)
_ref_to = ohlcv_plan['to_date'] or _as_plain_date(biz_day)
fr = _ref_from.strftime('%Y%m%d')
to = _ref_to.strftime('%Y%m%d')

# 참조 CSV 정합용 upsert (기본 OHLC 스키마)
query = """
        insert into krx_ohlcv (ticker, date, open, high, low, close, volume)
    values (%s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    open=new.open, high=new.high, low=new.low, close=new.close, volume=new.volume;
"""

batch_size = 50
error_list = []
ohlcv_dates = ohlcv_plan['dates']

krx_ohlcv_session = rq.Session()
krx_ohlcv_session.headers.update(KRX_INFO_HEADERS)
if not krx_login(krx_ohlcv_session):
    raise RuntimeError('KRX 로그인 실패(일봉). 환경변수 KRX_ID / KRX_PW 를 확인하세요.')

ticker_filter = set(str(t).zfill(6) if str(t).isdigit() else str(t) for t in ticker_codes)
if ohlcv_plan['mode'] == 'skip' or not ohlcv_dates:
    print('   (이미 적재됨 — 일봉 CSV 스킵)')
else:
    print(
        f'   대상 구간: {ohlcv_dates[0]}~{ohlcv_dates[-1]} '
        f'(캘린더 {len(ohlcv_dates)}일, 휴장은 응답으로 스킵)'
    )
    ohlcv_stats = collect_krx_ohlcv_by_days(
        krx_ohlcv_session, mycursor, con, ohlcv_dates, ticker_filter=ticker_filter
    )
    error_list = list(ohlcv_stats.get('error_days') or [])
    print(
        f'   요약: 수집거래일={ohlcv_stats.get("ok_days", 0)}, '
        f'신규행={ohlcv_stats.get("inserted", 0)}, 갱신행={ohlcv_stats.get("updated", 0)}'
    )

print(' - 일봉 참조 CSV 정합')
try:
    sync_krx_ohlcv_from_reference_csv_dir(
        mycursor, con, KRX_OHLCV_REFERENCE_DIR, fr, to, query, batch_size=400
    )
except Exception as e:
    print(f'⚠️ 일봉 참조 CSV 정합 중 오류: {e}')
    print(traceback.format_exc())

krx_ohlcv_session.close()



### 투자자별 매매동향 — KRX 12010 (MDCSTAT02401) 일자×투자자구분 CSV

print(' - 투자자별 매매동향 데이터를 저장합니다. (KRX 12010 MDCSTAT02401)')

# ---------------------------------------------------------------------------
# 스펙 캡처(2026-08-01 실측 OTP CSV, 추측 아님):
#   menuId=MDC0201020303 / screen=[12010] 투자자별 순매수상위종목
#   bld=dbms/MDC/STAT/standard/MDCSTAT02401
#   OTP params: strtDd,endDd,mktId=ALL,invstTpCd,share=1,money=1,csvxls_isNo=false
#   CSV: 종목코드,종목명,거래량_매도/매수/순매수,거래대금_매도/매수/순매수
#   invstTpCd 13종: 아래 목록(실측 다운로드 성공으로 확인)
#   커버리지: 당일 해당 투자자 거래 종목(순매도 포함). 무거래 종목 누락 가능.
#   예) 20260731 OHLCV=2644 vs 전체=2743 / 외국인=2625 / 은행=79
# ---------------------------------------------------------------------------
BLD_INVESTOR_NET = 'dbms/MDC/STAT/standard/MDCSTAT02401'
KRX_INVST_TP_CD = [
    ('1000', '금융투자'),
    ('2000', '보험'),
    ('3000', '투신'),
    ('3100', '사모'),
    ('4000', '은행'),
    ('5000', '기타금융'),
    ('6000', '연기금등'),
    ('7050', '기관합계'),
    ('7100', '기타법인'),
    ('8000', '개인'),
    ('9000', '외국인'),
    ('9001', '기타외국인'),
    ('9999', '전체'),
]
INVESTOR_KRX_INITIAL_TRADING_DAYS = 250
INVESTOR_KRX_INITIAL_START = None  # 예: '20240101'
INVESTOR_KRX_REFERER = (
    'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303'
)


def ensure_krx_investor_trade_krx_table(mycursor, con):
    mycursor.execute(
        """
        CREATE TABLE IF NOT EXISTS krx_investor_trade_krx (
            `date` DATE NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            invst_tp_cd VARCHAR(8) NOT NULL,
            invst_tp_nm VARCHAR(32) NULL,
            sell_qty BIGINT NULL,
            buy_qty BIGINT NULL,
            net_qty BIGINT NULL,
            sell_val BIGINT NULL,
            buy_val BIGINT NULL,
            net_val BIGINT NULL,
            PRIMARY KEY (`date`, ticker, invst_tp_cd),
            INDEX idx_ticker_date (ticker, `date`),
            INDEX idx_invst_date (invst_tp_cd, `date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    con.commit()


def ensure_krx_investor_trading_wide_table(mycursor, con):
    """기존 와이드 호환 테이블. 외국인_보유율은 12010에 없어 미갱신."""
    mycursor.execute(
        """
        CREATE TABLE IF NOT EXISTS krx_investor_trading (
            ticker VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            `종가` BIGINT DEFAULT NULL,
            `전일비` VARCHAR(64) DEFAULT NULL,
            `등락률` DECIMAL(10,4) DEFAULT NULL,
            `거래량` BIGINT DEFAULT NULL,
            `기관_순매매량` BIGINT DEFAULT NULL,
            `외국인_순매매량` BIGINT DEFAULT NULL,
            `외국인_보유주수` BIGINT DEFAULT NULL,
            `외국인_보유율` DECIMAL(10,4) DEFAULT NULL,
            PRIMARY KEY (ticker, date),
            INDEX idx_date (date),
            INDEX idx_ticker (ticker)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    con.commit()
    mycursor.execute('SHOW COLUMNS FROM krx_investor_trading')
    have = {row[0] for row in mycursor.fetchall()}
    for col, ddl in [
        ('종가', 'BIGINT DEFAULT NULL'),
        ('전일비', 'VARCHAR(64) DEFAULT NULL'),
        ('등락률', 'DECIMAL(10,4) DEFAULT NULL'),
        ('거래량', 'BIGINT DEFAULT NULL'),
        ('기관_순매매량', 'BIGINT DEFAULT NULL'),
        ('외국인_순매매량', 'BIGINT DEFAULT NULL'),
        ('외국인_보유주수', 'BIGINT DEFAULT NULL'),
        ('외국인_보유율', 'DECIMAL(10,4) DEFAULT NULL'),
    ]:
        if col not in have:
            try:
                mycursor.execute(
                    f'ALTER TABLE krx_investor_trading ADD COLUMN `{col}` {ddl}'
                )
                con.commit()
                print(f'  · krx_investor_trading.{col} 추가')
            except Exception as e:
                print(f'  ⚠️ 컬럼 추가 실패 {col}: {e}')
                con.rollback()


def resolve_investor_krx_collect_plan(mycursor, biz_day):
    end = _as_plain_date(biz_day)
    mycursor.execute('SELECT MAX(`date`) FROM krx_investor_trade_krx')
    row = mycursor.fetchone()
    db_max = _as_plain_date(row[0]) if row else None
    plan = {
        'mode': 'skip',
        'db_max': db_max,
        'biz_day': end,
        'from_date': None,
        'dates': [],
        'message': '',
    }
    if end is None:
        plan['message'] = '기준영업일 없음 — 스킵'
        return plan
    if db_max is None:
        start = (
            _as_plain_date(INVESTOR_KRX_INITIAL_START)
            if INVESTOR_KRX_INITIAL_START
            else end - timedelta(
                days=_calendar_days_for_trading_days(INVESTOR_KRX_INITIAL_TRADING_DAYS)
            )
        )
        plan['mode'] = 'initial'
        plan['from_date'] = start
        plan['dates'] = _calendar_ymd_range(start, end)
        plan['message'] = (
            f'DB 비어 있음 → 초기 백필 {start}~{end} '
            f'(캘린더 {len(plan["dates"])}일 × 투자자 {len(KRX_INVST_TP_CD)}종)'
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
        f'증분 수집 {start}~{end} (DB 최신={db_max}, '
        f'캘린더 {len(plan["dates"])}일 × {len(KRX_INVST_TP_CD)} CSV/일)'
    )
    return plan


def parse_krx_investor_net_csv(df, day_str, invst_tp_cd, invst_tp_nm):
    if df is None or len(df) == 0:
        return pd.DataFrame()
    d = df.copy()
    d.columns = d.columns.str.replace(' ', '')
    rename = {
        '종목코드': 'ticker',
        '종목명': 'name',
        '거래량_매도': 'sell_qty',
        '거래량_매수': 'buy_qty',
        '거래량_순매수': 'net_qty',
        '거래대금_매도': 'sell_val',
        '거래대금_매수': 'buy_val',
        '거래대금_순매수': 'net_val',
    }
    d = d.rename(columns={k: v for k, v in rename.items() if k in d.columns})
    if 'ticker' not in d.columns:
        raise ValueError(f'12010 CSV에 종목코드 없음: {list(d.columns)}')
    d['ticker'] = _krx_pad_ticker(d['ticker'])
    for c in ('sell_qty', 'buy_qty', 'net_qty', 'sell_val', 'buy_val', 'net_val'):
        if c in d.columns:
            d[c] = _krx_num_series(d[c])
        else:
            d[c] = np.nan
    d['date'] = datetime.strptime(day_str, '%Y%m%d').date()
    d['invst_tp_cd'] = str(invst_tp_cd)
    d['invst_tp_nm'] = invst_tp_nm
    cols = [
        'date', 'ticker', 'invst_tp_cd', 'invst_tp_nm',
        'sell_qty', 'buy_qty', 'net_qty', 'sell_val', 'buy_val', 'net_val',
    ]
    return d[cols].dropna(subset=['ticker'])


def download_krx_investor_net_csv(session, day_str, invst_tp_cd):
    content = get_krx_csv(
        session,
        BLD_INVESTOR_NET,
        {
            'strtDd': day_str,
            'endDd': day_str,
            'mktId': 'ALL',
            'invstTpCd': str(invst_tp_cd),
            'share': '1',
            'money': '1',
        },
    )
    try:
        raw = pd.read_csv(BytesIO(content), encoding='EUC-KR')
    except Exception:
        return pd.DataFrame(), content
    return raw, content


def upsert_krx_investor_trade_krx(mycursor, con, day_df, batch_size=1000):
    if day_df is None or day_df.empty:
        return 0
    sql = """
    INSERT INTO krx_investor_trade_krx
      (`date`, ticker, invst_tp_cd, invst_tp_nm,
       sell_qty, buy_qty, net_qty, sell_val, buy_val, net_val)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) AS new
    ON DUPLICATE KEY UPDATE
      invst_tp_nm=new.invst_tp_nm,
      sell_qty=new.sell_qty, buy_qty=new.buy_qty, net_qty=new.net_qty,
      sell_val=new.sell_val, buy_val=new.buy_val, net_val=new.net_val
    """
    rows = []
    for r in day_df.itertuples(index=False):
        tup = []
        for v in r:
            if isinstance(v, float) and (math.isnan(v) or pd.isna(v)):
                tup.append(None)
            elif not isinstance(v, (bytes, bytearray)) and pd.isna(v):
                tup.append(None)
            else:
                tup.append(v)
        rows.append(tuple(tup))
    for i in range(0, len(rows), batch_size):
        mycursor.executemany(sql, rows[i:i + batch_size])
        con.commit()
    return len(rows)


def upsert_investor_trading_wide_from_day(mycursor, con, by_invst_net_qty):
    """기관합계(7050)·외국인(9000) net_qty → krx_investor_trading."""
    inst = by_invst_net_qty.get('7050') or {}
    frgn = by_invst_net_qty.get('9000') or {}
    tickers = set(inst) | set(frgn)
    if not tickers:
        return 0
    sql = """
    INSERT INTO krx_investor_trading (ticker, date, `기관_순매매량`, `외국인_순매매량`)
    VALUES (%s, %s, %s, %s) AS new
    ON DUPLICATE KEY UPDATE
      `기관_순매매량`=COALESCE(new.`기관_순매매량`, `기관_순매매량`),
      `외국인_순매매량`=COALESCE(new.`외국인_순매매량`, `외국인_순매매량`)
    """
    rows = []
    for tk in tickers:
        d_i, n_i = inst.get(tk, (None, None))
        d_f, n_f = frgn.get(tk, (None, None))
        day = d_i or d_f
        if day is None:
            continue
        rows.append((tk, day, n_i, n_f))
    for i in range(0, len(rows), 1000):
        mycursor.executemany(sql, rows[i:i + 1000])
        con.commit()
    return len(rows)


def collect_krx_investor_trade_by_days(session, mycursor, con, dates, ohlcv_universe_n=None):
    total_rows = 0
    ok_days = 0
    empty_days = 0
    error_items = []
    for day in tqdm(dates, desc='KRX 12010 투자자 CSV'):
        day_any = False
        wide_maps = {}
        day_counts = {}
        for invst_cd, invst_nm in KRX_INVST_TP_CD:
            try:
                raw, content = download_krx_investor_net_csv(session, day, invst_cd)
                if raw is None or len(raw) == 0:
                    continue
                parsed = parse_krx_investor_net_csv(raw, day, invst_cd, invst_nm)
                if parsed.empty:
                    continue
                n = upsert_krx_investor_trade_krx(mycursor, con, parsed)
                total_rows += n
                day_any = True
                day_counts[invst_cd] = n
                if invst_cd in ('7050', '9000'):
                    m = {}
                    for _, r in parsed.iterrows():
                        nq = r['net_qty']
                        nq = None if pd.isna(nq) else int(nq)
                        m[str(r['ticker'])] = (r['date'], nq)
                    wide_maps[invst_cd] = m
                _save_krx_csv_backup(content, f'invst_{invst_cd}', day)
            except Exception as e:
                error_items.append(f'{day}:{invst_cd}')
                print(f'  ⚠️ {day} invst={invst_cd}({invst_nm}) 실패: {e}')
                print(traceback.format_exc())
        if day_any:
            ok_days += 1
            wn = upsert_investor_trading_wide_from_day(mycursor, con, wide_maps)
            if ohlcv_universe_n:
                for cd, cnt in sorted(day_counts.items()):
                    if cnt < ohlcv_universe_n * 0.5:
                        nm = dict(KRX_INVST_TP_CD).get(cd, cd)
                        print(
                            f'  · 커버리지 참고 {day} {cd}({nm}): {cnt}행 '
                            f'(OHLCV우주≈{ohlcv_universe_n}) — 무거래 종목 제외 가능'
                        )
            print(
                f'  · {day} 적재: invst행={sum(day_counts.values())}, '
                f'와이드갱신={wn}, CSV={len(day_counts)}/{len(KRX_INVST_TP_CD)}'
            )
        else:
            empty_days += 1
    print(
        f'  · 12010 완료: 거래일={ok_days}, 휴장/빈={empty_days}, '
        f'실패항목={len(error_items)}, long행={total_rows}'
    )
    return {
        'ok_days': ok_days,
        'empty_days': empty_days,
        'error_items': error_items,
        'rows': total_rows,
    }


ensure_krx_investor_trade_krx_table(mycursor, con)
ensure_krx_investor_trading_wide_table(mycursor, con)
print('✓ krx_investor_trade_krx / krx_investor_trading 테이블 확인')

inv_plan = resolve_investor_krx_collect_plan(mycursor, biz_day)
print(f'   {inv_plan["message"]}')

try:
    mycursor.execute(
        'SELECT COUNT(DISTINCT ticker) FROM krx_ohlcv WHERE `date`=%s',
        (_as_plain_date(biz_day),),
    )
    _ohlcv_univ = mycursor.fetchone()[0] or None
except Exception:
    _ohlcv_univ = None

ENABLE_KIS_INVESTOR_TRADE_KIS = False  # True: KIS 종목별 투자자매매동향(일별) 수집 활성화
if ENABLE_KIS_INVESTOR_TRADE_KIS:
    # --- KIS API: 종목별 투자자매매동향(일별) FHPTJ04160001 ---
    # https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily
    KIS_URL_BASE = "https://openapi.koreainvestment.com:9443"
    KIS_APP_KEY = require_env('KIS_APP_KEY')
    KIS_APP_SECRET = require_env('KIS_APP_SECRET')
    KIS_INVESTOR_TRADE_PATH = "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
    KIS_INVESTOR_TR_ID = "FHPTJ04160001"
    KIS_INVESTOR_MAX_DAYS = 300
    KIS_TOKEN_CACHE_PATH = os.path.join(_script_dir(), ".kis_token_cache.json")
    KIS_API_SLEEP_SEC = 0.06

    KIS_INVESTOR_TRADE_VALUE_COLUMNS = [
        'stck_clpr', 'prdy_vrss', 'prdy_ctrt', 'acml_vol', 'acml_tr_pbmn',
        'stck_oprc', 'stck_hgpr', 'stck_lwpr',
        'frgn_ntby_qty', 'frgn_reg_ntby_qty', 'frgn_nreg_ntby_qty',
        'prsn_ntby_qty', 'orgn_ntby_qty',
        'scrt_ntby_qty', 'ivtr_ntby_qty', 'pe_fund_ntby_vol',
        'bank_ntby_qty', 'insu_ntby_qty', 'mrbn_ntby_qty', 'fund_ntby_qty',
        'etc_ntby_qty', 'etc_corp_ntby_vol', 'etc_orgt_ntby_vol',
        'frgn_reg_ntby_pbmn', 'frgn_ntby_tr_pbmn', 'frgn_nreg_ntby_pbmn',
        'prsn_ntby_tr_pbmn', 'orgn_ntby_tr_pbmn',
        'scrt_ntby_tr_pbmn', 'pe_fund_ntby_tr_pbmn', 'ivtr_ntby_tr_pbmn',
        'bank_ntby_tr_pbmn', 'insu_ntby_tr_pbmn', 'mrbn_ntby_tr_pbmn',
        'fund_ntby_tr_pbmn', 'etc_ntby_tr_pbmn', 'etc_corp_ntby_tr_pbmn', 'etc_orgt_ntby_tr_pbmn',
        'frgn_seln_vol', 'frgn_shnu_vol', 'frgn_seln_tr_pbmn', 'frgn_shnu_tr_pbmn',
        'frgn_reg_askp_qty', 'frgn_reg_bidp_qty', 'frgn_reg_askp_pbmn', 'frgn_reg_bidp_pbmn',
        'frgn_nreg_askp_qty', 'frgn_nreg_bidp_qty', 'frgn_nreg_askp_pbmn', 'frgn_nreg_bidp_pbmn',
        'prsn_seln_vol', 'prsn_shnu_vol', 'prsn_seln_tr_pbmn', 'prsn_shnu_tr_pbmn',
        'orgn_seln_vol', 'orgn_shnu_vol', 'orgn_seln_tr_pbmn', 'orgn_shnu_tr_pbmn',
        'scrt_seln_vol', 'scrt_shnu_vol', 'scrt_seln_tr_pbmn', 'scrt_shnu_tr_pbmn',
        'ivtr_seln_vol', 'ivtr_shnu_vol', 'ivtr_seln_tr_pbmn', 'ivtr_shnu_tr_pbmn',
        'pe_fund_seln_vol', 'pe_fund_shnu_vol', 'pe_fund_seln_tr_pbmn', 'pe_fund_shnu_tr_pbmn',
        'bank_seln_vol', 'bank_shnu_vol', 'bank_seln_tr_pbmn', 'bank_shnu_tr_pbmn',
        'insu_seln_vol', 'insu_shnu_vol', 'insu_seln_tr_pbmn', 'insu_shnu_tr_pbmn',
        'mrbn_seln_vol', 'mrbn_shnu_vol', 'mrbn_seln_tr_pbmn', 'mrbn_shnu_tr_pbmn',
        'fund_seln_vol', 'fund_shnu_vol', 'fund_seln_tr_pbmn', 'fund_shnu_tr_pbmn',
        'etc_seln_vol', 'etc_shnu_vol', 'etc_seln_tr_pbmn', 'etc_shnu_tr_pbmn',
        'etc_orgt_seln_vol', 'etc_orgt_shnu_vol', 'etc_orgt_seln_tr_pbmn', 'etc_orgt_shnu_tr_pbmn',
        'etc_corp_seln_vol', 'etc_corp_shnu_vol', 'etc_corp_seln_tr_pbmn', 'etc_corp_shnu_tr_pbmn',
    ]


    def _kis_fetch_access_token(use_cache=True):
        """KIS OAuth2 접근토큰 발급 (캐시 재사용)."""
        url_token = f"{KIS_URL_BASE}/oauth2/tokenP"
        cache_key = f"{url_token}|{KIS_APP_KEY}"
        if use_cache and os.path.isfile(KIS_TOKEN_CACHE_PATH):
            try:
                with open(KIS_TOKEN_CACHE_PATH, "r", encoding="utf-8") as f:
                    cache = json.load(f)
                if cache.get("key") == cache_key:
                    exp = cache.get("expires_at", 0)
                    if exp > time.time() + 60 and cache.get("access_token"):
                        return cache["access_token"]
            except (OSError, json.JSONDecodeError, TypeError):
                pass

        res = rq.post(
            url_token,
            headers={"content-type": "application/json"},
            data=json.dumps({
                "grant_type": "client_credentials",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
            }),
            timeout=20,
        )
        data = res.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"KIS 토큰 발급 실패 (HTTP {res.status_code}): {data}")
        if use_cache:
            try:
                expires_in = int(data.get("expires_in", 86400))
                with open(KIS_TOKEN_CACHE_PATH, "w", encoding="utf-8") as f:
                    json.dump({
                        "key": cache_key,
                        "access_token": token,
                        "expires_at": time.time() + max(expires_in - 300, 3600),
                    }, f)
            except OSError:
                pass
        return token


    def _kis_bigint(val):
        if val is None:
            return None
        s = str(val).replace(',', '').strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            return None


    def _kis_decimal(val):
        if val is None:
            return None
        s = str(val).replace(',', '').replace('%', '').strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None


    def _kis_parse_bsop_date(ymd_str):
        s = str(ymd_str or '').strip()
        if len(s) != 8 or not s.isdigit():
            return None
        return datetime.strptime(s, '%Y%m%d').date()


    def _kis_investor_trade_row_to_values(row):
        vals = []
        for col in KIS_INVESTOR_TRADE_VALUE_COLUMNS:
            raw = row.get(col)
            if col == 'prdy_ctrt':
                vals.append(_kis_decimal(raw))
            else:
                vals.append(_kis_bigint(raw))
        return tuple(vals)


    def _ensure_krx_investor_trade_kis_table(mycursor, con):
        col_defs = ['ticker VARCHAR(10) NOT NULL', 'date DATE NOT NULL']
        for col in KIS_INVESTOR_TRADE_VALUE_COLUMNS:
            if col == 'prdy_ctrt':
                col_defs.append(f'`{col}` DECIMAL(12,4) DEFAULT NULL')
            else:
                col_defs.append(f'`{col}` BIGINT DEFAULT NULL')
        col_defs.append('PRIMARY KEY (ticker, date)')
        col_defs.append('INDEX idx_kis_inv_date (date)')
        col_defs.append('INDEX idx_kis_inv_ticker (ticker)')
        ddl = "CREATE TABLE IF NOT EXISTS krx_investor_trade_kis (\n    " + ",\n    ".join(col_defs) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        mycursor.execute(ddl)
        con.commit()


    def fetch_kis_investor_trade_by_stock_daily(
        access_token,
        ticker,
        base_date,
        market_code='J',
        max_days=None,
        after_date=None,
        max_pages=15,
        timeout=15,
    ):
        """
        KIS 종목별 투자자매매동향(일별) 조회.
        API 1회당 약 30거래일 → 기준일(FID_INPUT_DATE_1)을 과거로 이동하며 반복 호출.
        max_days: 최대 수집 거래일 수 (초기 적재)
        after_date: 이 날짜 이후만 반환 (증분 적재, exclusive)
        """
        url = f"{KIS_URL_BASE}{KIS_INVESTOR_TRADE_PATH}"
        merged = {}
        cur_base = str(base_date)
        biz_dt = datetime.strptime(cur_base, '%Y%m%d').date()
        calls = 0

        while calls < max_pages:
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {access_token}",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
                "tr_id": KIS_INVESTOR_TR_ID,
                "custtype": "P",
            }
            params = {
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": cur_base,
                "FID_ORG_ADJ_PRC": "",
                "FID_ETC_CLS_CODE": "",
            }
            res = rq.get(url, headers=headers, params=params, timeout=timeout)
            time.sleep(KIS_API_SLEEP_SEC)
            calls += 1

            try:
                data = res.json()
            except ValueError:
                raise RuntimeError(f"KIS JSON 파싱 실패 ({ticker}): {res.text[:300]}")

            if data.get('rt_cd') != '0':
                msg = data.get('msg1') or data.get('msg_cd') or data
                raise RuntimeError(f"KIS API 오류 ({ticker}): {msg}")

            output2 = data.get('output2') or []
            if not output2:
                break

            batch_dates = []
            for row in output2:
                d = _kis_parse_bsop_date(row.get('stck_bsop_date'))
                if d is None or d > biz_dt:
                    continue
                batch_dates.append(d)
                if after_date and d <= after_date:
                    continue
                merged[d] = row

            if max_days and len(merged) >= max_days:
                break

            if after_date and batch_dates and min(batch_dates) <= after_date:
                break

            if len(output2) < 30:
                break

            oldest = min(batch_dates) if batch_dates else None
            if oldest is None:
                break
            cur_base = (oldest - timedelta(days=1)).strftime('%Y%m%d')

        rows = sorted(merged.items(), key=lambda x: x[0], reverse=True)
        if max_days:
            rows = rows[:max_days]
        return [row for _, row in rows]


    def _load_kis_investor_trade_kis_status(mycursor):
        """종목별 최신일·건수 (증분/초기 적재 판단)."""
        try:
            mycursor.execute(
                "SELECT ticker, MAX(`date`) AS max_d, COUNT(*) AS cnt "
                "FROM krx_investor_trade_kis GROUP BY ticker"
            )
            out = {}
            for ticker, max_d, cnt in mycursor.fetchall():
                tkey = _investor_ticker_key(ticker)
                if isinstance(max_d, datetime):
                    max_d = max_d.date()
                out[tkey] = {'max_date': max_d, 'count': int(cnt)}
            return out
        except Exception as e:
            print(f"⚠️ krx_investor_trade_kis 상태 조회 실패 — 전 종목 초기 적재 모드: {e}")
            return {}


    def _kis_investor_rows_to_args(tkey, rows):
        args = []
        for row in rows:
            d = _kis_parse_bsop_date(row.get('stck_bsop_date'))
            if d is None:
                continue
            args.append((tkey, d) + _kis_investor_trade_row_to_values(row))
        return args


    def _save_kis_investor_trade_rows(mycursor, query, tkey, rows):
        args = _kis_investor_rows_to_args(tkey, rows)
        if args:
            mycursor.executemany(query, args)
        return len(args)



error_list_investor = []
krx_inv_session = rq.Session()
krx_inv_session.headers.update({
    'User-Agent': KRX_INFO_HEADERS.get('User-Agent', 'Mozilla/5.0'),
    'Referer': INVESTOR_KRX_REFERER,
})
if not krx_login(krx_inv_session):
    raise RuntimeError('KRX 로그인 실패(투자자 12010). KRX_ID/KRX_PW 확인')

if inv_plan['mode'] == 'skip' or not inv_plan['dates']:
    print('   (이미 적재됨 — 12010 CSV 스킵)')
else:
    print(
        f'   대상 구간: {inv_plan["dates"][0]}~{inv_plan["dates"][-1]} '
        f'(캘린더 {len(inv_plan["dates"])}일, 일당 CSV {len(KRX_INVST_TP_CD)}장)'
    )
    inv_stats = collect_krx_investor_trade_by_days(
        krx_inv_session, mycursor, con, inv_plan['dates'], ohlcv_universe_n=_ohlcv_univ
    )
    error_list_investor = list(inv_stats.get('error_items') or [])
    print(
        f'   요약: 수집거래일={inv_stats.get("ok_days", 0)}, '
        f'long행={inv_stats.get("rows", 0)}, 실패={len(error_list_investor)}'
    )

krx_inv_session.close()

if error_list_investor:
    print(f'\n⚠️ 투자자 12010 수집 실패 항목 수: {len(error_list_investor)}')
    if len(error_list_investor) <= 15:
        print(f'실패: {error_list_investor}')


# --- KIS API — 종목별 투자자매매동향(일별) [비활성화: ENABLE_KIS_INVESTOR_TRADE_KIS=False] ---
if ENABLE_KIS_INVESTOR_TRADE_KIS:

    print(' - KIS 종목별 투자자매매동향(일별) 데이터를 저장합니다.')

    try:
        _ensure_krx_investor_trade_kis_table(mycursor, con)
        print("✓ krx_investor_trade_kis 테이블 확인 완료")
    except Exception as e:
        print(f"⚠️ krx_investor_trade_kis 테이블 생성 오류: {e}")
        print(traceback.format_exc())

    _kis_value_cols_sql = ', '.join(f'`{c}`' for c in KIS_INVESTOR_TRADE_VALUE_COLUMNS)
    _kis_placeholders = ', '.join(['%s'] * len(KIS_INVESTOR_TRADE_VALUE_COLUMNS))
    _kis_update_sql = ', '.join(f'`{c}`=new.`{c}`' for c in KIS_INVESTOR_TRADE_VALUE_COLUMNS)
    query_kis_investor = f"""
        insert into krx_investor_trade_kis (ticker, date, {_kis_value_cols_sql})
        values (%s, %s, {_kis_placeholders}) as new
        on duplicate key update {_kis_update_sql};
    """

    try:
        kis_access_token = _kis_fetch_access_token()
        print("✓ KIS 접근 토큰 발급 완료")
    except Exception as e:
        kis_access_token = None
        print(f"⚠️ KIS 접근 토큰 발급 실패 — KIS 투자자매매동향 수집을 건너뜁니다: {e}")

    error_list_kis_investor = []
    commit_counter_kis = 0
    biz_day_date = datetime.strptime(biz_day, '%Y%m%d').date()

    if kis_access_token:
        kis_investor_status = _load_kis_investor_trade_kis_status(mycursor)

        for ticker_raw in tqdm(ticker_codes, desc="KIS 투자자매매동향(일별)"):
            tkey = _investor_ticker_key(ticker_raw)
            status = kis_investor_status.get(tkey)

            try:
                if status and status.get('max_date') and status['max_date'] >= biz_day_date:
                    continue

                if status is None or status.get('count', 0) < KIS_INVESTOR_MAX_DAYS:
                    rows = fetch_kis_investor_trade_by_stock_daily(
                        kis_access_token,
                        tkey,
                        biz_day,
                        max_days=KIS_INVESTOR_MAX_DAYS,
                        after_date=None,
                        max_pages=12,
                    )
                else:
                    rows = fetch_kis_investor_trade_by_stock_daily(
                        kis_access_token,
                        tkey,
                        biz_day,
                        max_days=None,
                        after_date=status['max_date'],
                        max_pages=2,
                    )

                if _save_kis_investor_trade_rows(mycursor, query_kis_investor, tkey, rows):
                    commit_counter_kis += 1
                    if commit_counter_kis >= batch_size:
                        con.commit()
                        commit_counter_kis = 0

            except RuntimeError as e:
                err_msg = str(e)
                if 'EGW00123' in err_msg or 'token' in err_msg.lower():
                    try:
                        kis_access_token = _kis_fetch_access_token(use_cache=False)
                        rows = fetch_kis_investor_trade_by_stock_daily(
                            kis_access_token, tkey, biz_day,
                            max_days=KIS_INVESTOR_MAX_DAYS if (status is None or status.get('count', 0) < KIS_INVESTOR_MAX_DAYS) else None,
                            after_date=None if (status is None or status.get('count', 0) < KIS_INVESTOR_MAX_DAYS) else status.get('max_date'),
                            max_pages=12 if (status is None or status.get('count', 0) < KIS_INVESTOR_MAX_DAYS) else 2,
                        )
                        if _save_kis_investor_trade_rows(mycursor, query_kis_investor, tkey, rows):
                            commit_counter_kis += 1
                            if commit_counter_kis >= batch_size:
                                con.commit()
                                commit_counter_kis = 0
                    except Exception as retry_e:
                        error_list_kis_investor.append(tkey)
                        if len(error_list_kis_investor) <= 5:
                            print(f"  KIS 재시도 실패 ({tkey}): {retry_e}")
                else:
                    error_list_kis_investor.append(tkey)
                    if len(error_list_kis_investor) <= 5:
                        print(f"  KIS 수집 오류 ({tkey}): {e}")
            except Exception as e:
                error_list_kis_investor.append(tkey)
                if len(error_list_kis_investor) <= 5:
                    print(f"  KIS 수집 오류 ({tkey}): {e}")

        if commit_counter_kis > 0:
            con.commit()

    if error_list_kis_investor:
        print(f"\n⚠️ KIS 투자자매매동향 수집 실패 종목 수: {len(error_list_kis_investor)}")
        if len(error_list_kis_investor) <= 10:
            print(f"실패 종목: {error_list_kis_investor}")


### 주봉 — 일봉(krx_ohlcv) W-FRI 리샘플 (종목별 외부 요청 없음)

print(' - 주봉 데이터를 저장합니다. (일봉 DB → W-FRI 리샘플)')
error_list = build_weekly_ohlcv_from_daily(mycursor, con, ticker_codes, batch_size=batch_size)
if error_list:
    print(f'⚠️ 주봉 리샘플 실패 종목 수: {len(error_list)}')
con.close()


### 지수

print('지수 OHLCV 가져오기 시작')

# 코스피와 코스닥 지수만 사용
# 네이버 파이낸스 지수 코드: https://finance.naver.com/sise/sise_index.nhn 에서 확인
index_codes = {
    'KOSPI': '코스피',
    'KOSDAQ': '코스닥',
}

# 네이버 파이낸스 코드를 DB ticker로 변환하는 매핑
index_ticker_mapping = {
    'KOSPI': '1001',   # 코스피 -> 1001
    'KOSDAQ': '2001',  # 코스닥 -> 2001
}

# 네이버 파이낸스에서 지수 목록 크롤링
def get_naver_index_list(session=None):
    """네이버 파이낸스에서 지수 목록을 가져옵니다."""
    index_list = []
    temp_session = None
    try:
        url = 'https://finance.naver.com/sise/sise_index.nhn'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        # 세션 사용 (있으면 재사용, 없으면 새로 생성)
        if session is None:
            temp_session = rq.Session()
            temp_session.headers.update(headers)
            response = temp_session.get(url, timeout=10)
        else:
            response = session.get(url, timeout=10)
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 지수 목록이 있는 테이블 찾기 (더 정확한 선택자 사용)
        # 코스피/코스닥 지수 섹션 찾기
        index_sections = soup.find_all('div', class_='lst_sub')
        
        if not index_sections:
            # 대체 방법: 테이블로 찾기
            tables = soup.find_all('table', class_='type_1')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows[1:]:  # 헤더 제외
                    tds = row.find_all('td')
                    if len(tds) >= 1:
                        a_tag = tds[0].find('a')
                        if a_tag and a_tag.get('href'):
                            href = a_tag.get('href')
                            if 'code=' in href:
                                index_code = href.split('code=')[-1].split('&')[0]
                                index_name = a_tag.text.strip()
                                if index_code and index_name:
                                    index_list.append([index_code, index_name])
        else:
            # 섹션별로 지수 목록 추출
            for section in index_sections:
                links = section.find_all('a', href=re.compile('code='))
                for link in links:
                    href = link.get('href', '')
                    if 'code=' in href:
                        index_code = href.split('code=')[-1].split('&')[0]
                        index_name = link.text.strip()
                        if index_code and index_name and index_code not in [item[0] for item in index_list]:
                            index_list.append([index_code, index_name])
        
        # 중복 제거
        seen = set()
        unique_list = []
        for code, name in index_list:
            if code not in seen:
                seen.add(code)
                unique_list.append([code, name])
        
        return unique_list
    except Exception as e:
        print(f"지수 목록 크롤링 실패: {e}")
        return []
    finally:
        if temp_session:
            temp_session.close()

# 코스피와 코스닥 지수만 사용
index_list = [[code, name] for code, name in index_codes.items()]

# 세션 미리 생성 (크롤링과 데이터 수집에서 재사용)
session = rq.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

print(f"코스피, 코스닥 지수 {len(index_list)}개 사용: {[name for _, name in index_list]}")

index_df = pd.DataFrame(index_list, columns = ['ticker', 'sector'])
index_df.set_index('ticker', inplace=True)


## DB 저장 쿼리

con = pymysql.connect(user=require_env('DB_USER'),
passwd=require_env('DB_PASSWORD'),
host='127.0.0.1',
db='kor_stock_db',
charset='utf8')

mycursor = con.cursor()

query = """
    insert into krx_index_ohlcv (ticker, date, open, high, low, close, volume, volume_amount, market_value)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    open=new.open, high=new.high, low=new.low, close=new.close, volume=new.volume, volume_amount=new.volume_amount, market_value=new.market_value;
"""

## 오류 방생시 저장할 리스트 생성

error_list2 = []

# 전 종목 주가 다운로드 및 지표 생성
commit_counter = 0

# 세션은 이미 위에서 생성됨 (재사용)


# 지수 OHLCV: krx_naver_ohlcv 공통 모듈 사용

# 성공한 지수만 저장할 리스트
successful_indices = []

# 가져온 데이터를 저장할 딕셔너리 (확인용)
collected_data = {}

for ticker in tqdm(index_df.index, desc="지수 데이터 수집"):
    index_name = index_df.loc[ticker, 'sector']
    print(f"\n[{ticker}] {index_name} 데이터 가져오기...")
    
    try:
        # 네이버 파이낸스에서 지수 데이터 가져오기
        df = get_index_ohlcv_from_naver(ticker, fr, to)
        
        if df is None or len(df) == 0:
            print(f"  ❌ 지수 {ticker} ({index_name}) 데이터 없음 - 스킵")
            error_list2.append(ticker)
            continue
        
        print(f"  ✓ 데이터 {len(df)}개 가져옴")
        
        # ticker 컬럼이 이미 있으면 제거 후 다시 추가
        if 'ticker' in df.columns:
            df = df.drop(columns=['ticker'])
        
        # 가져온 원본 데이터 저장 (확인용) - 네이버 코드로 저장
        collected_data[ticker] = df.copy()
        
        # 네이버 파이낸스 코드를 DB ticker로 변환
        db_ticker = index_ticker_mapping.get(ticker, ticker)  # 매핑이 없으면 원본 사용
        print(f"  → DB ticker 변환: {ticker} -> {db_ticker}")
        
        # ticker 컬럼 추가 (DB ticker 사용)
        df.insert(0, 'ticker', db_ticker)
        
        # 날짜 형식 확인 및 변환 (datetime -> date 또는 문자열)
        if df['date'].dtype == 'datetime64[ns]':
            # datetime을 date 객체로 변환 (MySQL DATE 타입과 호환)
            df['date'] = df['date'].dt.date
        
        # 컬럼 순서 조정 (DB 스키마에 맞춤)
        required_columns = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume', 'volume_amount', 'market_value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"\n지수 {ticker} - 누락된 컬럼: {missing_columns}")
            for col in missing_columns:
                df[col] = None
        
        df = df[required_columns]
        
        # NaN 값을 None으로 변환 (DB 저장시 NULL로 처리)
        df = df.where(pd.notnull(df), None)
        
        # 데이터 타입 확인 및 변환
        # 숫자 컬럼은 float로 변환 (None은 유지)
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # None 값을 다시 채워넣기 (NaN이 None으로 변환되었는지 확인)
        df = df.where(pd.notnull(df), None)
        
        # 데이터 검증
        if len(df) == 0:
            print(f"\n지수 {ticker} - 변환 후 데이터가 없음")
            error_list2.append(ticker)
            continue
        
        # 첫 번째 행 확인 (디버깅용)
        if len(successful_indices) == 0:
            print(f"\n첫 번째 데이터 샘플 (지수 {ticker}):")
            print(df.head(1))
            print(f"데이터 타입:")
            print(df.dtypes)
        
        args = fetch_ohlcv_args_only_changed(
            mycursor,
            'krx_index_ohlcv',
            db_ticker,
            df,
            ['open', 'high', 'low', 'close', 'volume', 'volume_amount', 'market_value'],
        )
        
        # SQL 실행 및 오류 확인
        try:
            if len(args) == 0:
                print(f"  ⏭ DB와 동일하여 스킵 ({ticker} -> {db_ticker}, 수집 {len(df)}행)")
                successful_indices.append(ticker)
                continue
                
            # 실제 SQL 실행
            affected_rows = mycursor.executemany(query, args)
            
            print(f"  ✓ SQL 실행 완료: 변경 {len(args)}행 / 수집 {len(df)}행 ({ticker} -> {db_ticker})")
            successful_indices.append(ticker)
            
            commit_counter += 1
            if commit_counter >= batch_size:
                con.commit()
                print(f"  → 커밋 완료 (batch_size={batch_size} 도달, 누적 커밋)")
                commit_counter = 0
        except Exception as sql_error:
            print(f"  ❌ 지수 {ticker} SQL 실행 오류: {sql_error}")
            print(f"  오류 타입: {type(sql_error).__name__}")
            if args and len(args) > 0:
                print(f"  첫 번째 데이터 행 (샘플): {args[0]}")
                print(f"  첫 번째 데이터 행 타입: {[type(x).__name__ for x in args[0]]}")
                # 날짜 확인
                if len(args[0]) > 1:
                    print(f"  날짜 값: {args[0][1]} (타입: {type(args[0][1]).__name__})")
            error_list2.append(ticker)
            try:
                con.rollback()
                print(f"  → 롤백 완료")
            except:
                pass
            continue
    
    except Exception as e:
        print(f"\n지수 {ticker} ({index_df.loc[ticker, 'sector']}) 처리 오류 - 스킵")
        error_list2.append(ticker)
        print(traceback.format_exc())
        try:
            con.rollback()
        except:
            pass

# 남은 데이터 커밋
if commit_counter > 0:
    con.commit()
    print(f"  → 최종 커밋 완료 (남은 {commit_counter}개 배치)")

session.close()
con.close()

print(f'\n=== 지수 OHLCV 수집 완료 ===')
print(f'성공: {len(successful_indices)}개')
if successful_indices:
    print(f'성공 지수 목록: {successful_indices}')

print(f'실패: {len(error_list2)}개')
if error_list2:
    print(f'실패 지수 목록: {error_list2}')

# DB 저장 확인
if successful_indices:
    print(f'\n=== DB 저장 확인 ===')
    try:
        con_check = pymysql.connect(user=require_env('DB_USER'),
                                    passwd=require_env('DB_PASSWORD'),
                                    host='127.0.0.1',
                                    db='kor_stock_db',
                                    charset='utf8')
        mycursor_check = con_check.cursor()
        
        for ticker_naver in successful_indices:
            db_ticker = index_ticker_mapping.get(ticker_naver, ticker_naver)
            check_query = "SELECT COUNT(*) as cnt, MAX(date) as max_date, MIN(date) as min_date FROM krx_index_ohlcv WHERE ticker = %s"
            mycursor_check.execute(check_query, (db_ticker,))
            result = mycursor_check.fetchone()
            
            if result:
                cnt, max_date, min_date = result
                print(f"  [{ticker_naver} -> {db_ticker}] 저장된 행 수: {cnt}개, 기간: {min_date} ~ {max_date}")
            else:
                print(f"  [{ticker_naver} -> {db_ticker}] 저장 확인 실패")
        
        con_check.close()
    except Exception as e:
        print(f"  DB 저장 확인 중 오류: {e}")

# 가져온 데이터 확인용 데이터프레임 생성
if collected_data:
    print(f'\n=== 수집된 지수 데이터 확인 ===')
    all_dfs = []
    for ticker in successful_indices:
        if ticker in collected_data:
            df_check = collected_data[ticker].copy()
            df_check.insert(0, 'ticker', ticker)
            df_check.insert(1, 'index_name', index_df.loc[ticker, 'sector'])
            all_dfs.append(df_check)
    
    if all_dfs:
        # 모든 지수 데이터를 하나의 데이터프레임으로 합치기
        index_data_summary = pd.concat(all_dfs, ignore_index=True)
        
        print(f'\n전체 수집 데이터 요약:')
        print(f'- 총 데이터 행 수: {len(index_data_summary)}개')
        print(f'- 수집된 지수: {index_data_summary["ticker"].unique().tolist()}')
        print(f'\n지수별 데이터 개수:')
        print(index_data_summary.groupby(['ticker', 'index_name']).size())
        
        print(f'\n각 지수별 최신 데이터 (최근 5일):')
        for ticker in successful_indices:
            if ticker in collected_data:
                ticker_df = index_data_summary[index_data_summary['ticker'] == ticker].copy()
                if len(ticker_df) > 0:
                    print(f'\n[{ticker}] {index_df.loc[ticker, "sector"]}:')
                    # 최신 날짜 기준으로 정렬
                    ticker_df_sorted = ticker_df.sort_values('date', ascending=False).head(5)
                    print(ticker_df_sorted[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']].to_string(index=False))
        
        print(f'\n=== 전체 데이터프레임 변수명: index_data_summary ===')
        print(f'데이터프레임을 확인하려면: print(index_data_summary) 또는 index_data_summary.head()')
    else:
        print('수집된 데이터가 없습니다.')
else:
    print('\n수집된 데이터가 없습니다.')


### 테마 정보 수집

print('테마 정보 수집 시작')

## DB 연결
con = pymysql.connect(user=require_env('DB_USER'),
passwd=require_env('DB_PASSWORD'),
host='127.0.0.1',
db='kor_stock_db',
charset='utf8')

mycursor = con.cursor()

# 테이블 구조 확인 및 필요한 컬럼 추가
print("테이블 구조 확인 중...")
try:
    # 기존 컬럼 목록 확인
    mycursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'kor_stock_db' 
        AND TABLE_NAME = 'krx_theme'
    """)
    existing_columns = [row[0] for row in mycursor.fetchall()]
    
    # 필요한 컬럼 목록
    required_columns = {
        'change_rate': "ADD COLUMN change_rate DECIMAL(10, 2)",
        'recent_3days_change_rate': "ADD COLUMN recent_3days_change_rate DECIMAL(10, 2)",
        'up_count': "ADD COLUMN up_count INT DEFAULT 0",
        'same_count': "ADD COLUMN same_count INT DEFAULT 0",
        'down_count': "ADD COLUMN down_count INT DEFAULT 0",
        'leading_stock1_ticker': "ADD COLUMN leading_stock1_ticker VARCHAR(10)",
        'leading_stock1_name': "ADD COLUMN leading_stock1_name VARCHAR(100)",
        'leading_stock2_ticker': "ADD COLUMN leading_stock2_ticker VARCHAR(10)",
        'leading_stock2_name': "ADD COLUMN leading_stock2_name VARCHAR(100)"
    }
    
    # 누락된 컬럼 추가
    for col_name, alter_stmt in required_columns.items():
        if col_name not in existing_columns:
            try:
                mycursor.execute(f"ALTER TABLE krx_theme {alter_stmt}")
                con.commit()
                print(f"✓ 컬럼 '{col_name}' 추가 완료")
            except Exception as e:
                print(f"⚠️ 컬럼 '{col_name}' 추가 실패: {e}")
        else:
            print(f"✓ 컬럼 '{col_name}' 이미 존재")
    
except Exception as e:
    print(f"테이블 구조 확인 중 오류: {e}")
    print("테이블이 존재하지 않거나 접근 권한이 없을 수 있습니다.")
    print("수동으로 다음 SQL을 실행해주세요:")
    print("""
    ALTER TABLE krx_theme ADD COLUMN change_rate DECIMAL(10, 2);
    ALTER TABLE krx_theme ADD COLUMN recent_3days_change_rate DECIMAL(10, 2);
    ALTER TABLE krx_theme ADD COLUMN up_count INT DEFAULT 0;
    ALTER TABLE krx_theme ADD COLUMN same_count INT DEFAULT 0;
    ALTER TABLE krx_theme ADD COLUMN down_count INT DEFAULT 0;
    ALTER TABLE krx_theme ADD COLUMN leading_stock1_ticker VARCHAR(10);
    ALTER TABLE krx_theme ADD COLUMN leading_stock1_name VARCHAR(100);
    ALTER TABLE krx_theme ADD COLUMN leading_stock2_ticker VARCHAR(10);
    ALTER TABLE krx_theme ADD COLUMN leading_stock2_name VARCHAR(100);
    """)

# krx_theme_stock 테이블 컬럼 확인 및 추가 (theme_name, stock_name)
try:
    mycursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'kor_stock_db'
          AND TABLE_NAME = 'krx_theme_stock';
    """)
    existing_theme_stock_columns = {row[0] for row in mycursor.fetchall()}
    
    required_theme_stock_columns = {
        'theme_name': 'VARCHAR(100)',
        'stock_name': 'VARCHAR(100)'
    }
    
    for column_name, column_type in required_theme_stock_columns.items():
        if column_name not in existing_theme_stock_columns:
            alter_sql = f"ALTER TABLE krx_theme_stock ADD COLUMN {column_name} {column_type}"
            print(f"krx_theme_stock 테이블에 {column_name} 컬럼을 추가합니다.")
            mycursor.execute(alter_sql)
            con.commit()
except Exception as e:
    print(f"krx_theme_stock 테이블 구조 확인 중 오류: {e}")
    print("테이블이 존재하지 않거나 접근 권한이 없을 수 있습니다.")
    print("수동으로 다음 SQL을 실행해주세요:")
    print("""
    ALTER TABLE krx_theme_stock ADD COLUMN theme_name VARCHAR(100);
    ALTER TABLE krx_theme_stock ADD COLUMN stock_name VARCHAR(100);
    """)

# 테마 정보 저장 쿼리
query_theme = """
    insert into krx_theme (theme_code, theme_name, change_rate, recent_3days_change_rate, 
                          up_count, same_count, down_count, leading_stock1_ticker, leading_stock1_name,
                          leading_stock2_ticker, leading_stock2_name, update_date)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    theme_name=new.theme_name, 
    change_rate=new.change_rate,
    recent_3days_change_rate=new.recent_3days_change_rate,
    up_count=new.up_count,
    same_count=new.same_count,
    down_count=new.down_count,
    leading_stock1_ticker=new.leading_stock1_ticker,
    leading_stock1_name=new.leading_stock1_name,
    leading_stock2_ticker=new.leading_stock2_ticker,
    leading_stock2_name=new.leading_stock2_name,
    update_date=new.update_date;
"""

# 테마-종목 매핑 저장 쿼리
query_theme_stock = """
    insert into krx_theme_stock (theme_code, theme_name, ticker, stock_name, update_date)
    values (%s, %s, %s, %s, %s) as new
    on duplicate key update
    theme_name=new.theme_name,
    stock_name=new.stock_name,
    update_date=new.update_date;
"""

# 종목명 텍스트 정리 함수
def clean_stock_text(raw_text):
    if raw_text is None:
        return None
    cleaned = re.sub(r'[\r\n\t]', ' ', raw_text)
    cleaned = re.sub(r'[▲▼△▽↑↓▶▷◀▴▾※⇧⇩→←↗↘⤴⤵▵▿■◆●★☆☞⇨➤➔⮕➟➠➤➣]+', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned or None

# 세션 생성
session = rq.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
})

# 테마 목록 페이지 가져오기
theme_list_url = 'https://finance.naver.com/sise/theme.naver'
theme_stocks_data = []

try:
    # 첫 페이지에서 총 페이지 수 확인
    print("페이지 수 확인 중...")
    response = session.get(theme_list_url, timeout=10)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 페이지네이션에서 마지막 페이지 번호 찾기
    max_pages = 1
    paging_div = None
    
    # 여러 가능한 페이지네이션 클래스명 시도
    possible_classes = ['Nnavi', 'paging', 'pager', 'page_navi', 'pageNav']
    for class_name in possible_classes:
        paging_div = soup.find('div', {'class': class_name})
        if paging_div:
            break
    
    # 또는 테이블 다음의 페이지네이션 찾기
    if not paging_div:
        # 테마 테이블 다음 요소들을 확인
        theme_table = soup.find('table', {'class': 'type_1'})
        if theme_table:
            # 테이블 다음 형제 요소들 확인
            next_siblings = theme_table.find_next_siblings()
            for sibling in next_siblings:
                if sibling.name == 'div' and ('navi' in sibling.get('class', []) or 'paging' in sibling.get('class', [])):
                    paging_div = sibling
                    break
    
    if paging_div:
        # 페이지 번호 링크들 찾기
        page_links = paging_div.find_all('a')
        page_numbers = []
        for link in page_links:
            href = link.get('href', '')
            # URL에서 page=번호 추출
            page_match = re.search(r'[?&]page=(\d+)', href)
            if page_match:
                page_num = int(page_match.group(1))
                if page_num > 0:
                    page_numbers.append(page_num)
            # 또는 링크 텍스트에서 숫자 추출
            try:
                text = link.get_text(strip=True)
                if text.isdigit():
                    page_num = int(text)
                    if page_num > 0:
                        page_numbers.append(page_num)
            except:
                pass
        
        if page_numbers:
            max_pages = max(page_numbers)
            print(f"총 {max_pages}개 페이지를 발견했습니다.")
        else:
            # 페이지네이션에서 숫자 텍스트로 확인
            paging_text = paging_div.get_text()
            # "이전", "다음" 등이 있는 경우 마지막 숫자 찾기
            numbers = re.findall(r'\b(\d+)\b', paging_text)
            if numbers:
                # 큰 숫자만 선택 (페이지 번호는 보통 작은 숫자)
                numbers = [int(n) for n in numbers if int(n) > 0 and int(n) <= 100]
                if numbers:
                    max_pages = max(numbers)
                    print(f"총 {max_pages}개 페이지를 발견했습니다.")
    
    # 페이지네이션을 찾지 못한 경우, 적당한 범위로 설정
    if max_pages == 1:
        print("페이지네이션을 찾을 수 없습니다. 데이터 기반으로 페이지 수를 확인합니다.")
        max_pages = 50  # 충분히 큰 값으로 시작
    
    all_themes = []
    
    # 연속으로 데이터가 없는 페이지 수를 카운트
    empty_page_count = 0
    max_empty_pages = 2  # 연속으로 2페이지에 데이터가 없으면 중단
    
    # 모든 페이지 순회
    for page in tqdm(range(1, max_pages + 1), desc="테마 목록 수집"):
        try:
            if page == 1:
                url = theme_list_url
            else:
                url = f'{theme_list_url}?&page={page}'
            
            response = session.get(url, timeout=10)
            response.encoding = 'euc-kr'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 테마 목록 테이블 찾기
            theme_table = soup.find('table', {'class': 'type_1'})
            if not theme_table:
                empty_page_count += 1
                if empty_page_count >= max_empty_pages:
                    print(f"\n페이지 {page}부터 연속으로 데이터가 없어 중단합니다.")
                    break
                continue
            
            rows = theme_table.find_all('tr')[1:]  # 헤더 제외
            
            if not rows:
                empty_page_count += 1
                if empty_page_count >= max_empty_pages:
                    print(f"\n페이지 {page}부터 연속으로 데이터가 없어 중단합니다.")
                    break
                continue
            
            # 데이터가 있으면 카운터 리셋
            empty_page_count = 0
            
            page_theme_count = 0
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if len(cols) < 4:
                        continue
                    
                    # 테마명과 링크 추출 (1열)
                    theme_link = cols[0].find('a')
                    if not theme_link:
                        continue
                    
                    theme_name = theme_link.get_text(strip=True)
                    theme_href = theme_link.get('href', '')
                    
                    # 테마 코드 추출 (URL에서 no=xxx 부분)
                    theme_code_match = re.search(r'no=(\d+)', theme_href)
                    if not theme_code_match:
                        continue
                    
                    theme_code = theme_code_match.group(1)
                    
                    # 중복 체크
                    if any(t['theme_code'] == theme_code for t in all_themes):
                        continue
                    
                    # 등락률 추출 (2열) - 전일대비 등락률
                    change_rate_text = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                    change_rate = None
                    if change_rate_text and change_rate_text != '':
                        # % 제거하고 숫자 추출
                        change_rate_match = re.search(r'([+-]?\d+\.?\d*)', change_rate_text.replace('%', ''))
                        if change_rate_match:
                            try:
                                change_rate = float(change_rate_match.group(1))
                            except:
                                change_rate = None
                    
                    # 최근 3일 등락률 추출 (3열)
                    recent_3days_change_rate_text = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                    recent_3days_change_rate = None
                    if recent_3days_change_rate_text and recent_3days_change_rate_text != '':
                        recent_3days_match = re.search(r'([+-]?\d+\.?\d*)', recent_3days_change_rate_text.replace('%', ''))
                        if recent_3days_match:
                            try:
                                recent_3days_change_rate = float(recent_3days_match.group(1))
                            except:
                                recent_3days_change_rate = None
                    
                    # 상승/보합/하락 종목수 추출 (4열)
                    # 실제 HTML 구조 확인: 여러 열에서 찾기
                    up_count = 0
                    same_count = 0
                    down_count = 0
                    
                    status_found = False
                    
                    # 우선 기본 테이블 구조(각 숫자가 별도 열)에 맞춰 추출
                    if len(cols) >= 6:
                        try:
                            up_text = cols[3].get_text(strip=True)
                            same_text = cols[4].get_text(strip=True)
                            down_text = cols[5].get_text(strip=True)
                            
                            up_count = int(re.findall(r'\d+', up_text)[0]) if re.findall(r'\d+', up_text) else 0
                            same_count = int(re.findall(r'\d+', same_text)[0]) if re.findall(r'\d+', same_text) else 0
                            down_count = int(re.findall(r'\d+', down_text)[0]) if re.findall(r'\d+', down_text) else 0
                            
                            status_found = any([up_count, same_count, down_count])
                        except Exception as e:
                            status_found = False
                    
                    # 다른 형식에 대한 백업 파싱
                    if not status_found:
                        for col_idx, col in enumerate(cols):
                            if col_idx < 3:
                                continue
                            
                            status_text = col.get_text(strip=True)
                            if not status_text:
                                continue
                            
                            numbers = re.findall(r'\d+', status_text)
                            if len(numbers) >= 3:
                                try:
                                    up_count = int(numbers[0])
                                    same_count = int(numbers[1])
                                    down_count = int(numbers[2])
                                    status_found = True
                                    break
                                except:
                                    pass
                            
                            if not status_found:
                                up_match = re.search(r'상승\s*[:]?\s*(\d+)', status_text)
                                same_match = re.search(r'보합\s*[:]?\s*(\d+)', status_text)
                                down_match = re.search(r'하락\s*[:]?\s*(\d+)', status_text)
                                
                                if up_match:
                                    up_count = int(up_match.group(1))
                                if same_match:
                                    same_count = int(same_match.group(1))
                                if down_match:
                                    down_count = int(down_match.group(1))
                                
                                if up_match or same_match or down_match:
                                    status_found = True
                                    break
                    
                    if not status_found and page == 1 and page_theme_count < 3:
                        print(f"[디버깅] 상승/보합/하락 추출 실패 - 테마: {theme_name}")
                        for col_idx, col in enumerate(cols):
                            print(f"  열 {col_idx} 텍스트: {col.get_text(strip=True)}")
                            if col_idx >= 3:
                                print(f"    HTML: {col.prettify()[:200]}...")
                    
                    # 주도주 정보 추출 (모든 열에서 종목 링크 찾기)
                    leading_stock1_ticker = None
                    leading_stock1_name = None
                    leading_stock2_ticker = None
                    leading_stock2_name = None
                    
                    # 모든 열을 순회하며 종목 링크 찾기 (처음 4개 열 제외)
                    all_stock_links = []
                    for col_idx, col in enumerate(cols):
                        if col_idx < 4:  # 처음 4개 열은 건너뛰기
                            continue
                        
                        # 이 열의 모든 종목 링크 찾기
                        stock_links = col.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                        for link in stock_links:
                            href = link.get('href', '')
                            code_match = re.search(r'code=(\d+)', href)
                            if code_match:
                                ticker = code_match.group(1)
                                name = link.get_text(strip=True)
                                if ticker and name and len(ticker) == 6:  # 종목코드는 6자리
                                    # 중복 제거
                                    if not any(s['ticker'] == ticker for s in all_stock_links):
                                        all_stock_links.append({'ticker': ticker, 'name': name})
                    
                    # 첫 번째와 두 번째 주도주 추출
                    if len(all_stock_links) > 0:
                        leading_stock1_ticker = all_stock_links[0]['ticker']
                        leading_stock1_name = all_stock_links[0]['name']
                    if len(all_stock_links) > 1:
                        leading_stock2_ticker = all_stock_links[1]['ticker']
                        leading_stock2_name = all_stock_links[1]['name']
                    
                    # 디버깅: 처음 몇 개만 상세 출력
                    if page_theme_count < 2 and page == 1:
                        print(f"\n[디버깅] 테마: {theme_name}")
                        print(f"  - 등락률: {change_rate}")
                        print(f"  - 최근 3일 등락률: {recent_3days_change_rate}")
                        print(f"  - 상승/보합/하락: {up_count}/{same_count}/{down_count}")
                        print(f"  - 주도주1: {leading_stock1_ticker} ({leading_stock1_name})")
                        print(f"  - 주도주2: {leading_stock2_ticker} ({leading_stock2_name})")
                        print(f"  - 총 열 개수: {len(cols)}")
                        for i, col in enumerate(cols):
                            print(f"    열 {i}: {col.get_text(strip=True)[:50]}")
                    
                    all_themes.append({
                        'theme_code': theme_code,
                        'theme_name': theme_name,
                        'change_rate': change_rate,
                        'recent_3days_change_rate': recent_3days_change_rate,
                        'up_count': up_count,
                        'same_count': same_count,
                        'down_count': down_count,
                        'leading_stock1_ticker': leading_stock1_ticker,
                        'leading_stock1_name': leading_stock1_name,
                        'leading_stock2_ticker': leading_stock2_ticker,
                        'leading_stock2_name': leading_stock2_name,
                        'url': f"https://finance.naver.com{theme_href}" if not theme_href.startswith('http') else theme_href
                    })
                    page_theme_count += 1
                except Exception as e:
                    print(f"테마 정보 추출 오류 (페이지 {page}): {e}")
                    print(traceback.format_exc())
                    continue
            
            if page_theme_count == 0:
                empty_page_count += 1
                if empty_page_count >= max_empty_pages:
                    print(f"\n페이지 {page}부터 연속으로 테마가 없어 중단합니다.")
                    break
                
        except Exception as e:
            print(f"페이지 {page} 처리 오류: {e}")
            empty_page_count += 1
            if empty_page_count >= max_empty_pages:
                print(f"\n페이지 {page}부터 연속으로 오류가 발생하여 중단합니다.")
                break
            continue
    
    print(f"총 {len(all_themes)}개 테마를 찾았습니다.")
    
    # 각 테마별 종목 정보 가져오기
    update_date = datetime.now().strftime('%Y-%m-%d')
    commit_counter = 0
    batch_size = 20
    
    # 통계 변수
    total_stocks_found = 0
    themes_with_stocks = 0
    themes_without_stocks = 0
    
    for theme in tqdm(all_themes, desc="테마 정보 수집"):
        try:
            theme_code = theme['theme_code']
            theme_name = theme['theme_name']
            theme_url = theme['url']
            
            # 테마 정보 저장 (추가 정보 포함)
            try:
                mycursor.execute(query_theme, (
                    theme_code, 
                    theme_name,
                    theme.get('change_rate'),
                    theme.get('recent_3days_change_rate'),
                    theme.get('up_count') or 0,
                    theme.get('same_count') or 0,
                    theme.get('down_count') or 0,
                    theme.get('leading_stock1_ticker'),
                    theme.get('leading_stock1_name'),
                    theme.get('leading_stock2_ticker'),
                    theme.get('leading_stock2_name'),
                    update_date
                ))
                commit_counter += 1
            except Exception as e:
                print(f"테마 저장 오류 (코드: {theme_code}, 이름: {theme_name}): {e}")
                print(f"  저장하려던 데이터: change_rate={theme.get('change_rate')}, "
                      f"up={theme.get('up_count')}, same={theme.get('same_count')}, "
                      f"down={theme.get('down_count')}, leading1={theme.get('leading_stock1_ticker')}")
                continue
            
            # 테마 상세 페이지에서 종목 목록 가져오기
            try:
                response = session.get(theme_url, timeout=10)
                response.encoding = 'euc-kr'
                soup = BeautifulSoup(response.text, 'html.parser')
                
                stocks_in_theme = {}

                def add_stock_link(link):
                    if not link:
                        return
                    href = link.get('href', '')
                    code_match = re.search(r'code=(\d+)', href)
                    if not code_match:
                        return
                    ticker = code_match.group(1)
                    if not ticker or len(ticker) != 6:
                        return
                    name_text = clean_stock_text(link.get_text(strip=True))
                    if ticker not in stocks_in_theme or not stocks_in_theme[ticker]:
                        stocks_in_theme[ticker] = name_text
                
                # 방법 1: 테이블에서 종목 코드 추출
                stock_tables = soup.find_all('table', {'class': 'type_1'})
                
                for table in stock_tables:
                    rows = table.find_all('tr')[1:]  # 헤더 제외
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) > 0:
                            # 종목코드가 포함된 링크 찾기
                            links = row.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                            for link in links:
                                add_stock_link(link)
                
                # 방법 2: 모든 링크에서 종목 코드 추출 (테이블 외부 포함)
                if len(stocks_in_theme) == 0:
                    all_links = soup.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                    for link in all_links:
                        add_stock_link(link)
                
                # 방법 3: 테이블의 모든 td에서 링크 찾기 (더 넓은 범위)
                if len(stocks_in_theme) == 0:
                    all_tds = soup.find_all('td')
                    for td in all_tds:
                        links = td.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                        for link in links:
                            add_stock_link(link)
                
                # 종목이 추출되었는지 확인
                if len(stocks_in_theme) > 0:
                    themes_with_stocks += 1
                    total_stocks_found += len(stocks_in_theme)
                    
                    # 테마-종목 매핑 저장
                    for ticker, stock_name in stocks_in_theme.items():
                        try:
                            mycursor.execute(
                                query_theme_stock,
                                (
                                    theme_code,
                                    theme_name,
                                    ticker,
                                    stock_name,
                                    update_date
                                )
                            )
                            commit_counter += 1
                        except Exception as e:
                            print(f"종목 저장 오류 (테마: {theme_code}, 종목: {ticker}): {e}")
                            if stock_name:
                                print(f"  종목명: {stock_name}")
                            continue
                    
                    if commit_counter >= batch_size:
                        con.commit()
                        commit_counter = 0
                    
                    # 처음 3개 테마만 상세 출력 (디버깅)
                    if themes_with_stocks <= 3:
                        sample_items = list(stocks_in_theme.items())[:3]
                        print(f"✓ 테마 '{theme_name}': {len(stocks_in_theme)}개 종목 추출")
                        for sample_ticker, sample_name in sample_items:
                            print(f"   - {sample_ticker} {sample_name}")
                else:
                    themes_without_stocks += 1
                    # 처음 3개만 경고 출력
                    if themes_without_stocks <= 3:
                        print(f"⚠️ 테마 '{theme_name}' (코드: {theme_code})에서 종목을 찾을 수 없습니다.")
                        print(f"   URL: {theme_url}")
                    
            except Exception as e:
                print(f"테마 {theme_name} (코드: {theme_code}) 종목 정보 추출 오류: {e}")
                print(traceback.format_exc())
                continue
                
        except Exception as e:
            print(f"테마 {theme.get('theme_name', 'Unknown')} 처리 오류: {e}")
            print(traceback.format_exc())
            continue
    
    # 남은 데이터 커밋
    if commit_counter > 0:
        con.commit()
    
    print(f"\n테마 정보 수집 완료:")
    print(f"  - 총 테마 수: {len(all_themes)}개")
    print(f"  - 종목이 있는 테마: {themes_with_stocks}개")
    print(f"  - 종목이 없는 테마: {themes_without_stocks}개")
    print(f"  - 총 추출된 종목 수: {total_stocks_found}개")
    
except Exception as e:
    print(f"테마 정보 수집 중 오류 발생: {e}")
    print(traceback.format_exc())

session.close()
con.close()


### 업종 정보 수집

print('업종 정보 수집 시작')

## DB 연결
con = pymysql.connect(user=require_env('DB_USER'),
passwd=require_env('DB_PASSWORD'),
host='127.0.0.1',
db='kor_stock_db',
charset='utf8')

mycursor = con.cursor()

# 테이블 구조 확인 및 필요한 컬럼 추가
print("업종 테이블 구조 확인 중...")
try:
    # 테이블 존재 여부 확인
    mycursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'kor_stock_db' 
        AND TABLE_NAME = 'krx_industry'
    """)
    table_exists = mycursor.fetchone()[0] > 0
    
    # 테이블이 없으면 생성
    if not table_exists:
        print("krx_industry 테이블이 존재하지 않습니다. 테이블을 생성합니다...")
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS krx_industry (
                industry_code VARCHAR(10) PRIMARY KEY,
                industry_name VARCHAR(100),
                change_rate DECIMAL(10, 2),
                recent_3days_change_rate DECIMAL(10, 2),
                up_count INT DEFAULT 0,
                same_count INT DEFAULT 0,
                down_count INT DEFAULT 0,
                leading_stock1_ticker VARCHAR(10),
                leading_stock1_name VARCHAR(100),
                leading_stock2_ticker VARCHAR(10),
                leading_stock2_name VARCHAR(100),
                update_date DATE,
                INDEX idx_update_date (update_date)
            )
        """)
        con.commit()
        print("✓ krx_industry 테이블 생성 완료")
        existing_columns = ['industry_code', 'industry_name', 'change_rate', 'recent_3days_change_rate',
                           'up_count', 'same_count', 'down_count', 'leading_stock1_ticker', 
                           'leading_stock1_name', 'leading_stock2_ticker', 'leading_stock2_name', 'update_date']
    else:
        # 기존 컬럼 목록 확인
        mycursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = 'kor_stock_db' 
            AND TABLE_NAME = 'krx_industry'
        """)
        existing_columns = [row[0] for row in mycursor.fetchall()]
    
    # 필요한 컬럼 목록
    required_columns = {
        'change_rate': "ADD COLUMN change_rate DECIMAL(10, 2)",
        'recent_3days_change_rate': "ADD COLUMN recent_3days_change_rate DECIMAL(10, 2)",
        'up_count': "ADD COLUMN up_count INT DEFAULT 0",
        'same_count': "ADD COLUMN same_count INT DEFAULT 0",
        'down_count': "ADD COLUMN down_count INT DEFAULT 0",
        'leading_stock1_ticker': "ADD COLUMN leading_stock1_ticker VARCHAR(10)",
        'leading_stock1_name': "ADD COLUMN leading_stock1_name VARCHAR(100)",
        'leading_stock2_ticker': "ADD COLUMN leading_stock2_ticker VARCHAR(10)",
        'leading_stock2_name': "ADD COLUMN leading_stock2_name VARCHAR(100)"
    }
    
    # 누락된 컬럼 추가
    for col_name, alter_stmt in required_columns.items():
        if col_name not in existing_columns:
            try:
                mycursor.execute(f"ALTER TABLE krx_industry {alter_stmt}")
                con.commit()
                print(f"✓ 컬럼 '{col_name}' 추가 완료")
            except Exception as e:
                print(f"⚠️ 컬럼 '{col_name}' 추가 실패: {e}")
        else:
            print(f"✓ 컬럼 '{col_name}' 이미 존재")
    
except Exception as e:
    print(f"테이블 구조 확인 중 오류: {e}")
    print(traceback.format_exc())

# krx_industry_stock 테이블 컬럼 확인 및 추가 (industry_name, stock_name)
try:
    # 테이블 존재 여부 확인
    mycursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'kor_stock_db' 
        AND TABLE_NAME = 'krx_industry_stock'
    """)
    table_exists = mycursor.fetchone()[0] > 0
    
    # 테이블이 없으면 생성
    if not table_exists:
        print("krx_industry_stock 테이블이 존재하지 않습니다. 테이블을 생성합니다...")
        mycursor.execute("""
            CREATE TABLE IF NOT EXISTS krx_industry_stock (
                industry_code VARCHAR(10),
                industry_name VARCHAR(100),
                ticker VARCHAR(10),
                stock_name VARCHAR(100),
                update_date DATE,
                PRIMARY KEY (industry_code, ticker),
                INDEX idx_ticker (ticker),
                INDEX idx_update_date (update_date)
            )
        """)
        con.commit()
        print("✓ krx_industry_stock 테이블 생성 완료")
        existing_industry_stock_columns = {'industry_code', 'industry_name', 'ticker', 'stock_name', 'update_date'}
    else:
        mycursor.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'kor_stock_db'
              AND TABLE_NAME = 'krx_industry_stock';
        """)
        existing_industry_stock_columns = {row[0] for row in mycursor.fetchall()}
    
    required_industry_stock_columns = {
        'industry_name': 'VARCHAR(100)',
        'stock_name': 'VARCHAR(100)'
    }
    
    for column_name, column_type in required_industry_stock_columns.items():
        if column_name not in existing_industry_stock_columns:
            alter_sql = f"ALTER TABLE krx_industry_stock ADD COLUMN {column_name} {column_type}"
            print(f"krx_industry_stock 테이블에 {column_name} 컬럼을 추가합니다.")
            mycursor.execute(alter_sql)
            con.commit()
            print(f"✓ 컬럼 '{column_name}' 추가 완료")
        else:
            print(f"✓ 컬럼 '{column_name}' 이미 존재")
except Exception as e:
    print(f"krx_industry_stock 테이블 구조 확인 중 오류: {e}")
    print(traceback.format_exc())

# 업종 정보 저장 쿼리
query_industry = """
    insert into krx_industry (industry_code, industry_name, change_rate, recent_3days_change_rate, 
                          up_count, same_count, down_count, leading_stock1_ticker, leading_stock1_name,
                          leading_stock2_ticker, leading_stock2_name, update_date)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    industry_name=new.industry_name, 
    change_rate=new.change_rate,
    recent_3days_change_rate=new.recent_3days_change_rate,
    up_count=new.up_count,
    same_count=new.same_count,
    down_count=new.down_count,
    leading_stock1_ticker=new.leading_stock1_ticker,
    leading_stock1_name=new.leading_stock1_name,
    leading_stock2_ticker=new.leading_stock2_ticker,
    leading_stock2_name=new.leading_stock2_name,
    update_date=new.update_date;
"""

# 업종-종목 매핑 저장 쿼리
query_industry_stock = """
    insert into krx_industry_stock (industry_code, industry_name, ticker, stock_name, update_date)
    values (%s, %s, %s, %s, %s) as new
    on duplicate key update
    industry_name=new.industry_name,
    stock_name=new.stock_name,
    update_date=new.update_date;
"""

# 세션 생성
session = rq.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
})

# 업종 목록 페이지 가져오기
industry_list_url = 'https://finance.naver.com/sise/sise_group.naver?type=upjong'

try:
    # 첫 페이지에서 총 페이지 수 확인
    print("업종 페이지 수 확인 중...")
    response = session.get(industry_list_url, timeout=10)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 페이지네이션에서 마지막 페이지 번호 찾기
    max_pages = 1
    paging_div = None
    
    # 여러 가능한 페이지네이션 클래스명 시도
    possible_classes = ['Nnavi', 'paging', 'pager', 'page_navi', 'pageNav']
    for class_name in possible_classes:
        paging_div = soup.find('div', {'class': class_name})
        if paging_div:
            break
    
    # 또는 테이블 다음의 페이지네이션 찾기
    if not paging_div:
        # 업종 테이블 다음 요소들을 확인
        industry_table = soup.find('table', {'class': 'type_1'})
        if industry_table:
            # 테이블 다음 형제 요소들 확인
            next_siblings = industry_table.find_next_siblings()
            for sibling in next_siblings:
                if sibling.name == 'div' and ('navi' in sibling.get('class', []) or 'paging' in sibling.get('class', [])):
                    paging_div = sibling
                    break
    
    if paging_div:
        # 페이지 번호 링크들 찾기
        page_links = paging_div.find_all('a')
        page_numbers = []
        for link in page_links:
            href = link.get('href', '')
            # URL에서 page=번호 추출
            page_match = re.search(r'[?&]page=(\d+)', href)
            if page_match:
                page_num = int(page_match.group(1))
                if page_num > 0:
                    page_numbers.append(page_num)
            # 또는 링크 텍스트에서 숫자 추출
            try:
                text = link.get_text(strip=True)
                if text.isdigit():
                    page_num = int(text)
                    if page_num > 0:
                        page_numbers.append(page_num)
            except:
                pass
        
        if page_numbers:
            max_pages = max(page_numbers)
            print(f"총 {max_pages}개 페이지를 발견했습니다.")
        else:
            # 페이지네이션에서 숫자 텍스트로 확인
            paging_text = paging_div.get_text()
            numbers = re.findall(r'\b(\d+)\b', paging_text)
            if numbers:
                numbers = [int(n) for n in numbers if int(n) > 0 and int(n) <= 100]
                if numbers:
                    max_pages = max(numbers)
                    print(f"총 {max_pages}개 페이지를 발견했습니다.")
    
    # 페이지네이션을 찾지 못한 경우, 적당한 범위로 설정
    if max_pages == 1:
        print("페이지네이션을 찾을 수 없습니다. 데이터 기반으로 페이지 수를 확인합니다.")
        max_pages = 50  # 충분히 큰 값으로 시작
    
    all_industries = []
    
    # 연속으로 데이터가 없는 페이지 수를 카운트
    empty_page_count = 0
    max_empty_pages = 2  # 연속으로 2페이지에 데이터가 없으면 중단
    
    # 모든 페이지 순회
    for page in tqdm(range(1, max_pages + 1), desc="업종 목록 수집"):
        try:
            if page == 1:
                url = industry_list_url
            else:
                url = f'{industry_list_url}&page={page}'
            
            response = session.get(url, timeout=10)
            response.encoding = 'euc-kr'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 업종 목록 테이블 찾기
            industry_table = soup.find('table', {'class': 'type_1'})
            if not industry_table:
                empty_page_count += 1
                if empty_page_count >= max_empty_pages:
                    print(f"\n페이지 {page}부터 연속으로 데이터가 없어 중단합니다.")
                    break
                continue
            
            rows = industry_table.find_all('tr')[1:]  # 헤더 제외
            
            if not rows:
                empty_page_count += 1
                if empty_page_count >= max_empty_pages:
                    print(f"\n페이지 {page}부터 연속으로 데이터가 없어 중단합니다.")
                    break
                continue
            
            # 데이터가 있으면 카운터 리셋
            empty_page_count = 0
            
            page_industry_count = 0
            for row in rows:
                try:
                    cols = row.find_all('td')
                    if len(cols) < 4:
                        continue
                    
                    # 업종명과 링크 추출 (1열)
                    industry_link = cols[0].find('a')
                    if not industry_link:
                        continue
                    
                    industry_name = industry_link.get_text(strip=True)
                    industry_href = industry_link.get('href', '')
                    
                    # 업종 코드 추출 (URL에서 no=xxx 부분)
                    industry_code_match = re.search(r'no=(\d+)', industry_href)
                    if not industry_code_match:
                        continue
                    
                    industry_code = industry_code_match.group(1)
                    
                    # 중복 체크
                    if any(i['industry_code'] == industry_code for i in all_industries):
                        continue
                    
                    # 등락률 추출 (2열) - 전일대비 등락률
                    change_rate_text = cols[1].get_text(strip=True) if len(cols) > 1 else ''
                    change_rate = None
                    if change_rate_text and change_rate_text != '':
                        # % 제거하고 숫자 추출
                        change_rate_match = re.search(r'([+-]?\d+\.?\d*)', change_rate_text.replace('%', ''))
                        if change_rate_match:
                            try:
                                change_rate = float(change_rate_match.group(1))
                            except:
                                change_rate = None
                    
                    # 최근 3일 등락률 추출 (3열)
                    recent_3days_change_rate_text = cols[2].get_text(strip=True) if len(cols) > 2 else ''
                    recent_3days_change_rate = None
                    if recent_3days_change_rate_text and recent_3days_change_rate_text != '':
                        recent_3days_match = re.search(r'([+-]?\d+\.?\d*)', recent_3days_change_rate_text.replace('%', ''))
                        if recent_3days_match:
                            try:
                                recent_3days_change_rate = float(recent_3days_match.group(1))
                            except:
                                recent_3days_change_rate = None
                    
                    # 상승/보합/하락 종목수 추출
                    up_count = 0
                    same_count = 0
                    down_count = 0
                    
                    status_found = False
                    
                    # 우선 기본 테이블 구조(각 숫자가 별도 열)에 맞춰 추출
                    if len(cols) >= 6:
                        try:
                            up_text = cols[3].get_text(strip=True)
                            same_text = cols[4].get_text(strip=True)
                            down_text = cols[5].get_text(strip=True)
                            
                            up_count = int(re.findall(r'\d+', up_text)[0]) if re.findall(r'\d+', up_text) else 0
                            same_count = int(re.findall(r'\d+', same_text)[0]) if re.findall(r'\d+', same_text) else 0
                            down_count = int(re.findall(r'\d+', down_text)[0]) if re.findall(r'\d+', down_text) else 0
                            
                            status_found = any([up_count, same_count, down_count])
                        except Exception as e:
                            status_found = False
                    
                    # 다른 형식에 대한 백업 파싱
                    if not status_found:
                        for col_idx, col in enumerate(cols):
                            if col_idx < 3:
                                continue
                            
                            status_text = col.get_text(strip=True)
                            if not status_text:
                                continue
                            
                            numbers = re.findall(r'\d+', status_text)
                            if len(numbers) >= 3:
                                try:
                                    up_count = int(numbers[0])
                                    same_count = int(numbers[1])
                                    down_count = int(numbers[2])
                                    status_found = True
                                    break
                                except:
                                    pass
                            
                            if not status_found:
                                up_match = re.search(r'상승\s*[:]?\s*(\d+)', status_text)
                                same_match = re.search(r'보합\s*[:]?\s*(\d+)', status_text)
                                down_match = re.search(r'하락\s*[:]?\s*(\d+)', status_text)
                                
                                if up_match:
                                    up_count = int(up_match.group(1))
                                if same_match:
                                    same_count = int(same_match.group(1))
                                if down_match:
                                    down_count = int(down_match.group(1))
                                
                                if up_match or same_match or down_match:
                                    status_found = True
                                    break
                    
                    # 주도주 정보 추출 (모든 열에서 종목 링크 찾기)
                    leading_stock1_ticker = None
                    leading_stock1_name = None
                    leading_stock2_ticker = None
                    leading_stock2_name = None
                    
                    # 모든 열을 순회하며 종목 링크 찾기 (처음 4개 열 제외)
                    all_stock_links = []
                    for col_idx, col in enumerate(cols):
                        if col_idx < 4:  # 처음 4개 열은 건너뛰기
                            continue
                        
                        # 이 열의 모든 종목 링크 찾기
                        stock_links = col.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                        for link in stock_links:
                            href = link.get('href', '')
                            code_match = re.search(r'code=(\d+)', href)
                            if code_match:
                                ticker = code_match.group(1)
                                name = link.get_text(strip=True)
                                if ticker and name and len(ticker) == 6:  # 종목코드는 6자리
                                    # 중복 제거
                                    if not any(s['ticker'] == ticker for s in all_stock_links):
                                        all_stock_links.append({'ticker': ticker, 'name': name})
                    
                    # 첫 번째와 두 번째 주도주 추출
                    if len(all_stock_links) > 0:
                        leading_stock1_ticker = all_stock_links[0]['ticker']
                        leading_stock1_name = all_stock_links[0]['name']
                    if len(all_stock_links) > 1:
                        leading_stock2_ticker = all_stock_links[1]['ticker']
                        leading_stock2_name = all_stock_links[1]['name']
                    
                    # 디버깅: 처음 몇 개만 상세 출력
                    if page_industry_count < 2 and page == 1:
                        print(f"\n[디버깅] 업종: {industry_name}")
                        print(f"  - 등락률: {change_rate}")
                        print(f"  - 최근 3일 등락률: {recent_3days_change_rate}")
                        print(f"  - 상승/보합/하락: {up_count}/{same_count}/{down_count}")
                        print(f"  - 주도주1: {leading_stock1_ticker} ({leading_stock1_name})")
                        print(f"  - 주도주2: {leading_stock2_ticker} ({leading_stock2_name})")
                    
                    all_industries.append({
                        'industry_code': industry_code,
                        'industry_name': industry_name,
                        'change_rate': change_rate,
                        'recent_3days_change_rate': recent_3days_change_rate,
                        'up_count': up_count,
                        'same_count': same_count,
                        'down_count': down_count,
                        'leading_stock1_ticker': leading_stock1_ticker,
                        'leading_stock1_name': leading_stock1_name,
                        'leading_stock2_ticker': leading_stock2_ticker,
                        'leading_stock2_name': leading_stock2_name,
                        'url': f"https://finance.naver.com{industry_href}" if not industry_href.startswith('http') else industry_href
                    })
                    page_industry_count += 1
                except Exception as e:
                    print(f"업종 정보 추출 오류 (페이지 {page}): {e}")
                    print(traceback.format_exc())
                    continue
            
            if page_industry_count == 0:
                empty_page_count += 1
                if empty_page_count >= max_empty_pages:
                    print(f"\n페이지 {page}부터 연속으로 업종이 없어 중단합니다.")
                    break
                
        except Exception as e:
            print(f"페이지 {page} 처리 오류: {e}")
            empty_page_count += 1
            if empty_page_count >= max_empty_pages:
                print(f"\n페이지 {page}부터 연속으로 오류가 발생하여 중단합니다.")
                break
            continue
    
    print(f"총 {len(all_industries)}개 업종을 찾았습니다.")
    
    # 각 업종별 종목 정보 가져오기
    update_date = datetime.now().strftime('%Y-%m-%d')
    commit_counter = 0
    batch_size = 20
    
    # 통계 변수
    total_stocks_found = 0
    industries_with_stocks = 0
    industries_without_stocks = 0
    
    for industry in tqdm(all_industries, desc="업종 정보 수집"):
        try:
            industry_code = industry['industry_code']
            industry_name = industry['industry_name']
            industry_url = industry['url']
            
            # 업종 정보 저장 (추가 정보 포함)
            try:
                mycursor.execute(query_industry, (
                    industry_code, 
                    industry_name,
                    industry.get('change_rate'),
                    industry.get('recent_3days_change_rate'),
                    industry.get('up_count') or 0,
                    industry.get('same_count') or 0,
                    industry.get('down_count') or 0,
                    industry.get('leading_stock1_ticker'),
                    industry.get('leading_stock1_name'),
                    industry.get('leading_stock2_ticker'),
                    industry.get('leading_stock2_name'),
                    update_date
                ))
                commit_counter += 1
            except Exception as e:
                print(f"업종 저장 오류 (코드: {industry_code}, 이름: {industry_name}): {e}")
                continue
            
            # 업종 상세 페이지에서 종목 목록 가져오기
            try:
                response = session.get(industry_url, timeout=10)
                response.encoding = 'euc-kr'
                soup = BeautifulSoup(response.text, 'html.parser')
                
                stocks_in_industry = {}

                def add_stock_link(link):
                    if not link:
                        return
                    href = link.get('href', '')
                    code_match = re.search(r'code=(\d+)', href)
                    if not code_match:
                        return
                    ticker = code_match.group(1)
                    if not ticker or len(ticker) != 6:
                        return
                    name_text = clean_stock_text(link.get_text(strip=True))
                    if ticker not in stocks_in_industry or not stocks_in_industry[ticker]:
                        stocks_in_industry[ticker] = name_text
                
                # 방법 1: 테이블에서 종목 코드 추출
                stock_tables = soup.find_all('table', {'class': 'type_1'})
                
                for table in stock_tables:
                    rows = table.find_all('tr')[1:]  # 헤더 제외
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) > 0:
                            # 종목코드가 포함된 링크 찾기
                            links = row.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                            for link in links:
                                add_stock_link(link)
                
                # 방법 2: 모든 링크에서 종목 코드 추출 (테이블 외부 포함)
                if len(stocks_in_industry) == 0:
                    all_links = soup.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                    for link in all_links:
                        add_stock_link(link)
                
                # 방법 3: 테이블의 모든 td에서 링크 찾기 (더 넓은 범위)
                if len(stocks_in_industry) == 0:
                    all_tds = soup.find_all('td')
                    for td in all_tds:
                        links = td.find_all('a', href=re.compile(r'/item/main.naver\?code='))
                        for link in links:
                            add_stock_link(link)
                
                # 종목이 추출되었는지 확인
                if len(stocks_in_industry) > 0:
                    industries_with_stocks += 1
                    total_stocks_found += len(stocks_in_industry)
                    
                    # 업종-종목 매핑 저장
                    for ticker, stock_name in stocks_in_industry.items():
                        try:
                            mycursor.execute(
                                query_industry_stock,
                                (
                                    industry_code,
                                    industry_name,
                                    ticker,
                                    stock_name,
                                    update_date
                                )
                            )
                            commit_counter += 1
                        except Exception as e:
                            print(f"종목 저장 오류 (업종: {industry_code}, 종목: {ticker}): {e}")
                            if stock_name:
                                print(f"  종목명: {stock_name}")
                            continue
                    
                    if commit_counter >= batch_size:
                        con.commit()
                        commit_counter = 0
                    
                    # 처음 3개 업종만 상세 출력 (디버깅)
                    if industries_with_stocks <= 3:
                        sample_items = list(stocks_in_industry.items())[:3]
                        print(f"✓ 업종 '{industry_name}': {len(stocks_in_industry)}개 종목 추출")
                        for sample_ticker, sample_name in sample_items:
                            print(f"   - {sample_ticker} {sample_name}")
                else:
                    industries_without_stocks += 1
                    # 처음 3개만 경고 출력
                    if industries_without_stocks <= 3:
                        print(f"⚠️ 업종 '{industry_name}' (코드: {industry_code})에서 종목을 찾을 수 없습니다.")
                        print(f"   URL: {industry_url}")
                    
            except Exception as e:
                print(f"업종 {industry_name} (코드: {industry_code}) 종목 정보 추출 오류: {e}")
                print(traceback.format_exc())
                continue
                
        except Exception as e:
            print(f"업종 {industry.get('industry_name', 'Unknown')} 처리 오류: {e}")
            print(traceback.format_exc())
            continue
    
    # 남은 데이터 커밋
    if commit_counter > 0:
        con.commit()
    
    print(f"\n업종 정보 수집 완료:")
    print(f"  - 총 업종 수: {len(all_industries)}개")
    print(f"  - 종목이 있는 업종: {industries_with_stocks}개")
    print(f"  - 종목이 없는 업종: {industries_without_stocks}개")
    print(f"  - 총 추출된 종목 수: {total_stocks_found}개")
    
except Exception as e:
    print(f"업종 정보 수집 중 오류 발생: {e}")
    print(traceback.format_exc())

session.close()
con.close()






# ### 재무

# print('재무 데이터 가져오기 시작')

# for i in tqdm(range(0, len(ticker_list))):

#     ticker = ticker_list['종목코드'][i]

#     df = stock.get_market_fundamental(fr, to, ticker)





# con.close()


### 상대강도(RS) 산출 및 저장
# RS_BACKFILL=True: krx_ohlcv 전체 날짜를 벡터 로직으로 재계산·upsert (일회성 백필)
# RS_BACKFILL=False: 미처리 날짜만 (기본)
RS_BACKFILL = False

print('상대강도(RS) 산출 및 저장 시작')
print(f"· RS_BACKFILL={RS_BACKFILL}")

## DB 연결
con = pymysql.connect(user=require_env('DB_USER'),
passwd=require_env('DB_PASSWORD'),
host='127.0.0.1',
db='kor_stock_db',
charset='utf8')

mycursor = con.cursor()

# 상대강도 테이블 생성
create_table_query = """
CREATE TABLE IF NOT EXISTS krx_relative_strength (
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    market_type VARCHAR(10) NOT NULL,
    rs_10d DECIMAL(10, 2),
    rs_20d DECIMAL(10, 2),
    rs_50d DECIMAL(10, 2),
    rs_120d DECIMAL(10, 2),
    rs_200d DECIMAL(10, 2),
    PRIMARY KEY (ticker, date),
    INDEX idx_date (date),
    INDEX idx_ticker (ticker),
    INDEX idx_market_type (market_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

try:
    mycursor.execute(create_table_query)
    con.commit()
    print("✓ 상대강도 테이블 확인 완료")
except Exception as e:
    print(f"⚠️ 테이블 생성 확인 중 오류: {e}")

# 상대강도 저장 쿼리
query_rs = """
    insert into krx_relative_strength (ticker, date, market_type, rs_10d, rs_20d, rs_50d, rs_120d, rs_200d)
    values (%s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    market_type=new.market_type,
    rs_10d=new.rs_10d,
    rs_20d=new.rs_20d,
    rs_50d=new.rs_50d,
    rs_120d=new.rs_120d,
    rs_200d=new.rs_200d;
"""

# 코스피/코스닥 종목 목록 가져오기
query_kospi = """
    SELECT DISTINCT t.종목코드, t.시장구분
    FROM krx_ticker t
    INNER JOIN krx_ticker_sector ts ON t.종목코드 = ts.ticker
    WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
    AND t.종목구분 = '보통주'
    AND ts.sector_cd = '1001';
"""

query_kosdaq = """
    SELECT DISTINCT t.종목코드, t.시장구분
    FROM krx_ticker t
    INNER JOIN krx_ticker_sector ts ON t.종목코드 = ts.ticker
    WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
    AND t.종목구분 = '보통주'
    AND ts.sector_cd = '2001';
"""

kospi_stocks = pd.read_sql(query_kospi, con=engine)
kosdaq_stocks = pd.read_sql(query_kosdaq, con=engine)

print(f"코스피 종목 수: {len(kospi_stocks)}")
print(f"코스닥 종목 수: {len(kosdaq_stocks)}")

# 처리 대상 날짜 결정
if RS_BACKFILL:
    print("백필 모드: krx_ohlcv 전체 날짜를 재계산합니다.")
    dates_to_process = pd.read_sql(
        """
        SELECT DISTINCT date
        FROM krx_ohlcv
        ORDER BY date;
        """,
        con=engine,
    )
    if len(dates_to_process) > 0:
        dates_to_process['date'] = pd.to_datetime(dates_to_process['date']).dt.date
        dates_to_process = dates_to_process.sort_values('date').reset_index(drop=True)
        print(
            f"백필 대상 날짜: {dates_to_process['date'].min()} ~ {dates_to_process['date'].max()} "
            f"({len(dates_to_process)}일)"
        )
    else:
        dates_to_process = pd.DataFrame(columns=['date'])
        print("krx_ohlcv에 날짜가 없습니다.")
else:
    print("DB에 저장된 최신 날짜 확인 중...")
    query_max_date = """
        SELECT MAX(date) as max_date
        FROM krx_relative_strength;
    """

    max_rs_date = pd.read_sql(query_max_date, con=engine)
    if len(max_rs_date) > 0 and max_rs_date.iloc[0]['max_date'] is not None:
        last_processed_date = pd.to_datetime(max_rs_date.iloc[0]['max_date']).date()
        print(f"DB에 저장된 최신 날짜: {last_processed_date}")

        today = date.today()
        print(f"오늘 날짜: {today}")

        if last_processed_date < today:
            query_available_dates = """
                SELECT DISTINCT date
                FROM krx_ohlcv
                WHERE date > %s
                AND date <= %s
                ORDER BY date;
            """
            available_dates = pd.read_sql(
                query_available_dates, con=engine, params=(last_processed_date, today)
            )

            if len(available_dates) > 0:
                available_dates['date'] = pd.to_datetime(available_dates['date']).dt.date

                query_existing_dates = """
                    SELECT DISTINCT date
                    FROM krx_relative_strength
                    WHERE date > %s AND date <= %s;
                """
                existing_dates_df = pd.read_sql(
                    query_existing_dates, con=engine, params=(last_processed_date, today)
                )
                if len(existing_dates_df) > 0:
                    existing_dates_df['date'] = pd.to_datetime(existing_dates_df['date']).dt.date
                    existing_dates_set = set(existing_dates_df['date'].tolist())
                else:
                    existing_dates_set = set()

                dates_to_process = available_dates[
                    ~available_dates['date'].isin(existing_dates_set)
                ].copy()
                dates_to_process = dates_to_process.sort_values('date').reset_index(drop=True)

                print(f"DB에 없는 날짜 수: {len(dates_to_process)}개")
                if len(dates_to_process) > 0:
                    print(
                        f"처리할 날짜 범위: {dates_to_process['date'].min()} ~ "
                        f"{dates_to_process['date'].max()}"
                    )
            else:
                dates_to_process = pd.DataFrame(columns=['date'])
                print("처리할 새로운 날짜가 없습니다.")
        else:
            dates_to_process = pd.DataFrame(columns=['date'])
            print("DB에 저장된 최신 날짜가 오늘과 같거나 이후입니다. 처리할 데이터가 없습니다.")
    else:
        print("DB에 저장된 데이터가 없습니다. 최근 200일 데이터를 처리합니다.")
        query_dates = """
            SELECT DISTINCT date
            FROM krx_ohlcv
            WHERE date >= DATE_SUB((SELECT MAX(date) FROM krx_ohlcv), INTERVAL 250 DAY)
            ORDER BY date DESC
            LIMIT 200;
        """
        dates_to_process = pd.read_sql(query_dates, con=engine)
        if len(dates_to_process) > 0:
            dates_to_process['date'] = pd.to_datetime(dates_to_process['date']).dt.date
            dates_to_process = dates_to_process.sort_values('date').reset_index(drop=True)

if len(dates_to_process) > 0 and 'date' in dates_to_process.columns:
    dates_to_process['date'] = pd.to_datetime(dates_to_process['date']).dt.date
    dates_to_process = dates_to_process.sort_values('date').reset_index(drop=True)
    print(f"처리할 날짜 수: {len(dates_to_process)}")
else:
    dates_to_process = pd.DataFrame(columns=['date'])
    print("처리할 새로운 데이터가 없습니다.")

periods = [10, 20, 50, 120, 200]
batch_size = 5000
lookback_need = max(periods) + 20  # 200 + 여유


def _period_returns(close_df: pd.DataFrame, period: int) -> pd.DataFrame:
    """(close / close.shift(period) - 1) * 100. 당일 결측은 NaN, 과거 부족/비정상은 NaN(상위에서 0 처리)."""
    past = close_df.shift(period)
    ret = (close_df / past - 1.0) * 100.0
    valid_curr = close_df.notna()
    valid_past = past.notna() & (past > 0)
    return ret.where(valid_curr & valid_past)


def _index_period_returns(index_close: pd.Series, period: int) -> pd.Series:
    past = index_close.shift(period)
    ret = (index_close / past - 1.0) * 100.0
    valid = index_close.notna() & past.notna() & (past > 0)
    out = ret.where(valid, 0.0)
    return out.where(index_close.notna())


def _percentile_rank_rs(momentum: pd.DataFrame) -> pd.DataFrame:
    """날짜별 종목 간 percentile rank (rank(pct=True)*100). 전부 0이면 50."""
    ranks = momentum.rank(axis=1, method="min", pct=True) * 100.0
    valid_n = momentum.notna().sum(axis=1)
    zero_n = (momentum == 0).sum(axis=1)
    all_zero = (valid_n > 0) & (zero_n == valid_n)
    if all_zero.any():
        fill50 = pd.DataFrame(50.0, index=momentum.index, columns=momentum.columns)
        ranks = ranks.mask(all_zero, fill50).where(momentum.notna())
    return ranks


def compute_market_rs_rows(
    close_wide: pd.DataFrame,
    index_close: pd.Series,
    process_dates: list,
    market_type: str,
    periods: list,
) -> list:
    """시장 단위 벡터 RS 계산 → DB insert 튜플 리스트.

    저장 컬럼: rs_10d·rs_20d·rs_50d·rs_120d·rs_200d (전부 유지).
    소비 측 평균은 indicators_core.rs_avg(cols=RS_AVG_COLS_D)
    = mean(rs_20d, rs_50d, rs_120d, rs_200d) — rs_10 제외.

    Talent(전일종가 +10%) 산출은 본 파일에 없음.
    필요 시 indicators_core.talent_up_count / talent_score 사용.

    단기 거래정지(≤20거래일)는 ffill로 과거종가를 보간해 모멘텀 왜곡을 막되,
    당일 원본 종가가 NaN인 종목은 순위·저장에서 제외합니다.
    """
    if close_wide.empty or len(process_dates) == 0 or index_close.empty:
        return []

    idx_dates = set(index_close.dropna().index)
    proc = [d for d in process_dates if d in close_wide.index and d in idx_dates]
    if not proc:
        return []

    # 당일 실제 거래 여부: ffill 전 원본 기준 (거래 없는 종목은 순위 제외)
    close_raw = close_wide
    valid_mask = close_raw.loc[proc].notna()

    # 단기 정지 보정: 최대 20거래일까지 직전 종가 사용 (장기정지/상폐는 NaN 유지)
    close_filled = close_raw.ffill(limit=20)

    rs_long = {}
    for period in periods:
        past = close_filled.shift(period)
        past_ok = past.notna() & (past > 0)
        stock_ret = _period_returns(close_filled, period)
        idx_ret = _index_period_returns(index_close, period)
        rel = stock_ret.sub(idx_ret, axis=0)
        rel = rel.where(past_ok, 0.0)
        # 당일 원본 종가 없는 종목은 무조건 제외 (ffill로 채워진 당일 값 사용 금지)
        rel = rel.where(close_raw.notna())
        rs = _percentile_rank_rs(rel.loc[proc])
        rs_long[period] = rs.where(valid_mask).stack().dropna()

    if not rs_long or rs_long[periods[0]].empty:
        return []

    merged = pd.DataFrame(rs_long)
    merged = merged.reset_index()
    merged.columns = ["date", "ticker"] + list(periods)

    rows = [
        (ticker, d, market_type, float(r10), float(r20), float(r50), float(r120), float(r200))
        for d, ticker, r10, r20, r50, r120, r200 in merged[
            ["date", "ticker", 10, 20, 50, 120, 200]
        ].itertuples(index=False, name=None)
    ]
    return rows


def _lookback_start(chunk_min_date, n_days: int):
    lookback_dates = pd.read_sql(
        """
        SELECT DISTINCT date
        FROM krx_ohlcv
        WHERE date <= %s
        ORDER BY date DESC
        LIMIT %s;
        """,
        con=engine,
        params=(chunk_min_date, n_days),
    )
    if len(lookback_dates) > 0:
        lookback_dates['date'] = pd.to_datetime(lookback_dates['date']).dt.date
        return lookback_dates['date'].min()
    return chunk_min_date


def _save_rs_rows(rows: list) -> int:
    saved = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            mycursor.executemany(query_rs, batch)
            con.commit()
            saved += len(batch)
        except Exception as e:
            print(f"⚠️ 배치 저장 오류 (offset={i}): {e}")
            for row in batch:
                try:
                    mycursor.execute(query_rs, row)
                    saved += 1
                except Exception:
                    continue
            con.commit()
    return saved


# 처리할 날짜가 없으면 종료
if len(dates_to_process) == 0:
    print("처리할 새로운 데이터가 없습니다.")
    con.close()
    print('\n=== 상대강도(RS) 산출 및 저장 완료 (처리할 데이터 없음) ===')
else:
    kospi_tickers = kospi_stocks['종목코드'].astype(str).tolist() if len(kospi_stocks) > 0 else []
    kosdaq_tickers = kosdaq_stocks['종목코드'].astype(str).tolist() if len(kosdaq_stocks) > 0 else []
    all_tickers = set(kospi_tickers) | set(kosdaq_tickers)

    if not all_tickers:
        print("처리할 종목이 없습니다.")
        con.close()
        print('\n=== 상대강도(RS) 산출 및 저장 완료 (처리할 데이터 없음) ===')
    else:
        dates_to_process = dates_to_process.copy()
        dates_to_process['_year'] = pd.to_datetime(dates_to_process['date']).dt.year
        years = sorted(dates_to_process['_year'].unique().tolist())

        total_dates_done = 0
        total_rows_saved = 0

        for yr in years:
            chunk = dates_to_process.loc[dates_to_process['_year'] == yr, 'date'].tolist()
            if not chunk:
                continue
            chunk_min, chunk_max = min(chunk), max(chunk)
            load_start = _lookback_start(chunk_min, lookback_need)
            print(
                f"\n[청크 {yr}] 처리일 {chunk_min} ~ {chunk_max} ({len(chunk)}일) "
                f"| 로드 {load_start} ~ {chunk_max} (lookback={lookback_need}거래일)"
            )

            ohlcv_all = pd.read_sql(
                """
                SELECT ticker, date, close
                FROM krx_ohlcv
                WHERE date >= %s AND date <= %s
                ORDER BY date, ticker;
                """,
                con=engine,
                params=(load_start, chunk_max),
            )
            if len(ohlcv_all) > 0:
                ohlcv_all['ticker'] = ohlcv_all['ticker'].astype(str)
                ohlcv_all = ohlcv_all[ohlcv_all['ticker'].isin(all_tickers)].copy()
            print(
                f"  · OHLCV: {len(ohlcv_all):,}행 / "
                f"종목 {ohlcv_all['ticker'].nunique() if len(ohlcv_all) else 0}개"
            )

            index_data = pd.read_sql(
                """
                SELECT ticker, date, close
                FROM krx_index_ohlcv
                WHERE ticker IN ('1001', '2001')
                  AND date >= %s AND date <= %s
                ORDER BY ticker, date;
                """,
                con=engine,
                params=(load_start, chunk_max),
            )
            print(f"  · 지수: {len(index_data):,}행")

            if len(ohlcv_all) == 0:
                print(f"  · {yr}년 OHLCV 없음 — 스킵")
                continue

            ohlcv_all['date'] = pd.to_datetime(ohlcv_all['date']).dt.date
            ohlcv_all['close'] = pd.to_numeric(ohlcv_all['close'], errors='coerce')

            if len(index_data) > 0:
                index_data['date'] = pd.to_datetime(index_data['date']).dt.date
                index_data['ticker'] = index_data['ticker'].astype(str)
                index_data['close'] = pd.to_numeric(index_data['close'], errors='coerce')
                kospi_index_close = (
                    index_data.loc[index_data['ticker'] == '1001']
                    .set_index('date')['close']
                    .sort_index()
                )
                kosdaq_index_close = (
                    index_data.loc[index_data['ticker'] == '2001']
                    .set_index('date')['close']
                    .sort_index()
                )
            else:
                kospi_index_close = pd.Series(dtype=float)
                kosdaq_index_close = pd.Series(dtype=float)

            all_rows = []
            if kospi_tickers:
                kospi_ohlcv = ohlcv_all[ohlcv_all['ticker'].isin(kospi_tickers)]
                if len(kospi_ohlcv) > 0:
                    close_kospi = kospi_ohlcv.pivot(
                        index='date', columns='ticker', values='close'
                    ).sort_index()
                    all_rows.extend(
                        compute_market_rs_rows(
                            close_kospi, kospi_index_close, chunk, 'KOSPI', periods
                        )
                    )
            if kosdaq_tickers:
                kosdaq_ohlcv = ohlcv_all[ohlcv_all['ticker'].isin(kosdaq_tickers)]
                if len(kosdaq_ohlcv) > 0:
                    close_kosdaq = kosdaq_ohlcv.pivot(
                        index='date', columns='ticker', values='close'
                    ).sort_index()
                    all_rows.extend(
                        compute_market_rs_rows(
                            close_kosdaq, kosdaq_index_close, chunk, 'KOSDAQ', periods
                        )
                    )

            print(f"  · 저장 대상: {len(all_rows):,}행")
            n_saved = _save_rs_rows(all_rows)
            total_dates_done += len(chunk)
            total_rows_saved += n_saved
            print(f"  · {yr}년 완료: 날짜 {len(chunk)}일 / 저장 {n_saved:,}행")

            del ohlcv_all, index_data, all_rows

        con.close()
        print(
            f"\n=== 상대강도(RS) 산출 및 저장 완료 ===\n"
            f"처리 날짜 수: {total_dates_done:,}일\n"
            f"저장 행 수: {total_rows_saved:,}행"
            + (" (백필 upsert)" if RS_BACKFILL else "")
        )

# -----------------------------------------------------------------------------
# 마켓분석/대시보드 출력 코드는 분리됨
# - `KRX_market_analysis.py`로 이동 (단독 실행 가능)
# -----------------------------------------------------------------------------
