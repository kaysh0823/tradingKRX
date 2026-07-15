# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 15:35:14 2024

@author: hachi

v4.0: OHLCV + KRX 종목/ETF 정보(krx_info_v3.0) 통합.
       본 파일만 실행하면 종목·ETF 적재 후 OHLCV 수집까지 수행.
"""


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

from pykrx import stock
import math


# 임시 해결: 스크립트 실행 전에 셀에서 직접 주입
os.environ['KRX_ID'] = 'hachimitsu79'
os.environ['KRX_PW'] = 'GloriaDahn0823$$'

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
    uid, upw = os.getenv('KRX_ID'), os.getenv('KRX_PW')
    if not (uid and upw):
        print('⚠️ 환경변수 KRX_ID / KRX_PW 가 설정되지 않았습니다.')
        print('   Windows(PowerShell): setx KRX_ID "아이디" / setx KRX_PW "비번"  (후 새 터미널)')
        return False

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
        user='root',
        passwd='GloriaDahn03240701',
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




    # engine = create_engine('mysql+pymysql://root:GloriaDahn03240701@127.0.0.1:3306/kor_stock_db')

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
    # passwd='GloriaDahn03240701',
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
        user='root',
        passwd='GloriaDahn03240701',
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
engine = create_engine('mysql+pymysql://root:GloriaDahn03240701@127.0.0.1:3306/kor_stock_db')

print('=' * 60)
print('1. KRX 종목/ETF 정보 업데이트')
print('=' * 60)
biz_day = update_krx_info()
print(f'기준 영업일(biz_day): {biz_day}')

print('=' * 60)
print('2. OHLCV 가져오기 시작')
print('=' * 60)

con = pymysql.connect(user='root',
passwd='GloriaDahn03240701',
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


### 일봉

print(' - 일봉 데이터를 저장합니다.')

query = """
        insert into krx_ohlcv (ticker, date, open, high, low, close, volume)
    values (%s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    open=new.open, high=new.high, low=new.low, close=new.close, volume=new.volume;
"""

## 오류 방생시 저장할 리스트 생성

error_list = []

# 날짜 계산을 루프 밖으로 이동
fr = (datetime.strptime(biz_day, '%Y%m%d') + relativedelta(years=-3)).strftime("%Y%m%d")
to = datetime.strptime(biz_day, '%Y%m%d').strftime("%Y%m%d")

# 전 종목 주가 다운로드 및 지표 생성
batch_size = 50  # executemany N회(종목 단위) 적재 후 커밋
commit_counter = 0

# 세션 재사용 (HTTP 연결 재사용으로 속도 향상)
session = rq.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

for ticker in tqdm(ticker_codes):

    try:
        url = f'''https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1&startTime={fr}&endTime={to}&timeframe=day'''
        
        data = session.get(url, timeout=5).content
        data_price = pd.read_csv(BytesIO(data))
        
        price = data_price.iloc[:, 0:6]
        price.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        price = price.dropna()
        price['date'] = price['date'].str.extract('(\\d+)')
        price['date'] = pd.to_datetime(price['date'])
        price['ticker'] = ticker
        
        price = price[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        args = fetch_ohlcv_args_only_changed(
            mycursor, 'krx_ohlcv', ticker, price, ['open', 'high', 'low', 'close', 'volume']
        )
        if args:
            mycursor.executemany(query, args)
            commit_counter += 1
            if commit_counter >= batch_size:
                con.commit()
                commit_counter = 0
    
    except:
        print(ticker)
        error_list.append(ticker)
        print(traceback.format_exc())

# 남은 데이터 커밋
if commit_counter > 0:
    con.commit()

print(' - 일봉 참조 CSV 정합')
try:
    sync_krx_ohlcv_from_reference_csv_dir(
        mycursor, con, KRX_OHLCV_REFERENCE_DIR, fr, to, query, batch_size=400
    )
except Exception as e:
    print(f'⚠️ 일봉 참조 CSV 정합 중 오류: {e}')
    print(traceback.format_exc())

session.close()
    


### 투자자별 매매동향

print(' - 투자자별 매매동향 데이터를 저장합니다.')

# 네이버 frgn.naver「외국인·기관 순매매 거래량」표와 동일 컬럼 구성
create_table_query = """
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

try:
    mycursor.execute(create_table_query)
    con.commit()
    print("✓ 투자자별 매매동향 테이블 확인 완료")
except Exception as e:
    print(f"⚠️ 테이블 생성 확인 중 오류: {e}")

# 기존 DB에 예전 스키마만 있을 때: 네이버 표에 맞는 컬럼 추가
def _ensure_krx_investor_trading_columns():
    try:
        mycursor.execute("SHOW COLUMNS FROM krx_investor_trading")
        have = {row[0] for row in mycursor.fetchall()}
    except Exception as e:
        print(f"⚠️ krx_investor_trading 컬럼 확인 실패: {e}")
        return
    adds = [
        ("종가", "BIGINT DEFAULT NULL"),
        ("전일비", "VARCHAR(64) DEFAULT NULL"),
        ("등락률", "DECIMAL(10,4) DEFAULT NULL"),
        ("거래량", "BIGINT DEFAULT NULL"),
        ("기관_순매매량", "BIGINT DEFAULT NULL"),
        ("외국인_순매매량", "BIGINT DEFAULT NULL"),
        ("외국인_보유주수", "BIGINT DEFAULT NULL"),
        ("외국인_보유율", "DECIMAL(10,4) DEFAULT NULL"),
    ]
    for col, ddl in adds:
        if col not in have:
            try:
                mycursor.execute(
                    "ALTER TABLE krx_investor_trading ADD COLUMN `{}` {}".format(col, ddl)
                )
                con.commit()
                print(f"✓ krx_investor_trading 컬럼 추가: {col}")
            except Exception as e:
                print(f"⚠️ 컬럼 추가 실패 `{col}`: {e}")

_ensure_krx_investor_trading_columns()

query_investor = """
    insert into krx_investor_trading (
        ticker, date, `종가`, `전일비`, `등락률`, `거래량`,
        `기관_순매매량`, `외국인_순매매량`, `외국인_보유주수`, `외국인_보유율`
    )
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    `종가`=new.`종가`, `전일비`=new.`전일비`, `등락률`=new.`등락률`, `거래량`=new.`거래량`,
    `기관_순매매량`=new.`기관_순매매량`, `외국인_순매매량`=new.`외국인_순매매량`,
    `외국인_보유주수`=new.`외국인_보유주수`, `외국인_보유율`=new.`외국인_보유율`;
"""

error_list_investor = []
commit_counter_investor = 0

# 세션 재사용
session = rq.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
})

# 네이버 frgn 표는 페이지당 약 20거래일 → page 순회로 최대 일수만큼 수집
INVESTOR_MAX_TRADING_DAYS = 250


def _investor_ticker_key(t):
    """종목코드 정규화 (DB·URL·건수 맵 키 통일)."""
    p = str(t).strip()
    return p.zfill(6) if p.isdigit() else p

def parse_investor_trading_data(html_content, ticker):
    """네이버 frgn.naver「외국인·기관 순매매 거래량」표 파싱 (2행 헤더 + 9열 데이터)."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        investor_data = []

        def extract_unsigned_int(text):
            if not text:
                return None
            text = str(text).replace(',', '').strip()
            m = re.search(r'(\d+)', text)
            return int(m.group(1)) if m else None

        def extract_signed_int(text):
            """순매매량 등: +1,074,644 / -9,595,937 / ▼ 표기"""
            if not text:
                return None
            raw = str(text).replace(',', '').strip()
            is_neg = (
                raw.startswith('-')
                or '▼' in raw
                or '↓' in raw
                or '하락' in raw
            )
            is_pos = raw.startswith('+') or '▲' in raw or '↑' in raw or '상승' in raw
            m = re.search(r'(\d+)', raw)
            if not m:
                return None
            n = int(m.group(1))
            if is_neg:
                return -n
            if is_pos:
                return n
            return -n if raw.startswith('-') else n

        def extract_percent(text):
            if not text:
                return None
            t = str(text).replace(',', '').replace('%', '').strip()
            m = re.search(r'-?\d+\.?\d*', t)
            return float(m.group(0)) if m else None

        def is_target_table(table):
            summary = (table.get('summary') or '')
            if '순매매' in summary and '외국인' in summary and '기관' in summary:
                return True
            rows = table.find_all('tr')
            if len(rows) < 2:
                return False
            h0 = [c.get_text(strip=True) for c in rows[0].find_all(['th', 'td'])]
            if len(h0) < 7:
                return False
            return h0[0] == '날짜' and '종가' in h0 and '기관' in h0 and '외국인' in h0

        for table in soup.find_all('table'):
            if not is_target_table(table):
                continue
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) < 9:
                    continue
                date_text = cols[0].get_text(strip=True)
                date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', date_text)
                if not date_match:
                    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_text)
                if not date_match:
                    continue
                date_str = f"{date_match.group(1)}{date_match.group(2)}{date_match.group(3)}"
                종가 = extract_unsigned_int(cols[1].get_text())
                전일비 = cols[2].get_text(' ', strip=True)[:64] or None
                등락률 = extract_percent(cols[3].get_text())
                거래량 = extract_unsigned_int(cols[4].get_text())
                기관_순매매량 = extract_signed_int(cols[5].get_text())
                외국인_순매매량 = extract_signed_int(cols[6].get_text())
                외국인_보유주수 = extract_unsigned_int(cols[7].get_text())
                외국인_보유율 = extract_percent(cols[8].get_text())
                investor_data.append({
                    'date': date_str,
                    '종가': 종가,
                    '전일비': 전일비,
                    '등락률': 등락률,
                    '거래량': 거래량,
                    '기관_순매매량': 기관_순매매량,
                    '외국인_순매매량': 외국인_순매매량,
                    '외국인_보유주수': 외국인_보유주수,
                    '외국인_보유율': 외국인_보유율,
                })
            break

        return investor_data

    except Exception as e:
        if ticker and len(str(ticker)) == 6:
            print(f"  [디버깅] 투자자별 매매동향 파싱 오류 (ticker: {ticker}): {e}")
        return []


def fetch_investor_trading_paged(session, ticker, max_trading_days=INVESTOR_MAX_TRADING_DAYS, timeout=10):
    """frgn.naver `page=` 를 넘기며 최근 max_trading_days 거래일까지 누적 (중복 날짜 제외)."""
    merged = []
    seen_dates = set()
    page = 1
    max_pages = (max_trading_days + 19) // 20 + 3
    while len(merged) < max_trading_days and page <= max_pages:
        url = f'https://finance.naver.com/item/frgn.naver?code={ticker}&page={page}'
        response = session.get(url, timeout=timeout)
        response.encoding = 'euc-kr'
        batch = parse_investor_trading_data(response.text, ticker)
        if not batch:
            break
        for data in batch:
            ds = data.get('date')
            if not ds or ds in seen_dates:
                continue
            seen_dates.add(ds)
            merged.append(data)
            if len(merged) >= max_trading_days:
                return merged
        if len(batch) < 20:
            break
        page += 1
    return merged


def fetch_investor_trading_first_page(session, ticker, timeout=10):
    """최신 페이지(page=1)만 요청. 이미 DB에 과거가 채워진 경우 증분 갱신용."""
    tkey = _investor_ticker_key(ticker)
    url = f'https://finance.naver.com/item/frgn.naver?code={tkey}&page=1'
    response = session.get(url, timeout=timeout)
    response.encoding = 'euc-kr'
    return parse_investor_trading_data(response.text, tkey)



ENABLE_KIS_INVESTOR_TRADE_KIS = False  # True: KIS 종목별 투자자매매동향(일별) 수집 활성화
if ENABLE_KIS_INVESTOR_TRADE_KIS:
    # --- KIS API: 종목별 투자자매매동향(일별) FHPTJ04160001 ---
    # https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily
    KIS_URL_BASE = "https://openapi.koreainvestment.com:9443"
    KIS_APP_KEY = "PSIwqIqeDd7TF8HKATCI74UP0fCGycmdUbrJ"
    KIS_APP_SECRET = (
        "ZkOYH8sqVy+4R+OGaSBKtz1tQezHUDBePqq00ukLwXB4N1xnNXW+c4mfc4ebuOKS45kFTAL2LWmx/lcrKoETGbheNE1f5jHkR1XydF0Xxd9XHl2TGDl43P8gwXLHqWAkpC8TbjRQRdTwq7i8W/nXrWaYboatpSFBQSbnG68PgV/AfEpzOEw="
    )
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


# 종목별 저장 건수 (한 번 조회 → 티커마다 COUNT 쿼리 생략)
try:
    mycursor.execute(
        "SELECT ticker, COUNT(*) FROM krx_investor_trading GROUP BY ticker"
    )
    investor_row_counts = {
        _investor_ticker_key(r[0]): int(r[1]) for r in mycursor.fetchall()
    }
except Exception as e:
    print(f"⚠️ krx_investor_trading 건수 조회 실패 — 전 종목 풀 페이지 수집: {e}")
    investor_row_counts = {}


for i, ticker_raw in enumerate(tqdm(ticker_codes, desc="투자자별 매매동향 수집")):
    tkey = _investor_ticker_key(ticker_raw)

    try:
        if investor_row_counts.get(tkey, 0) >= INVESTOR_MAX_TRADING_DAYS:
            investor_data_list = fetch_investor_trading_first_page(session, tkey)
        else:
            investor_data_list = fetch_investor_trading_paged(session, tkey)
        
        # 디버깅: 처음 3개만 출력
        if i < 3 and len(investor_data_list) == 0:
            print(f"  [디버깅] ticker {tkey}: 투자자별 매매동향 데이터를 찾을 수 없습니다.")
        
        investor_args = []
        for data in investor_data_list:
            date_str = data.get('date')
            if not date_str:
                continue
            try:
                if len(date_str) == 8:
                    date_obj = datetime.strptime(date_str, '%Y%m%d').date()
                else:
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                continue
            investor_args.append((
                tkey,
                date_obj,
                data.get('종가'),
                data.get('전일비'),
                data.get('등락률'),
                data.get('거래량'),
                data.get('기관_순매매량'),
                data.get('외국인_순매매량'),
                data.get('외국인_보유주수'),
                data.get('외국인_보유율'),
            ))

        if investor_args:
            mycursor.executemany(query_investor, investor_args)
            commit_counter_investor += 1
            if commit_counter_investor >= batch_size:
                con.commit()
                commit_counter_investor = 0
    
    except Exception as e:
        print(f"투자자별 매매동향 수집 오류 (ticker: {tkey}): {e}")
        error_list_investor.append(tkey)
        if i < 5:  # 처음 5개만 상세 오류 출력
            print(traceback.format_exc())

# 남은 데이터 커밋
if commit_counter_investor > 0:
    con.commit()

session.close()

if error_list_investor:
    print(f"\n⚠️ 투자자별 매매동향 수집 실패 종목 수: {len(error_list_investor)}")
    if len(error_list_investor) <= 10:
        print(f"실패 종목: {error_list_investor}")


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


### 주봉

print(' - 주봉 데이터를 저장합니다.')

query = """
    insert into krx_ohlcv_week (ticker, date, open, high, low, close, volume)
    values (%s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    open=new.open, high=new.high, low=new.low, close=new.close, volume=new.volume;
"""

## 오류 방생시 저장할 리스트 생성

error_list = []

# 전 종목 주가 다운로드 및 지표 생성
commit_counter = 0

# 세션 재사용
session = rq.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

for ticker in tqdm(ticker_codes):

    try:
        url = f'''https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1&startTime={fr}&endTime={to}&timeframe=week'''
        
        data = session.get(url, timeout=5).content
        data_price = pd.read_csv(BytesIO(data))
        
        price = data_price.iloc[:, 0:6]
        price.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        price = price.dropna()
        price['date'] = price['date'].str.extract('(\\d+)')
        price['date'] = pd.to_datetime(price['date'])
        price['ticker'] = ticker
        
        price = price[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        args = fetch_ohlcv_args_only_changed(
            mycursor, 'krx_ohlcv_week', ticker, price, ['open', 'high', 'low', 'close', 'volume']
        )
        if args:
            mycursor.executemany(query, args)
            commit_counter += 1
            if commit_counter >= batch_size:
                con.commit()
                commit_counter = 0
    
    except:
        print(ticker)
        error_list.append(ticker)
        print(traceback.format_exc())

# 남은 데이터 커밋
if commit_counter > 0:
    con.commit()

session.close()
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

con = pymysql.connect(user='root',
passwd='GloriaDahn03240701',
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

def get_index_ohlcv_from_naver_api(index_code, start_date, end_date):
    """네이버 파이낸스 API를 사용하여 지수 OHLCV 데이터를 가져옵니다."""
    try:
        # 네이버 파이낸스 API URL (주식과 동일한 형식 사용)
        url = f'https://api.finance.naver.com/siseJson.naver?symbol={index_code}&requestType=1&startTime={start_date}&endTime={end_date}&timeframe=day'
        
        response = session.get(url, timeout=10)
        
        # 응답 확인
        if response.status_code != 200:
            print(f"  API 응답 오류: {response.status_code} (지수: {index_code})")
            return None
        
        data = response.content
        
        # 응답이 JSON인지 확인 (지수는 JSON 형식일 수 있음)
        try:
            import json
            json_data = json.loads(data.decode('utf-8'))
            # JSON 형식이면 pandas로 변환
            if isinstance(json_data, list) and len(json_data) > 1:
                # 첫 번째 행은 헤더일 수 있음
                data_price = pd.DataFrame(json_data[1:], columns=json_data[0] if json_data[0] else None)
            else:
                data_price = pd.DataFrame(json_data)
        except:
            # JSON이 아니면 CSV로 시도
            data_price = pd.read_csv(BytesIO(data))
        
        if data_price.empty or len(data_price.columns) < 6:
            print(f"  API 데이터 비어있음 (지수: {index_code})")
            return None
        
        price = data_price.iloc[:, 0:6]
        price.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        
        # 빈 행 제거
        price = price.dropna(how='all')
        
        if len(price) == 0:
            print(f"  유효한 데이터 없음 (지수: {index_code})")
            return None
        
        # 날짜 형식 변환
        # 날짜가 문자열인 경우 처리
        if price['date'].dtype == 'object':
            price['date'] = price['date'].astype(str).str.extract('(\\d+)')[0]
        
        price['date'] = pd.to_datetime(price['date'], format='%Y%m%d', errors='coerce')
        price = price.dropna(subset=['date'])
        
        if len(price) == 0:
            print(f"  날짜 변환 후 데이터 없음 (지수: {index_code})")
            return None
        
        # 데이터 타입 변환
        for col in ['open', 'high', 'low', 'close', 'volume']:
            # 문자열인 경우 쉼표 제거
            if price[col].dtype == 'object':
                price[col] = price[col].astype(str).str.replace(',', '').str.replace(' ', '')
            price[col] = pd.to_numeric(price[col], errors='coerce')
        
        # 숫자 변환이 실패한 행 제거
        price = price.dropna(subset=['open', 'high', 'low', 'close'])
        
        if len(price) == 0:
            print(f"  숫자 변환 후 데이터 없음 (지수: {index_code})")
            return None
        
        # volume_amount와 market_value는 지수 데이터에 없으므로 None으로 설정
        price['volume_amount'] = None
        price['market_value'] = None
        
        print(f"  API로 {len(price)}개 데이터 가져옴 (지수: {index_code})")
        return price
        
    except Exception as e:
        print(f"  API 오류 (지수: {index_code}): {e}")
        return None

def get_index_ohlcv_from_naver_crawl(index_code, start_date, end_date):
    """네이버 파이낸스 웹페이지를 크롤링하여 지수 OHLCV 데이터를 가져옵니다."""
    try:
        # 날짜 변환
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        
        all_data = []
        page = 1
        
        while True:
            url = f'https://finance.naver.com/sise/sise_index_day.nhn?code={index_code}&page={page}'
            response = session.get(url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 테이블 찾기
            table = soup.find('table', class_='type_1')
            if not table:
                break
            
            rows = table.find_all('tr')[1:]  # 헤더 제외
            
            if not rows:
                break
            
            page_data_found = False
            
            for row in rows:
                tds = row.find_all('td')
                if len(tds) < 6:
                    continue
                
                try:
                    date_str = tds[0].text.strip()
                    if not date_str:
                        continue
                    
                    date_val = datetime.strptime(date_str, '%Y.%m.%d')
                    
                    # 시작일 이전이면 중단
                    if date_val < start_dt:
                        return pd.DataFrame(all_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'volume_amount', 'market_value']) if all_data else None
                    
                    # 종료일 이후면 다음 페이지로
                    if date_val > end_dt:
                        page += 1
                        continue
                    
                    close = float(tds[1].text.replace(',', ''))
                    # 전일비는 무시
                    open_val = float(tds[3].text.replace(',', ''))
                    high = float(tds[4].text.replace(',', ''))
                    low = float(tds[5].text.replace(',', ''))
                    volume = float(tds[6].text.replace(',', '')) if len(tds) > 6 and tds[6].text.strip() else 0
                    
                    all_data.append([date_val, open_val, high, low, close, volume, None, None])
                    page_data_found = True
                    
                except (ValueError, IndexError) as e:
                    continue
            
            if not page_data_found:
                break
            
            page += 1
            
            # 너무 많은 페이지 요청 방지
            if page > 100:
                break
        
        if all_data:
            df = pd.DataFrame(all_data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'volume_amount', 'market_value'])
            df = df.sort_values('date').reset_index(drop=True)
            return df
        
        return None
        
    except Exception as e:
        return None

def get_index_ohlcv_from_naver(index_code, start_date, end_date):
    """네이버 파이낸스에서 지수 OHLCV 데이터를 가져옵니다 (API 우선, 실패시 크롤링)."""
    # 먼저 API 시도
    df = get_index_ohlcv_from_naver_api(index_code, start_date, end_date)
    
    # API 실패시 크롤링 시도
    if df is None or len(df) == 0:
        df = get_index_ohlcv_from_naver_crawl(index_code, start_date, end_date)
    
    return df

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
        con_check = pymysql.connect(user='root',
                                    passwd='GloriaDahn03240701',
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
con = pymysql.connect(user='root',
passwd='GloriaDahn03240701',
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
con = pymysql.connect(user='root',
passwd='GloriaDahn03240701',
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
con = pymysql.connect(user='root',
passwd='GloriaDahn03240701',
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
