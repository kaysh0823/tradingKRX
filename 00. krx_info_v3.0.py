# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 20:32:58 2024
v3.0: KRX 정보데이터시스템에서 CSV를 직접 다운로드하도록 변경 (수동 다운로드 불필요)

@author: hachi
"""


import io
import os
import pandas as pd
import numpy as np
import pymysql
import requests
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
import time
from tqdm import tqdm
#import indicators_stock

import os
print(os.getenv('KRX_ID'))  # None이면 환경변수가 안 잡힌 것

# 임시 해결: 스크립트 실행 전에 셀에서 직접 주입
os.environ['KRX_ID'] = 'hachimitsu79'
os.environ['KRX_PW'] = 'GloriaDahn0823$$'

## 서버 설정
engine = create_engine('mysql+pymysql://root:GloriaDahn03240701@127.0.0.1:3306/kor_stock_db')


### KRX 정보데이터시스템 CSV 자동 다운로드
# 기존: 수동으로 CSV 다운로드 후 경로 지정 -> 변경: OTP 발급 후 직접 다운로드
# http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050201

# 다운로드한 원본 CSV 백업 (False면 저장 안 함)
SAVE_BACKUP = True
BACKUP_DIR = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data'

KRX_HEADERS = {
    'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050201',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}
OTP_URL = 'https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
DOWN_URL = 'https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'

# 로그인 엔드포인트 (32. ETF_PDF_v1.0.py 와 동일)
_KRX_BASE = 'https://data.krx.co.kr'
LOGIN_PAGE = f'{_KRX_BASE}/contents/MDC/COMS/client/MDCCOMS001.cmd'
LOGIN_JSP = f'{_KRX_BASE}/contents/MDC/COMS/client/view/login.jsp?site=mdc'
LOGIN_URL = f'{_KRX_BASE}/contents/MDC/COMS/client/MDCCOMS001D1.cmd'
_LOGIN_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
)


def krx_login(session: requests.Session) -> bool:
    """
    KRX_ID / KRX_PW 환경변수로 로그인.
    CD001=성공, CD011=중복로그인(skipDup=Y 재시도), CD010=비밀번호 변경 필요.
    """
    uid, upw = os.getenv('KRX_ID'), os.getenv('KRX_PW')
    if not (uid and upw):
        print('⚠️ 환경변수 KRX_ID / KRX_PW 가 설정되지 않았습니다.')
        print('   Windows(PowerShell): setx KRX_ID "아이디" / setx KRX_PW "비번"  (후 새 터미널)')
        return False

    # 1) 세션 준비(JSESSIONID 발급)
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
    if code == 'CD011':  # 중복 로그인 → 강제 진행
        payload['skipDup'] = 'Y'
        data = session.post(LOGIN_URL, data=payload, headers=h, timeout=15).json()
        code = data.get('_error_code', '')

    if code == 'CD001':
        print('· KRX 로그인 성공')
        return True
    print(f"⚠️ 로그인 실패: {code} / {data.get('_error_message', '')}")
    return False


# 세션 쿠키 유지 + 로그인 필수 (미로그인 시 OTP='LOGOUT')
krx_session = requests.Session()
krx_session.headers.update(KRX_HEADERS)
if not krx_login(krx_session):
    raise RuntimeError('KRX 로그인 실패. 환경변수 KRX_ID / KRX_PW 를 확인하세요.')


def get_krx_csv(bld, params, retries=3):
    """
    KRX 정보데이터시스템에서 OTP 발급 후 CSV를 다운로드하여 bytes로 반환.
    실패 시 최대 retries회 재시도. 성공/실패 후 호출 간 1초 대기.
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
            otp = krx_session.post(OTP_URL, data=otp_params, timeout=30).text
            res = krx_session.post(DOWN_URL, data={'code': otp}, timeout=30)
            res.raise_for_status()

            if len(res.content) < 100:  # 휴장일 등 비정상 응답
                print(f'OTP 응답 앞 200자: {otp[:200]!r}')
                raise ValueError(f'응답이 비정상적으로 짧음 ({len(res.content)} bytes)')

            time.sleep(1)
            return res.content

        except Exception as e:
            last_err = e
            print(f'KRX 다운로드 실패 ({bld}, {attempt}/{retries}): {e}')
            time.sleep(1 if attempt < retries else 0)

    raise RuntimeError(f'KRX CSV 다운로드 실패 (bld={bld}): {last_err}')


def _save_backup(content, name, day_str):
    """원본 CSV를 {name}_{biz_day}.csv 로 백업. SAVE_BACKUP=False면 스킵."""
    if not SAVE_BACKUP:
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    path = os.path.join(BACKUP_DIR, f'{name}_{day_str}.csv')
    with open(path, 'wb') as f:
        f.write(content)
    print(f'  백업 저장: {path}')


def _is_valid_sector_csv(content):
    """업종분류 CSV: 데이터 행이 있고 종가가 전부 비어 있지 않으면 유효."""
    try:
        df = pd.read_csv(io.BytesIO(content), encoding='EUC-KR')
    except Exception:
        return False
    if df is None or len(df) == 0:
        return False
    if '종가' in df.columns:
        close = df['종가'].astype(str).str.strip().replace({'': np.nan, '-': np.nan, 'nan': np.nan})
        if close.isna().all():
            return False
    return True


def find_biz_day(max_lookback=10):
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


## 최근 영업일자 (pykrx 없이 KRX 업종분류 응답으로 판별)
biz_day, kospi_csv = find_biz_day()
_save_backup(kospi_csv, 'sector_kospi', biz_day)

## 주식 > 세부안내 > 업종분류 현황 (구 data_1943 / data_2036)
kosdaq_csv = get_krx_csv(
    'dbms/MDC/STAT/standard/MDCSTAT03901',
    {'mktId': 'KSQ', 'trdDd': biz_day, 'money': '1'},
)
_save_backup(kosdaq_csv, 'sector_kosdaq', biz_day)

## 세부안내 > PER/PBR/배당수익률 (구 data_2107)
ratio_csv = get_krx_csv(
    'dbms/MDC/STAT/standard/MDCSTAT03501',
    {'searchType': '1', 'mktId': 'ALL', 'trdDd': biz_day},
)
_save_backup(ratio_csv, 'ratio', biz_day)

## ETF > 전종목 시세 (구 data_2142)
etf_csv = get_krx_csv(
    'dbms/MDC/STAT/standard/MDCSTAT04301',
    {'trdDd': biz_day, 'share': '1', 'money': '1'},
)
_save_backup(etf_csv, 'etf', biz_day)


### 코스피 업종 분류 가져오기 from KRX 세부안내 업종분류 현황
## http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050201

sector_stk = pd.read_csv(io.BytesIO(kospi_csv), encoding='EUC-KR')
sector_ksq = pd.read_csv(io.BytesIO(kosdaq_csv), encoding='EUC-KR')

krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)

krx_sector['종목명'] = krx_sector['종목명'].str.strip()
krx_sector['기준일'] = biz_day



### 개별종목 지표 PER/PBR/배당수익률(개별종목)

krx_ratio = pd.read_csv(io.BytesIO(ratio_csv), encoding='EUC-KR')

krx_ratio['종목명'] = krx_ratio['종목명'].str.strip()
krx_ratio['기준일'] = biz_day

# krx_ind.head()

### 합치기
krx_diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ratio['종목명'])))

krx_ticker = pd.merge(krx_sector, krx_ratio,
                      on=krx_sector.columns.intersection(
                          krx_ratio.columns).tolist(),
                      how='outer')

# krx_ticker.head()


### 종목 구분
krx_ticker['종목구분'] = np.where(krx_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                              np.where(krx_ticker['종목명'].str.endswith('리츠'), '리츠',
                                       np.where(krx_ticker['종목명'].isin(krx_diff), '기타',
                                                '보통주')))

krx_ticker = krx_ticker.reset_index(drop=True)
krx_ticker.columns = krx_ticker.columns.str.replace(' ', '')
krx_ticker = krx_ticker[['종목코드', '종목명', '시장구분', '업종명', '종가','대비', '등락률',
                         '시가총액', '기준일', 'EPS', 'PER', 'BPS', 'PBR', '주당배당금', '배당수익률', '종목구분']]
krx_ticker = krx_ticker.replace({np.nan : None})
krx_ticker['기준일'] = pd.to_datetime(krx_ticker['기준일'])

# krx_ticker.head()

krx_ticker = krx_ticker.dropna(subset=['업종명'])





## DB 적재

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




##### 지수 정보 가져오기

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


# CSV 파일 읽기 (최적화: 필요한 컬럼만 읽기)
etf_df = pd.read_csv(io.BytesIO(etf_csv), encoding='cp949', dtype=str)  # 모든 컬럼을 문자열로 읽어서 후처리 최적화
etf_df.columns = etf_df.columns.str.strip()
etf_df = etf_df.rename(columns={'순자산가치(NAV)': '순자산가치'})

# 문자열 컬럼 전처리 (벡터화 연산)
etf_df['종목코드'] = etf_df['종목코드'].astype(str).str.zfill(6)
etf_df['종목명'] = etf_df['종목명'].str.strip()

# 숫자 컬럼 일괄 처리 (반복문 제거, 벡터화 연산으로 최적화)
numeric_cols = [
    '종가', '대비', '등락률', '순자산가치', '시가', '고가', '저가',
    '거래량', '거래대금', '시가총액', '순자산총액', '상장좌수',
    '기초지수_종가', '기초지수_대비', '기초지수_등락률'
]

# 존재하는 컬럼만 필터링하여 처리
existing_numeric_cols = [col for col in numeric_cols if col in etf_df.columns]

# 벡터화된 문자열 처리 및 숫자 변환 (한 번에 처리)
for col in existing_numeric_cols:
    # 문자열에서 쉼표 제거 및 '-' 처리 후 숫자 변환
    etf_df[col] = etf_df[col].str.replace(',', '', regex=False).str.replace(r'^\-$', '', regex=True)
    etf_df[col] = pd.to_numeric(etf_df[col], errors='coerce')

# 기초지수_지수명 처리
if '기초지수_지수명' in etf_df.columns:
    etf_df['기초지수_지수명'] = etf_df['기초지수_지수명'].astype(str).str.strip()
    etf_df['기초지수_지수명'] = etf_df['기초지수_지수명'].replace('nan', None)
else:
    etf_df['기초지수_지수명'] = None

# 기준일 설정
etf_df['기준일'] = pd.to_datetime(biz_day)

# NaN을 None으로 변환 (DB 저장을 위해)
etf_df = etf_df.replace({np.nan: None, pd.NaT: None})

# 필요한 컬럼만 선택
etf_cols = ['종목코드', '종목명', '종가', '대비', '등락률', '순자산가치',
            '시가', '고가', '저가', '거래량', '거래대금', '시가총액',
            '순자산총액', '상장좌수', '기초지수_지수명', '기초지수_종가',
            '기초지수_대비', '기초지수_등락률', '기준일']

etf_df = etf_df[etf_cols]

# DB 연결 생성
con = pymysql.connect(
    user='root',
    passwd='GloriaDahn03240701',
    host='127.0.0.1',
    db='kor_stock_db',
    charset='utf8'
)

mycursor = con.cursor()

# 테이블 생성
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

# 배치 처리로 최적화 (메모리 효율성 및 속도 향상)
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

# 배치 크기 설정 (1000개씩 처리하여 메모리 효율성 향상)
batch_size = 1000
args = etf_df.values.tolist()

# 배치 단위로 처리 (진행 상황 표시)
total_batches = (len(args) + batch_size - 1) // batch_size
for i in range(0, len(args), batch_size):
    batch = args[i:i + batch_size]
    mycursor.executemany(query, batch)
    con.commit()
    if total_batches > 1:
        print(f'ETF 배치 처리: {i // batch_size + 1}/{total_batches} 완료', end='\r')

con.close()

print('\nETF 정보 적재 완료')
