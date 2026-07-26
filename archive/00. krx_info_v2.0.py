    # -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 20:32:58 2024

@author: hachi
"""


import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
import time
from tqdm import tqdm
#import indicators_stock

from pykrx import stock


## 최근 영업일자 생성

if datetime.now().hour in range(9, 15):
    
    biz_day = str(int(date.today().strftime('%Y%m%d')) - 1)

else:
    biz_day = date.today().strftime('%Y%m%d')

# 최근 영업일자 확인 및 조정
try:
    # 영업일자 확인을 위해 간단한 API 호출 시도
    test_tickers = stock.get_market_ticker_list(biz_day, market="KOSPI")
    if len(test_tickers) == 0:
        # 영업일이 아닐 경우 이전 날짜로 조정
        for i in range(1, 10):
            prev_day = (datetime.strptime(biz_day, '%Y%m%d') - timedelta(days=i)).strftime('%Y%m%d')
            test_tickers = stock.get_market_ticker_list(prev_day, market="KOSPI")
            if len(test_tickers) > 0:
                biz_day = prev_day
                print(f"영업일자 조정: {biz_day}")
                break
except Exception as e:
    print(f"영업일자 확인 중 오류 (계속 진행): {e}")



## 서버 설정
engine = create_engine('mysql+pymysql://root:GloriaDahn03240701@127.0.0.1:3306/kor_stock_db')

## 주식 > 세부안내 > 업종분류 현황
kospi_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_1943_20260711.csv'
kosdaq_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_2036_20260711.csv'
## 세부안내 > per
ratio_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_2107_20260711.csv'
## etf > 전종목 시세
etf_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_2142_20260711.csv'

### 코스피 업종 분류 가져오기 from KRX 세부안내 업종분류 현황
## https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050201
# kospi_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_1926_20251119.csv'
# kosdaq_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_3242_20251119.csv'
# etf_path = 'C:/Users/hachi/OneDrive/00. Code/KRX/KRX_Data' + '/data_4233_20251120.csv'

sector_stk = pd.read_csv(kospi_path, encoding='EUC-KR')
sector_ksq = pd.read_csv(kosdaq_path, encoding='EUC-KR')

krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop=True)

krx_sector['종목명'] = krx_sector['종목명'].str.strip()
krx_sector['기준일'] = biz_day



### 개별종목 지표 PER/PBR/배당수익률(개별종목)

krx_ratio = pd.read_csv(ratio_path, encoding='EUC-KR')

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
etf_df = pd.read_csv(etf_path, encoding='cp949', dtype=str)  # 모든 컬럼을 문자열로 읽어서 후처리 최적화
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

