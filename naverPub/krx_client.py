"""
KRX 정보데이터시스템 공통 클라이언트.
- 로그인 / OTP CSV / getJsonData PDF
- pykrx 사용 금지 (.cursor/rules/krx-data-fetch.mdc)
- 영업일: Timeout/ConnectionError → 동일 일자 재시도 후 RuntimeError
         정상 응답 + 빈 데이터만 휴장으로 하루 뒤로
"""
from __future__ import annotations

import io
import time
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import requests

from config import (
    BIZDAY_LOOKBACK,
    BIZDAY_NET_RETRIES,
    BIZDAY_NET_RETRY_WAIT,
    HTTP_TIMEOUT,
    KRX_ID,
    KRX_PW,
    SLEEP_SEC,
    require_krx_credentials,
)

BASE = "https://data.krx.co.kr"
OTP_URL = f"{BASE}/comm/fileDn/GenerateOTP/generate.cmd"
DOWN_URL = f"{BASE}/comm/fileDn/download_csv/download.cmd"
JSON_URL = f"{BASE}/comm/bldAttendant/getJsonData.cmd"
LOGIN_PAGE = f"{BASE}/contents/MDC/COMS/client/MDCCOMS001.cmd"
LOGIN_JSP = f"{BASE}/contents/MDC/COMS/client/view/login.jsp?site=mdc"
LOGIN_URL = f"{BASE}/contents/MDC/COMS/client/MDCCOMS001D1.cmd"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
CSV_HEADERS = {
    "Referer": f"{BASE}/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
    "User-Agent": UA,
}
JSON_HEADERS = {
    "User-Agent": UA,
    "Referer": f"{BASE}/contents/MDC/MDI/outerLoader/index.cmd",
    "X-Requested-With": "XMLHttpRequest",
}

# [12001] 전종목 시세
BLD_OHLCV = "dbms/MDC/STAT/standard/MDCSTAT01501"
# 업종분류(영업일 프로브용)
BLD_SECTOR = "dbms/MDC/STAT/standard/MDCSTAT03901"
# ETF 전종목 시세 (액티브 필터용 이름 목록)
BLD_ETF = "dbms/MDC/STAT/standard/MDCSTAT04301"
# PDF
BLD_PDF = "dbms/MDC/STAT/standard/MDCSTAT05001"
BLD_FINDER = "dbms/comm/finder/finder_secuprodisu"
# 지수 일별 (KOSPI/KOSDAQ)
BLD_INDEX = "dbms/MDC/STAT/standard/MDCSTAT00301"

PDF_COLS = {
    "COMPST_ISU_CD": "티커",
    "COMPST_ISU_NM": "구성종목명",
    "COMPST_ISU_CU1_SHRS": "계약수",
    "VALU_AMT": "금액",
    "COMPST_AMT": "시가총액",
    "COMPST_RTO": "시가총액기준 구성비중",
}


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(CSV_HEADERS)
    return s


def krx_login(session: requests.Session) -> bool:
    """KRX_ID/KRX_PW로 로그인. CD001=성공, CD011=중복→skipDup."""
    require_krx_credentials()
    session.get(LOGIN_PAGE, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
    session.get(
        LOGIN_JSP,
        headers={"User-Agent": UA, "Referer": LOGIN_PAGE},
        timeout=HTTP_TIMEOUT,
    )
    payload = {
        "mbrNm": "",
        "telNo": "",
        "di": "",
        "certType": "",
        "mbrId": KRX_ID,
        "pw": KRX_PW,
    }
    h = {"User-Agent": UA, "Referer": LOGIN_PAGE}
    data = session.post(LOGIN_URL, data=payload, headers=h, timeout=HTTP_TIMEOUT).json()
    code = data.get("_error_code", "")
    if code == "CD010":
        print("[!] 비밀번호 변경이 필요합니다.")
        return False
    if code == "CD011":
        payload["skipDup"] = "Y"
        data = session.post(LOGIN_URL, data=payload, headers=h, timeout=HTTP_TIMEOUT).json()
        code = data.get("_error_code", "")
    if code == "CD001":
        print("· KRX 로그인 성공")
        return True
    print(f"[!] 로그인 실패: {code} / {data.get('_error_message', '')}")
    return False


def get_krx_csv(
    session: requests.Session,
    bld: str,
    params: dict,
    retries: int = 3,
    min_bytes: int = 100,
) -> bytes:
    """OTP 발급 → CSV 다운로드. 네트워크 오류는 재시도, 짧은 응답은 ValueError."""
    otp_params = {
        "locale": "ko_KR",
        "name": "fileDown",
        "csvxls_isNo": "false",
        "url": bld,
    }
    otp_params.update(params)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            otp = session.post(OTP_URL, data=otp_params, timeout=HTTP_TIMEOUT).text
            if "LOGOUT" in otp.upper():
                raise RuntimeError("OTP=LOGOUT — 로그인 세션이 없습니다.")
            res = session.post(DOWN_URL, data={"code": otp}, timeout=HTTP_TIMEOUT)
            res.raise_for_status()
            if min_bytes > 0 and len(res.content) < min_bytes:
                raise ValueError(f"응답이 비정상적으로 짧음 ({len(res.content)} bytes)")
            time.sleep(SLEEP_SEC)
            return res.content
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            print(f"  → 네트워크 오류 ({attempt}/{retries}): {type(e).__name__}: {e}")
            if attempt < retries:
                time.sleep(BIZDAY_NET_RETRY_WAIT)
        except Exception as e:
            last_err = e
            print(f"  → 다운로드 실패 ({attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(SLEEP_SEC)
    raise RuntimeError(f"KRX CSV 다운로드 실패 (bld={bld}): {last_err}") from last_err


def read_csv_bytes(content: bytes, encoding: str = "EUC-KR") -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(content), encoding=encoding)


def _is_valid_sector_csv(content: bytes) -> bool:
    try:
        df = read_csv_bytes(content)
    except Exception:
        return False
    if df is None or len(df) == 0:
        return False
    if "종가" in df.columns:
        close = (
            df["종가"]
            .astype(str)
            .str.strip()
            .replace({"": np.nan, "-": np.nan, "nan": np.nan})
        )
        if close.isna().all():
            return False
    return True


def _probe_sector_biz_day(session: requests.Session, day_str: str) -> bool:
    """
    업종분류(KOSPI, MDCSTAT03901) CSV가 있고 종가가 채워진 날만 True.
    Timeout/ConnectionError → 재시도 후 RuntimeError (휴장 오판 금지).
    """
    last_err: Exception | None = None
    for attempt in range(1, BIZDAY_NET_RETRIES + 1):
        try:
            content = get_krx_csv(
                session,
                BLD_SECTOR,
                {"mktId": "STK", "trdDd": day_str, "money": "1"},
                retries=1,
            )
            return _is_valid_sector_csv(content)
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            print(
                f"  → 업종분류 네트워크 오류 {day_str} "
                f"({attempt}/{BIZDAY_NET_RETRIES}): {type(e).__name__}: {e}"
            )
            if attempt < BIZDAY_NET_RETRIES:
                time.sleep(BIZDAY_NET_RETRY_WAIT)
        except ValueError:
            return False
        except RuntimeError as e:
            if "LOGOUT" in str(e):
                raise
            last_err = e
            if attempt < BIZDAY_NET_RETRIES:
                time.sleep(BIZDAY_NET_RETRY_WAIT)
            else:
                break
    raise RuntimeError(
        f"KRX 업종분류 조회 실패({day_str}): 네트워크 오류 "
        f"{BIZDAY_NET_RETRIES}회 연속 → 휴장으로 처리하지 않고 중단"
    ) from last_err


def find_latest_biz_day(
    session: requests.Session,
    max_lookback: int = BIZDAY_LOOKBACK,
) -> str:
    """
    업종분류 CSV(종가 유효) 기준 최신 영업일(YYYYMMDD).
    주말·공휴일·빈 응답은 하루씩 뒤로. --force도 이 판별을 우회하지 않음.
    15시 이전이면 당일 제외하고 전일부터.
    """
    if datetime.now().hour < 15:
        candidate = date.today() - timedelta(days=1)
        print(f"· 15시 이전 → 탐색 시작 {candidate.strftime('%Y%m%d')} (당일 제외)")
    else:
        candidate = date.today()

    probed = 0
    for _ in range(max_lookback * 3):
        if probed >= max_lookback:
            break
        day_str = candidate.strftime("%Y%m%d")
        if candidate.weekday() >= 5:
            print(f"  → {day_str} 주말 → 전일")
            candidate -= timedelta(days=1)
            continue
        probed += 1
        print(f"영업일 확인(업종분류): {day_str} ({probed}/{max_lookback})")
        if _probe_sector_biz_day(session, day_str):
            print(f"· 기준 거래일: {day_str}")
            return day_str
        print("  → 업종분류 무효/빈종가(휴장) → 하루 전")
        time.sleep(0.3)
        candidate -= timedelta(days=1)
    raise RuntimeError(f"최근 {max_lookback}영업일 내 유효 거래일을 찾지 못했습니다.")


def is_holiday_today(biz_day: str) -> bool:
    """오늘(로컬)이 기준 거래일이 아니면 True (휴장/주말)."""
    today = date.today().strftime("%Y%m%d")
    return biz_day != today


def download_ohlcv_csv(session: requests.Session, day_str: str) -> tuple[pd.DataFrame, bytes]:
    """전종목 시세 CSV. (DataFrame, raw_bytes) 반환."""
    content = get_krx_csv(
        session,
        BLD_OHLCV,
        {"mktId": "ALL", "trdDd": day_str, "share": "1", "money": "1"},
    )
    return read_csv_bytes(content), content


def download_etf_list_csv(session: requests.Session, day_str: str) -> pd.DataFrame:
    content = get_krx_csv(
        session,
        BLD_ETF,
        {"trdDd": day_str, "share": "1", "money": "1"},
    )
    return read_csv_bytes(content)


def download_index_csv(
    session: requests.Session,
    day_str: str,
    ind_idx: str,
    ind_idx2: str,
) -> tuple[pd.DataFrame, bytes]:
    """
    [11003] 개별지수 시세 추이 (MDCSTAT00301).
    KOSPI: indIdx=1, indIdx2=001 / KOSDAQ: indIdx=2, indIdx2=001
    strtDd=endDd=수집일.
    Returns (DataFrame, raw_bytes) — 빈 응답 진단용으로 raw 포함.
    """
    params = {
        "indIdx": str(ind_idx),
        "indIdx2": str(ind_idx2),
        "strtDd": day_str,
        "endDd": day_str,
    }
    # 지수 CSV는 헤더만 오면 수십 바이트일 수 있어 min_bytes=0
    content = get_krx_csv(session, BLD_INDEX, params, min_bytes=0)
    try:
        df = read_csv_bytes(content)
    except Exception:
        df = pd.DataFrame()
    return df, content


def build_isin_map(session: requests.Session, market: str = "ALL") -> dict[str, str]:
    params = {"bld": BLD_FINDER, "mktsel": market, "searchText": ""}
    rows = session.post(
        JSON_URL, data=params, headers=JSON_HEADERS, timeout=HTTP_TIMEOUT
    ).json()
    rows = rows.get("block1", [])
    return {r["short_code"]: r["full_code"] for r in rows}


def fetch_pdf(session: requests.Session, isin: str, day_str: str) -> pd.DataFrame:
    params = {"bld": BLD_PDF, "trdDd": day_str, "isuCd": isin}
    out = session.post(
        JSON_URL, data=params, headers=JSON_HEADERS, timeout=HTTP_TIMEOUT
    ).json()
    df = pd.DataFrame(out.get("output", []))
    if df.empty:
        return df
    for src in PDF_COLS:
        if src not in df.columns:
            df[src] = None
    return df[list(PDF_COLS.keys())].rename(columns=PDF_COLS)


def probe_pdf_has_data(session: requests.Session, isin: str, day_str: str) -> bool:
    last_err: Exception | None = None
    for attempt in range(1, BIZDAY_NET_RETRIES + 1):
        try:
            df = fetch_pdf(session, isin, day_str)
            return df is not None and not df.empty
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            print(
                f"  → PDF 네트워크 오류 {day_str} "
                f"({attempt}/{BIZDAY_NET_RETRIES}): {type(e).__name__}: {e}"
            )
            if attempt < BIZDAY_NET_RETRIES:
                time.sleep(BIZDAY_NET_RETRY_WAIT)
    raise RuntimeError(
        f"KRX PDF 조회 실패({day_str}): 네트워크 오류 "
        f"{BIZDAY_NET_RETRIES}회 연속 → 휴장으로 처리하지 않고 중단"
    ) from last_err


def login_session() -> requests.Session:
    session = new_session()
    if not krx_login(session):
        raise RuntimeError("KRX 로그인 실패")
    return session
