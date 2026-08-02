"""
네이버 금융 지수 OHLCV (OTP/네이버 직접 조회).
11. KRX_ohlcv_v4.0 구현을 공통 모듈로 분리 — 30.ETF 등에서 재활용.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests as rq
from bs4 import BeautifulSoup

# DB ticker → 네이버 심볼
DB_TO_NAVER_INDEX = {
    "1001": "KOSPI",
    "2001": "KOSDAQ",
}

_SESSION = None


def _session() -> rq.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = rq.Session()
        _SESSION.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                )
            }
        )
    return _SESSION


def resolve_naver_index_code(index_code: str) -> str:
    code = str(index_code).strip()
    return DB_TO_NAVER_INDEX.get(code, code)


def get_ohlcv_from_naver_api(symbol, start_date, end_date):
    """
    네이버 파이낸스 siseJson API로 일봉 OHLCV.
    symbol: 종목/ETF 6자리 또는 지수 심볼(KOSPI 등). start/end: YYYYMMDD.
    """
    try:
        import ast
        import json

        symbol = str(symbol).strip()
        url = (
            f"https://api.finance.naver.com/siseJson.naver?"
            f"symbol={symbol}&requestType=1&startTime={start_date}"
            f"&endTime={end_date}&timeframe=day"
        )
        response = _session().get(url, timeout=10)
        if response.status_code != 200:
            return None
        data = response.content
        text = data.decode("utf-8").strip()
        json_data = None
        try:
            json_data = json.loads(text)
        except Exception:
            try:
                json_data = ast.literal_eval(text)
            except Exception:
                return None

        if isinstance(json_data, list) and len(json_data) > 1:
            data_price = pd.DataFrame(
                json_data[1:], columns=json_data[0] if json_data[0] else None
            )
        elif json_data is not None:
            data_price = pd.DataFrame(json_data)
        else:
            return None

        if data_price.empty or len(data_price.columns) < 6:
            return None
        price = data_price.iloc[:, 0:6].copy()
        price.columns = ["date", "open", "high", "low", "close", "volume"]
        price = price.dropna(how="all")
        if len(price) == 0:
            return None
        if price["date"].dtype == object:
            price["date"] = price["date"].astype(str).str.extract(r"(\d+)")[0]
        price["date"] = pd.to_datetime(price["date"], format="%Y%m%d", errors="coerce")
        price = price.dropna(subset=["date"])
        if len(price) == 0:
            return None
        for col in ("open", "high", "low", "close", "volume"):
            if price[col].dtype == object:
                price[col] = (
                    price[col].astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
                )
            price[col] = pd.to_numeric(price[col], errors="coerce")
        price = price.dropna(subset=["open", "high", "low", "close"])
        if len(price) == 0:
            return None
        price["volume_amount"] = None
        price["market_value"] = None
        return price.reset_index(drop=True)
    except Exception:
        return None


def get_index_ohlcv_from_naver_api(index_code, start_date, end_date):
    """네이버 파이낸스 API로 지수 OHLCV (DB 코드 1001 등 → KOSPI 매핑)."""
    return get_ohlcv_from_naver_api(
        resolve_naver_index_code(index_code), start_date, end_date
    )


def get_index_ohlcv_from_naver_crawl(index_code, start_date, end_date):
    """네이버 일봉 페이지 크롤링. (신형 테이블은 종가만 제공 → OHLC=종가 복제)"""
    try:
        symbol = resolve_naver_index_code(index_code)
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        all_data = []
        page = 1
        while True:
            url = f"https://finance.naver.com/sise/sise_index_day.nhn?code={symbol}&page={page}"
            response = _session().get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", class_="type_1")
            if not table:
                break
            rows = table.find_all("tr")[1:]
            if not rows:
                break
            page_data_found = False
            stop_paging = False
            for row in rows:
                tds = row.find_all("td")
                if len(tds) < 2:
                    continue
                try:
                    date_str = tds[0].text.strip()
                    if not date_str:
                        continue
                    date_val = datetime.strptime(date_str, "%Y.%m.%d")
                    if date_val < start_dt:
                        stop_paging = True
                        break
                    if date_val > end_dt:
                        continue
                    close = float(tds[1].text.replace(",", ""))
                    # 구형: date, close, diff, open, high, low, volume (≥7)
                    # 신형: date, close, diff, pct, volume, amount (6)
                    if len(tds) >= 7:
                        open_val = float(tds[3].text.replace(",", ""))
                        high = float(tds[4].text.replace(",", ""))
                        low = float(tds[5].text.replace(",", ""))
                        volume = (
                            float(tds[6].text.replace(",", ""))
                            if tds[6].text.strip()
                            else 0.0
                        )
                    else:
                        open_val = high = low = close
                        volume = (
                            float(tds[4].text.replace(",", ""))
                            if len(tds) > 4 and tds[4].text.strip()
                            else 0.0
                        )
                    all_data.append([date_val, open_val, high, low, close, volume, None, None])
                    page_data_found = True
                except (ValueError, IndexError):
                    continue
            if stop_paging:
                break
            if not page_data_found:
                break
            page += 1
            if page > 100:
                break
        if all_data:
            df = pd.DataFrame(
                all_data,
                columns=[
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "volume_amount",
                    "market_value",
                ],
            )
            return df.sort_values("date").reset_index(drop=True)
        return None
    except Exception:
        return None


def get_index_ohlcv_from_naver(index_code, start_date, end_date):
    """API 우선, 실패 시 크롤링. start/end: YYYYMMDD."""
    df = get_index_ohlcv_from_naver_api(index_code, start_date, end_date)
    if df is None or len(df) == 0:
        df = get_index_ohlcv_from_naver_crawl(index_code, start_date, end_date)
    return df


def index_close_series_from_naver(index_code, ref_date, lookback_calendar_days: int = 400):
    """종가 Series (오름차순). ref_date=date/datetime."""
    if hasattr(ref_date, "date") and not isinstance(ref_date, type(datetime.now().date())):
        try:
            ref_date = ref_date.date()
        except Exception:
            pass
    from datetime import timedelta

    fr = (ref_date - timedelta(days=int(lookback_calendar_days))).strftime("%Y%m%d")
    to = ref_date.strftime("%Y%m%d")
    df = get_index_ohlcv_from_naver(index_code, fr, to)
    if df is None or df.empty:
        return None
    s = pd.to_numeric(df["close"], errors="coerce")
    s.index = pd.to_datetime(df["date"], errors="coerce")
    s = s.dropna()
    return s if not s.empty else None
