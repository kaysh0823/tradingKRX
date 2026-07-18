"""MySQL 스키마·커넥션 헬퍼."""
from __future__ import annotations

import pymysql
from sqlalchemy import create_engine

from config import db_config, db_url

SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS ohlcv (
        ticker VARCHAR(12) NOT NULL,
        date DATE NOT NULL,
        name VARCHAR(100) NULL,
        market VARCHAR(20) NULL,
        open DOUBLE NULL,
        high DOUBLE NULL,
        low DOUBLE NULL,
        close DOUBLE NULL,
        volume BIGINT NULL,
        trading_value BIGINT NULL COMMENT '거래대금(원)',
        mcap BIGINT NULL COMMENT '시가총액(원)',
        chg_pct DOUBLE NULL COMMENT '등락률(%)',
        PRIMARY KEY (ticker, date),
        INDEX idx_ohlcv_date (date),
        INDEX idx_ohlcv_market_date (market, date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS index_ohlcv (
        ticker VARCHAR(12) NOT NULL COMMENT '1001=KOSPI, 2001=KOSDAQ',
        date DATE NOT NULL,
        close DOUBLE NULL,
        PRIMARY KEY (ticker, date),
        INDEX idx_index_date (date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS etf_pdf (
        etf_ticker VARCHAR(12) NOT NULL,
        etf_name VARCHAR(200) NULL,
        sector VARCHAR(40) NULL COMMENT 'ETF 섹터 그룹',
        date DATE NOT NULL,
        ticker VARCHAR(20) NOT NULL,
        name VARCHAR(200) NULL,
        shares DOUBLE NULL,
        amount DOUBLE NULL,
        mcap DOUBLE NULL,
        weight DOUBLE NULL COMMENT '시가총액기준 구성비중(%)',
        PRIMARY KEY (etf_ticker, date, ticker),
        INDEX idx_etf_pdf_date (date),
        INDEX idx_etf_pdf_ticker (ticker),
        INDEX idx_etf_pdf_sector (sector)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS rs (
        ticker VARCHAR(12) NOT NULL,
        date DATE NOT NULL,
        market_type VARCHAR(10) NOT NULL,
        rs_10 DOUBLE NULL,
        rs_20 DOUBLE NULL,
        rs_50 DOUBLE NULL,
        rs_120 DOUBLE NULL,
        rs_200 DOUBLE NULL,
        PRIMARY KEY (ticker, date),
        INDEX idx_rs_date (date),
        INDEX idx_rs_market (market_type, date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS talent (
        ticker VARCHAR(12) NOT NULL,
        date DATE NOT NULL,
        talent_pct DOUBLE NULL COMMENT '최근120거래일 중 일간+10% 이상 비중(%)',
        PRIMARY KEY (ticker, date),
        INDEX idx_talent_date (date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS tickers (
        ticker VARCHAR(12) NOT NULL PRIMARY KEY,
        name VARCHAR(100) NULL,
        market VARCHAR(20) NULL,
        updated_at DATE NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def connect(**kwargs):
    cfg = db_config()
    cfg.update(kwargs)
    return pymysql.connect(**cfg)


def engine():
    return create_engine(db_url(), pool_pre_ping=True)


def ensure_schema(con=None) -> None:
    own = con is None
    if own:
        con = connect()
    try:
        with con.cursor() as cur:
            for sql in SCHEMA_SQL:
                cur.execute(sql)
            # 기존 etf_pdf에 sector 컬럼 없으면 추가 (NULL 허용)
            cur.execute("SHOW COLUMNS FROM etf_pdf LIKE 'sector'")
            if cur.fetchone() is None:
                cur.execute(
                    "ALTER TABLE etf_pdf "
                    "ADD COLUMN sector VARCHAR(40) NULL COMMENT 'ETF 섹터 그룹' "
                    "AFTER etf_name"
                )
        con.commit()
    finally:
        if own:
            con.close()
