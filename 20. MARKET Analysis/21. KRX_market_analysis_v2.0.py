#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
KRX 마켓 분석/대시보드 출력 전용 스크립트.

- 원본: `KRX_ohlcv_test.py`의 2660~4009 구간을 분리
- 실행: `python KRX_market_analysis.py`
- 분석 스냅샷(구스키마)만 삭제: `python KRX_market_analysis.py --drop-analysis-tables`  
  (`report_date` 컬럼이 있는 `krx_analysis_*`만 DROP. 이미 `ref_trade_date`만 쓰는 테이블은 건너뜀)

환경변수(선택):
- KRX_DB_URL: SQLAlchemy DB URL (예: mysql+pymysql://user:pw@127.0.0.1:3306/kor_stock_db)
- KRX_OUTPUT_DIR: HTML 저장 상위 폴더 (기본: C:\\Users\\hachi\\OneDrive\\01. Trading\\picking\\KRX)
- KRX_DISABLE_ANALYSIS_DB: 1 이면 `kor_stock_db`에 리포트 표 스냅샷 저장을 건너뜀

분석 테이블(`krx_analysis_*`)은 달력 실행일이 아니라 **`ref_trade_date`(해당 리포트가 사용한 데이터의 기준 거래일)** 로 구분합니다.
구 스키마(`report_date` 컬럼)가 남아 있으면 `--drop-analysis-tables`로 해당 테이블만 제거한 뒤 재실행하면 됩니다.
"""

from __future__ import annotations

import argparse
import html
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

import plotly.graph_objs as go
import plotly.io as pio
from plotly.subplots import make_subplots


# Windows 콘솔(cp949)에서 한글/UTF-8 출력 깨짐 방지
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


DEFAULT_DB_URL = "mysql+pymysql://root:GloriaDahn03240701@127.0.0.1:3306/kor_stock_db"
DEFAULT_OUTPUT_BASE_DIR = r"C:\Users\hachi\OneDrive\01. Trading\picking\KRX"

# kor_stock_db: 일별 리포트 표 스냅샷 (KRX_market_analysis 산출물)
_KRX_ANALYSIS_TABLES = frozenset(
    {
        "krx_analysis_mj_tv_rank",
        "krx_analysis_rs_high_list",
        "krx_analysis_rs_daily_top20",
        "krx_analysis_breakout_120d",
        "krx_analysis_vol_spread_top100",
        "krx_analysis_mj_top100",
        "krx_analysis_mj_daily_tv_top20",
    }
)
_krx_analysis_schema_engines: set[int] = set()


def _krx_analysis_db_enabled() -> bool:
    return os.getenv("KRX_DISABLE_ANALYSIS_DB", "").strip() not in ("1", "true", "TRUE", "yes", "YES")


def _krx_max_ohlcv_trade_date(engine) -> date | None:
    """krx_ohlcv 최신 거래일(일자)."""
    try:
        r = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
        d = pd.to_datetime(r.iloc[0]["d"], errors="coerce")
        if pd.isna(d):
            return None
        return pd.Timestamp(d).normalize().date()
    except Exception:
        return None


def _krx_max_rs_trade_date(engine) -> date | None:
    """krx_relative_strength 최신 일자."""
    try:
        r = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_relative_strength", con=engine)
        d = pd.to_datetime(r.iloc[0]["d"], errors="coerce")
        if pd.isna(d):
            return None
        return pd.Timestamp(d).normalize().date()
    except Exception:
        return None


def _rs_snapshot_ref_trade_date(engine, df: pd.DataFrame | None) -> date | None:
    """RS 리포트 스냅샷 기준 거래일: 표 데이터의 RS 기준일 우선, 없으면 RS 테이블 MAX(date)."""
    if df is not None and not df.empty and "date" in df.columns:
        s = pd.to_datetime(df["date"], errors="coerce")
        if s.notna().any():
            return pd.Timestamp(s.max()).normalize().date()
    return _krx_max_rs_trade_date(engine)


def _ensure_krx_analysis_schema(engine) -> None:
    """리포트 표용 분석 테이블이 없으면 생성 (kor_stock_db)."""
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_mj_tv_rank (
            ref_trade_date DATE NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            sector_cd VARCHAR(8),
            tv_rank INT,
            current_price DOUBLE,
            trade_value DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, ticker),
            KEY idx_krx_mj_tv_rank_sector (ref_trade_date, sector_cd)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_rs_high_list (
            ref_trade_date DATE NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            rs_date DATE,
            market_type VARCHAR(16),
            rs_10d DOUBLE, rs_20d DOUBLE, rs_50d DOUBLE, rs_120d DOUBLE,
            rs_avg DOUBLE,
            name VARCHAR(256),
            mcap DOUBLE,
            last_close DOUBLE,
            last_volume DOUBLE,
            theme_str TEXT,
            tv_rank DOUBLE,
            energy_ratio_d0 DOUBLE,
            energy_ratio_d1 DOUBLE,
            energy_ratio_d2 DOUBLE,
            new_high_250d_flag VARCHAR(8),
            talent_pct DOUBLE,
            chg_pct_1d DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, ticker),
            KEY idx_krx_rs_hi_mkt (ref_trade_date, market_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_rs_daily_top20 (
            ref_trade_date DATE NOT NULL,
            market VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            top_rank TINYINT NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            name VARCHAR(256),
            rs_10d DOUBLE,
            rs_avg DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, market, trade_date, top_rank),
            KEY idx_krx_rs20_tk (ref_trade_date, ticker)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_breakout_120d (
            ref_trade_date DATE NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            breakout_date DATE,
            elapsed_high_td INT,
            elapsed_td INT,
            up_from_low_pct DOUBLE,
            trade_value DOUBLE,
            chg_1d_pct DOUBLE,
            ret_5d_pct DOUBLE,
            is_250d_high VARCHAR(8),
            rs_rank DOUBLE,
            name VARCHAR(256),
            theme_str TEXT,
            market VARCHAR(16),
            mcap DOUBLE,
            current_price DOUBLE,
            last_trade_value DOUBLE,
            tv_rank DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, ticker),
            KEY idx_krx_bo_mkt (ref_trade_date, market)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_vol_spread_top100 (
            ref_trade_date DATE NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            name VARCHAR(256),
            theme_str TEXT,
            market VARCHAR(16),
            rs_rank DOUBLE,
            rs_score DOUBLE,
            tv_rank DOUBLE,
            current_price DOUBLE,
            pct_b DOUBLE,
            chg_1d_pct DOUBLE,
            chg_3d_pct DOUBLE,
            elapsed_high_td DOUBLE,
            last_tv DOUBLE,
            clv_avg DOUBLE,
            net_dir DOUBLE,
            drb_avg DOUBLE,
            tv_rank_prev DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, ticker),
            KEY idx_krx_vs_mkt (ref_trade_date, market)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_mj_top100 (
            ref_trade_date DATE NOT NULL,
            market_segment VARCHAR(16) NOT NULL,
            row_rank TINYINT NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            name VARCHAR(256),
            theme_str TEXT,
            sector_cd VARCHAR(8),
            last_date DATE,
            close_price DOUBLE,
            volume DOUBLE,
            atr14 DOUBLE,
            atr_over_close DOUBLE,
            chg_pct DOUBLE,
            chg_pct_3d DOUBLE,
            tv_3d DOUBLE,
            talent_120 DOUBLE,
            sma10 DOUBLE,
            sma20 DOUBLE,
            mcap DOUBLE,
            rs_rank DOUBLE,
            tv_rank_prev DOUBLE,
            mcap_rank DOUBLE,
            trading_value DOUBLE,
            new_high_250d_flag VARCHAR(8),
            tv_pct DOUBLE,
            mcap_pct DOUBLE,
            energy_ratio DOUBLE,
            tv_3d_pct DOUBLE,
            energy_ratio_3d DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, market_segment, ticker),
            KEY idx_krx_mj100_rd (ref_trade_date, market_segment, row_rank)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS krx_analysis_mj_daily_tv_top20 (
            ref_trade_date DATE NOT NULL,
            market VARCHAR(16) NOT NULL,
            trade_date DATE NOT NULL,
            top_rank TINYINT NOT NULL,
            ticker VARCHAR(16) NOT NULL,
            name VARCHAR(256),
            trading_value DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ref_trade_date, market, trade_date, top_rank),
            KEY idx_krx_mjtv20_tk (ref_trade_date, ticker)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))


def _mj_daily_trade_value_top20_long_df(
    tv_mkt: pd.DataFrame, dates_sorted: list, market: str
) -> pd.DataFrame:
    """시장 판단 HTML의 '일별 거래대금 Top20'과 동일 스냅샷(긴 형식)."""
    if tv_mkt is None or tv_mkt.empty or not dates_sorted:
        return pd.DataFrame()
    tv = tv_mkt.copy()
    tv["_d_norm"] = pd.to_datetime(tv["date"], errors="coerce").dt.normalize()
    rows: list[dict[str, object]] = []
    for d in sorted(dates_sorted):
        d_ts = pd.Timestamp(d).normalize()
        dd = tv[tv["_d_norm"] == d_ts].copy()
        if dd.empty:
            continue
        dd = dd.sort_values("trading_value", ascending=False).head(20).reset_index(drop=True)
        for i, (_, r) in enumerate(dd.iterrows(), start=1):
            tv_val = r.get("trading_value")
            rows.append(
                {
                    "market": market,
                    "trade_date": d_ts.date(),
                    "top_rank": int(i),
                    "ticker": str(r["ticker"]),
                    "name": str(r.get("name", "") or ""),
                    "trading_value": float(tv_val) if pd.notna(tv_val) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def _build_rs20_long_df(_rs20: pd.DataFrame, dates20: list, mkt: str) -> pd.DataFrame:
    """RS HTML의 '일별 RS Top20'과 동일(긴 형식)."""
    if _rs20 is None or _rs20.empty or not dates20:
        return pd.DataFrame()
    _rs = _rs20.copy()
    _rs["_d_norm"] = pd.to_datetime(_rs["date"], errors="coerce").dt.normalize()
    rows: list[dict[str, object]] = []
    mkt_u = str(mkt).strip().upper()
    for d in sorted(dates20):
        d_ts = pd.Timestamp(d).normalize()
        dkey = d_ts.date()
        dd = _rs[(_rs["market_type"] == mkt_u) & (_rs["_d_norm"] == d_ts)].copy()
        if dd.empty:
            continue
        dd = dd.sort_values("_rs_avg", ascending=False, na_position="last").head(20).reset_index(drop=True)
        for i, (_, r) in enumerate(dd.iterrows(), start=1):
            rows.append(
                {
                    "market": mkt_u,
                    "trade_date": dkey,
                    "top_rank": int(i),
                    "ticker": str(r["ticker"]),
                    "name": str(r.get("name", "") or ""),
                    "rs_10d": float(r["rs_10d"]) if pd.notna(r.get("rs_10d")) else np.nan,
                    "rs_avg": float(r["_rs_avg"]) if pd.notna(r.get("_rs_avg")) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def _krx_analysis_table_has_column(conn, table: str, column: str) -> bool:
    if table not in _KRX_ANALYSIS_TABLES:
        return False
    row = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :tn
              AND COLUMN_NAME = :cn
            LIMIT 1
            """
        ),
        {"tn": table, "cn": column},
    ).first()
    return row is not None


def _migrate_krx_analysis_tv_rank_prev_columns(conn) -> None:
    """기존 DB: CREATE IF NOT EXISTS는 신규 컬럼을 추가하지 않으므로 ALTER로 보강."""
    specs = (
        ("krx_analysis_mj_top100", "tv_rank_prev", "DOUBLE NULL", "rs_rank"),
        ("krx_analysis_vol_spread_top100", "tv_rank_prev", "DOUBLE NULL", "drb_avg"),
    )
    for _tbl, _col, _typ, _after in specs:
        if _krx_analysis_table_has_column(conn, _tbl, _col):
            continue
        ex = conn.execute(
            text(
                """
                SELECT 1 FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tn
                LIMIT 1
                """
            ),
            {"tn": _tbl},
        ).first()
        if not ex:
            continue
        conn.execute(text(f"ALTER TABLE `{_tbl}` ADD COLUMN `{_col}` {_typ} AFTER `{_after}`"))


def _krx_analysis_snapshot_date_columns(conn, table: str) -> list[str]:
    """테이블에 실제로 있는 스냅샷 일자 컬럼(신규 ref_trade_date / 구 report_date, 둘 다 있을 수 있음)."""
    out: list[str] = []
    for col in ("ref_trade_date", "report_date"):
        if _krx_analysis_table_has_column(conn, table, col):
            out.append(col)
    return out


def _save_krx_analysis_table(engine, table: str, df: pd.DataFrame | None, ref_trade_date: date | None) -> None:
    """동일 기준 거래일 행은 삭제 후 재삽입(구·신 스키마 및 두 일자 컬럼 공존 시에도 PK 충돌 방지)."""
    if not _krx_analysis_db_enabled() or table not in _KRX_ANALYSIS_TABLES:
        return
    if ref_trade_date is None:
        return
    eid = id(engine)
    if eid not in _krx_analysis_schema_engines:
        _ensure_krx_analysis_schema(engine)
        _krx_analysis_schema_engines.add(eid)
    rd = ref_trade_date.isoformat()
    with engine.begin() as conn:
        _migrate_krx_analysis_tv_rank_prev_columns(conn)
        date_cols = _krx_analysis_snapshot_date_columns(conn, table)
        if not date_cols:
            # CREATE 직후 등: DDL 기준
            date_cols = ["ref_trade_date"]

        if len(date_cols) == 2:
            conn.execute(
                text(
                    f"DELETE FROM `{table}` WHERE ref_trade_date = :rd OR report_date = :rd"
                ),
                {"rd": rd},
            )
        else:
            c0 = date_cols[0]
            conn.execute(text(f"DELETE FROM `{table}` WHERE `{c0}` = :rd"), {"rd": rd})

        if df is None or df.empty:
            return
        out = df.copy()
        for k in ("ref_trade_date", "report_date"):
            if k in out.columns and k not in date_cols:
                out = out.drop(columns=[k], errors="ignore")
        for c in date_cols:
            out[c] = ref_trade_date
        # PK (일자, ticker) 등: 동일 ticker 중복 행이 있으면 INSERT 단계에서 1062 발생 (JOIN 중복 등)
        if table == "krx_analysis_mj_tv_rank" and "ticker" in out.columns:
            out = out.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
            if "tv_rank" in out.columns:
                out["tv_rank"] = np.arange(1, len(out) + 1, dtype=int)
        # PK (일자, market_segment, ticker): 시장 내 동일 ticker 중복 시 1062
        if table == "krx_analysis_mj_top100" and "ticker" in out.columns and "market_segment" in out.columns:
            if "row_rank" in out.columns:
                out = out.sort_values(["market_segment", "row_rank"], ascending=[True, True])
            out = out.drop_duplicates(subset=["market_segment", "ticker"], keep="first").reset_index(drop=True)
            out["row_rank"] = out.groupby("market_segment", sort=False).cumcount() + 1
        if table == "krx_analysis_vol_spread_top100" and "ticker" in out.columns:
            out = out.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
        # datetime64 → MySQL 호환
        for c in out.columns:
            if str(out[c].dtype).startswith("datetime"):
                try:
                    out[c] = pd.to_datetime(out[c], errors="coerce").dt.date
                except Exception:
                    pass
        out.to_sql(table, con=conn, if_exists="append", index=False, chunksize=800)


def drop_krx_analysis_tables(engine, *, quiet: bool = False) -> list[str]:
    """
    `krx_analysis_*` 분석 테이블 중 **구 스키마(`report_date` 컬럼이 있는 경우)** 만 DROP합니다.

    - `_KRX_ANALYSIS_TABLES` 화이트리스트만 대상.
    - 이미 `ref_trade_date`만 있는 신규 스키마 테이블은 DROP하지 않습니다.
    - 실제로 DROP이 발생한 경우에만 `_krx_analysis_schema_engines` 캐시를 비웁니다.
    """
    _log = (lambda *a, **k: None) if quiet else print
    dropped: list[str] = []
    names = sorted(_KRX_ANALYSIS_TABLES)
    eid = id(engine)

    def _has_report_date(conn, table: str) -> bool:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :tn
                  AND COLUMN_NAME = 'report_date'
                LIMIT 1
                """
            ),
            {"tn": table},
        ).first()
        return row is not None

    with engine.begin() as conn:
        for t in names:
            if not _has_report_date(conn, t):
                _log(f"건너뜀(구스키마 아님 또는 테이블 없음): `{t}`")
                continue
            conn.execute(text(f"DROP TABLE IF EXISTS `{t}`"))
            dropped.append(t)
            _log(f"DROP TABLE IF EXISTS `{t}` (report_date 컬럼 존재)")
    if dropped:
        _krx_analysis_schema_engines.discard(eid)
    if not quiet:
        if dropped:
            _log(f"완료: 구스키마 테이블 {len(dropped)}개 DROP (엔진 id={eid})")
        else:
            _log("완료: DROP 대상 없음(모든 테이블에 report_date 없음 또는 테이블 없음)")
    return dropped


def _mj_top100_df_for_db(df: pd.DataFrame, market_segment: str) -> pd.DataFrame:
    """HTML용 _mj_top100_table_fig 내부 df → DB 컬럼명 정리."""
    d = df.copy()
    if "신고가여부" in d.columns:
        d = d.rename(columns={"신고가여부": "new_high_250d_flag"})
    rename_map = {
        "close": "close_price",
    }
    d = d.rename(columns={k: v for k, v in rename_map.items() if k in d.columns})
    d["market_segment"] = market_segment
    d["row_rank"] = np.arange(1, len(d) + 1, dtype=int)
    want = [
        "market_segment",
        "row_rank",
        "ticker",
        "name",
        "theme_str",
        "sector_cd",
        "last_date",
        "close_price",
        "volume",
        "atr14",
        "atr_over_close",
        "chg_pct",
        "chg_pct_3d",
        "tv_3d",
        "talent_120",
        "sma10",
        "sma20",
        "mcap",
        "rs_rank",
        "tv_rank_prev",
        "mcap_rank",
        "trading_value",
        "new_high_250d_flag",
        "tv_pct",
        "mcap_pct",
        "energy_ratio",
        "tv_3d_pct",
        "energy_ratio_3d",
    ]
    for c in want:
        if c not in d.columns:
            d[c] = np.nan
    return d[want].copy()


def _mj_energy_ratio_font_color(er: float) -> str:
    """거래대금 상위 표·요약표 공통: 에너지배율 글자색."""
    if not np.isfinite(er):
        return "#9e9e9e"
    if er >= 3.0:
        return "#c62828"
    if er >= 1.5:
        return "#f57f17"
    if er >= 0.7:
        return "#2e7d32"
    if er >= 0.3:
        return "#1565c0"
    return "#9e9e9e"


def _mj_html_tv200_top50_energy3d_combined(
    df_k: pd.DataFrame,
    df_q: pd.DataFrame,
    total_tv_k: float,
    total_tv_q: float,
    total_mcap_k: float,
    total_mcap_q: float,
    total_tv_3d_k: float,
    total_tv_3d_q: float,
    highlight_set: set[str],
    prev_tv_by_ticker: dict[str, float],
) -> str:
    """
    코스피·코스닥 각 당일 거래대금 상위 100(최대 200종)을 합친 뒤,
    3일 에너지배율(해당 시장 거래대금 3일 전체비중 ÷ 시총 전체비중) 내림차순 상위 50만 표시.
    """

    def _prep_slice(df_mkt: pd.DataFrame) -> pd.DataFrame:
        if df_mkt is None or df_mkt.empty:
            return pd.DataFrame()
        d0 = df_mkt.copy()
        if "theme_str" not in d0.columns:
            d0["theme_str"] = ""
        d0["trading_value"] = pd.to_numeric(d0.get("close"), errors="coerce").astype(float) * pd.to_numeric(
            d0.get("volume"), errors="coerce"
        ).astype(float)
        d0["mcap"] = pd.to_numeric(d0.get("mcap"), errors="coerce")
        d0["tv_3d"] = pd.to_numeric(d0.get("tv_3d"), errors="coerce")
        if "sector_cd" in d0.columns and d0["sector_cd"].notna().any():
            d0["tv_rank_mkt"] = d0.groupby("sector_cd")["trading_value"].rank(ascending=False, method="min")
        else:
            d0["tv_rank_mkt"] = d0["trading_value"].rank(ascending=False, method="min")
        return d0.sort_values("trading_value", ascending=False).head(100).copy()

    k100 = _prep_slice(df_k)
    q100 = _prep_slice(df_q)
    blocks: list[pd.DataFrame] = []
    if not k100.empty:
        blocks.append(k100)
    if not q100.empty:
        blocks.append(q100)
    if not blocks:
        return (
            '<div class="mj-html-table-wrap"><h3 style="margin:10px 0 6px 0;font-size:1.05rem;">'
            "코스피·코스닥 거래대금 각 상위 100 합산 (3일 에너지배율 높은 순, 상위 50)</h3>"
            "<p style='font-size:12px;color:#666;'>데이터 없음</p></div>"
        )
    d = pd.concat(blocks, ignore_index=True)
    sc = d["sector_cd"].astype(str)
    tot_tv = np.where(sc == "1001", float(total_tv_k), float(total_tv_q))
    tot_mcap = np.where(sc == "1001", float(total_mcap_k), float(total_mcap_q))
    tot_tv3 = np.where(sc == "1001", float(total_tv_3d_k), float(total_tv_3d_q))
    d["tv_pct"] = np.where(tot_tv > 0, d["trading_value"] / tot_tv * 100.0, np.nan)
    d["mcap_pct"] = np.where((tot_mcap > 0) & d["mcap"].notna(), d["mcap"].astype(float) / tot_mcap * 100.0, np.nan)
    d["energy_ratio"] = np.where(
        np.isfinite(d["tv_pct"]) & np.isfinite(d["mcap_pct"]) & (d["mcap_pct"].astype(float) > 0),
        d["tv_pct"].astype(float) / d["mcap_pct"].astype(float),
        np.nan,
    )
    d["tv_3d_pct"] = np.where(tot_tv3 > 0, d["tv_3d"].astype(float) / tot_tv3 * 100.0, np.nan)
    d["energy_ratio_3d"] = np.where(
        np.isfinite(d["tv_3d_pct"])
        & np.isfinite(d["mcap_pct"])
        & (d["mcap_pct"].astype(float) > 0),
        d["tv_3d_pct"].astype(float) / d["mcap_pct"].astype(float),
        np.nan,
    )
    d = d.sort_values("energy_ratio_3d", ascending=False, na_position="last").head(50).reset_index(drop=True)
    d["tv_rank_prev"] = d["ticker"].astype(str).map(prev_tv_by_ticker)

    def _fmt_int(v):
        if pd.isna(v):
            return ""
        return f"{int(round(v)):,}"

    def _fmt_pct(v):
        if pd.isna(v):
            return ""
        return f"{v:.2f}%"

    def _fmt_theme_cell(th):
        th = (th or "").strip() if pd.notna(th) else ""
        if len(th) > 96:
            return th[:95] + "…"
        return th

    def _sv(x) -> str:
        try:
            if x is None or (isinstance(x, float) and (np.isnan(x) or not np.isfinite(x))):
                return ""
            v = float(x)
            if not np.isfinite(v):
                return ""
            return f' data-sort-value="{v}"'
        except (TypeError, ValueError):
            return ""

    n = len(d)
    parts: list[str] = [
        '<div class="mj-html-table-wrap"><h3 style="margin:10px 0 6px 0;font-size:1.05rem;">'
        "코스피·코스닥 거래대금 각 상위 100 합산 (3일 에너지배율 높은 순, 상위 50)</h3>",
        '<table class="krx-sortable mjtop100" border="0" cellpadding="5" cellspacing="0" '
        'style="border-collapse:collapse;width:100%;font-size:11px;background:#fff;border:1px solid #ddd;">',
        "<thead><tr style='background:#37474f;color:#fff;font-weight:600;'>",
        "<th style='text-align:center;padding:8px 4px;'>순위</th>",
        "<th style='text-align:right;padding:8px 4px;'>전일 순위</th>",
        "<th style='text-align:right;padding:8px 4px;'>순위 변동</th>",
        "<th style='text-align:center;padding:8px 4px;'>시장</th>",
        "<th style='text-align:center;padding:8px 4px;'>종목코드</th>",
        "<th style='text-align:left;padding:8px 4px;'>종목명</th>",
        "<th style='text-align:left;padding:8px 4px;'>테마</th>",
        "<th style='text-align:right;padding:8px 4px;'>당일 거래대금 순위</th>",
        "<th style='text-align:right;padding:8px 4px;'>3일 에너지배율</th>",
        "<th style='text-align:right;padding:8px 4px;'>당일 에너지배율</th>",
        "<th style='text-align:right;padding:8px 4px;'>Talent(%)</th>",
        "<th style='text-align:right;padding:8px 4px;'>거래대금</th>",
        "<th style='text-align:right;padding:8px 4px;'>거래대금 전체비중</th>",
        "<th style='text-align:right;padding:8px 4px;'>시총 전체비중</th>",
        "</tr></thead><tbody>",
    ]
    fc_base = "#212121"
    for i in range(n):
        tk = "" if pd.isna(d["ticker"].iloc[i]) else str(d["ticker"].iloc[i])
        nm = "" if pd.isna(d["name"].iloc[i]) else str(d["name"].iloc[i])
        tk_cell = html.escape(tk)
        nm_cell = html.escape(nm)
        if tk in highlight_set:
            tk_cell = f"<b>{tk_cell}</b>"
            nm_cell = f"<b>{nm_cell}</b>"
        er3 = float(d["energy_ratio_3d"].iloc[i]) if np.isfinite(d["energy_ratio_3d"].iloc[i]) else np.nan
        er1 = float(d["energy_ratio"].iloc[i]) if np.isfinite(d["energy_ratio"].iloc[i]) else np.nan
        col3 = _mj_energy_ratio_font_color(er3)
        col1 = _mj_energy_ratio_font_color(er1)
        tv_r = d["tv_rank_mkt"].iloc[i]
        tv_r_txt = "" if pd.isna(tv_r) else str(int(float(tv_r)))
        prv = d["tv_rank_prev"].iloc[i] if "tv_rank_prev" in d.columns else np.nan
        prv_txt = "" if pd.isna(prv) or not np.isfinite(float(prv)) else str(int(float(prv)))
        _rc_txt, _rc_sv, _rc_col = _krx_fmt_rank_change_cell(i + 1, prv)
        mkt_lbl = "코스피" if str(d["sector_cd"].iloc[i]) == "1001" else "코스닥"
        tal = d["talent_120"].iloc[i] if "talent_120" in d.columns else np.nan
        tal_txt = f"{float(tal):.1f}" if pd.notna(tal) and np.isfinite(float(tal)) else ""
        parts.append("<tr>")
        parts.append(f"<td style='text-align:center;color:{fc_base}'{_sv(i + 1)}>{i + 1}</td>")
        parts.append(f"<td style='text-align:right;color:{fc_base}'{_sv(prv)}>{html.escape(prv_txt)}</td>")
        parts.append(
            f"<td style='text-align:right;color:{_rc_col}'{_sv(_rc_sv)}>{html.escape(_rc_txt)}</td>"
        )
        parts.append(f"<td style='text-align:center;color:{fc_base}'>{html.escape(mkt_lbl)}</td>")
        parts.append(f"<td style='text-align:center;color:{fc_base}'>{tk_cell}</td>")
        parts.append(f"<td style='text-align:left;color:{fc_base}'>{nm_cell}</td>")
        parts.append(
            f"<td style='text-align:left;color:{fc_base}'>{html.escape(_fmt_theme_cell(d['theme_str'].iloc[i]))}</td>"
        )
        parts.append(f"<td style='text-align:right;color:{fc_base}'{_sv(tv_r)}>{tv_r_txt}</td>")
        parts.append(
            f"<td style='text-align:right;color:{col3}'{_sv(er3)}>"
            f"{(f'{er3:.2f}' if np.isfinite(er3) else '')}</td>"
        )
        parts.append(
            f"<td style='text-align:right;color:{col1}'{_sv(er1)}>"
            f"{(f'{er1:.2f}' if np.isfinite(er1) else '')}</td>"
        )
        parts.append(
            f"<td style='text-align:right;color:{fc_base}'{_sv(pd.to_numeric(tal, errors='coerce'))}>{tal_txt}</td>"
        )
        parts.append(
            f"<td style='text-align:right;color:{fc_base}'{_sv(d['trading_value'].iloc[i])}>"
            f"{_fmt_int(d['trading_value'].iloc[i])}</td>"
        )
        parts.append(
            f"<td style='text-align:right;color:{fc_base}'{_sv(d['tv_pct'].iloc[i])}>{_fmt_pct(d['tv_pct'].iloc[i])}</td>"
        )
        parts.append(
            f"<td style='text-align:right;color:{fc_base}'{_sv(d['mcap_pct'].iloc[i])}>"
            f"{_fmt_pct(d['mcap_pct'].iloc[i])}</td>"
        )
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
    return "".join(parts)


def _rs_high_list_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """RS 고분위 리스트 HTML 표와 동일 스냅샷."""
    d = df.copy()
    if "date" in d.columns:
        d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date
    ren: dict[str, str] = {"date": "rs_date", "Talent": "talent_pct", "chg_pct": "chg_pct_1d"}
    d = d.rename(columns=ren)
    if "신고가여부" in d.columns:
        d = d.rename(columns={"신고가여부": "new_high_250d_flag"})
    if "_rs_avg" in d.columns:
        d = d.rename(columns={"_rs_avg": "rs_avg"})
    cols = [
        "ticker",
        "rs_date",
        "market_type",
        "rs_10d",
        "rs_20d",
        "rs_50d",
        "rs_120d",
        "rs_avg",
        "name",
        "mcap",
        "last_close",
        "last_volume",
        "theme_str",
        "tv_rank",
        "energy_ratio_d0",
        "energy_ratio_d1",
        "energy_ratio_d2",
        "new_high_250d_flag",
        "talent_pct",
        "chg_pct_1d",
    ]
    for c in cols:
        if c not in d.columns:
            d[c] = np.nan
    return d[cols].copy()


_KRX_COLOR_UP = "#c62828"
_KRX_COLOR_DOWN = "#1565c0"
_KRX_COLOR_FLAT = "#212121"
_KRX_COLOR_NA = "#757575"


def _krx_rank_change_value(curr_rank, prev_rank) -> float | None:
    """순위 변동 = 전일 순위 − 당일 순위 (양수면 순위 상승·개선)."""
    try:
        c = float(curr_rank)
        p = float(prev_rank)
        if not (np.isfinite(c) and np.isfinite(p)):
            return None
        return p - c
    except (TypeError, ValueError):
        return None


def _krx_rank_change_font_color(delta: float | None) -> str:
    if delta is None or not np.isfinite(float(delta)):
        return _KRX_COLOR_NA
    d = float(delta)
    if d > 0:
        return _KRX_COLOR_UP
    if d < 0:
        return _KRX_COLOR_DOWN
    return _KRX_COLOR_FLAT


def _krx_fmt_rank_change_cell(curr_rank, prev_rank) -> tuple[str, float | None, str]:
    """(표시문자열, 정렬용 숫자, 글자색) — 순위 변동 = 전일순위−당일순위."""
    delta = _krx_rank_change_value(curr_rank, prev_rank)
    if delta is None:
        return "", None, _KRX_COLOR_NA
    if abs(delta - round(delta)) < 1e-6:
        iv = int(round(delta))
        txt = f"+{iv}" if iv > 0 else str(iv)
        return txt, float(iv), _krx_rank_change_font_color(iv)
    txt = f"+{delta:.1f}" if delta > 0 else f"{delta:.1f}"
    return txt, float(delta), _krx_rank_change_font_color(delta)


# HTML 표: thead 칼럼 클릭 시 오름·내림차순 정렬 (table에 class="krx-sortable" 부여)
KRX_SORTABLE_TABLE_CSS_JS = """
<style>
table.krx-sortable thead th { cursor: pointer; user-select: none; padding-right: 16px; }
table.krx-sortable thead th:hover { background: rgba(0,0,0,0.06); }
table.krx-sortable thead th.sort-asc::after { content: " ▲"; font-size: 10px; color: #1976d2; }
table.krx-sortable thead th.sort-desc::after { content: " ▼"; font-size: 10px; color: #1976d2; }
</style>
<script>
(function () {
  function cellSortValue(td) {
    if (!td) return "";
    if (td.dataset && td.dataset.sortValue !== undefined && td.dataset.sortValue !== "") {
      if (td.dataset.sortValue === "__STR__") return (td.textContent || "").trim();
      var n = parseFloat(td.dataset.sortValue);
      if (!isNaN(n)) return n;
    }
    var t = (td.textContent || "").trim().replace(/,/g, "");
    if (t === "" || t === "-") return NaN;
    var n2 = parseFloat(t);
    if (!isNaN(n2)) return n2;
    return (td.textContent || "").trim();
  }
  function compare(a, b, dir) {
    var va = cellSortValue(a), vb = cellSortValue(b);
    var bothNum = typeof va === "number" && typeof vb === "number" && !isNaN(va) && !isNaN(vb);
    var cmp = bothNum ? (va - vb) : String(va).localeCompare(String(vb), "ko", { numeric: true });
    return dir === "asc" ? cmp : -cmp;
  }
  function initTable(table) {
    if (!table || table.dataset.krxSortInit) return;
    table.dataset.krxSortInit = "1";
    var thRow = table.tHead && table.tHead.rows[0];
    if (!thRow) return;
    var ths = thRow.cells;
    for (var c = 0; c < ths.length; c++) {
      (function (colIdx) {
        ths[colIdx].addEventListener("click", function () {
          var cur = ths[colIdx].dataset.sortDir;
          var dir = cur === "asc" ? "desc" : "asc";
          for (var k = 0; k < ths.length; k++) {
            ths[k].classList.remove("sort-asc", "sort-desc");
            delete ths[k].dataset.sortDir;
          }
          ths[colIdx].dataset.sortDir = dir;
          ths[colIdx].classList.add(dir === "asc" ? "sort-asc" : "sort-desc");
          var tbody = table.tBodies[0];
          if (!tbody) return;
          var rows = Array.prototype.slice.call(tbody.rows);
          rows.sort(function (r1, r2) {
            return compare(r1.cells[colIdx], r2.cells[colIdx], dir);
          });
          rows.forEach(function (r) { tbody.appendChild(r); });
        });
      })(c);
    }
  }
  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("table.krx-sortable").forEach(initTable);
  });
})();
</script>
"""


def _html_sort_num_attr(v) -> str:
    """정렬용 data-sort-value (숫자 칼럼)."""
    try:
        if v is None or (isinstance(v, float) and (np.isnan(v) or not np.isfinite(v))):
            return ""
        x = float(v)
        if not np.isfinite(x):
            return ""
        return f' data-sort-value="{x}"'
    except (TypeError, ValueError):
        return ""


def _krx_chg_font_color(v) -> str:
    """당일 등락률(%) 기준 글자색: 상승 빨강, 하락 파랑."""
    try:
        if v is None or pd.isna(v):
            return _KRX_COLOR_NA
    except (ValueError, TypeError):
        return _KRX_COLOR_NA
    try:
        x = float(v)
    except (TypeError, ValueError):
        return _KRX_COLOR_NA
    if not np.isfinite(x):
        return _KRX_COLOR_NA
    if x > 0:
        return _KRX_COLOR_UP
    if x < 0:
        return _KRX_COLOR_DOWN
    return _KRX_COLOR_FLAT


def _krx_colored_html(inner_html: str, chg_pct) -> str:
    """inner_html(이미 escape·볼드 처리된 문자열)에 등락 색 span 적용."""
    col = _krx_chg_font_color(chg_pct)
    return f'<span style="color:{col}">{inner_html}</span>'


def _build_ticker_date_chg_map(
    ohlcv_df: pd.DataFrame,
    *,
    ticker_col: str = "ticker",
    date_col: str = "date",
    close_col: str = "close",
) -> dict[tuple[str, str], float]:
    """(ticker, YYYY-MM-DD) → 해당 거래일 종가 전일 대비 등락률(%)."""
    if ohlcv_df is None or ohlcv_df.empty:
        return {}
    gdf = ohlcv_df.loc[:, [ticker_col, date_col, close_col]].copy()
    gdf[ticker_col] = gdf[ticker_col].astype(str)
    gdf[date_col] = pd.to_datetime(gdf[date_col], errors="coerce")
    gdf[close_col] = pd.to_numeric(gdf[close_col], errors="coerce")
    gdf = gdf.dropna(subset=[date_col])
    if gdf.empty:
        return {}
    out: dict[tuple[str, str], float] = {}
    for tk, g in gdf.groupby(ticker_col, sort=False):
        g = g.sort_values(date_col)
        cl = g[close_col].astype(float)
        prev = cl.shift(1)
        for i in range(len(g)):
            c0, c1 = float(cl.iloc[i]), float(prev.iloc[i])
            dkey = pd.Timestamp(g[date_col].iloc[i]).strftime("%Y-%m-%d")
            if np.isfinite(c0) and np.isfinite(c1) and c1 != 0:
                out[(str(tk), dkey)] = (c0 - c1) / c1 * 100.0
    return out


def _load_ticker_d0_chg_pct_map(engine, tickers: list[str], chunk: int = 400) -> dict[str, float]:
    """최신 OHLCV 거래일(D-0) 기준 전일 대비 등락률(%) — ticker → float."""
    if not tickers:
        return {}
    out: dict[str, float] = {}
    try:
        drows = pd.read_sql_query(
            "SELECT DISTINCT date FROM krx_ohlcv ORDER BY date DESC LIMIT 2",
            con=engine,
        )
        dl = pd.to_datetime(drows["date"], errors="coerce").dropna().sort_values(ascending=False).tolist()
        if len(dl) < 2:
            return {}
        d0s, d1s = dl[0].strftime("%Y-%m-%d"), dl[1].strftime("%Y-%m-%d")
        for i in range(0, len(tickers), chunk):
            chunk_t = [str(t) for t in tickers[i : i + chunk]]
            ph = ",".join(["%s"] * len(chunk_t))
            q = f"""
                SELECT ticker, date, close
                FROM krx_ohlcv
                WHERE date IN (%s, %s) AND ticker IN ({ph})
            """
            bind = (d0s, d1s) + tuple(chunk_t)
            odf = pd.read_sql_query(q, con=engine, params=bind)
            if odf.empty:
                continue
            odf["ticker"] = odf["ticker"].astype(str)
            odf["date"] = pd.to_datetime(odf["date"], errors="coerce")
            odf["close"] = pd.to_numeric(odf["close"], errors="coerce")
            for tk, g in odf.groupby("ticker"):
                g = g.sort_values("date")
                if len(g) < 2:
                    continue
                c0 = float(g["close"].iloc[-1])
                c1 = float(g["close"].iloc[-2])
                if np.isfinite(c0) and np.isfinite(c1) and c1 != 0:
                    out[str(tk)] = (c0 - c1) / c1 * 100.0
    except Exception:
        pass
    return out


def _load_tv_top100_universe(
    engine,
) -> tuple[set[str], dict[str, int], set[str], set[str]]:
    """
    최신 OHLCV 일자 기준 코스피·코스닥 각각 거래대금(close×volume) Top100.
    반환: (전체 티커 집합, ticker→tv_rank, 코스피 Top100 set, 코스닥 Top100 set)
    """
    try:
        ref = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
        ref_d = pd.to_datetime(ref.iloc[0]["d"], errors="coerce")
    except Exception:
        return set(), {}, set(), set()
    if pd.isna(ref_d):
        return set(), {}, set(), set()

    universe: set[str] = set()
    rank_map: dict[str, int] = {}
    kospi_tv: set[str] = set()
    kosdaq_tv: set[str] = set()
    d_str = pd.Timestamp(ref_d).strftime("%Y-%m-%d")

    for sector_cd, market_set in (("1001", kospi_tv), ("2001", kosdaq_tv)):
        q = """
            SELECT o.ticker AS ticker
            FROM krx_ohlcv o
            INNER JOIN krx_ticker_sector ts
                ON ts.ticker = o.ticker AND ts.sector_cd = %s
            INNER JOIN krx_ticker t
                ON t.종목코드 = o.ticker
               AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
               AND t.종목구분 = '보통주'
            WHERE DATE(o.date) = DATE(%s)
            ORDER BY (o.close * o.volume) DESC
            LIMIT 100
        """
        try:
            df = pd.read_sql_query(q, con=engine, params=(sector_cd, d_str))
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for i, tk in enumerate(df["ticker"].astype(str).tolist(), start=1):
            universe.add(tk)
            market_set.add(tk)
            rank_map[tk] = i
    return universe, rank_map, kospi_tv, kosdaq_tv


def _krx_tv_rank_prev_by_ticker(engine) -> dict[str, float]:
    """직전 거래일 시장(sector_cd) 내 거래대금 순위 (1=최대). RS·방향우세 등 HTML 공통용."""
    try:
        drows = pd.read_sql_query(
            "SELECT DISTINCT date FROM krx_ohlcv ORDER BY date DESC LIMIT 2",
            con=engine,
        )
        dl = pd.to_datetime(drows["date"], errors="coerce").dropna().tolist()
        if len(dl) < 2:
            return {}
        d_prev_s = pd.Timestamp(dl[1]).normalize().strftime("%Y-%m-%d")
        q = """
            SELECT o.ticker AS ticker, ts.sector_cd AS sector_cd,
                   (o.close * o.volume) AS tv
            FROM krx_ohlcv o
            INNER JOIN krx_ticker t ON t.종목코드 = o.ticker
                AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
                AND t.종목구분 = '보통주'
            INNER JOIN krx_ticker_sector ts ON ts.ticker = o.ticker
                AND ts.sector_cd IN ('1001', '2001')
            WHERE DATE(o.date) = DATE(%s)
        """
        df = pd.read_sql_query(q, con=engine, params=(d_prev_s,))
        if df is None or df.empty:
            return {}
        df["ticker"] = df["ticker"].astype(str)
        df["tv"] = pd.to_numeric(df["tv"], errors="coerce")
        df["rk"] = df.groupby("sector_cd")["tv"].rank(ascending=False, method="min")
        return {str(r["ticker"]): float(r["rk"]) for _, r in df.iterrows() if pd.notna(r["rk"])}
    except Exception:
        return {}


def _mj_fast_top100_tickers_from_db(engine, quiet: bool = False) -> set[str]:
    """
    교집합 산출용(초경량): 대시보드/Plotly 생성 없이
    '최신 OHLCV 일자' 기준 코스피·코스닥 각각 거래대금 Top100 티커 집합만 DB에서 산출.

    - 거래대금 = close * volume (당일)
    - 유니버스: krx_ticker_sector.sector_cd in ('1001','2001') + krx_ticker 최신 기준일 보통주
    - 반환: (코스피 Top100) ∪ (코스닥 Top100)
    - 부가: 기존 로직 호환을 위해 market_judgment_tv_rank.csv 형태로 함께 저장
    """
    _log = print if not quiet else (lambda *a, **k: None)
    universe, rank_map, kospi_tv, kosdaq_tv = _load_tv_top100_universe(engine)
    if not universe:
        return set()

    rows_out: list[dict[str, object]] = []
    out_base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
    out_dir = os.path.join(out_base, date.today().strftime("%Y-%m-%d"))
    os.makedirs(out_dir, exist_ok=True)
    out_rank = os.path.join(out_dir, "market_judgment_tv_rank.csv")

    try:
        ref = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
        ref_d = pd.to_datetime(ref.iloc[0]["d"], errors="coerce")
        d_str = pd.Timestamp(ref_d).strftime("%Y-%m-%d")
    except Exception:
        d_str = ""

    for sector_cd in ("1001", "2001"):
        q = """
            SELECT
                o.ticker AS ticker,
                %s AS sector_cd,
                o.close AS current_price,
                (o.close * o.volume) AS trade_value
            FROM krx_ohlcv o
            INNER JOIN krx_ticker_sector ts
                ON ts.ticker = o.ticker
               AND ts.sector_cd = %s
            INNER JOIN krx_ticker t
                ON t.종목코드 = o.ticker
               AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
               AND t.종목구분 = '보통주'
            WHERE DATE(o.date) = DATE(%s)
            ORDER BY trade_value DESC
            LIMIT 100
        """
        try:
            df = pd.read_sql_query(q, con=engine, params=(sector_cd, sector_cd, d_str))
        except Exception:
            continue
        if df is None or df.empty:
            continue
        df["ticker"] = df["ticker"].astype(str)
        df = df.drop_duplicates(subset=["ticker"], keep="first").reset_index(drop=True)
        df["tv_rank"] = np.arange(1, len(df) + 1, dtype=int)
        for _, r in df.iterrows():
            rows_out.append(
                {
                    "ticker": str(r["ticker"]),
                    "sector_cd": str(r["sector_cd"]),
                    "tv_rank": int(r["tv_rank"]),
                    "current_price": float(r["current_price"]) if pd.notna(r["current_price"]) else np.nan,
                    "trade_value": float(r["trade_value"]) if pd.notna(r["trade_value"]) else np.nan,
                }
            )

    if rows_out:
        try:
            pd.DataFrame(rows_out).to_csv(out_rank, index=False, encoding="utf-8-sig")
        except Exception:
            pass
        try:
            _td_mj = pd.to_datetime(d_str, errors="coerce").date() if d_str else _krx_max_ohlcv_trade_date(engine)
            _save_krx_analysis_table(
                engine, "krx_analysis_mj_tv_rank", pd.DataFrame(rows_out), _td_mj
            )
        except Exception as e:
            if not quiet:
                print(f"경고: 분석 DB(krx_analysis_mj_tv_rank) 저장 실패 ({type(e).__name__}: {e})")
        _log(f"  → 교집합용 시장판단 Top100(코스피/코스닥) 티커 {len(universe)}개 산출: {out_rank}")
    return universe


def _load_latest_rs_rank_map(engine) -> dict[str, int]:
    """최신 krx_relative_strength 기준: 시장별(RS10·20·50·120 평균) 내림차순 RS 순위(1=최상위)."""
    rank_map, _ = _load_latest_rs_rank_and_score_maps(engine)
    return rank_map


def _load_latest_rs_rank_and_score_maps(engine) -> tuple[dict[str, int], dict[str, float]]:
    """RS 순위(1=최상위) 및 RS 점수(RS10·20·50·120 평균, 백분위)."""
    q = """
        SELECT r.ticker, r.market_type, r.rs_10d, r.rs_20d, r.rs_50d, r.rs_120d
        FROM krx_relative_strength r
        INNER JOIN (SELECT MAX(date) AS d FROM krx_relative_strength) latest
            ON r.date = latest.d
    """
    try:
        df = pd.read_sql_query(q, con=engine)
    except Exception:
        return {}, {}
    if df is None or df.empty:
        return {}, {}
    df["ticker"] = df["ticker"].astype(str)
    df["market_type"] = df["market_type"].astype(str).str.upper()
    for c in ("rs_10d", "rs_20d", "rs_50d", "rs_120d"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["_rs_avg"] = df[["rs_10d", "rs_20d", "rs_50d", "rs_120d"]].mean(axis=1, skipna=True)
    df["_rs_rank"] = df.groupby("market_type")["_rs_avg"].rank(ascending=False, method="min")
    rank_out: dict[str, int] = {}
    score_out: dict[str, float] = {}
    for _, r in df.iterrows():
        tk = str(r["ticker"])
        try:
            rank_out[tk] = int(float(r["_rs_rank"]))
        except Exception:
            pass
        try:
            sc = float(r["_rs_avg"])
            if np.isfinite(sc):
                score_out[tk] = sc
        except Exception:
            pass
    return rank_out, score_out


def _compute_250d_high_flag_map(engine, tickers: list[str], roll_d: int = 250, lag_td: int = 3) -> dict[str, str]:
    """D-0 종가가 D-{lag_td} 말 기준 roll_d 거래일 최고가(high rolling max) 초과면 O, 아니면 X."""
    if not tickers:
        return {}
    try:
        ref_m = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
        ref_d = pd.to_datetime(ref_m.iloc[0]["d"], errors="coerce")
    except Exception:
        return {}
    if pd.isna(ref_d):
        return {}
    cutoff = (ref_d.normalize() - pd.Timedelta(days=900)).strftime("%Y-%m-%d")
    parts: list[pd.DataFrame] = []
    chunk_size = 450
    for i in range(0, len(tickers), chunk_size):
        chunk = [str(x) for x in tickers[i : i + chunk_size]]
        ph = ",".join(["%s"] * len(chunk))
        q = f"""
            SELECT ticker, date, high, close
            FROM krx_ohlcv
            WHERE date >= %s AND ticker IN ({ph})
        """
        bind = tuple([cutoff] + chunk)
        try:
            parts.append(pd.read_sql_query(q, con=engine, params=bind))
        except Exception:
            continue
    if not parts:
        return {}
    ohlcv = pd.concat(parts, ignore_index=True)
    if ohlcv.empty:
        return {}
    ohlcv["ticker"] = ohlcv["ticker"].astype(str)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"], errors="coerce")
    ohlcv = ohlcv.dropna(subset=["date"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    for c in ("high", "close"):
        ohlcv[c] = pd.to_numeric(ohlcv[c], errors="coerce")

    def _close_gt_dlag_rollhigh_ox(g: pd.DataFrame) -> str:
        g = g.sort_values("date").reset_index(drop=True)
        hi = pd.to_numeric(g["high"], errors="coerce").to_numpy(dtype=float)
        cl = pd.to_numeric(g["close"], errors="coerce").to_numpy(dtype=float)
        if len(g) < roll_d + lag_td + 1:
            return ""
        i_ref_end = len(g) - 1 - lag_td
        if i_ref_end < roll_d - 1:
            return ""
        ref_hi = np.nanmax(hi[i_ref_end - (roll_d - 1) : i_ref_end + 1])
        if not (np.isfinite(ref_hi) and np.isfinite(cl[-1])):
            return ""
        return "O" if float(cl[-1]) > float(ref_hi) else "X"

    out: dict[str, str] = {}
    for t, g in ohlcv.groupby("ticker", sort=False):
        out[str(t)] = _close_gt_dlag_rollhigh_ox(g)
    return out


def write_rs_high_list_html(
    engine,
    output_base_dir: str | None = None,
    highlight_tickers: set[str] | None = None,
    quiet: bool = False,
) -> tuple[str | None, set[str]]:
    """
    krx_relative_strength 최신 일자 기준, rs_10d >= 90 인 종목만.
    시장별 순위: RS10·20·50·120d 산술평균(내부 _rs_avg, 표시 없음) 내림차순.
    RS200d는 조회·표시 없음. 테마는 krx_theme_stock 기준.
    에너지배율은 D-0·D-1·D-2 세 칼럼: 각각 krx_ohlcv 최신일·그 전날·그전날(거래일 기준)의
    (당일 거래대금÷시장 당일 거래대금×100)÷(시총÷시장시총×100). 시총은 최신 krx_ticker 기준일.
    신고가여부: D-0 종가가 D-3 말 기준 250거래일 최고가(고가 rolling)보다 크면 O, 아니면 X.
    """
    base = output_base_dir or os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
    out_dir = os.path.join(base, date.today().strftime("%Y-%m-%d"))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "rs_high_list.html")

    q = """
        SELECT
            r.ticker,
            r.date,
            r.market_type,
            r.rs_10d,
            r.rs_20d,
            r.rs_50d,
            r.rs_120d,
            t.종목명 AS name,
            t.시가총액 AS mcap,
            o.close AS last_close,
            o.volume AS last_volume
        FROM krx_relative_strength r
        INNER JOIN (
            SELECT MAX(date) AS d FROM krx_relative_strength
        ) latest ON r.date = latest.d
        LEFT JOIN krx_ticker t
            ON t.종목코드 = r.ticker
            AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
        LEFT JOIN krx_ohlcv o
            ON o.ticker = r.ticker
            AND o.date = (SELECT MAX(date) FROM krx_ohlcv)
        WHERE r.rs_10d >= 90
        ORDER BY r.market_type ASC, r.ticker ASC
    """
    try:
        df = pd.read_sql_query(q, con=engine)
    except Exception as e:
        print(f"실패: RS 고분위 리스트 조회 ({type(e).__name__}: {e})")
        return None, set()

    if df.empty:
        rs_html_doc = """<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"/><title>RS 고분위 리스트</title></head>
<body>
  <p>조건: 최신일 기준 <code>rs_10d</code> &gt;= 90 — 해당 데이터가 없습니다.</p>
  <p><code>krx_relative_strength</code>가 채워졌는지, RS 산출 스크립트 실행 여부를 확인하세요.</p>
</body>
</html>"""
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(rs_html_doc)
        if not quiet:
            print(f"완료: RS 리스트 HTML 저장(0건): {out_path}")
        try:
            _rd0 = _rs_snapshot_ref_trade_date(engine, None)
            _save_krx_analysis_table(engine, "krx_analysis_rs_high_list", pd.DataFrame(), _rd0)
            _save_krx_analysis_table(engine, "krx_analysis_rs_daily_top20", pd.DataFrame(), _rd0)
        except Exception as _e_rsdb0:
            print(f"경고: RS 분석 DB 저장 실패 ({type(_e_rsdb0).__name__}: {_e_rsdb0})")
        return out_path, set()

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for c in ("rs_10d", "rs_20d", "rs_50d", "rs_120d"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    _rs_cols = ["rs_10d", "rs_20d", "rs_50d", "rs_120d"]
    df["_rs_avg"] = df[_rs_cols].mean(axis=1, skipna=True)

    df["mcap"] = pd.to_numeric(df.get("mcap"), errors="coerce")
    df["last_close"] = pd.to_numeric(df.get("last_close"), errors="coerce")
    df["last_volume"] = pd.to_numeric(df.get("last_volume"), errors="coerce")

    _d3: list[pd.Timestamp | None] = [None, None, None]
    try:
        _drows = pd.read_sql_query(
            "SELECT DISTINCT date FROM krx_ohlcv ORDER BY date DESC LIMIT 3",
            con=engine,
        )
        _dl = pd.to_datetime(_drows["date"], errors="coerce").dropna().sort_values(ascending=False).tolist()
        for _i in range(min(3, len(_dl))):
            _d3[_i] = pd.Timestamp(_dl[_i]).normalize()
    except Exception:
        pass
    _d0, _d1, _d2 = _d3[0], _d3[1], _d3[2]
    _active_strs = [d.strftime("%Y-%m-%d") for d in (_d0, _d1, _d2) if d is not None]

    q_mcap_only = """
        SELECT ts.sector_cd, SUM(t.시가총액) AS total_mcap
        FROM krx_ticker t
        INNER JOIN krx_ticker_sector ts ON t.종목코드 = ts.ticker
        WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
          AND t.종목구분 = '보통주'
          AND ts.sector_cd IN ('1001', '2001')
        GROUP BY ts.sector_cd
    """
    mcap_map: dict[str, float] = {}
    try:
        _mcdf = pd.read_sql_query(q_mcap_only, con=engine)
        mcap_map = {str(r["sector_cd"]): float(r["total_mcap"] or 0) for _, r in _mcdf.iterrows()}
    except Exception:
        pass

    # (거래일 문자열, sector_cd) -> 시장 당일 거래대금 합
    market_tv_by_date_sec: dict[tuple[str, str], float] = {}
    if _active_strs:
        _phd = ",".join(["%s"] * len(_active_strs))
        q_mkt_tv = f"""
            SELECT DATE(o.date) AS d, ts.sector_cd, SUM(o.close * o.volume) AS total_tv
            FROM krx_ohlcv o
            INNER JOIN krx_ticker t ON t.종목코드 = o.ticker
                AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
            INNER JOIN krx_ticker_sector ts ON ts.ticker = o.ticker
            WHERE t.종목구분 = '보통주'
              AND ts.sector_cd IN ('1001', '2001')
              AND DATE(o.date) IN ({_phd})
            GROUP BY DATE(o.date), ts.sector_cd
        """
        try:
            _mkdf = pd.read_sql_query(q_mkt_tv, con=engine, params=tuple(_active_strs))
            for _, rr in _mkdf.iterrows():
                _dk = pd.Timestamp(rr["d"]).strftime("%Y-%m-%d")
                market_tv_by_date_sec[(_dk, str(rr["sector_cd"]))] = float(rr["total_tv"] or 0.0)
        except Exception:
            pass

    _tickers = df["ticker"].astype(str).unique().tolist()
    _chunk = 400
    per_ticker_tv: dict[tuple[str, str], float] = {}
    if _active_strs and _tickers:
        _phd = ",".join(["%s"] * len(_active_strs))
        try:
            for _i in range(0, len(_tickers), _chunk):
                _chunk_t = _tickers[_i : _i + _chunk]
                _pht = ",".join(["%s"] * len(_chunk_t))
                q_tv = f"""
                    SELECT o.ticker, DATE(o.date) AS d, SUM(o.close * o.volume) AS tv
                    FROM krx_ohlcv o
                    WHERE DATE(o.date) IN ({_phd})
                      AND o.ticker IN ({_pht})
                    GROUP BY o.ticker, DATE(o.date)
                """
                _bind = tuple(_active_strs + _chunk_t)
                _tvdf = pd.read_sql_query(q_tv, con=engine, params=_bind)
                for _, rr in _tvdf.iterrows():
                    _dk = pd.Timestamp(rr["d"]).strftime("%Y-%m-%d")
                    per_ticker_tv[(str(rr["ticker"]), _dk)] = float(rr["tv"] or 0.0)
        except Exception:
            pass

    try:
        q_theme = """
        SELECT ticker,
               GROUP_CONCAT(DISTINCT theme_name ORDER BY theme_name SEPARATOR ' · ') AS theme_str
        FROM krx_theme_stock
        GROUP BY ticker
        """
        th_df = pd.read_sql_query(q_theme, con=engine)
        th_df["ticker"] = th_df["ticker"].astype(str)
        df["ticker"] = df["ticker"].astype(str)
        df = df.merge(th_df, on="ticker", how="left")
    except Exception:
        th_df = pd.DataFrame(columns=["ticker", "theme_str"])
        df["theme_str"] = ""
    df["theme_str"] = df["theme_str"].fillna("").astype(str)

    # 코스피·코스닥 전체 RS 기준 시장별 순위 상위 100 (Talent 순 표용 티커 집합)
    set_rs_top100_talent: set[str] = set()
    df_rs_k100 = pd.DataFrame()
    df_rs_q100 = pd.DataFrame()
    try:
        q_mkt_rs = """
            SELECT
                r.ticker,
                r.market_type,
                r.rs_10d,
                r.rs_20d,
                r.rs_50d,
                r.rs_120d,
                t.종목명 AS name,
                t.시가총액 AS mcap
            FROM krx_relative_strength r
            INNER JOIN (SELECT MAX(date) AS d FROM krx_relative_strength) latest ON r.date = latest.d
            LEFT JOIN krx_ticker t
                ON t.종목코드 = r.ticker
                AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
            WHERE UPPER(TRIM(r.market_type)) IN ('KOSPI', 'KOSDAQ')
        """
        df_rs_mkt = pd.read_sql_query(q_mkt_rs, con=engine)
        if df_rs_mkt is not None and not df_rs_mkt.empty:
            df_rs_mkt["ticker"] = df_rs_mkt["ticker"].astype(str)
            df_rs_mkt["market_type"] = df_rs_mkt["market_type"].astype(str).str.strip().str.upper()
            for c in _rs_cols:
                if c in df_rs_mkt.columns:
                    df_rs_mkt[c] = pd.to_numeric(df_rs_mkt[c], errors="coerce")
            df_rs_mkt["_rs_avg"] = df_rs_mkt[_rs_cols].mean(axis=1, skipna=True)
            df_rs_mkt["rs_mkt_rank"] = df_rs_mkt.groupby("market_type")["_rs_avg"].rank(ascending=False, method="min")
            df_rs_mkt["mcap"] = pd.to_numeric(df_rs_mkt.get("mcap"), errors="coerce")
            df_rs_mkt = df_rs_mkt.merge(th_df, on="ticker", how="left")
            df_rs_mkt["theme_str"] = df_rs_mkt["theme_str"].fillna("").astype(str)
            df_rs_k100 = df_rs_mkt[
                (df_rs_mkt["market_type"] == "KOSPI") & (df_rs_mkt["rs_mkt_rank"] <= 100)
            ].copy()
            df_rs_q100 = df_rs_mkt[
                (df_rs_mkt["market_type"] == "KOSDAQ") & (df_rs_mkt["rs_mkt_rank"] <= 100)
            ].copy()
            set_rs_top100_talent = set(df_rs_k100["ticker"].astype(str).tolist()) | set(
                df_rs_q100["ticker"].astype(str).tolist()
            )
    except Exception:
        pass

    _extra_tv = sorted(set_rs_top100_talent - set(_tickers))
    if _active_strs and _extra_tv:
        _phd = ",".join(["%s"] * len(_active_strs))
        try:
            for _i in range(0, len(_extra_tv), _chunk):
                _chunk_t = _extra_tv[_i : _i + _chunk]
                _pht = ",".join(["%s"] * len(_chunk_t))
                q_tv = f"""
                    SELECT o.ticker, DATE(o.date) AS d, SUM(o.close * o.volume) AS tv
                    FROM krx_ohlcv o
                    WHERE DATE(o.date) IN ({_phd})
                      AND o.ticker IN ({_pht})
                    GROUP BY o.ticker, DATE(o.date)
                """
                _bind = tuple(_active_strs + _chunk_t)
                _tvdf = pd.read_sql_query(q_tv, con=engine, params=_bind)
                for _, rr in _tvdf.iterrows():
                    _dk = pd.Timestamp(rr["d"]).strftime("%Y-%m-%d")
                    per_ticker_tv[(str(rr["ticker"]), _dk)] = float(rr["tv"] or 0.0)
        except Exception:
            pass

    _tickers_all = sorted(set(_tickers) | set_rs_top100_talent)
    output_base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
    _rank_dir = os.path.join(output_base, date.today().strftime("%Y-%m-%d"))
    tv_rank_path = os.path.join(_rank_dir, "market_judgment_tv_rank.csv")
    tv_rank_map: dict[str, float] = {}
    try:
        rk = pd.read_csv(tv_rank_path, dtype={"ticker": str, "sector_cd": str})
        rk["ticker"] = rk["ticker"].astype(str)
        tv_rank_map = {str(r["ticker"]): float(pd.to_numeric(r["tv_rank"], errors="coerce")) for _, r in rk.iterrows()}
    except Exception:
        tv_rank_map = {}
    df["tv_rank"] = pd.to_numeric(df["ticker"].map(tv_rank_map), errors="coerce")
    prev_tv_map_rs = _krx_tv_rank_prev_by_ticker(engine)
    df["tv_rank_prev"] = pd.to_numeric(df["ticker"].astype(str).map(prev_tv_map_rs), errors="coerce")

    # 3개 리포트 교집합 종목 볼드 표시용
    _highlight_set = set([str(x) for x in (highlight_tickers or set())])

    def _sector_cd(market_type) -> str:
        s = str(market_type).strip().upper()
        return "1001" if s == "KOSPI" else "2001"

    def _energy_ratio_for_day(row, trade_date):
        if trade_date is None:
            return np.nan
        dkey = trade_date.strftime("%Y-%m-%d")
        sec = _sector_cd(row["market_type"])
        total_tv = market_tv_by_date_sec.get((dkey, sec), 0.0) or 0.0
        total_mcap = mcap_map.get(sec, 0.0) or 0.0
        tv = per_ticker_tv.get((str(row["ticker"]), dkey))
        mc = row.get("mcap")
        if total_tv <= 0 or total_mcap <= 0 or tv is None or pd.isna(tv) or pd.isna(mc) or float(mc) <= 0:
            return np.nan
        tv_pct = float(tv) / total_tv * 100.0
        mcap_pct = float(mc) / total_mcap * 100.0
        if mcap_pct <= 0 or not np.isfinite(mcap_pct):
            return np.nan
        er = tv_pct / mcap_pct
        return float(er) if np.isfinite(er) else np.nan

    df["energy_ratio_d0"] = df.apply(lambda r: _energy_ratio_for_day(r, _d0), axis=1)
    df["energy_ratio_d1"] = df.apply(lambda r: _energy_ratio_for_day(r, _d1), axis=1)
    df["energy_ratio_d2"] = df.apply(lambda r: _energy_ratio_for_day(r, _d2), axis=1)

    _ROLL_HIGH_D = 250
    _HIGH_REF_LAG_TRADING_D = 3  # D-3 말 시점의 250일 고가와 D-0 종가 비교

    def _close_gt_dlag_250high_ox(g: pd.DataFrame, roll_d: int, lag_td: int) -> str:
        """D-0 종가 > (D-{lag_td} 말 기준 roll_d 거래일 최고가) 이면 O, 아니면 X."""
        need = roll_d + lag_td
        if g is None or len(g) < need:
            return ""
        g = g.sort_values("date")
        hi = pd.to_numeric(g["high"], errors="coerce").to_numpy(dtype=float)
        cl = pd.to_numeric(g["close"], errors="coerce").to_numpy(dtype=float)
        if len(cl) < need or not np.isfinite(cl[-1]):
            return ""
        hi_roll = pd.Series(hi).rolling(roll_d, min_periods=roll_d).max()
        # iloc[-1]=D-0, -2=D-1, -3=D-2, -4=D-3 … → D-k = iloc[-(1+k)]
        ref = hi_roll.iloc[-(1 + lag_td)]
        if not np.isfinite(ref):
            return ""
        return "O" if float(cl[-1]) > float(ref) else "X"

    gh_flag_map: dict[str, str] = {}
    if _tickers_all:
        try:
            ref_m = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
            ref_d_ohlcv = pd.to_datetime(ref_m.iloc[0]["d"], errors="coerce")
            if pd.notna(ref_d_ohlcv):
                # 250거래일 + D-3 + 여유(휴장·상장 직후 등)
                co_cut = (ref_d_ohlcv.normalize() - pd.Timedelta(days=600)).strftime("%Y-%m-%d")
                for _i in range(0, len(_tickers_all), _chunk):
                    _chunk_t = _tickers_all[_i : _i + _chunk]
                    _pht = ",".join(["%s"] * len(_chunk_t))
                    q_oh = f"""
                        SELECT o.ticker, o.date, o.high, o.close
                        FROM krx_ohlcv o
                        WHERE o.date >= %s AND o.ticker IN ({_pht})
                        ORDER BY o.ticker, o.date
                    """
                    _bind_oh = tuple([co_cut] + _chunk_t)
                    _oh = pd.read_sql_query(q_oh, con=engine, params=_bind_oh)
                    if _oh.empty:
                        continue
                    _oh["date"] = pd.to_datetime(_oh["date"], errors="coerce")
                    for _tk, _g in _oh.groupby("ticker"):
                        gh_flag_map[str(_tk)] = _close_gt_dlag_250high_ox(_g, _ROLL_HIGH_D, _HIGH_REF_LAG_TRADING_D)
        except Exception:
            pass
    df["신고가여부"] = df["ticker"].astype(str).map(gh_flag_map).fillna("")
    if not df_rs_k100.empty:
        df_rs_k100["신고가여부"] = df_rs_k100["ticker"].astype(str).map(gh_flag_map).fillna("")
    if not df_rs_q100.empty:
        df_rs_q100["신고가여부"] = df_rs_q100["ticker"].astype(str).map(gh_flag_map).fillna("")

    # Talent: 최근 120거래일 중 (종가가 시가 대비 +10% 이상)인 날 비중(%)
    _TALENT_WINDOW = 120
    _TALENT_UP = 0.10

    def _talent_pct(g: pd.DataFrame, window: int = _TALENT_WINDOW, thr: float = _TALENT_UP) -> float:
        if g is None or g.empty:
            return np.nan
        g = g.sort_values("date")
        if len(g) > window:
            g = g.tail(window)
        op = pd.to_numeric(g.get("open"), errors="coerce")
        cl = pd.to_numeric(g.get("close"), errors="coerce")
        m = op.notna() & cl.notna() & (op.astype(float) > 0)
        if not m.any():
            return np.nan
        r = (cl[m].astype(float) / op[m].astype(float)) - 1.0
        return float((r >= thr).mean() * 100.0)

    talent_map: dict[str, float] = {}
    if _tickers_all:
        try:
            ref_m2 = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
            ref_d2 = pd.to_datetime(ref_m2.iloc[0]["d"], errors="coerce")
            if pd.notna(ref_d2):
                # 120거래일 확보를 위한 달력일 여유(휴장/상장 직후 등)
                cut2 = (ref_d2.normalize() - pd.Timedelta(days=260)).strftime("%Y-%m-%d")
                for _i in range(0, len(_tickers_all), _chunk):
                    _chunk_t = _tickers_all[_i : _i + _chunk]
                    _pht = ",".join(["%s"] * len(_chunk_t))
                    q_tc = f"""
                        SELECT o.ticker, o.date, o.open, o.close
                        FROM krx_ohlcv o
                        WHERE o.date >= %s AND o.ticker IN ({_pht})
                        ORDER BY o.ticker, o.date
                    """
                    _bind_tc = tuple([cut2] + _chunk_t)
                    _tc = pd.read_sql_query(q_tc, con=engine, params=_bind_tc)
                    if _tc.empty:
                        continue
                    _tc["date"] = pd.to_datetime(_tc["date"], errors="coerce")
                    for _tk, _g in _tc.groupby("ticker"):
                        talent_map[str(_tk)] = _talent_pct(_g, window=_TALENT_WINDOW, thr=_TALENT_UP)
        except Exception:
            pass
    df["Talent"] = df["ticker"].astype(str).map(talent_map)
    if not df_rs_k100.empty:
        df_rs_k100["Talent"] = pd.to_numeric(df_rs_k100["ticker"].astype(str).map(talent_map), errors="coerce")
        df_rs_k100["tv_rank"] = pd.to_numeric(df_rs_k100["ticker"].map(tv_rank_map), errors="coerce")
    if not df_rs_q100.empty:
        df_rs_q100["Talent"] = pd.to_numeric(df_rs_q100["ticker"].astype(str).map(talent_map), errors="coerce")
        df_rs_q100["tv_rank"] = pd.to_numeric(df_rs_q100["ticker"].map(tv_rank_map), errors="coerce")

    df_rs_talent_top50 = pd.DataFrame()
    _rs_t_parts: list[pd.DataFrame] = []
    if not df_rs_k100.empty:
        _rs_t_parts.append(df_rs_k100)
    if not df_rs_q100.empty:
        _rs_t_parts.append(df_rs_q100)
    if _rs_t_parts:
        df_rs_talent_top50 = pd.concat(_rs_t_parts, ignore_index=True)
        df_rs_talent_top50["tv_rank_prev"] = pd.to_numeric(
            df_rs_talent_top50["ticker"].astype(str).map(prev_tv_map_rs), errors="coerce"
        )
        df_rs_talent_top50 = (
            df_rs_talent_top50.sort_values("Talent", ascending=False, na_position="last")
            .head(50)
            .reset_index(drop=True)
        )

    _chg_d0_map = _load_ticker_d0_chg_pct_map(engine, _tickers_all)
    df["chg_pct"] = df["ticker"].astype(str).map(_chg_d0_map)

    _tal_all = pd.to_numeric(df.get("Talent"), errors="coerce").to_numpy(dtype=float)
    _tal_all = _tal_all[np.isfinite(_tal_all)]
    talent_mean_all = float(np.mean(_tal_all)) if len(_tal_all) else np.nan
    talent_p95_all = float(np.percentile(_tal_all, 95)) if len(_tal_all) >= 2 else np.nan

    def _fmt_talent_stat(v):
        try:
            if v is None or (isinstance(v, float) and (np.isnan(v) or not np.isfinite(v))):
                return "—"
        except Exception:
            return "—"
        try:
            x = float(v)
        except (TypeError, ValueError):
            return "—"
        return f"{x:.1f}%"

    ref_d = df["date"].iloc[0] if len(df) else ""
    _d0s = _d0.strftime("%Y-%m-%d") if _d0 is not None else "—"
    _d1s = _d1.strftime("%Y-%m-%d") if _d1 is not None else "—"
    _d2s = _d2.strftime("%Y-%m-%d") if _d2 is not None else "—"

    def _fmt_rs(v):
        if pd.isna(v):
            return ""
        try:
            return f"{float(v):.2f}"
        except (TypeError, ValueError):
            return str(v)

    def _fmt_pct(v):
        if v is None or (isinstance(v, float) and (np.isnan(v) or not np.isfinite(v))):
            return ""
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x:.1f}"

    def _energy_td(er) -> str:
        """D-0/D-1/D-2 에너지배율 공통 색: 2 이상 빨강, 1 미만 파랑, 그 외 기본색."""
        if er is None or (isinstance(er, float) and (np.isnan(er) or not np.isfinite(er))):
            return "<td style='text-align:right'></td>"
        try:
            x = float(er)
        except (TypeError, ValueError):
            return "<td style='text-align:right'></td>"
        if not np.isfinite(x):
            return "<td style='text-align:right'></td>"
        txt = f"{x:.2f}"
        if x >= 2.0:
            col = "#c62828"
        elif x < 1.0:
            col = "#1565c0"
        else:
            col = "#212121"
        return f"<td style='text-align:right;color:{col}' data-sort-value=\"{x}\">{txt}</td>"

    def _fmt_theme_cell(th):
        th = (th or "").strip() if pd.notna(th) else ""
        if len(th) > 96:
            return th[:95] + "…"
        return th

    _MCAP_1조 = 1_000_000_000_000.0
    _MCAP_5000억 = 500_000_000_000.0

    def _mcap_row_bg(mcap) -> str:
        """시총(원): 1조 이상 / 5천억~1조 미만 / 5천억 미만 행 배경."""
        if mcap is None:
            return "#ffffff"
        try:
            if isinstance(mcap, float) and pd.isna(mcap):
                return "#ffffff"
            v = float(mcap)
        except (TypeError, ValueError):
            return "#ffffff"
        if not np.isfinite(v) or v <= 0:
            return "#ffffff"
        if v >= _MCAP_1조:
            return "#c8e6c9"
        if v >= _MCAP_5000억:
            return "#bbdefb"
        return "#ffe0b2"

    def _table_rows(sub: pd.DataFrame) -> str:
        if sub.empty:
            return "<p>해당 없음</p>"
        sub = sub.sort_values("_rs_avg", ascending=False, na_position="last")
        rows = []
        for rank, (_, row) in enumerate(sub.iterrows(), start=1):
            nm = row.get("name")
            th = _fmt_theme_cell(row.get("theme_str", ""))
            bg = _mcap_row_bg(row.get("mcap"))
            _tk = str(row.get("ticker", ""))
            _is_hi = _tk in _highlight_set
            _chg = row.get("chg_pct")
            _ticker_inner = html.escape(_tk)
            if _is_hi:
                _ticker_inner = f"<strong>{_ticker_inner}</strong>"
            _ticker_cell = _krx_colored_html(_ticker_inner, _chg)
            _name_raw = "" if pd.isna(nm) else str(nm)
            _name_inner = html.escape(_name_raw)
            if _is_hi:
                _name_inner = f"<strong>{_name_inner}</strong>"
            _name_cell = _krx_colored_html(_name_inner, _chg)
            _tv_rank = row.get("tv_rank")
            _tv_rank_txt = ""
            try:
                if _tv_rank is not None and not (isinstance(_tv_rank, float) and (np.isnan(_tv_rank) or not np.isfinite(_tv_rank))):
                    _tv_rank_txt = f"{int(float(_tv_rank)):,}"
            except Exception:
                _tv_rank_txt = ""
            _tv_pr = row.get("tv_rank_prev")
            _tv_pr_txt = ""
            try:
                if _tv_pr is not None and not (isinstance(_tv_pr, float) and (np.isnan(_tv_pr) or not np.isfinite(_tv_pr))):
                    _tv_pr_txt = f"{int(float(_tv_pr)):,}"
            except Exception:
                _tv_pr_txt = ""
            _rc_txt, _rc_sv, _rc_col = _krx_fmt_rank_change_cell(rank, _tv_pr)
            rows.append(
                f"<tr style=\"background-color:{bg};\">"
                f"<td style='text-align:center'{_html_sort_num_attr(rank)}>{rank}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_tv_pr)}>{_tv_pr_txt}</td>"
                f"<td style='text-align:right;color:{_rc_col}'{_html_sort_num_attr(_rc_sv)}>{html.escape(_rc_txt)}</td>"
                f"<td>{_ticker_cell}</td>"
                f"<td>{_name_cell}</td>"
                f"<td>{html.escape(th)}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('Talent'))}>{_fmt_pct(row.get('Talent'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_10d'))}>{_fmt_rs(row['rs_10d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_20d'))}>{_fmt_rs(row['rs_20d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_50d'))}>{_fmt_rs(row['rs_50d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_120d'))}>{_fmt_rs(row['rs_120d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_tv_rank)}>{_tv_rank_txt}</td>"
                f"{_energy_td(row.get('energy_ratio_d0'))}"
                f"{_energy_td(row.get('energy_ratio_d1'))}"
                f"{_energy_td(row.get('energy_ratio_d2'))}"
                f"<td style='text-align:center'>{'' if pd.isna(row.get('신고가여부')) else row.get('신고가여부', '')}</td>"
                "</tr>"
            )
        return (
            "<table class='krx-sortable' border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:12px;width:100%;'>"
            "<thead><tr>"
            "<th>순위</th><th>전일 순위</th><th>순위 변동</th><th>종목코드</th><th>종목명</th><th>테마</th>"
            "<th>Talent</th>"
            "<th>RS10d</th><th>RS20d</th><th>RS50d</th><th>RS120d</th>"
            "<th>거래대금 순위</th>"
            "<th>D-0 에너지배율</th><th>D-1 에너지배율</th><th>D-2 에너지배율</th>"
            "<th>신고가여부</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

    def _table_rows_rs_merged_talent_top50(sub: pd.DataFrame) -> str:
        """코스피·코스닥 RS 시장순위 각 상위 100(최대 200) 합산 후 Talent(%) 내림차순 상위 50."""
        if sub is None or sub.empty:
            return "<p>해당 없음</p>"
        rows: list[str] = []
        for rank, (_, row) in enumerate(sub.iterrows(), start=1):
            nm = row.get("name")
            th = _fmt_theme_cell(row.get("theme_str", ""))
            bg = _mcap_row_bg(row.get("mcap"))
            _tk = str(row.get("ticker", ""))
            _is_hi = _tk in _highlight_set
            _chg = _chg_d0_map.get(_tk)
            _ticker_inner = html.escape(_tk)
            if _is_hi:
                _ticker_inner = f"<strong>{_ticker_inner}</strong>"
            _ticker_cell = _krx_colored_html(_ticker_inner, _chg)
            _name_raw = "" if pd.isna(nm) else str(nm)
            _name_inner = html.escape(_name_raw)
            if _is_hi:
                _name_inner = f"<strong>{_name_inner}</strong>"
            _name_cell = _krx_colored_html(_name_inner, _chg)
            _rsmk = row.get("rs_mkt_rank")
            _rsmk_txt = ""
            try:
                if _rsmk is not None and not (isinstance(_rsmk, float) and (np.isnan(_rsmk) or not np.isfinite(_rsmk))):
                    _rsmk_txt = str(int(float(_rsmk)))
            except Exception:
                _rsmk_txt = ""
            _tv_rank = row.get("tv_rank")
            _tv_rank_txt = ""
            try:
                if _tv_rank is not None and not (
                    isinstance(_tv_rank, float) and (np.isnan(_tv_rank) or not np.isfinite(_tv_rank))
                ):
                    _tv_rank_txt = f"{int(float(_tv_rank)):,}"
            except Exception:
                _tv_rank_txt = ""
            _tv_pr = row.get("tv_rank_prev")
            _tv_pr_txt = ""
            try:
                if _tv_pr is not None and not (
                    isinstance(_tv_pr, float) and (np.isnan(_tv_pr) or not np.isfinite(_tv_pr))
                ):
                    _tv_pr_txt = f"{int(float(_tv_pr)):,}"
            except Exception:
                _tv_pr_txt = ""
            _rc_txt, _rc_sv, _rc_col = _krx_fmt_rank_change_cell(rank, _tv_pr)
            mkt_disp = str(row.get("market_type", "") or "").strip().upper()
            rows.append(
                f"<tr style=\"background-color:{bg};\">"
                f"<td style='text-align:center'{_html_sort_num_attr(rank)}>{rank}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_tv_pr)}>{html.escape(_tv_pr_txt)}</td>"
                f"<td style='text-align:right;color:{_rc_col}'{_html_sort_num_attr(_rc_sv)}>{html.escape(_rc_txt)}</td>"
                f"<td style='text-align:center'>{html.escape(mkt_disp)}</td>"
                f"<td>{_ticker_cell}</td>"
                f"<td>{_name_cell}</td>"
                f"<td>{html.escape(th)}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_rsmk)}>{html.escape(_rsmk_txt)}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_10d'))}>{_fmt_rs(row['rs_10d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_20d'))}>{_fmt_rs(row['rs_20d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_50d'))}>{_fmt_rs(row['rs_50d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('rs_120d'))}>{_fmt_rs(row['rs_120d'])}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(row.get('Talent'))}>{_fmt_pct(row.get('Talent'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_tv_rank)}>{_tv_rank_txt}</td>"
                f"<td style='text-align:center'>{'' if pd.isna(row.get('신고가여부')) else row.get('신고가여부', '')}</td>"
                "</tr>"
            )
        return (
            "<table class='krx-sortable' border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:12px;width:100%;'>"
            "<thead><tr>"
            "<th>순위</th><th>전일 순위</th><th>순위 변동</th><th>시장</th><th>종목코드</th><th>종목명</th><th>테마</th>"
            "<th>RS시장순위</th>"
            "<th>RS10d</th><th>RS20d</th><th>RS50d</th><th>RS120d</th>"
            "<th>Talent(%)</th><th>거래대금 순위</th><th>신고가여부</th>"
            "</tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

    def _rs_top_theme_terms_html(sub: pd.DataFrame, market_label: str, top_n: int = 22) -> str:
        """표 위 요약: 테마 문자열( · 구분)에서 등장 빈도 상위 테마명."""
        if sub.empty or "theme_str" not in sub.columns:
            return (
                f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                f"(본 표 <code>테마</code> 칼럼 기준): 종목 없음</p>"
            )
        # RS 리스트 표 '위' 요약용 불용어(업종/테마명과 무관한 단어) 제거
        _theme_stopwords_norm = {
            "등",
            "기업가치",
            "제고계획",
            "발표",
            "밸류업",
            "코리아",
            "지수",
            "주요종목",
            "value-up",
            "valueup",
        }

        def _norm_theme_token(x: str) -> str:
            # 괄호/구두점/중복공백 제거 후 비교/출력에 사용
            s0 = (x or "").strip()
            if not s0:
                return ""
            s0 = re.sub(r"[().,]", " ", s0)
            s0 = re.sub(r"\s+", " ", s0).strip()
            # '등', '등)' 같은 꼬리표 제거
            s0 = re.sub(r"\s*등\s*$", "", s0).strip()
            return s0

        def _is_stopword_token(x: str) -> bool:
            nx = _norm_theme_token(x)
            if not nx:
                return True
            k = nx.casefold()
            if k in _theme_stopwords_norm:
                return True
            # "발표)", "지수)" 같은 변형/결합 형태도 제외
            for sw in _theme_stopwords_norm:
                if not sw:
                    continue
                if sw in k:
                    return True
            return False

        cnt: Counter[str] = Counter()
        for raw in sub["theme_str"].astype(str):
            s = raw.strip()
            if not s:
                continue
            seen_row: set[str] = set()
            for part in re.split(r"\s*·\s*", s):
                t = _norm_theme_token(part)
                if len(t) < 1 or _is_stopword_token(t):
                    continue
                if t not in seen_row:
                    seen_row.add(t)
                    cnt[t] += 1
                for w in re.split(r"\s+", t):
                    w = _norm_theme_token(w)
                    if len(w) < 2 or w == t or _is_stopword_token(w):
                        continue
                    if w not in seen_row:
                        seen_row.add(w)
                        cnt[w] += 1
        if not cnt:
            return (
                f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                f"(본 표 <code>테마</code> 칼럼 기준): (비어 있음)</p>"
            )
        top = cnt.most_common(top_n)
        parts_esc = [f"{html.escape(name)} <span class='tc'>({c})</span>" for name, c in top]
        body = ", ".join(parts_esc)
        return (
            f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
            f"(아래 표 <code>테마</code> 칼럼에서 자주 나온 이름·단어, 괄호는 해당 시장 리스트 내 등장 종목 수):<br/>{body}</p>"
        )

    k = df[df["market_type"] == "KOSPI"].copy().sort_values("_rs_avg", ascending=False, na_position="last")
    qm = df[df["market_type"] == "KOSDAQ"].copy().sort_values("_rs_avg", ascending=False, na_position="last")
    _theme_blurb_k = _rs_top_theme_terms_html(k, "코스피 (KOSPI)")
    _theme_blurb_q = _rs_top_theme_terms_html(qm, "코스닥 (KOSDAQ)")

    # 최근 20거래일 '일별 RS Top20' 표 (KOSPI/KOSDAQ 각각, 전일 포함 종목은 볼드)
    rs20_k_html = ""
    rs20_q_html = ""
    rs20_long = pd.DataFrame()
    try:
        _d20 = pd.read_sql_query(
            "SELECT DISTINCT date FROM krx_relative_strength ORDER BY date DESC LIMIT 20",
            con=engine,
        )
        _dates20 = pd.to_datetime(_d20["date"], errors="coerce").dropna().sort_values().tolist()
        if _dates20:
            _ph = ",".join(["%s"] * len(_dates20))
            q_rs20 = f"""
                SELECT
                    r.ticker,
                    r.date,
                    r.market_type,
                    r.rs_10d,
                    r.rs_20d,
                    r.rs_50d,
                    r.rs_120d,
                    t.종목명 AS name
                FROM krx_relative_strength r
                LEFT JOIN krx_ticker t
                    ON t.종목코드 = r.ticker
                    AND t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
                WHERE r.market_type IN ('KOSPI', 'KOSDAQ')
                  AND r.date IN ({_ph})
            """
            _rs20 = pd.read_sql_query(
                q_rs20,
                con=engine,
                params=tuple([pd.Timestamp(d).strftime("%Y-%m-%d") for d in _dates20]),
            )
            if _rs20 is not None and not _rs20.empty:
                _rs20["ticker"] = _rs20["ticker"].astype(str)
                _rs20["market_type"] = _rs20["market_type"].astype(str).str.upper()
                _rs20["date"] = pd.to_datetime(_rs20["date"], errors="coerce")
                _rs20 = _rs20.dropna(subset=["date"]).copy()
                for c in ("rs_10d", "rs_20d", "rs_50d", "rs_120d"):
                    _rs20[c] = pd.to_numeric(_rs20.get(c), errors="coerce")
                _rs20["_rs_avg"] = _rs20[["rs_10d", "rs_20d", "rs_50d", "rs_120d"]].mean(axis=1, skipna=True)
                _rs20["name"] = _rs20.get("name", "").fillna("").astype(str)

                _rs20_chg_map: dict[tuple[str, str], float] = {}
                try:
                    _rs20_tks = _rs20["ticker"].astype(str).unique().tolist()
                    _d0_rs20 = min(_dates20)
                    _dprev = (
                        pd.read_sql_query(
                            "SELECT MAX(date) AS d FROM krx_ohlcv WHERE date < %s",
                            con=engine,
                            params=(_d0_rs20.strftime("%Y-%m-%d"),),
                        )
                    )
                    _dprev_ts = pd.to_datetime(_dprev.iloc[0]["d"], errors="coerce") if len(_dprev) else pd.NaT
                    _date_list = list(_dates20)
                    if pd.notna(_dprev_ts):
                        _date_list = [pd.Timestamp(_dprev_ts)] + _date_list
                    _phd = ",".join(["%s"] * len(_date_list))
                    _chunk_rs = 400
                    _oh_parts: list[pd.DataFrame] = []
                    for _i in range(0, len(_rs20_tks), _chunk_rs):
                        _ct = _rs20_tks[_i : _i + _chunk_rs]
                        _pht = ",".join(["%s"] * len(_ct))
                        _qoh = f"""
                            SELECT ticker, date, close
                            FROM krx_ohlcv
                            WHERE date IN ({_phd}) AND ticker IN ({_pht})
                        """
                        _bind = tuple([pd.Timestamp(d).strftime("%Y-%m-%d") for d in _date_list] + _ct)
                        _oh_parts.append(pd.read_sql_query(_qoh, con=engine, params=_bind))
                    if _oh_parts:
                        _rs20_chg_map = _build_ticker_date_chg_map(pd.concat(_oh_parts, ignore_index=True))
                except Exception:
                    _rs20_chg_map = {}

                def _build_rs20_table(mkt: str) -> str:
                    cols = [f"Top{i}" for i in range(1, 21)]
                    rows_20: list[dict[str, str]] = []
                    prev_set: set[str] = set()
                    for d in sorted(_dates20):
                        dkey = pd.Timestamp(d).strftime("%Y-%m-%d")
                        dd = _rs20[(_rs20["market_type"] == mkt) & (_rs20["date"] == pd.Timestamp(d))].copy()
                        row = {"date": dkey}
                        if dd.empty:
                            for c in cols:
                                row[c] = ""
                            rows_20.append(row)
                            prev_set = set()
                            continue
                        dd = dd.sort_values("_rs_avg", ascending=False, na_position="last").head(20).reset_index(drop=True)
                        day_set = set(dd["ticker"].astype(str).tolist())
                        for i in range(20):
                            if i >= len(dd):
                                row[f"Top{i+1}"] = ""
                                continue
                            tk = str(dd.loc[i, "ticker"])
                            nm = str(dd.loc[i, "name"]).strip()
                            r10 = dd.loc[i, "rs_10d"]
                            ravg = dd.loc[i, "_rs_avg"]
                            label = f"{nm}({tk})" if nm else tk
                            cell_main = html.escape(label)
                            if tk in prev_set:
                                cell_main = f"<b>{cell_main}</b>"
                            cell_main = _krx_colored_html(cell_main, _rs20_chg_map.get((tk, dkey)))
                            tail_parts = []
                            try:
                                if r10 is not None and np.isfinite(float(r10)):
                                    tail_parts.append(f"RS10 {float(r10):.1f}")
                            except Exception:
                                pass
                            try:
                                if ravg is not None and np.isfinite(float(ravg)):
                                    tail_parts.append(f"AVG {float(ravg):.1f}")
                            except Exception:
                                pass
                            tail = " · ".join(tail_parts)
                            row[f"Top{i+1}"] = cell_main + (f"<br/><span class='tv'>{html.escape(tail)}</span>" if tail else "")
                        rows_20.append(row)
                        prev_set = day_set

                    df20 = pd.DataFrame(rows_20).set_index("date")
                    return df20.to_html(escape=False, index=True, border=0, classes="rs20 krx-sortable")

                rs20_k_html = _build_rs20_table("KOSPI")
                rs20_q_html = _build_rs20_table("KOSDAQ")
                rs20_long = pd.concat(
                    [
                        _build_rs20_long_df(_rs20, _dates20, "KOSPI"),
                        _build_rs20_long_df(_rs20, _dates20, "KOSDAQ"),
                    ],
                    ignore_index=True,
                )
    except Exception:
        rs20_k_html = ""
        rs20_q_html = ""

    rs_html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>RS 리스트 (RS10d&gt;=90, RS4구간 평균 정렬)</title>
  <style>
    body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; color: #111; background: #fafafa; }}
    h1 {{ padding: 16px 20px; margin: 0; font-size: 1.25rem; background: #fff; border-bottom: 1px solid #e0e0e0; }}
    .note {{ padding: 10px 20px; font-size: 13px; color: #444; background: #fff; border-bottom: 1px solid #eee; line-height: 1.5; }}
    section {{ padding: 16px 20px 24px; }}
    section h2 {{ font-size: 1.05rem; margin: 0 0 10px; }}
    .theme-summary {{ font-size: 12px; color: #333; margin: 0 0 12px 0; line-height: 1.55; max-width: 100%; }}
    .theme-summary .tc {{ color: #666; font-weight: 600; }}
    table.rs20 {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e6e6e6; }}
    table.rs20 thead th {{ position: sticky; top: 0; background: #fafafa; z-index: 1; }}
    table.rs20 th, table.rs20 td {{ border: 1px solid #eee; padding: 8px 8px; font-size: 12px; vertical-align: top; }}
    table.rs20 th {{ text-align: center; font-weight: 700; color: #222; }}
    table.rs20 td {{ min-width: 110px; }}
    table.rs20 td .tv {{ color: #666; font-size: 11px; }}
    .rs20-wrap {{ overflow: auto; max-height: 520px; border-radius: 8px; border: 1px solid #eee; background: #fff; }}
  </style>
</head>
<body>
  <h1>RS 고분위 리스트</h1>
  <div class="note">
    기준일: <strong>{ref_d}</strong> (<code>krx_relative_strength</code> 최신 <code>date</code>).<br/>
    조건: <code>rs_10d</code> &gt;= <strong>90</strong> (백분위). 순위: 시장별 <strong>RS10·20·50·120d 산술평균 내림차순</strong>(평균 컬럼 미표시). RS200d는 미사용.<br/>
    테마는 <code>krx_theme_stock</code> 기준입니다.<br/>
    Talent(%) = 최근 120거래일 중 (종가 ≥ 시가×1.10) 비중이며, 본 리스트 전체 평균 {_fmt_talent_stat(talent_mean_all)} / 상위5% {_fmt_talent_stat(talent_p95_all)} 입니다.<br/>
    D-0 / D-1 / D-2 에너지배율: 각 거래일별로 (해당일 거래대금 ÷ 해당 시장 그날 거래대금 합 ×100) ÷ (시총 ÷ 해당 시장 시총 합 ×100).
    거래일은 <code>krx_ohlcv</code> 기준 <strong>D-0={_d0s}</strong>, <strong>D-1={_d1s}</strong>, <strong>D-2={_d2s}</strong>, 시총은 <code>krx_ticker</code> 최신 기준일입니다.<br/>
    <strong>신고가여부</strong>: 당일(D-0) 종가가 <em>D-3 거래일 말</em>까지의 250거래일 최고가(고가)보다 크면 <strong>O</strong>, 같거나 작으면 <strong>X</strong>(비교에 필요한 OHLCV 봉 수 부족 시 빈칸).<br/>
    행 배경(시가총액, 원): <span style="background:#c8e6c9;padding:0 6px">1조 이상</span>,
    <span style="background:#bbdefb;padding:0 6px">5천억 이상 1조 미만</span>,
    <span style="background:#ffe0b2;padding:0 6px">5천억 미만</span>, 결측·0은 흰색.<br/>
    아래 절 번호·순서는 <code>market_judgment.html</code>(시장 판단)과 동일하게 맞추었습니다. 코스피·코스닥은 좌우 2열이 아니라 위에서 아래 순서입니다.<br/>
    파일: {os.path.basename(out_path)}<br/>
    <strong>표 정렬</strong>: 각 표의 칼럼 헤더를 클릭하면 해당 열 기준으로 오름차순·내림차순이 번갈아 적용됩니다.<br/>
    <strong>2절 요약표</strong>: 코스피·코스닥 각 RS 시장순위 상위 100(최대 200종)을 합친 뒤 Talent(%)가 높은 순으로 상위 50만 표시합니다. 전일 순위는 직전 거래일 해당 시장 내 거래대금 순위입니다.<br/>
  </div>
  <section>
    <h2>1. ATR14/종가 vs 시가총액 (분포)</h2>
    <p style="margin:0;font-size:13px;color:#555;line-height:1.55;">
      RS 전용 페이지에는 변동성 산점도를 넣지 않습니다. 동일 출력 폴더의
      <a href="market_judgment.html"><code>market_judgment.html</code></a> 1절을 참고하세요.
    </p>
  </section>
  <section>
    <h2>2. 코스피·코스닥 RS 시장순위 상위 100 합산 (Talent 높은 순, 상위 50)</h2>
    <p style="margin:0 0 10px 0;font-size:12px;color:#555;line-height:1.55;">
      코스피·코스닥 전체 종목 각각에서 RS10·20·50·120d 산술평균의 시장 내 순위 1~100위에 드는 종목만 모은 유니버스(최대 200종)를 Talent(%) 내림차순으로 정렬해 상위 50만 표시합니다. 거래대금 순위·전일 순위는 <code>market_judgment_tv_rank.csv</code> 및 직전 거래일 OHLCV 기준입니다.
    </p>
    {_table_rows_rs_merged_talent_top50(df_rs_talent_top50)}
  </section>
  <section>
    <h2>3. 코스피 — RS 고분위 리스트 (rs_10d≥90, {len(k)}종목)</h2>
    {_theme_blurb_k}
    {_table_rows(k)}
  </section>
  <section>
    <h2>4. 코스피 — 최근 20거래일 일별 RS Top20</h2>
    <div class="note" style="margin: 0 0 10px 0;">
      해당 시장 전체 유니버스에서 일별 RS10·20·50·120d 산술평균 상위 20입니다. 각 칸은 <code>종목명(티커)</code>와 RS10·AVG 요약입니다.<br/>
      <strong>볼드</strong>: 전일 Top20에 있던 종목이 당일에도 포함된 경우(시장별 표에만 적용).
    </div>
    <div class="rs20-wrap">
      {rs20_k_html if rs20_k_html else "<p style='margin:0;color:#666;font-size:12px;'>표를 만들 데이터가 부족합니다.</p>"}
    </div>
  </section>
  <section>
    <h2>5. 코스닥 — RS 고분위 리스트 (rs_10d≥90, {len(qm)}종목)</h2>
    {_theme_blurb_q}
    {_table_rows(qm)}
  </section>
  <section>
    <h2>6. 코스닥 — 최근 20거래일 일별 RS Top20</h2>
    <div class="note" style="margin: 0 0 10px 0;">
      규칙은 위 코스피(4절)와 동일합니다.
    </div>
    <div class="rs20-wrap">
      {rs20_q_html if rs20_q_html else "<p style='margin:0;color:#666;font-size:12px;'>표를 만들 데이터가 부족합니다.</p>"}
    </div>
  </section>
{KRX_SORTABLE_TABLE_CSS_JS}
</body>
</html>"""

    try:
        _rd_rs = _rs_snapshot_ref_trade_date(engine, df)
        _save_krx_analysis_table(engine, "krx_analysis_rs_high_list", _rs_high_list_df_for_db(df), _rd_rs)
        _save_krx_analysis_table(engine, "krx_analysis_rs_daily_top20", rs20_long, _rd_rs)
    except Exception as _e_rsdb:
        print(f"경고: RS 분석 DB 저장 실패 ({type(_e_rsdb).__name__}: {_e_rsdb})")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(rs_html_doc)

    if not quiet:
        try:
            import webbrowser

            webbrowser.open(out_path)
        except Exception:
            pass

        print(f"완료: RS 고분위 리스트 HTML 저장: {out_path} (총 {len(df)}건, rs_10d>=90, RS10·20·50·120 평균 순)")
    return out_path, set(df["ticker"].astype(str).tolist())


def _weekly_close_high_breakout_ox(g: pd.DataFrame) -> tuple[str, str, str]:
    """
    일봉 OHLCV로부터 주봉(주간 종가 = 해당 주 마지막 거래일 종가, `W-FRI` 리샘플) 기준 신고가 여부.

    각 N주: **최신 주봉 종가**가 **직전 N개 주봉 종가**의 최고값을 **초과(>)** 하면 ``O``, 아니면 ``X``.
    주봉 봉 수가 부족하면 ``''`` (일간 120일·250일 신고가 판정과 동일하게 엄격 비교).
    """
    if g is None or g.empty or "date" not in g.columns or "close" not in g.columns:
        return ("", "", "")
    g = g.sort_values("date").copy()
    g["date"] = pd.to_datetime(g["date"], errors="coerce")
    g = g.dropna(subset=["date"])
    g["close"] = pd.to_numeric(g["close"], errors="coerce")
    if g.empty:
        return ("", "", "")
    idx = pd.DatetimeIndex(g["date"])
    g2 = g.set_index(idx).sort_index()
    w = g2.resample("W-FRI").agg({"close": "last"})
    w = w.dropna(subset=["close"])
    if w.empty:
        return ("", "", "")
    cw = w["close"].to_numpy(dtype=float)

    def ox(nweeks: int) -> str:
        if cw.size < nweeks + 1:
            return ""
        br = float(cw[-1])
        ref = np.nanmax(cw[-(nweeks + 1) : -1])
        if not np.isfinite(ref) or not np.isfinite(br):
            return ""
        return "O" if br > ref else "X"

    return (ox(10), ox(20), ox(50))


def write_120d_breakout_list_html(
    engine,
    output_base_dir: str | None = None,
    highlight_tickers: set[str] | None = None,
    quiet: bool = False,
) -> tuple[str | None, set[str]]:
    """
    최근 5거래일 동안 '6거래일 전 시점의 120일 최고 종가'를 상향 돌파(신고가)한 종목 리스트 HTML 생성.

    - 코스피/코스닥 구분: `krx_ticker_sector.sector_cd` (1001/2001)
    - 컬럼: 순번, 종목코드, 종목명, 테마, 이전 신고가 경과일수, 이전 신저가 경과일수, 최저가대비 상승률
    """
    base = output_base_dir or os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
    out_dir = os.path.join(base, date.today().strftime("%Y-%m-%d"))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "breakout_120d_high_list.html")

    try:
        ref_m = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
        ref_d = pd.to_datetime(ref_m.iloc[0]["d"], errors="coerce")
    except Exception as e:
        print(f"실패: 신고가 리스트 기준일 조회 ({type(e).__name__}: {e})")
        return None, set()
    if pd.isna(ref_d):
        print("실패: 신고가 리스트 기준일(ref_d) 파싱 실패")
        return None, set()

    bo_ref_trade_date = pd.Timestamp(ref_d).normalize().date()

    # 250일 신고가 판정까지 커버하려면 최소 250+6=256 거래일 봉이 필요.
    # 휴일/비거래일을 고려해 달력일로 넉넉히 로드한다.
    cutoff = (ref_d.normalize() - pd.Timedelta(days=900)).strftime("%Y-%m-%d")

    try:
        kospi_list = pd.read_sql_query(
            "SELECT ticker FROM krx_ticker_sector WHERE sector_cd = '1001';",
            con=engine,
        )["ticker"].astype(str).tolist()
        kosdaq_list = pd.read_sql_query(
            "SELECT ticker FROM krx_ticker_sector WHERE sector_cd = '2001';",
            con=engine,
        )["ticker"].astype(str).tolist()
    except Exception as e:
        print(f"실패: 신고가 리스트 시장 티커 조회 ({type(e).__name__}: {e})")
        return None, set()

    universe = sorted(set(kospi_list) | set(kosdaq_list))
    if not universe:
        print("실패: 신고가 리스트 유니버스가 비었습니다.")
        return None, set()

    # OHLCV 로드 (chunked IN)
    parts: list[pd.DataFrame] = []
    chunk_size = 450
    for i in range(0, len(universe), chunk_size):
        chunk = universe[i : i + chunk_size]
        ph = ",".join(["%s"] * len(chunk))
        q = f"""
            SELECT ticker, date, high, low, close, volume
            FROM krx_ohlcv
            WHERE date >= %s AND ticker IN ({ph})
        """
        bind = tuple([cutoff] + [str(x) for x in chunk])
        try:
            parts.append(pd.read_sql_query(q, con=engine, params=bind))
        except Exception:
            continue

    if not parts:
        print("실패: 신고가 리스트 OHLCV 로드 결과가 없습니다.")
        return None, set()

    ohlcv = pd.concat(parts, ignore_index=True)
    if ohlcv.empty:
        print("실패: 신고가 리스트 OHLCV 데이터가 비었습니다.")
        return None, set()

    ohlcv["ticker"] = ohlcv["ticker"].astype(str)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"], errors="coerce")
    ohlcv = ohlcv.dropna(subset=["date"]).sort_values(["ticker", "date"]).reset_index(drop=True)
    for c in ("high", "low", "close", "volume"):
        ohlcv[c] = pd.to_numeric(ohlcv[c], errors="coerce")

    # 최신일 기준 현재가/거래대금 순위는 '시장 판단' 산출물을 우선 사용
    output_base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
    _rank_dir = os.path.join(output_base, date.today().strftime("%Y-%m-%d"))
    rank_path = os.path.join(_rank_dir, "market_judgment_tv_rank.csv")
    rank_map: dict[str, float] = {}
    cur_map: dict[str, float] = {}
    tv_map: dict[str, float] = {}
    try:
        rk = pd.read_csv(rank_path, dtype={"ticker": str, "sector_cd": str})
        rk["ticker"] = rk["ticker"].astype(str)
        rank_map = {str(r["ticker"]): float(pd.to_numeric(r["tv_rank"], errors="coerce")) for _, r in rk.iterrows()}
        cur_map = {str(r["ticker"]): float(pd.to_numeric(r["current_price"], errors="coerce")) for _, r in rk.iterrows()}
        tv_map = {str(r["ticker"]): float(pd.to_numeric(r["trade_value"], errors="coerce")) for _, r in rk.iterrows()}
    except Exception:
        # fallback: 이 함수 내에서 최신일 거래대금/현재가 계산(동일 정의)
        last_rows = ohlcv.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).tail(1).copy()
        last_rows["ticker"] = last_rows["ticker"].astype(str)
        last_rows["last_close"] = pd.to_numeric(last_rows["close"], errors="coerce")
        last_rows["last_tv"] = pd.to_numeric(last_rows["close"], errors="coerce") * pd.to_numeric(last_rows["volume"], errors="coerce")
        cur_map = {str(r["ticker"]): float(r["last_close"]) if pd.notna(r["last_close"]) else np.nan for _, r in last_rows.iterrows()}
        tv_map = {str(r["ticker"]): float(r["last_tv"]) if pd.notna(r["last_tv"]) else np.nan for _, r in last_rows.iterrows()}

    # 종목명
    name_map: dict[str, str] = {}
    mcap_map: dict[str, float] = {}
    try:
        tl = pd.read_sql_query(
            """
            SELECT 종목코드 AS ticker, 종목명 AS name, 시가총액 AS mcap
            FROM krx_ticker
            WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
              AND 종목구분 = '보통주'
            """,
            con=engine,
        )
        name_map = {str(r["ticker"]): str(r["name"]) for _, r in tl.iterrows()}
        mcap_map = {str(r["ticker"]): float(pd.to_numeric(r["mcap"], errors="coerce")) for _, r in tl.iterrows()}
    except Exception:
        name_map = {}
        mcap_map = {}

    # 테마
    theme_map: dict[str, str] = {}
    try:
        th_df = pd.read_sql_query(
            """
            SELECT ticker,
                   GROUP_CONCAT(DISTINCT theme_name ORDER BY theme_name SEPARATOR ' · ') AS theme_str
            FROM krx_theme_stock
            GROUP BY ticker
            """,
            con=engine,
        )
        th_df["ticker"] = th_df["ticker"].astype(str)
        theme_map = {str(r["ticker"]): str(r["theme_str"] or "") for _, r in th_df.iterrows()}
    except Exception:
        theme_map = {}

    kospi_set = set(kospi_list)
    kosdaq_set = set(kosdaq_list)

    def _calc_one(g: pd.DataFrame) -> dict | None:
        g = g.sort_values("date").reset_index(drop=True)
        if len(g) < 140:  # 120 + 6 + 약간의 여유
            return None

        close = g["close"].to_numpy(dtype=float)
        high = g["high"].to_numpy(dtype=float)
        low = g["low"].to_numpy(dtype=float)
        vol = g["volume"].to_numpy(dtype=float) if "volume" in g.columns else np.full(len(g), np.nan, dtype=float)
        dates = pd.to_datetime(g["date"]).to_numpy()

        # 조건(변경): 당일(D-0) 종가가 전일(D-1) 기준 120일 최고 종가를 상향 돌파(>)하면 포함
        # ref_end = D-1
        if len(g) < 121:
            return None
        ref_end = len(g) - 2
        if ref_end < 119:
            return None
        ref_win_start = ref_end - 119
        ref_high = np.nanmax(close[ref_win_start : ref_end + 1])
        if not np.isfinite(ref_high):
            return None

        # ref window에서 마지막 최고가 도달 일자
        ref_slice = close[ref_win_start : ref_end + 1]
        hit = np.where(np.isfinite(ref_slice) & (ref_slice >= ref_high))[0]
        if len(hit) == 0:
            return None
        prev_high_idx = ref_win_start + int(hit[-1])

        # 돌파일은 당일(D-0)로 고정
        breakout_idx = len(g) - 1
        br_close = close[breakout_idx]
        if not (np.isfinite(br_close) and br_close > ref_high):
            return None

        # 이전 신고가 경과일수(거래일 기준): 전일(D-1) 기준 120일 최고 종가(도달일, 가장 최근 도달)로부터 당일(D-0)까지
        elapsed_high_td = breakout_idx - prev_high_idx
        if elapsed_high_td < 0:
            elapsed_high_td = 0

        # (요청 반영) 120일 신저가 대비 상승률 및 경과일수:
        # - 기준 최저가: 당일(D-0) 포함 최근 120거래일 '종가' 최저값
        # - 경과일수: 최저 종가 도달일(가장 최근 도달) ~ 당일(D-0)까지 거래일 간격
        low_win_start = max(0, breakout_idx - 119)
        low_slice = close[low_win_start : breakout_idx + 1]
        low120 = np.nanmin(low_slice) if low_slice.size else np.nan
        low120_idx = None
        if np.isfinite(low120):
            hit_low = np.where(np.isfinite(low_slice) & (low_slice <= low120))[0]
            if len(hit_low) > 0:
                low120_idx = low_win_start + int(hit_low[-1])

        elapsed_td = 0
        if low120_idx is not None:
            elapsed_td = breakout_idx - int(low120_idx)
            if elapsed_td < 0:
                elapsed_td = 0

        if np.isfinite(low120) and low120 > 0 and np.isfinite(br_close):
            up_pct = (br_close / low120 - 1.0) * 100.0
        else:
            up_pct = np.nan

        tv = np.nan
        if breakout_idx < len(vol) and np.isfinite(br_close) and np.isfinite(vol[breakout_idx]):
            tv = float(br_close) * float(vol[breakout_idx])

        chg1 = np.nan
        if breakout_idx >= 1 and np.isfinite(br_close) and np.isfinite(close[breakout_idx - 1]) and close[breakout_idx - 1] != 0:
            chg1 = (float(br_close) / float(close[breakout_idx - 1]) - 1.0) * 100.0

        ret5 = np.nan
        if breakout_idx >= 5 and np.isfinite(br_close) and np.isfinite(close[breakout_idx - 5]) and close[breakout_idx - 5] != 0:
            ret5 = (float(br_close) / float(close[breakout_idx - 5]) - 1.0) * 100.0

        # 250일 신고가 달성 여부(요청 반영):
        # 당일(D-0) 종가가 전일(D-1) 기준 250일 최고 종가를 상향 돌파(>)하면 O
        high250_ox = ""
        try:
            if len(g) >= 251:
                ref_end_250 = len(g) - 2  # D-1
                if ref_end_250 >= 249:
                    ref_high250 = np.nanmax(close[ref_end_250 - 249 : ref_end_250 + 1])
                    if np.isfinite(ref_high250) and np.isfinite(br_close):
                        high250_ox = "O" if float(br_close) > float(ref_high250) else "X"
        except Exception:
            high250_ox = ""

        return {
            "ticker": str(g.loc[0, "ticker"]),
            "breakout_date": pd.Timestamp(dates[breakout_idx]).strftime("%Y-%m-%d"),
            "elapsed_high_td": int(elapsed_high_td),
            "elapsed_td": int(elapsed_td),
            "up_from_low_pct": float(up_pct) if np.isfinite(up_pct) else np.nan,
            "trade_value": float(tv) if np.isfinite(tv) else np.nan,
            "chg_1d_pct": float(chg1) if np.isfinite(chg1) else np.nan,
            "ret_5d_pct": float(ret5) if np.isfinite(ret5) else np.nan,
            "is_250d_high": high250_ox,
        }

    rows = []
    for t, g in ohlcv.groupby("ticker", sort=False):
        r = _calc_one(g)
        if r is not None:
            rows.append(r)

    if not rows:
        html_doc = f"""<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"/><title>120일 신고가 달성 리스트</title></head>
<body>
  <p>기준일(OHLCV 최신): <strong>{ref_d.strftime('%Y-%m-%d')}</strong></p>
  <p>최근 5거래일 동안 (6거래일 전 시점의 120일 최고 종가) 상향 돌파 종목이 없습니다.</p>
</body>
</html>"""
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_doc)
        if not quiet:
            print(f"완료: 120일 신고가 리스트 HTML 저장(0건): {out_path}")
        try:
            _save_krx_analysis_table(
                engine, "krx_analysis_breakout_120d", pd.DataFrame(), bo_ref_trade_date
            )
        except Exception as e:
            if not quiet:
                print(f"경고: 신고가 리스트 DB 저장 실패 ({type(e).__name__}: {e})")
        return out_path, set()

    out_df = pd.DataFrame(rows)
    # RS 순위(시장별 RS4구간 평균 내림차순, 1=최상위)
    rs_rank_map = _load_latest_rs_rank_map(engine)
    out_df["rs_rank"] = out_df["ticker"].astype(str).map(rs_rank_map)
    out_df["name"] = out_df["ticker"].map(name_map).fillna("")
    out_df["theme_str"] = out_df["ticker"].map(theme_map).fillna("")
    out_df["market"] = np.where(out_df["ticker"].isin(kospi_set), "KOSPI", "KOSDAQ")
    out_df["mcap"] = pd.to_numeric(out_df["ticker"].map(mcap_map), errors="coerce")
    out_df["current_price"] = pd.to_numeric(out_df["ticker"].map(cur_map), errors="coerce")
    out_df["last_trade_value"] = pd.to_numeric(out_df["ticker"].map(tv_map), errors="coerce")
    out_df["tv_rank"] = pd.to_numeric(out_df["ticker"].map(rank_map), errors="coerce")
    if out_df["tv_rank"].isna().all():
        # rank 파일이 없거나 비정상인 경우: 이 함수에서 동일 정의로 순위 산출
        out_df["tv_rank"] = np.nan
        for _m in ("KOSPI", "KOSDAQ"):
            _mask = out_df["market"] == _m
            if not _mask.any():
                continue
            _r = out_df.loc[_mask, "last_trade_value"].rank(ascending=False, method="min")
            out_df.loc[_mask, "tv_rank"] = _r

    # 표 정렬: 시장별 거래대금 순위(1등이 최상단)
    out_df = out_df.sort_values(["market", "tv_rank"], ascending=[True, True], na_position="last").reset_index(drop=True)

    # 주봉(금요일 주간) 종가 기준 10·20·50주 신고가 여부 — 본 리스트 종목만 산출
    _wk_need = set(out_df["ticker"].astype(str).tolist())
    _wk_flags: dict[str, tuple[str, str, str]] = {}
    for _tk, _g in ohlcv.groupby("ticker", sort=False):
        _tks = str(_tk)
        if _tks not in _wk_need:
            continue
        _wk_flags[_tks] = _weekly_close_high_breakout_ox(_g)
    _wk_rows = [_wk_flags.get(str(t), ("", "", "")) for t in out_df["ticker"].astype(str)]
    out_df["is_10w_high"] = [r[0] for r in _wk_rows]
    out_df["is_20w_high"] = [r[1] for r in _wk_rows]
    out_df["is_50w_high"] = [r[2] for r in _wk_rows]

    def _fmt_pct(x):
        try:
            if x is None or (isinstance(x, float) and (np.isnan(x) or not np.isfinite(x))):
                return ""
            return f"{float(x):.1f}"
        except Exception:
            return ""

    def _fmt_money(x):
        try:
            if x is None or (isinstance(x, float) and (np.isnan(x) or not np.isfinite(x))):
                return ""
            v = float(x)
            if not np.isfinite(v):
                return ""
            # 10억원(=1e9원) 단위 표기
            return f"{v/1_000_000_000.0:,.1f}"
        except Exception:
            return ""

    _MCAP_1조 = 1_000_000_000_000.0
    _MCAP_5000억 = 500_000_000_000.0

    def _mcap_row_bg(mcap) -> str:
        """시총(원): 1조 이상 / 5천억~1조 미만 / 5천억 미만."""
        if mcap is None:
            return "#ffffff"
        try:
            if isinstance(mcap, float) and pd.isna(mcap):
                return "#ffffff"
            v = float(mcap)
        except (TypeError, ValueError):
            return "#ffffff"
        if not np.isfinite(v) or v <= 0:
            return "#ffffff"
        if v >= _MCAP_1조:
            return "#c8e6c9"
        if v >= _MCAP_5000억:
            return "#bbdefb"
        return "#ffe0b2"

    def _table(sub: pd.DataFrame) -> str:
        if sub.empty:
            return "<p>해당 없음</p>"
        lines = [
            "<table class='krx-sortable' border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:12px;width:100%;'>",
            "<thead><tr>",
            "<th>순번</th><th>종목코드</th><th>종목명</th><th>테마</th><th>RS순위</th><th>시가총액(10억원)</th><th>거래대금 순위</th><th>현재가</th><th>당일 상승률(%)</th><th>최근 5거래일 상승률(%)</th><th>이전 신고가 경과일수</th><th>이전 신저가 경과일수</th><th>최저가대비 상승률(%)</th><th>신고가 달성일</th><th>250일 신고가 달성 여부</th>",
            "</tr></thead><tbody>",
        ]
        for i, (_, r) in enumerate(sub.iterrows(), start=1):
            th = (r.get("theme_str") or "").strip()
            if len(th) > 96:
                th = th[:95] + "…"
            bg = _mcap_row_bg(r.get("mcap"))
            _tk = str(r.get("ticker", ""))
            _is_hi = _tk in set([str(x) for x in (highlight_tickers or set())])
            tv_rank = r.get("tv_rank")
            tv_rank_txt = "" if tv_rank is None or (isinstance(tv_rank, float) and (np.isnan(tv_rank) or not np.isfinite(tv_rank))) else f"{int(tv_rank):,}"
            cur = r.get("current_price")
            cur_txt = "" if cur is None or (isinstance(cur, float) and (np.isnan(cur) or not np.isfinite(cur))) else f"{float(cur):,.0f}"
            rsr = r.get("rs_rank")
            rsr_txt = "" if rsr is None or (isinstance(rsr, float) and (np.isnan(rsr) or not np.isfinite(rsr))) else f"{int(float(rsr)):,}"
            _chg = r.get("chg_1d_pct")
            _tk_inner = html.escape(_tk)
            if _is_hi:
                _tk_inner = f"<strong>{_tk_inner}</strong>"
            _tk_cell = _krx_colored_html(_tk_inner, _chg)
            _nm_inner = html.escape(str(r.get("name", "")))
            if _is_hi:
                _nm_inner = f"<strong>{_nm_inner}</strong>"
            _nm_cell = _krx_colored_html(_nm_inner, _chg)
            lines.append(
                f"<tr style=\"background-color:{bg};\">"
                f"<td style='text-align:center'{_html_sort_num_attr(i)}>{i}</td>"
                f"<td style='text-align:center'>{_tk_cell}</td>"
                f"<td>{_nm_cell}</td>"
                f"<td>{html.escape(th)}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(rsr)}>{rsr_txt}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('mcap'))}>{_fmt_money(r.get('mcap'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(tv_rank)}>{tv_rank_txt}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(cur)}>{cur_txt}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('chg_1d_pct'))}>{_fmt_pct(r.get('chg_1d_pct'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('ret_5d_pct'))}>{_fmt_pct(r.get('ret_5d_pct'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('elapsed_high_td'))}>{int(r.get('elapsed_high_td', 0))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('elapsed_td'))}>{int(r.get('elapsed_td', 0))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('up_from_low_pct'))}>{_fmt_pct(r.get('up_from_low_pct'))}</td>"
                f"<td style='text-align:center'>{html.escape(str(r.get('breakout_date','')))}</td>"
                f"<td style='text-align:center'>{html.escape(str(r.get('is_250d_high','')))}</td>"
                "</tr>"
            )
        lines.append("</tbody></table>")
        return "".join(lines)

    def _ox_sort_attr(ox: str) -> str:
        s = (ox or "").strip().upper()
        if s == "O":
            return ' data-sort-value="2"'
        if s == "X":
            return ' data-sort-value="1"'
        return ' data-sort-value="0"'

    def _table_weekly(sub: pd.DataFrame) -> str:
        """위 메인 표와 동일 종목·순서로 주봉 10·20·50주 신고가 O/X만 표시."""
        if sub.empty:
            return "<p>해당 없음</p>"
        lines = [
            "<table class='krx-sortable' border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:12px;width:100%;'>",
            "<thead><tr>",
            "<th>순번</th><th>종목코드</th><th>종목명</th><th>10주 신고가</th><th>20주 신고가</th><th>50주 신고가</th>",
            "</tr></thead><tbody>",
        ]
        for i, (_, r) in enumerate(sub.iterrows(), start=1):
            bg = _mcap_row_bg(r.get("mcap"))
            _tk = str(r.get("ticker", ""))
            _is_hi = _tk in set([str(x) for x in (highlight_tickers or set())])
            _chg = r.get("chg_1d_pct")
            _tk_inner = html.escape(_tk)
            if _is_hi:
                _tk_inner = f"<strong>{_tk_inner}</strong>"
            _tk_cell = _krx_colored_html(_tk_inner, _chg)
            _nm_inner = html.escape(str(r.get("name", "")))
            if _is_hi:
                _nm_inner = f"<strong>{_nm_inner}</strong>"
            _nm_cell = _krx_colored_html(_nm_inner, _chg)
            o10 = str(r.get("is_10w_high", "") or "")
            o20 = str(r.get("is_20w_high", "") or "")
            o50 = str(r.get("is_50w_high", "") or "")
            lines.append(
                f"<tr style=\"background-color:{bg};\">"
                f"<td style='text-align:center'{_html_sort_num_attr(i)}>{i}</td>"
                f"<td style='text-align:center'>{_tk_cell}</td>"
                f"<td>{_nm_cell}</td>"
                f"<td style='text-align:center'{_ox_sort_attr(o10)}>{html.escape(o10)}</td>"
                f"<td style='text-align:center'{_ox_sort_attr(o20)}>{html.escape(o20)}</td>"
                f"<td style='text-align:center'{_ox_sort_attr(o50)}>{html.escape(o50)}</td>"
                "</tr>"
            )
        lines.append("</tbody></table>")
        return "".join(lines)

    def _top_theme_terms_html(sub: pd.DataFrame, market_label: str, top_n: int = 22) -> str:
        """표 위 요약: 테마 문자열( · 구분)에서 등장 빈도 상위 테마명(불용어 제거 포함)."""
        if sub is None or sub.empty or "theme_str" not in sub.columns:
            return (
                f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                f"(본 표 <code>테마</code> 칼럼 기준): 종목 없음</p>"
            )

        _theme_stopwords_norm = {
            "등",
            "기업가치",
            "제고계획",
            "발표",
            "밸류업",
            "코리아",
            "지수",
            "주요종목",
            "value-up",
            "valueup",
        }

        def _norm_theme_token(x: str) -> str:
            s0 = (x or "").strip()
            if not s0:
                return ""
            s0 = re.sub(r"[().,]", " ", s0)
            s0 = re.sub(r"\s+", " ", s0).strip()
            s0 = re.sub(r"\s*등\s*$", "", s0).strip()
            return s0

        def _is_stopword_token(x: str) -> bool:
            nx = _norm_theme_token(x)
            if not nx:
                return True
            k = nx.casefold()
            if k in _theme_stopwords_norm:
                return True
            for sw in _theme_stopwords_norm:
                if sw and sw in k:
                    return True
            return False

        cnt: Counter[str] = Counter()
        for raw in sub["theme_str"].astype(str):
            s = raw.strip()
            if not s:
                continue
            seen_row: set[str] = set()
            for part in re.split(r"\s*·\s*", s):
                t = _norm_theme_token(part)
                if len(t) < 1 or _is_stopword_token(t):
                    continue
                if t not in seen_row:
                    seen_row.add(t)
                    cnt[t] += 1
                for w in re.split(r"\s+", t):
                    w = _norm_theme_token(w)
                    if len(w) < 2 or w == t or _is_stopword_token(w):
                        continue
                    if w not in seen_row:
                        seen_row.add(w)
                        cnt[w] += 1

        if not cnt:
            return (
                f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                f"(본 표 <code>테마</code> 칼럼 기준): (비어 있음)</p>"
            )
        top = cnt.most_common(top_n)
        parts_esc = [f"{html.escape(name)} <span class='tc'>({c})</span>" for name, c in top]
        body = ", ".join(parts_esc)
        return (
            f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
            f"(아래 표 <code>테마</code> 칼럼에서 자주 나온 이름·단어, 괄호는 해당 시장 리스트 내 등장 종목 수):<br/>{body}</p>"
        )

    k = out_df[out_df["market"] == "KOSPI"].copy()
    q = out_df[out_df["market"] == "KOSDAQ"].copy()
    _theme_blurb_k = _top_theme_terms_html(k, "코스피 (KOSPI)")
    _theme_blurb_q = _top_theme_terms_html(q, "코스닥 (KOSDAQ)")

    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>당일 120일 신고가 달성 리스트</title>
  <style>
    body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 16px 20px; color: #111; }}
    h1 {{ font-size: 1.2rem; margin: 0 0 10px 0; }}
    .note {{ color: #444; font-size: 13px; margin: 10px 0 18px; line-height: 1.55; }}
    .tables-2col {{ display: flex; flex-wrap: wrap; gap: 20px; align-items: flex-start; }}
    .tables-2col .col {{ flex: 1 1 520px; min-width: 460px; }}
    h2 {{ font-size: 1.05rem; margin: 0 0 10px 0; }}
    h3 {{ font-size: 0.95rem; margin: 16px 0 8px 0; color: #222; font-weight: 600; }}
    .theme-summary {{ font-size: 12px; color: #333; margin: 0 0 12px 0; line-height: 1.55; max-width: 100%; }}
    .theme-summary .tc {{ color: #666; font-weight: 600; }}
  </style>
</head>
<body>
  <h1>당일 120일 신고가 달성 종목</h1>
  <div class="note">
    기준일(OHLCV 최신): <strong>{ref_d.strftime('%Y-%m-%d')}</strong><br/>
    조건: <strong>당일(D-0) 종가</strong>가 <strong>전일(D-1) 기준 120일 최고 종가</strong>를 상향 돌파(&gt;)한 종목.<br/>
    이전 신고가 경과일수: 전일(D-1) 기준 120일 최고 종가(도달일, 가장 최근 도달)로부터 당일(D-0)까지의 <strong>거래일 간격</strong>.<br/>
    경과일수: <strong>당일(D-0) 포함 최근 120거래일 최저 종가</strong>(도달일, 가장 최근 도달)로부터 당일(D-0)까지의 <strong>거래일 간격</strong>.<br/>
    최저가대비 상승률(%): (당일(D-0) 종가 ÷ (당일 포함 최근 120거래일 최저 종가) − 1) × 100.<br/>
    시가총액: <strong>10억원 단위</strong>로 표기합니다(예: 150.0 = 1,500억원).<br/>
    거래대금 순위: <code>market_judgment.html</code>과 동일하게 <strong>최신일 거래대금(종가×거래량) 기준</strong>으로 코스피/코스닥 시장 내 순위를 매깁니다.<br/>
    현재가: 최신일 종가 기준.<br/>
    <strong>주봉 신고가 표</strong>(아래 각 시장 표): 일봉을 금요일 주간(<code>W-FRI</code>)으로 묶어 주간 종가(해당 주 <strong>마지막 거래일 종가</strong>)를 사용합니다.
    당일이 속한 주의 주봉 종가가 직전 10·20·50개 주봉 종가 각각의 최고값을 <strong>초과(&gt;)</strong>하면 <strong>O</strong>, 아니면 <strong>X</strong>, 주봉 이력이 부족하면 빈칸입니다(위 250일 신고가와 동일한 엄격 비교).<br/>
    테마: <code>krx_theme_stock</code> 기준. 행 배경(시가총액, 원): <span style="background:#c8e6c9;padding:0 6px">1조 이상</span>,
    <span style="background:#bbdefb;padding:0 6px">5천억 이상 1조 미만</span>,
    <span style="background:#ffe0b2;padding:0 6px">5천억 미만</span>.<br/>
    <strong>표 정렬</strong>: 칼럼 헤더 클릭 시 해당 열 기준 오름·내림차순이 번갈아 적용됩니다.<br/>
    파일: {os.path.basename(out_path)}
  </div>
  <div class="tables-2col">
    <div class="col">
      <h2>코스피 (KOSPI) — {len(k)}종목</h2>
      {_theme_blurb_k}
      {_table(k)}
      <h3>주봉 기준 신고가 여부 (10주 · 20주 · 50주)</h3>
      {_table_weekly(k)}
    </div>
    <div class="col">
      <h2>코스닥 (KOSDAQ) — {len(q)}종목</h2>
      {_theme_blurb_q}
      {_table(q)}
      <h3>주봉 기준 신고가 여부 (10주 · 20주 · 50주)</h3>
      {_table_weekly(q)}
    </div>
  </div>
{KRX_SORTABLE_TABLE_CSS_JS}
</body>
</html>"""

    try:
        _bo_db = out_df.copy()
        _bo_db = _bo_db.drop(columns=["is_10w_high", "is_20w_high", "is_50w_high"], errors="ignore")
        if "breakout_date" in _bo_db.columns:
            _bo_db["breakout_date"] = pd.to_datetime(_bo_db["breakout_date"], errors="coerce").dt.date
        _save_krx_analysis_table(engine, "krx_analysis_breakout_120d", _bo_db, bo_ref_trade_date)
    except Exception as e:
        if not quiet:
            print(f"경고: 신고가 리스트 DB 저장 실패 ({type(e).__name__}: {e})")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    if not quiet:
        try:
            import webbrowser

            webbrowser.open(out_path)
        except Exception:
            pass

        print(f"완료: 최근 5일 120일 신고가 달성 리스트 HTML 저장: {out_path} (총 {len(out_df)}건)")
    return out_path, set(out_df["ticker"].astype(str).tolist())


def _create_engine():
    db_url = os.getenv("KRX_DB_URL", DEFAULT_DB_URL).strip()
    return create_engine(db_url)


def _load_latest_ticker_list(engine) -> pd.DataFrame:
    query = """
        SELECT *
        FROM krx_ticker
        WHERE 기준일 = (SELECT MAX(기준일) FROM krx_ticker)
          AND 종목구분 = '보통주';
    """
    df = pd.read_sql_query(query, con=engine)
    if df is None or df.empty:
        raise RuntimeError("ticker_list 로드 실패: krx_ticker 데이터가 없습니다.")
    return df


def _market_dash_load_single_ticker_ohlcv(ticker, ticker_list_idx, eng, memory_cache=None):
    """단일 티커 OHLCV 로드 (대시보드 breadth·변동성용). memory_cache 있으면 DB 생략."""
    ticker = str(ticker)
    try:
        if memory_cache is not None and ticker in memory_cache:
            raw = memory_cache[ticker]
            if raw is None or raw.empty:
                return ticker, None
            ohlcv = raw.sort_values("date").reset_index(drop=True).copy()
            ohlcv.insert(0, "ticker", ticker)
            try:
                ohlcv.insert(1, "name", ticker_list_idx.loc[ticker, "종목명"])
                ohlcv.insert(2, "sector", ticker_list_idx.loc[ticker, "업종명"])
                try:
                    if "시가총액" in ticker_list_idx.columns and ticker in ticker_list_idx.index:
                        market_cap = ticker_list_idx.loc[ticker, "시가총액"]
                        if pd.notna(market_cap):
                            ohlcv.insert(3, "market_cap", market_cap)
                        else:
                            ohlcv.insert(3, "market_cap", None)
                    else:
                        ohlcv.insert(3, "market_cap", None)
                except (KeyError, IndexError):
                    ohlcv.insert(3, "market_cap", None)
            except (KeyError, IndexError):
                ohlcv.insert(1, "name", ticker)
                ohlcv.insert(2, "sector", "")
                ohlcv.insert(3, "market_cap", None)
            ohlcv = ohlcv.set_index("date")
            return ticker, ohlcv

        query = """select * from krx_ohlcv where ticker = '{}';""".format(ticker)
        ohlcv = pd.read_sql_query(query, con=eng)
        if ohlcv is not None and not ohlcv.empty:
            try:
                ohlcv.insert(1, "name", ticker_list_idx.loc[ticker, "종목명"])
                ohlcv.insert(2, "sector", ticker_list_idx.loc[ticker, "업종명"])
                try:
                    if "시가총액" in ticker_list_idx.columns and ticker in ticker_list_idx.index:
                        market_cap = ticker_list_idx.loc[ticker, "시가총액"]
                        if pd.notna(market_cap):
                            ohlcv.insert(3, "market_cap", market_cap)
                        else:
                            ohlcv.insert(3, "market_cap", None)
                    else:
                        ohlcv.insert(3, "market_cap", None)
                except (KeyError, IndexError):
                    ohlcv.insert(3, "market_cap", None)
            except (KeyError, IndexError):
                ohlcv.insert(1, "name", ticker)
                ohlcv.insert(2, "sector", "")
                ohlcv.insert(3, "market_cap", None)
            ohlcv = ohlcv.set_index("date")
            return ticker, ohlcv
        return ticker, None
    except Exception as e:
        print(f"대시보드 OHLCV 로드 실패 {ticker}: {e}")
        return ticker, None


def _market_dash_load_ohlcv_parallel(tickers, ticker_list_idx, eng, memory_cache=None, max_workers=10, quiet: bool = False):
    ohlcv_data = {}
    tickers = [str(t) for t in tickers]
    n_mem = sum(1 for t in tickers if memory_cache and t in memory_cache)
    n_db = len(tickers) - n_mem
    if not quiet:
        print(f"  → OHLCV 소스: 메모리 {n_mem}종목 / DB 조회 {n_db}종목")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ticker = {
            executor.submit(_market_dash_load_single_ticker_ohlcv, t, ticker_list_idx, eng, memory_cache): t
            for t in tickers
        }
        for future in tqdm(as_completed(future_to_ticker), total=len(tickers), desc="대시보드 OHLCV", disable=quiet):
            ticker, data = future.result()
            if data is not None:
                ohlcv_data[ticker] = data
    return ohlcv_data


MARKET_DASH_HOLIDAYS = [
    "2023-08-15",
    "2023-09-28",
    "2023-09-29",
    "2023-10-02",
    "2023-10-03",
    "2023-10-09",
    "2023-12-25",
    "2023-12-29",
    "2024-01-01",
    "2024-02-09",
    "2024-02-12",
    "2024-03-01",
    "2024-04-10",
    "2024-05-06",
    "2024-05-01",
    "2024-05-15",
    "2024-06-06",
    "2024-08-15",
    "2024-09-16",
    "2024-09-17",
    "2024-09-18",
    "2024-10-01",
    "2024-10-03",
    "2024-10-09",
    "2024-12-25",
    "2024-12-31",
    "2025-01-01",
    "2025-01-27",
    "2025-01-28",
    "2025-01-29",
    "2025-01-30",
    "2025-03-03",
    "2025-05-01",
    "2025-05-05",
    "2025-05-06",
    "2025-06-03",
    "2025-06-06",
    "2025-08-15",
    "2025-10-03",
    "2025-10-06",
    "2025-10-07",
    "2025-10-08",
    "2025-10-09",
    "2025-12-25",
    "2025-12-31",
    "2026-01-01",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-03-02",
    "2026-05-01",
    "2026-05-05",
    "2026-05-25",
    "2026-06-03",    
]

MARKET_DASH_PAGE_DESCS: dict[int, str] = {
    1: "AD line: 전일 대비 상승 종목 수 − 하락 종목 수를 누적한 선입니다. SMA20은 그 추세선입니다. 아래 막대는 같은 날짜의 (상승−하락) 일별 값입니다. 지수와 함께 시장 참여 종목의 방향성 강도를 봅니다.",
    2: "시장 평균 변동성: 유니버스 각 종목의 ATR14÷종가를 날짜별로 평균한 값입니다. Vol SMA20은 변동성의 20일 이동평균으로, 시장 전체 가격 변동 폭의 수준·추세를 봅니다.",
    3: "맥클레란 오실레이터: 일별 Net AD(상승−하락 종목 수)에 EMA19·EMA39를 적용한 차이입니다. 0선 위·아래로 breadth 모멘텀을 확인합니다.",
    4: "Zweig Breadth Thrust: 상승÷(상승+하락) 종목 비율의 10일 SMA(%)입니다. 최근 10일 안에 40% 미만을 거친 뒤 61.5%를 처음 돌파하면 별(★)로 표시합니다.",
    5: "CVI(거래대금): 전일 대비 상승 종목 거래대금 합을 하락 종목 거래대금 합으로 나눈 뒤, 최근 20거래일 합(상승 합 ÷ 하락 합)으로 본 지표입니다. 가운데는 20일 비·SMA20, 아래는 일별 비율(당일 상승 합÷하락 합) 막대입니다.",
    6: "종가>SMA 비중: 해당 시장 유니버스에서 종가가 SMA5·10·20 위에 있는 종목 비율(%)입니다. 코스피·코스닥 각각 SMA 길이별로 한 패널씩 나누어 표시합니다.",
    7: "종가>SMA5 · <SMA10 비중: 단기(5일) 강세 종목 비율과 중기(10일) 약세 종목 비율을 겹쳐 봅니다. 단기 과열·중기 약세 괴리를 확인합니다.",
    8: "120일 신고가/신저가 종목 수: 종가가 최근 120거래일 최고·최저 종가인 종목 수입니다. 신고가·신저가 확산 정도를 봅니다.",
    9: "ADR: 최근 20거래일 상승 종목 수 합 ÷ 같은 기간 하락 종목 수 합에 100을 곱한 값입니다. 일별 값은 들쭉날쭉하므로 ADR의 10일 SMA로 추세를 보조합니다. 약 100 근처는 균형, 120~125 이상은 단기 과열, 70~75 이하는 침체(과매도) 권역으로 자주 해석합니다.",
    10: "모멘텀 속도: 지수 종가 기준 ROC(기간 변화율 %) ÷ 기간으로 나눈 하루 평균 변화율(%/일)입니다. 5·10·20·50일 선을 겹쳐 단기·중기 추세 속도를 비교합니다. 0선 위는 상승 모멘텀, 아래는 하락 모멘텀입니다.",
}

_MOMENTUM_SPEED_PERIODS = (5, 10, 20, 50)
_MOMENTUM_SPEED_COLORS = {5: "#E53935", 10: "#FB8C00", 20: "#43A047", 50: "#1E88E5"}


def _compute_momentum_speed(close: pd.Series, periods: tuple[int, ...] = _MOMENTUM_SPEED_PERIODS) -> pd.DataFrame:
    """Pine Script 모멘텀 속도: ta.roc(close, N) / N → 하루 평균 변화율(%)."""
    s = pd.to_numeric(close, errors="coerce")
    out = pd.DataFrame(index=s.index)
    for length in periods:
        prev = s.shift(length)
        roc_pct = (s - prev) / prev.replace(0, np.nan) * 100.0
        out[f"mom_{length}"] = roc_pct / length
    return out


def _load_krx_theme_map(engine) -> dict[str, str]:
    try:
        th_df = pd.read_sql_query(
            """
            SELECT ticker,
                   GROUP_CONCAT(DISTINCT theme_name ORDER BY theme_name SEPARATOR ' · ') AS theme_str
            FROM krx_theme_stock
            GROUP BY ticker
            """,
            con=engine,
        )
        th_df["ticker"] = th_df["ticker"].astype(str)
        return {str(r["ticker"]): str(r["theme_str"] or "") for _, r in th_df.iterrows()}
    except Exception:
        return {}


VOL_SPREAD_AVG_WINDOW = 5


def _ohlcv_df_sorted(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    if getattr(df.index, "dtype", None) is not None and not np.issubdtype(df.index.dtype, np.datetime64):
        df = df.copy()
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()].sort_index()
    else:
        df = df.sort_index()
    return df


def _calc_elapsed_high_td_from_close(close: np.ndarray) -> int | None:
    """전일(D-1) 기준 120일 최고 종가(최근 도달) ~ 당일(D-0) 거래일 간격."""
    n = len(close)
    if n < 121:
        return None
    ref_end = n - 2
    ref_win_start = ref_end - 119
    ref_slice = close[ref_win_start : ref_end + 1]
    if not np.isfinite(ref_slice).any():
        return None
    ref_high = float(np.nanmax(ref_slice))
    hit = np.where(np.isfinite(ref_slice) & (ref_slice >= ref_high))[0]
    if len(hit) == 0:
        return None
    prev_high_idx = ref_win_start + int(hit[-1])
    elapsed = (n - 1) - prev_high_idx
    return int(elapsed) if elapsed >= 0 else 0


def _calc_pct_b_last(df: pd.DataFrame, timeperiod: int = 20) -> float | None:
    """볼린저(20,2) %b = (종가−하단)/(상단−하단), 최신일."""
    df = _ohlcv_df_sorted(df)
    if df is None or len(df) < timeperiod:
        return None
    try:
        import talib

        close = pd.to_numeric(df["close"], errors="coerce").astype(float).values
        if len(close) < timeperiod:
            return None
        upper, _middle, lower = talib.BBANDS(close, timeperiod=timeperiod, nbdevup=2, nbdevdn=2, matype=0)
        u, lo, c = float(upper[-1]), float(lower[-1]), float(close[-1])
        if not (np.isfinite(u) and np.isfinite(lo) and np.isfinite(c)):
            return None
        span = u - lo
        if span == 0 or not np.isfinite(span):
            return None
        pct_b = (c - lo) / span
        return float(pct_b) if np.isfinite(pct_b) else None
    except Exception:
        return None


def _calc_ohlcv_chg_and_elapsed(df: pd.DataFrame) -> dict:
    """당일·3거래일 전 대비 등락률(%), 현재가, %b, 이전 신고가 경과일수."""
    out = {
        "chg_1d_pct": np.nan,
        "chg_3d_pct": np.nan,
        "elapsed_high_td": None,
        "last_tv": np.nan,
        "current_price": np.nan,
        "pct_b": None,
    }
    df = _ohlcv_df_sorted(df)
    if df is None or len(df) < 2:
        return out
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float)
    if len(close) >= 2 and np.isfinite(close[-1]) and np.isfinite(close[-2]) and close[-2] != 0:
        out["chg_1d_pct"] = (close[-1] / close[-2] - 1.0) * 100.0
    if len(close) >= 4 and np.isfinite(close[-1]) and np.isfinite(close[-4]) and close[-4] != 0:
        out["chg_3d_pct"] = (close[-1] / close[-4] - 1.0) * 100.0
    out["elapsed_high_td"] = _calc_elapsed_high_td_from_close(close)
    if len(close) and np.isfinite(close[-1]):
        out["current_price"] = float(close[-1])
    out["pct_b"] = _calc_pct_b_last(df)
    if "volume" in df.columns:
        vol = pd.to_numeric(df["volume"], errors="coerce")
        if len(vol) and np.isfinite(close[-1]) and np.isfinite(vol.iloc[-1]):
            out["last_tv"] = float(close[-1]) * float(vol.iloc[-1])
    return out


def _calc_directional_bias_metrics(df: pd.DataFrame, window: int = VOL_SPREAD_AVG_WINDOW) -> dict | None:
    """
    상승·하락 의지 지표 3종 (최근 N거래일).
    1) CLV 평균: ((C−L)−(H−C))/(H−L), −1~+1 (종가가 레인지 상단에 가까울수록 +)
    2) 순방향 변동: Σ양(일수익률) − Σ음(일수익률 절대값), %p
    3) DRB 평균: ((C−L)−(H−C))/전일종가 의 N일 평균 (소수, 표시 시 %)
    """
    if df is None or df.empty:
        return None
    if "high" not in df.columns or "low" not in df.columns or "close" not in df.columns:
        return None

    df = _ohlcv_df_sorted(df)
    if df is None or len(df) < window + 1:
        return None

    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    hl = high - low
    clv = np.where(hl > 0, ((close - low) - (high - close)) / hl, np.nan)
    clv_s = pd.Series(clv, index=close.index)
    clv_tail = clv_s.iloc[-window:]
    if clv_tail.notna().sum() < window:
        return None
    clv_avg = float(clv_tail.mean())
    if not np.isfinite(clv_avg):
        return None

    ret = close.pct_change()
    ret_tail = ret.iloc[-window:]
    if ret_tail.notna().sum() < window:
        return None
    up_sum = float(ret_tail[ret_tail > 0].sum())
    down_sum = float((-ret_tail[ret_tail < 0]).sum())
    net_dir = up_sum - down_sum

    prev_close = close.shift(1)
    denom = prev_close.replace(0, np.nan)
    drb = ((close - low) - (high - close)) / denom
    drb_avg = float(drb.iloc[-window:].mean())
    if not np.isfinite(drb_avg):
        return None

    return {"clv_avg": clv_avg, "net_dir": net_dir, "drb_avg": drb_avg}


def _html_vol_spread_composite_top50_table(
    full_df: pd.DataFrame,
    prev_tv_rank_map: dict[str, float],
    highlight_set: set[str],
    title_esc: str,
) -> str:
    """
    거래대금 Top100(∩RS Top100) 합산 유니버스에서 CLV·순방향·DRB 백분위를
    동일 풀에서 산출한 뒤 종합백분위 내림차순 상위 50.
    """
    if full_df is None or full_df.empty:
        return ""
    sub = full_df.copy()
    for col in ("clv_avg", "net_dir", "drb_avg"):
        sub[f"pct_{col}"] = sub[col].rank(method="average", pct=True, ascending=True) * 100.0
    sub["composite_pct"] = (sub["pct_clv_avg"] + sub["pct_net_dir"] + sub["pct_drb_avg"]) / 3.0
    sub = sub.sort_values("composite_pct", ascending=False, na_position="last").head(50).reset_index(drop=True)
    sub["tv_rank_prev"] = pd.to_numeric(sub["ticker"].astype(str).map(prev_tv_rank_map), errors="coerce")

    def _fclv(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        return f"{x:.3f}" if np.isfinite(x) else ""

    def _fnet(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        return f"{x * 100:.2f}%" if np.isfinite(x) else ""

    def _fdrb(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        return f"{x * 100:.2f}%" if np.isfinite(x) else ""

    def _fpct(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        return f"{x:.1f}" if np.isfinite(x) else ""

    def _frank(v) -> str:
        try:
            if v is None or (isinstance(v, float) and (np.isnan(v) or not np.isfinite(v))):
                return ""
            return f"{int(float(v)):,}"
        except (TypeError, ValueError):
            return ""

    lines = [
        f"<h2 style='font-size:1.05rem;margin:24px 0 10px 0;'>{title_esc}</h2>",
        "<table class='krx-sortable' border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:12px;width:100%;'>",
        "<thead><tr>",
        "<th style='text-align:center'>방향우세순위</th>",
        "<th style='text-align:right'>전일 순위</th>",
        "<th style='text-align:right'>순위 변동</th>",
        "<th style='text-align:center'>시장</th>",
        "<th style='text-align:center'>종목코드</th>",
        "<th style='text-align:left'>종목명</th>",
        "<th style='text-align:left'>테마</th>",
        "<th style='text-align:right'>종합백분위</th>",
        "<th style='text-align:right'>CLV 백분위</th>",
        "<th style='text-align:right'>순방향 백분위</th>",
        "<th style='text-align:right'>DRB 백분위</th>",
        "<th style='text-align:right'>CLV(5일)</th>",
        "<th style='text-align:right'>순방향변동(5일)</th>",
        "<th style='text-align:right'>DRB(5일)</th>",
        "<th style='text-align:right'>거래대금 순위</th>",
        "<th style='text-align:right'>RS순위</th>",
        "<th style='text-align:right'>RS점수</th>",
        "</tr></thead><tbody>",
    ]
    for i, (_, r) in enumerate(sub.iterrows(), start=1):
        _tk = str(r["ticker"])
        _hi = _tk in highlight_set
        _nm = html.escape(str(r.get("name", "")))
        _tk_e = html.escape(_tk)
        if _hi:
            _tk_e = f"<strong>{_tk_e}</strong>"
            _nm = f"<strong>{_nm}</strong>"
        _rs_s_txt = ""
        try:
            _rsv = r.get("rs_score")
            if _rsv is not None and np.isfinite(float(_rsv)):
                _rs_s_txt = f"{float(_rsv):.1f}"
        except (TypeError, ValueError):
            pass
        _prv = r.get("tv_rank_prev")
        _prv_txt = _frank(_prv)
        _rc_txt, _rc_sv, _rc_col = _krx_fmt_rank_change_cell(i, _prv)
        _mkt = html.escape(str(r.get("market", "") or ""))
        lines.append(
            "<tr>"
            f"<td style='text-align:center'{_html_sort_num_attr(i)}>{i}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(_prv)}>{html.escape(_prv_txt)}</td>"
            f"<td style='text-align:right;color:{_rc_col}'{_html_sort_num_attr(_rc_sv)}>{html.escape(_rc_txt)}</td>"
            f"<td style='text-align:center'>{_mkt}</td>"
            f"<td style='text-align:center'>{_tk_e}</td>"
            f"<td style='text-align:left'>{_nm}</td>"
            f"<td style='text-align:left'>{html.escape(str(r.get('theme_str', '')))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('composite_pct'))}>{_fpct(r.get('composite_pct'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('pct_clv_avg'))}>{_fpct(r.get('pct_clv_avg'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('pct_net_dir'))}>{_fpct(r.get('pct_net_dir'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('pct_drb_avg'))}>{_fpct(r.get('pct_drb_avg'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('clv_avg'))}>{_fclv(r.get('clv_avg'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('net_dir'))}>{_fnet(r.get('net_dir'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('drb_avg'))}>{_fdrb(r.get('drb_avg'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('tv_rank'))}>{_frank(r.get('tv_rank'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('rs_rank'))}>{_frank(r.get('rs_rank'))}</td>"
            f"<td style='text-align:right'{_html_sort_num_attr(r.get('rs_score'))}>{_rs_s_txt}</td>"
            "</tr>"
        )
    lines.append("</tbody></table>")
    return "\n".join(lines)


def write_volatility_spread_top100_html(
    engine,
    ohlcv_data: dict | None,
    ticker_list: pd.DataFrame,
    output_dir: str | None = None,
    highlight_tickers: set[str] | None = None,
    quiet: bool = False,
    memory_cache=None,
) -> str | None:
    """방향 우세(CLV·순방향변동·DRB) — 코스피·코스닥 거래대금 Top100 ∩ RS Top100 유니버스 (volatility_spread_top100.html)."""
    ticker_list_idx = ticker_list.set_index("종목코드")
    ticker_list_idx.index = ticker_list_idx.index.astype(str)

    tv_universe, tv_rank_map, kospi_tv_set, kosdaq_tv_set = _load_tv_top100_universe(engine)
    if not tv_universe:
        tv_universe, tv_rank_map, kospi_tv_set, kosdaq_tv_set = set(), {}, set(), set()

    rs_rank_map, rs_score_map = _load_latest_rs_rank_and_score_maps(engine)
    vol_spread_rs_top_n = 100
    rs_top100_note = ""
    if rs_rank_map:
        kospi_tv_set = {
            t
            for t in kospi_tv_set
            if rs_rank_map.get(t, vol_spread_rs_top_n + 1) <= vol_spread_rs_top_n
        }
        kosdaq_tv_set = {
            t
            for t in kosdaq_tv_set
            if rs_rank_map.get(t, vol_spread_rs_top_n + 1) <= vol_spread_rs_top_n
        }
        tv_universe = kospi_tv_set | kosdaq_tv_set
    else:
        rs_top100_note = (
            "<code>krx_relative_strength</code> 최신 데이터가 없어 <strong>RS Top100 교집합은 생략</strong>되고, "
            "거래대금 Top100만 유니버스로 사용합니다.<br/>"
        )

    if ohlcv_data:
        ohlcv_data = {str(k): v for k, v in ohlcv_data.items() if str(k) in tv_universe}
    else:
        ohlcv_data = {}

    _need_load = [t for t in sorted(tv_universe) if t not in ohlcv_data]
    if _need_load:
        try:
            loaded = _market_dash_load_ohlcv_parallel(
                _need_load, ticker_list_idx, engine, memory_cache=memory_cache, quiet=quiet
            )
            ohlcv_data.update(loaded)
        except Exception:
            pass

    base = output_dir or os.path.join(
        os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR),
        date.today().strftime("%Y-%m-%d"),
    )
    os.makedirs(base, exist_ok=True)
    out_path = os.path.join(base, "volatility_spread_top100.html")

    vol_ref_td = _krx_max_ohlcv_trade_date(engine)

    theme_map = _load_krx_theme_map(engine)
    _highlight_set = {str(x) for x in (highlight_tickers or set())}

    rank_map: dict[str, float] = {k: float(v) for k, v in tv_rank_map.items()}
    _rank_dir = base
    rank_path = os.path.join(_rank_dir, "market_judgment_tv_rank.csv")
    if not rank_map:
        try:
            rk = pd.read_csv(rank_path, dtype={"ticker": str, "sector_cd": str})
            rk["ticker"] = rk["ticker"].astype(str)
            rank_map = {str(r["ticker"]): float(pd.to_numeric(r["tv_rank"], errors="coerce")) for _, r in rk.iterrows()}
        except Exception:
            rank_map = {}

    rows: list[dict] = []
    for ticker in sorted(tv_universe):
        df = ohlcv_data.get(ticker)
        if df is None:
            continue
        stats = _calc_directional_bias_metrics(df)
        if stats is None:
            continue
        extra = _calc_ohlcv_chg_and_elapsed(df)
        name = ticker
        try:
            if "name" in df.columns and len(df):
                name = str(df["name"].iloc[-1])
            elif ticker in ticker_list_idx.index:
                name = str(ticker_list_idx.loc[ticker, "종목명"])
        except (KeyError, IndexError, TypeError):
            pass
        th = theme_map.get(str(ticker), "")
        if len(th) > 96:
            th = th[:95] + "…"
        tk = str(ticker)
        market = "KOSPI" if tk in kospi_tv_set else ("KOSDAQ" if tk in kosdaq_tv_set else "")
        rows.append(
            {
                "ticker": tk,
                "name": name,
                "theme_str": th,
                "market": market,
                "rs_rank": rs_rank_map.get(tk),
                "rs_score": rs_score_map.get(tk),
                "tv_rank": rank_map.get(tk),
                "current_price": extra["current_price"],
                "pct_b": extra["pct_b"],
                "chg_1d_pct": extra["chg_1d_pct"],
                "chg_3d_pct": extra["chg_3d_pct"],
                "elapsed_high_td": extra["elapsed_high_td"],
                "last_tv": extra["last_tv"],
                **stats,
            }
        )

    rs_universe_applied = bool(rs_rank_map)

    if not rows:
        _empty_title = (
            "방향 우세 — 거래대금 Top100 ∩ RS Top100"
            if rs_universe_applied
            else "방향 우세 — 거래대금 Top100"
        )
        _empty_p = (
            "방향 우세 지표를 산출할 수 있는 종목이 없습니다. OHLCV 봉 수(최소 6거래일), 또는 거래대금 Top100과 RS Top100 교집합이 비었는지 확인하세요."
            if rs_universe_applied
            else "방향 우세 지표를 산출할 수 있는 종목이 없습니다. OHLCV 봉 수(최소 6거래일)를 확인하세요."
        )
        html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <title>{html.escape(_empty_title)}</title>
  <style>
    body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 16px 20px; color: #111; background: #fafafa; }}
  </style>
</head>
<body>
  <p>{html.escape(_empty_p)}</p>
{KRX_SORTABLE_TABLE_CSS_JS}
</body>
</html>"""
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_doc)
        if not quiet:
            print(f"완료: 방향 우세 Top100 HTML 저장(0건): {out_path}")
        try:
            _save_krx_analysis_table(engine, "krx_analysis_vol_spread_top100", pd.DataFrame(), vol_ref_td)
        except Exception:
            pass
        return out_path

    full_df = pd.DataFrame(rows)
    full_df["tv_rank"] = full_df["ticker"].map(rank_map)
    full_df["tv_rank"] = pd.to_numeric(full_df["tv_rank"], errors="coerce")
    _prev_tv_vol_map = _krx_tv_rank_prev_by_ticker(engine)
    full_df["tv_rank_prev"] = pd.to_numeric(
        full_df["ticker"].astype(str).map(_prev_tv_vol_map), errors="coerce"
    )

    _sort_cols = ["clv_avg", "net_dir", "drb_avg"]
    k_df = full_df[full_df["market"] == "KOSPI"].sort_values(
        _sort_cols, ascending=[False, False, False], na_position="last"
    ).reset_index(drop=True)
    q_df = full_df[full_df["market"] == "KOSDAQ"].sort_values(
        _sort_cols, ascending=[False, False, False], na_position="last"
    ).reset_index(drop=True)

    def _fmt_clv(v) -> str:
        """CLV −1~+1."""
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x:.3f}"

    def _fmt_net_dir(v) -> str:
        """순방향 변동(%p)."""
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x * 100:.2f}%"

    def _fmt_drb(v) -> str:
        """DRB 소수 → %."""
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x * 100:.2f}%"

    def _fmt_price(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x:,.0f}"

    def _fmt_rs_score(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x:.1f}"

    def _fmt_pct_b(v) -> str:
        try:
            x = float(v)
        except (TypeError, ValueError):
            return ""
        if not np.isfinite(x):
            return ""
        return f"{x * 100:.1f}"

    def _fmt_pct_cell(v) -> str:
        try:
            if v is None or (isinstance(v, float) and (np.isnan(v) or not np.isfinite(v))):
                return ""
            return f"{float(v):.1f}"
        except (TypeError, ValueError):
            return ""

    def _fmt_rank_cell(v) -> str:
        try:
            if v is None or (isinstance(v, float) and (np.isnan(v) or not np.isfinite(v))):
                return ""
            return f"{int(float(v)):,}"
        except (TypeError, ValueError):
            return ""

    def _table_for_market(sub_df: pd.DataFrame) -> str:
        lines = [
            "<table class='krx-sortable' border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-size:12px;width:100%;'>",
            "<thead><tr>",
            "<th>방향우세순위</th><th>전일 순위</th><th>순위 변동</th><th>종목코드</th><th>종목명</th><th>테마</th><th>현재가</th>"
            "<th>CLV(5일평균)</th><th>순방향변동(5일)</th><th>DRB(5일평균)</th>"
            "<th>거래대금 순위</th><th>RS순위</th><th>RS점수</th>"
            "<th>당일 상승률(%)</th><th>3일간 상승률(%)</th><th>%b</th>"
            "<th>이전 신고가 경과일수</th>",
            "</tr></thead><tbody>",
        ]
        for i, (_, r) in enumerate(sub_df.iterrows(), start=1):
            _tk = str(r["ticker"])
            _is_hi = _tk in _highlight_set
            _chg = r.get("chg_1d_pct")
            _tk_inner = html.escape(_tk)
            _nm_inner = html.escape(str(r.get("name", "")))
            if _is_hi:
                _tk_inner = f"<strong>{_tk_inner}</strong>"
                _nm_inner = f"<strong>{_nm_inner}</strong>"
            _tk_cell = _krx_colored_html(_tk_inner, _chg)
            _nm_cell = _krx_colored_html(_nm_inner, _chg)
            _chg1_txt = _fmt_pct_cell(_chg)
            _chg1_cell = _krx_colored_html(_chg1_txt, _chg) if _chg1_txt else ""
            _chg3 = r.get("chg_3d_pct")
            _chg3_txt = _fmt_pct_cell(_chg3)
            _chg3_cell = _krx_colored_html(_chg3_txt, _chg3) if _chg3_txt else ""
            _eh = r.get("elapsed_high_td")
            _eh_txt = "" if _eh is None or (isinstance(_eh, float) and np.isnan(_eh)) else str(int(_eh))
            _tvp = r.get("tv_rank_prev")
            _rc_txt, _rc_sv, _rc_col = _krx_fmt_rank_change_cell(i, _tvp)
            lines.append(
                f"<tr>"
                f"<td style='text-align:center'{_html_sort_num_attr(i)}>{i}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_tvp)}>{_fmt_rank_cell(_tvp)}</td>"
                f"<td style='text-align:right;color:{_rc_col}'{_html_sort_num_attr(_rc_sv)}>{html.escape(_rc_txt)}</td>"
                f"<td style='text-align:center'>{_tk_cell}</td>"
                f"<td>{_nm_cell}</td>"
                f"<td>{html.escape(str(r.get('theme_str', '')))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('current_price'))}>{_fmt_price(r.get('current_price'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('clv_avg'))}>{_fmt_clv(r.get('clv_avg'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('net_dir'))}>{_fmt_net_dir(r.get('net_dir'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('drb_avg'))}>{_fmt_drb(r.get('drb_avg'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('tv_rank'))}>{_fmt_rank_cell(r.get('tv_rank'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('rs_rank'))}>{_fmt_rank_cell(r.get('rs_rank'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('rs_score'))}>{_fmt_rs_score(r.get('rs_score'))}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_chg)}>{_chg1_cell}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(_chg3)}>{_chg3_cell}</td>"
                f"<td style='text-align:right'{_html_sort_num_attr(r.get('pct_b'))}>{_fmt_pct_b(r.get('pct_b'))}</td>"
                f"<td style='text-align:center'{_html_sort_num_attr(_eh)}>{html.escape(_eh_txt)}</td>"
                f"</tr>"
            )
        lines.append("</tbody></table>")
        return "\n".join(lines)

    _combo_title = (
        "KOSPI·KOSDAQ 거래대금 Top100 ∩ RS Top100 합산 — 방향 지표 종합 순위 (상위 50)"
        if rs_universe_applied
        else "KOSPI·KOSDAQ 거래대금 Top100 합산 — 방향 지표 종합 순위 (상위 50)"
    )
    vol_combo_html = _html_vol_spread_composite_top50_table(
        full_df,
        _prev_tv_vol_map,
        _highlight_set,
        html.escape(_combo_title),
    )

    table_parts: list[str] = []
    if vol_combo_html.strip():
        table_parts.append(vol_combo_html)
    _sec_k = (
        "KOSPI — 거래대금 Top100 ∩ RS Top100 · 방향 우세 순"
        if rs_universe_applied
        else "KOSPI — 거래대금 Top100 · 방향 우세 순"
    )
    _sec_q = (
        "KOSDAQ — 거래대금 Top100 ∩ RS Top100 · 방향 우세 순"
        if rs_universe_applied
        else "KOSDAQ — 거래대금 Top100 · 방향 우세 순"
    )
    if len(k_df):
        table_parts.append(
            f"<h2 style='font-size:1.05rem;margin:24px 0 10px 0;'>{html.escape(_sec_k)}</h2>"
        )
        table_parts.append(_table_for_market(k_df))
    if len(q_df):
        table_parts.append(
            f"<h2 style='font-size:1.05rem;margin:24px 0 10px 0;'>{html.escape(_sec_q)}</h2>"
        )
        table_parts.append(_table_for_market(q_df))
    table_html = "\n".join(table_parts)

    ref_note = ""
    try:
        ref_m = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=engine)
        ref_d = pd.to_datetime(ref_m.iloc[0]["d"], errors="coerce")
        if pd.notna(ref_d):
            ref_note = ref_d.strftime("%Y-%m-%d")
    except Exception:
        pass

    if rs_universe_applied:
        html_title = "방향 우세 — 거래대금 Top100 ∩ RS Top100"
        html_h1 = "방향 우세 — 거래대금 Top100 ∩ RS Top100 유니버스"
        univ_line = (
            "<strong>유니버스</strong>: 당일 거래대금(종가×거래량) 기준 코스피·코스닥 각 Top100 보통주 중, "
            "<code>krx_relative_strength</code> 최신일 RS10·20·50·120 평균의 <strong>시장별 RS 순위 상위 100</strong>에 드는 종목만 "
            "교집합으로 포함합니다(시장별 최대 100종, 실제 행 수는 교집합 크기). "
            "시장별로 CLV·순방향변동·DRB 기준 방향 우세 순으로 정렬합니다.<br/>"
        )
        rs_detail_line = (
            "RS순위·RS점수: <code>krx_relative_strength</code> 최신일 RS10·20·50·120 평균의 시장별 순위(1=최상위) 및 평균 점수(백분위). "
            "유니버스가 RS 상위 100과의 교집합이므로 표에 보이는 종목의 RS순위는 모두 100 이내입니다.<br/>"
        )
    else:
        html_title = "방향 우세 — 거래대금 Top100"
        html_h1 = "방향 우세 — 거래대금 Top100 유니버스"
        univ_line = (
            "<strong>유니버스</strong>: 당일 거래대금(종가×거래량) 기준 코스피 Top100 + 코스닥 Top100 보통주입니다. "
            "시장별로 CLV·순방향변동·DRB 기준 방향 우세 순으로 정렬합니다(각 최대 100종목).<br/>"
        )
        rs_detail_line = (
            "RS순위·RS점수: <code>krx_relative_strength</code> 최신일 기준(시장별 RS10·20·50·120 평균의 순위·백분위). "
            "RS 테이블이 비어 있으면 교집합 필터는 적용하지 않으며, 해당 칼럼은 비울 수 있습니다.<br/>"
        )

    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(html_title)}</title>
  <style>
    body {{ font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 16px 20px; color: #111; background: #fafafa; }}
    h1 {{ font-size: 1.2rem; margin: 0 0 10px 0; }}
    .note {{ color: #444; font-size: 13px; margin: 10px 0 18px; line-height: 1.55; }}
  </style>
</head>
<body>
  <h1>{html.escape(html_h1)}</h1>
  <div class="note">
    {rs_top100_note}
    기준일(OHLCV 최신): <strong>{html.escape(ref_note) if ref_note else '—'}</strong><br/>
    {univ_line}
    <strong>CLV(5일 평균)</strong> = ((종가−저가)−(고가−종가))÷(고가−저가) 의 5일 평균. +1에 가까울수록 종가가 당일 레인지 상단(상승 쪽 마감 우세), −1은 하단.<br/>
    <strong>순방향변동(5일)</strong> = 최근 5거래일 양(+) 일수익률 합 − 음(−) 일수익률 절대값 합(%p). 실제 누적 방향 성과.<br/>
    <strong>DRB(5일 평균)</strong> = ((종가−저가)−(고가−종가))÷전일종가 의 5일 평균(%). 일중 상·하단 압력을 전일 종가 대비로 본 방향 레인지 편향.<br/>
    정렬: CLV 평균 ↓, 동률 시 순방향변동·DRB 순(시장별).<br/>
    %b = 볼린저밴드(20, 2σ) 기준 (종가−하단)÷(상단−하단)×100, 최신일.<br/>
    거래대금 순위: 거래대금 Top100 내 당일 시장 내 순위(1~100).<br/>
    {rs_detail_line}
    당일 상승률·3일간 상승률: 전일·3거래일 전 종가 대비 최신 종가 등락률(%). 이전 신고가 경과일수: 전일 기준 120일 최고 종가 도달일~당일 거래일 간격.<br/>
    테마: <code>krx_theme_stock</code> 기준. 파일: {html.escape(os.path.basename(out_path))}<br/>
    <strong>맨 위 표</strong>: 코스피·코스닥 유니버스를 합친 동일 풀에서 CLV·순방향·DRB 각각의 백분위(<code>rank(pct=True)</code>×100)를 구한 뒤, 세 값의 산술평균을 <strong>종합백분위</strong>로 두고 높은 순으로 상위 50만 표시합니다. 전일 순위는 직전 거래일 시장 내 거래대금 순위입니다. 그 아래는 시장별 방향 우세 순 표입니다.<br/>
    <strong>표 정렬</strong>: 칼럼 헤더 클릭 시 해당 열 기준 오름·내림차순이 번갈아 적용됩니다.
  </div>
  {table_html}
{KRX_SORTABLE_TABLE_CSS_JS}
</body>
</html>"""

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_doc)

    if not quiet:
        try:
            import webbrowser

            webbrowser.open(out_path)
        except Exception:
            pass
        print(
            f"완료: 방향 우세 HTML 저장: {out_path} "
            f"(KOSPI {len(k_df)}건, KOSDAQ {len(q_df)}건)"
        )
    try:
        _save_krx_analysis_table(engine, "krx_analysis_vol_spread_top100", full_df, vol_ref_td)
    except Exception as e:
        if not quiet:
            print(f"경고: 방향 우세 DB 저장 실패 ({type(e).__name__}: {e})")
    return out_path


def run_market_dashboard(
    engine,
    ticker_list: pd.DataFrame,
    memory_cache=None,
    highlight_tickers: set[str] | None = None,
    quiet: bool = False,
) -> tuple[set[str], dict]:
    """코스피/코스닥 시장 대시보드 + 시장 판단 HTML 생성. (Top100 티커 집합, OHLCV 캐시) 반환."""

    _log = print if not quiet else (lambda *a, **k: None)
    ohlcv_data: dict = {}

    # 1) 코스피/코스닥 분리: (지수, AD line, 시장 평균 변동성) 총 6개 지표
    try:
        _log("\n" + "=" * 80)
        _log("코스피/코스닥 지수+AD line+변동성 차트 생성")
        _log("=" * 80)

        BREADTH_WINDOW = 250  # 최근 N거래일 (AD line·변동성 공통 x축)
        holidays = MARKET_DASH_HOLIDAYS

        # universe 구성 (krx_ticker_sector에 코스피/코스닥 메인에 해당하는 sector_cd 존재)
        kospi_list = pd.read_sql_query(
            "SELECT ticker FROM krx_ticker_sector WHERE sector_cd = '1001';",
            con=engine,
        )["ticker"].astype(str).values.tolist()
        kosdaq_list = pd.read_sql_query(
            "SELECT ticker FROM krx_ticker_sector WHERE sector_cd = '2001';",
            con=engine,
        )["ticker"].astype(str).values.tolist()

        kospi_set = set(kospi_list)
        kosdaq_set = set(kosdaq_list)

        ticker_list_idx = ticker_list.set_index("종목코드")
        ticker_list_idx.index = ticker_list_idx.index.astype(str)
        _universe_dash = sorted(kospi_set | kosdaq_set)
        _log(f"대시보드: 코스피+코스닥 유니버스 {len(_universe_dash)}종목 OHLCV 로드 중...")

        ohlcv_data = _market_dash_load_ohlcv_parallel(
            _universe_dash, ticker_list_idx, engine, memory_cache=memory_cache, quiet=quiet
        )

        def load_index_ohlcv(index_ticker: str) -> pd.DataFrame:
            q = f"select date, close, volume from krx_index_ohlcv where ticker = '{index_ticker}';"
            df = pd.read_sql_query(q, con=engine)
            if df.empty:
                return pd.DataFrame(columns=["close", "volume"])
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date").set_index("date")
            # volume이 문자열/NULL일 수 있으니 숫자로 정리
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
            return df

        kospi_index_df = load_index_ohlcv("1001")  # 코스피 지수(종가+거래량)
        kosdaq_index_df = load_index_ohlcv("2001")  # 코스닥 지수(종가+거래량)

        # 10/20/50일 SMA는 전체 인덱스 데이터 위에서 계산 (reindex 후에도 값이 유지되도록)
        def add_sma(df: pd.DataFrame) -> pd.DataFrame:
            out = df.copy()
            if "close" not in out.columns:
                out["sma10"] = np.nan
                out["sma20"] = np.nan
                out["sma50"] = np.nan
                return out
            out["sma10"] = out["close"].rolling(10).mean()
            out["sma20"] = out["close"].rolling(20).mean()
            out["sma50"] = out["close"].rolling(50).mean()
            return out

        kospi_index_df = add_sma(kospi_index_df)
        kosdaq_index_df = add_sma(kosdaq_index_df)

        def compute_market_ad_line_and_volatility(universe_set: set) -> pd.DataFrame:
            import talib

            def _norm_market_date(d) -> pd.Timestamp:
                return pd.Timestamp(d).normalize()

            up_cnt = defaultdict(int)
            down_cnt = defaultdict(int)
            up_tv_sum = defaultdict(float)
            down_tv_sum = defaultdict(float)

            # AD line / CVI: 전일 대비 상승·하락을 날짜별 집계(종목 수·당일 거래대금 합), CVI는 (20일 상승 거래대금 합)/(20일 하락 거래대금 합)
            for t in universe_set:
                df = ohlcv_data.get(t)
                if df is None or df.empty:
                    continue
                if "close" not in df.columns:
                    continue

                # DB에서 date가 datetime이 아닐 수 있어서 안전하게 변환
                if getattr(df.index, "dtype", None) is not None and not np.issubdtype(df.index.dtype, np.datetime64):
                    df = df.copy()
                    df.index = pd.to_datetime(df.index, errors="coerce")
                    df = df[~df.index.isna()].sort_index()
                else:
                    df = df.sort_index()

                if len(df) < 2:
                    continue

                diff = df["close"].diff()
                pos_mask = diff.to_numpy() > 0
                neg_mask = diff.to_numpy() < 0
                close_s = pd.to_numeric(df["close"], errors="coerce")
                vol_s = (
                    pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
                    if "volume" in df.columns
                    else pd.Series(0.0, index=df.index)
                )
                tv_s = (close_s * vol_s).replace([np.inf, -np.inf], np.nan).fillna(0.0)

                if pos_mask.any():
                    pos_dates = diff.index[pos_mask]
                    uniq_dates, counts = np.unique(pos_dates.to_numpy(), return_counts=True)
                    for d, c in zip(uniq_dates, counts):
                        up_cnt[_norm_market_date(d)] += int(c)
                    for dt, vv in zip(diff.index[pos_mask], tv_s[pos_mask].to_numpy()):
                        if np.isfinite(vv):
                            up_tv_sum[_norm_market_date(dt)] += float(vv)

                if neg_mask.any():
                    neg_dates = diff.index[neg_mask]
                    uniq_dates, counts = np.unique(neg_dates.to_numpy(), return_counts=True)
                    for d, c in zip(uniq_dates, counts):
                        down_cnt[_norm_market_date(d)] += int(c)
                    for dt, vv in zip(diff.index[neg_mask], tv_s[neg_mask].to_numpy()):
                        if np.isfinite(vv):
                            down_tv_sum[_norm_market_date(dt)] += float(vv)

            all_dates = sorted(
                {_norm_market_date(d) for d in (set(up_cnt.keys()) | set(down_cnt.keys()) | set(up_tv_sum.keys()) | set(down_tv_sum.keys()))}
            )
            if len(all_dates) == 0:
                return pd.DataFrame(
                    columns=[
                        "ad_line",
                        "net_ad_daily",
                        "market_avg_volatility",
                        "mcclellan",
                        "zweig_ma10_pct",
                        "cvi",
                        "cvi_daily",
                        "adr",
                    ]
                )

            idx = pd.DatetimeIndex(all_dates)
            breadth_df = pd.DataFrame(index=idx)
            breadth_df["up"] = [up_cnt.get(d, 0) for d in idx]
            breadth_df["down"] = [down_cnt.get(d, 0) for d in idx]

            ad_daily = breadth_df["up"] - breadth_df["down"]
            breadth_df["ad_line"] = ad_daily.cumsum()
            breadth_df["net_ad_daily"] = ad_daily

            uv = np.array([up_tv_sum.get(d, 0.0) for d in idx], dtype=float)
            dv = np.array([down_tv_sum.get(d, 0.0) for d in idx], dtype=float)
            breadth_df["cvi_daily"] = np.where(dv > 0.0, uv / dv, np.nan)
            uv_s = pd.Series(uv, index=breadth_df.index, dtype=float)
            dv_s = pd.Series(dv, index=breadth_df.index, dtype=float)
            roll_up = uv_s.rolling(20, min_periods=1).sum()
            roll_dn = dv_s.rolling(20, min_periods=1).sum()
            breadth_df["cvi"] = np.where(roll_dn.to_numpy(dtype=float) > 0.0, roll_up.to_numpy(dtype=float) / roll_dn.to_numpy(dtype=float), np.nan)
            # ADR: (20일 상승 종목 수 합 ÷ 20일 하락 종목 수 합) × 100 (하락 합 0이면 미정의)
            up_ct = breadth_df["up"].astype(float)
            dn_ct = breadth_df["down"].astype(float)
            roll_up_cnt = up_ct.rolling(20, min_periods=20).sum()
            roll_dn_cnt = dn_ct.rolling(20, min_periods=20).sum()
            breadth_df["adr"] = np.where(
                roll_dn_cnt.to_numpy(dtype=float) > 0.0,
                roll_up_cnt.to_numpy(dtype=float) / roll_dn_cnt.to_numpy(dtype=float) * 100.0,
                np.nan,
            )
            # McClellan Oscillator: EMA19(Net AD) − EMA39(Net AD), 전체 구간에서 계산 후 tail
            ema19 = ad_daily.ewm(span=19, adjust=False).mean()
            ema39 = ad_daily.ewm(span=39, adjust=False).mean()
            breadth_df["mcclellan"] = ema19 - ema39

            # Zweig Breadth Thrust 입력: Adv/(Adv+Dec) 의 10일 SMA (%), 전체 구간에서 rolling 후 tail
            _ad_denom = breadth_df["up"].to_numpy(dtype=float) + breadth_df["down"].to_numpy(dtype=float)
            breadth_df["adv_ratio"] = np.where(_ad_denom > 0, breadth_df["up"].to_numpy(dtype=float) / _ad_denom, np.nan)
            breadth_df["zweig_ma10_pct"] = breadth_df["adv_ratio"].rolling(10, min_periods=10).mean() * 100.0

            # 시장 평균 변동성: OHLCV에서 ATR14/Close를 직접 계산 (외부 indicators 모듈 불필요)
            breadth_df = breadth_df.tail(BREADTH_WINDOW)
            target_index = pd.DatetimeIndex(breadth_df.index)
            vol_sum = np.zeros(len(target_index), dtype=float)
            vol_cnt = np.zeros(len(target_index), dtype=int)

            for t in universe_set:
                df = ohlcv_data.get(t)
                if df is None or df.empty:
                    continue
                if not all(c in df.columns for c in ("high", "low", "close")):
                    continue

                if getattr(df.index, "dtype", None) is not None and not np.issubdtype(df.index.dtype, np.datetime64):
                    df = df.copy()
                    df.index = pd.to_datetime(df.index, errors="coerce")
                    df = df[~df.index.isna()].sort_index()
                else:
                    df = df.sort_index()

                if len(df) < 15:
                    continue

                hi = pd.to_numeric(df["high"], errors="coerce").astype(float).values
                lo = pd.to_numeric(df["low"], errors="coerce").astype(float).values
                cl = pd.to_numeric(df["close"], errors="coerce").astype(float).values
                atr14 = talib.ATR(hi, lo, cl, timeperiod=14)
                close_s = pd.Series(cl, index=df.index)
                ratio = pd.Series(atr14, index=df.index) / close_s.replace(0, np.nan)
                ratio = ratio.replace([np.inf, -np.inf], np.nan)
                ratio.index = pd.to_datetime(ratio.index, errors="coerce").normalize()
                ratio = ratio[~ratio.index.isna()]
                ratio = ratio.reindex(target_index)
                mask = ratio.notna().to_numpy()
                if mask.any():
                    vol_sum[mask] += ratio.to_numpy()[mask]
                    vol_cnt[mask] += 1

            breadth_df["market_avg_volatility"] = np.where(vol_cnt > 0, vol_sum / vol_cnt, np.nan)
            return breadth_df[
                ["ad_line", "net_ad_daily", "market_avg_volatility", "mcclellan", "zweig_ma10_pct", "cvi", "cvi_daily", "adr"]
            ]

        kospi_df = compute_market_ad_line_and_volatility(kospi_set)
        kosdaq_df = compute_market_ad_line_and_volatility(kosdaq_set)
        # 지수 데이터는 AD line 구간(마지막 BREADTH_WINDOW)으로 정렬해서 x축을 동일하게 맞춤
        kospi_index_aligned = kospi_index_df.reindex(kospi_df.index)
        kosdaq_index_aligned = kosdaq_index_df.reindex(kosdaq_df.index)

        def zweig_breadth_thrust_flags(zweig_ma10_pct: pd.Series) -> pd.Series:
            """Zweig: 10일 SMA(Adv/(Adv+Dec))%가 10거래일 안에 40% 미만 구간을 거쳐 61.5% 초과(당일 최초 돌파)."""
            s = zweig_ma10_pct.astype(float)
            out = pd.Series(False, index=s.index)
            vals = s.to_numpy()
            n = len(s)
            for i in range(n):
                v = vals[i]
                if np.isnan(v) or v <= 61.5:
                    continue
                prev = vals[i - 1] if i > 0 else np.nan
                if not np.isnan(prev) and prev > 61.5:
                    continue
                lo = max(0, i - 9)
                segment = vals[lo:i]
                if segment.size == 0:
                    continue
                if np.nanmin(segment) < 40.0:
                    out.iloc[i] = True
            return out

        kospi_bt = zweig_breadth_thrust_flags(kospi_df["zweig_ma10_pct"])
        kosdaq_bt = zweig_breadth_thrust_flags(kosdaq_df["zweig_ma10_pct"])

        def _apply_common_layout(f, title_text: str, layout_height: int = 980, max_xaxis_row: int = 2):
            f.update_layout(
                title=title_text,
                plot_bgcolor="white",
                paper_bgcolor="white",
                font=dict(color="black", size=11),
                legend=dict(
                    bgcolor="rgba(255, 255, 255, 0.9)",
                    bordercolor="rgba(128, 128, 128, 0.5)",
                    borderwidth=1,
                    font=dict(size=10),
                ),
                hovermode="x unified",
                height=layout_height,
                margin=dict(l=45, r=45, t=70, b=35),
            )
            for rr in range(1, max_xaxis_row + 1):
                for cc in (1, 2):
                    f.update_xaxes(
                        row=rr,
                        col=cc,
                        tickformat="%Y-%m-%d",
                        rangeslider_visible=False,
                        rangebreaks=[dict(bounds=["sat", "mon"]), dict(values=holidays)],
                    )

        # Page 1
        fig_page1 = make_subplots(
            rows=3,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.08,
            horizontal_spacing=0.07,
            specs=[
                [{"secondary_y": True}, {"secondary_y": True}],
                [{}, {}],
                [{}, {}],
            ],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 AD line (상승−하락 누적) + SMA20",
                "코스닥 AD line (상승−하락 누적) + SMA20",
                "코스피 Net AD (상승−하락, 일별)",
                "코스닥 Net AD (상승−하락, 일별)",
            ],
        )
        fig_page1.add_trace(
            go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)),
            row=1,
            col=1,
        )
        fig_page1.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page1.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page1.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page1.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page1.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page1.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page1.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page1.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page1.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        fig_page1.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["ad_line"], mode="lines", name="코스피 AD line", line=dict(color="#FF6B35", width=2.5)), row=2, col=1)
        fig_page1.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["ad_line"].rolling(20).mean(), mode="lines", name="코스피 AD line SMA20", line=dict(color="#FF6B35", width=2.2, dash="dash")), row=2, col=1)
        fig_page1.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["ad_line"], mode="lines", name="코스닥 AD line", line=dict(color="#FF6B35", width=2.5)), row=2, col=2)
        fig_page1.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["ad_line"].rolling(20).mean(), mode="lines", name="코스닥 AD line SMA20", line=dict(color="#FF6B35", width=2.2, dash="dash")), row=2, col=2)

        _nad_k = kospi_df["net_ad_daily"].fillna(0.0).to_numpy(dtype=float)
        _nad_q = kosdaq_df["net_ad_daily"].fillna(0.0).to_numpy(dtype=float)
        _col_k = [("#e57373" if v >= 0 else "#64b5f6") for v in _nad_k]
        _col_q = [("#e57373" if v >= 0 else "#64b5f6") for v in _nad_q]
        fig_page1.add_trace(
            go.Bar(x=kospi_df.index, y=_nad_k, name="코스피 Net AD(일별)", marker_color=_col_k, showlegend=False),
            row=3,
            col=1,
        )
        fig_page1.add_trace(
            go.Bar(x=kosdaq_df.index, y=_nad_q, name="코스닥 Net AD(일별)", marker_color=_col_q, showlegend=False),
            row=3,
            col=2,
        )

        _apply_common_layout(fig_page1, "Page 1: (KOSPI/KOSDAQ Index) x (KOSPI/KOSDAQ AD line)", layout_height=1280, max_xaxis_row=3)
        fig_page1.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page1.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page1.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page1.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page1.update_yaxes(title_text="AD line", row=2, col=1)
        fig_page1.update_yaxes(title_text="AD line", row=2, col=2)
        fig_page1.update_yaxes(title_text="상승−하락(종목 수)", row=3, col=1)
        fig_page1.update_yaxes(title_text="상승−하락(종목 수)", row=3, col=2)

        # Page 2
        fig_page2 = make_subplots(
            rows=2,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.10,
            horizontal_spacing=0.07,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 시장 평균 변동성 (ATR14/Close) + Vol SMA20",
                "코스닥 시장 평균 변동성 (ATR14/Close) + Vol SMA20",
            ],
        )
        fig_page2.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page2.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page2.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page2.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page2.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page2.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page2.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page2.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page2.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page2.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        fig_page2.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["market_avg_volatility"], mode="lines", name="코스피 변동성(ATR14/Close)", line=dict(color="#2ECC71", width=2)), row=2, col=1)
        fig_page2.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["market_avg_volatility"].rolling(20).mean(), mode="lines", name="코스피 Vol SMA20", line=dict(color="#2ECC71", width=2.2, dash="dash")), row=2, col=1)
        fig_page2.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["market_avg_volatility"], mode="lines", name="코스닥 변동성(ATR14/Close)", line=dict(color="#2ECC71", width=2)), row=2, col=2)
        fig_page2.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["market_avg_volatility"].rolling(20).mean(), mode="lines", name="코스닥 Vol SMA20", line=dict(color="#2ECC71", width=2.2, dash="dash")), row=2, col=2)

        _apply_common_layout(fig_page2, "Page 2: (KOSPI/KOSDAQ Index) x (KOSPI/KOSDAQ Volatility)")
        fig_page2.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page2.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page2.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page2.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page2.update_yaxes(title_text="ATR14/Close", row=2, col=1)
        fig_page2.update_yaxes(title_text="ATR14/Close", row=2, col=2)

        # Page 3
        fig_page3 = make_subplots(
            rows=2,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.10,
            horizontal_spacing=0.07,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 맥클레란 오실레이터 (EMA19−EMA39 of Net AD)",
                "코스닥 맥클레란 오실레이터 (EMA19−EMA39 of Net AD)",
            ],
        )
        fig_page3.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page3.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page3.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page3.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page3.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page3.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page3.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page3.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page3.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page3.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        _zero_k = np.zeros(len(kospi_df.index), dtype=float)
        fig_page3.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["mcclellan"], mode="lines", name="코스피 맥클레란", line=dict(color="#E67E22", width=2)), row=2, col=1)
        fig_page3.add_trace(go.Scatter(x=kospi_df.index, y=_zero_k, mode="lines", name="0", line=dict(color="rgba(0,0,0,0.35)", width=1, dash="dash"), showlegend=False), row=2, col=1)

        _zero_q = np.zeros(len(kosdaq_df.index), dtype=float)
        fig_page3.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["mcclellan"], mode="lines", name="코스닥 맥클레란", line=dict(color="#E67E22", width=2)), row=2, col=2)
        fig_page3.add_trace(go.Scatter(x=kosdaq_df.index, y=_zero_q, mode="lines", name="0", line=dict(color="rgba(0,0,0,0.35)", width=1, dash="dash"), showlegend=False), row=2, col=2)

        _apply_common_layout(fig_page3, "Page 3: (KOSPI/KOSDAQ Index) x (McClellan Oscillator)")
        fig_page3.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page3.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page3.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page3.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page3.update_yaxes(title_text="McClellan", row=2, col=1)
        fig_page3.update_yaxes(title_text="McClellan", row=2, col=2)

        # Page 4
        fig_page4 = make_subplots(
            rows=2,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.10,
            horizontal_spacing=0.07,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 Zweig Breadth Thrust (10일 SMA of Adv/(Adv+Dec), %)",
                "코스닥 Zweig Breadth Thrust (10일 SMA of Adv/(Adv+Dec), %)",
            ],
        )
        fig_page4.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page4.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page4.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page4.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page4.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page4.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page4.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page4.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page4.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page4.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        ix_k = kospi_df.index
        _y40k = np.full(len(ix_k), 40.0, dtype=float)
        _y615k = np.full(len(ix_k), 61.5, dtype=float)
        fig_page4.add_trace(go.Scatter(x=ix_k, y=kospi_df["zweig_ma10_pct"], mode="lines", name="코스피 Zweig MA10%", line=dict(color="#8E44AD", width=2.2)), row=2, col=1)
        fig_page4.add_trace(go.Scatter(x=ix_k, y=_y40k, mode="lines", name="40%", line=dict(color="rgba(200,80,80,0.7)", width=1, dash="dot"), showlegend=True), row=2, col=1)
        fig_page4.add_trace(go.Scatter(x=ix_k, y=_y615k, mode="lines", name="61.5%", line=dict(color="rgba(46,125,50,0.75)", width=1, dash="dot"), showlegend=True), row=2, col=1)
        if kospi_bt.any():
            fig_page4.add_trace(
                go.Scatter(
                    x=kospi_df.index[kospi_bt],
                    y=kospi_df.loc[kospi_bt, "zweig_ma10_pct"],
                    mode="markers",
                    marker=dict(size=11, symbol="star", color="#C0392B", line=dict(width=1, color="#fff")),
                    name="코스피 BT 신호",
                ),
                row=2,
                col=1,
            )

        ix_q = kosdaq_df.index
        _y40q = np.full(len(ix_q), 40.0, dtype=float)
        _y615q = np.full(len(ix_q), 61.5, dtype=float)
        fig_page4.add_trace(go.Scatter(x=ix_q, y=kosdaq_df["zweig_ma10_pct"], mode="lines", name="코스닥 Zweig MA10%", line=dict(color="#8E44AD", width=2.2)), row=2, col=2)
        fig_page4.add_trace(go.Scatter(x=ix_q, y=_y40q, mode="lines", name="40%", line=dict(color="rgba(200,80,80,0.7)", width=1, dash="dot"), showlegend=False), row=2, col=2)
        fig_page4.add_trace(go.Scatter(x=ix_q, y=_y615q, mode="lines", name="61.5%", line=dict(color="rgba(46,125,50,0.75)", width=1, dash="dot"), showlegend=False), row=2, col=2)
        if kosdaq_bt.any():
            fig_page4.add_trace(
                go.Scatter(
                    x=kosdaq_df.index[kosdaq_bt],
                    y=kosdaq_df.loc[kosdaq_bt, "zweig_ma10_pct"],
                    mode="markers",
                    marker=dict(size=11, symbol="star", color="#C0392B", line=dict(width=1, color="#fff")),
                    name="코스닥 BT 신호",
                ),
                row=2,
                col=2,
            )

        _apply_common_layout(fig_page4, "Page 4: Zweig Breadth Thrust (Zweig)")
        fig_page4.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page4.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page4.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page4.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page4.update_yaxes(title_text="Breadth %", row=2, col=1)
        fig_page4.update_yaxes(title_text="Breadth %", row=2, col=2)

        # Page 5
        fig_page5 = make_subplots(
            rows=3,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.08,
            horizontal_spacing=0.07,
            specs=[
                [{"secondary_y": True}, {"secondary_y": True}],
                [{}, {}],
                [{}, {}],
            ],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 CVI (최근 20일 상승 거래대금 합 ÷ 하락 합) + SMA20",
                "코스닥 CVI (최근 20일 상승 거래대금 합 ÷ 하락 합) + SMA20",
                "코스피 일별 거래대금 비 (당일 상승 합 ÷ 하락 합)",
                "코스닥 일별 거래대금 비 (당일 상승 합 ÷ 하락 합)",
            ],
        )
        fig_page5.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page5.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page5.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page5.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page5.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page5.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page5.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page5.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page5.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page5.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        fig_page5.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["cvi"], mode="lines", name="코스피 CVI", line=dict(color="#16A085", width=2.2)), row=2, col=1)
        fig_page5.add_trace(go.Scatter(x=kospi_df.index, y=kospi_df["cvi"].rolling(20).mean(), mode="lines", name="코스피 CVI SMA20", line=dict(color="#16A085", width=2, dash="dash")), row=2, col=1)
        fig_page5.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["cvi"], mode="lines", name="코스닥 CVI", line=dict(color="#16A085", width=2.2)), row=2, col=2)
        fig_page5.add_trace(go.Scatter(x=kosdaq_df.index, y=kosdaq_df["cvi"].rolling(20).mean(), mode="lines", name="코스닥 CVI SMA20", line=dict(color="#16A085", width=2, dash="dash")), row=2, col=2)

        _cd_k = kospi_df["cvi_daily"].to_numpy(dtype=float)
        _cd_q = kosdaq_df["cvi_daily"].to_numpy(dtype=float)
        _cc_k = [("#bdbdbd" if not np.isfinite(v) else ("#e57373" if v >= 1.0 else "#64b5f6")) for v in _cd_k]
        _cc_q = [("#bdbdbd" if not np.isfinite(v) else ("#e57373" if v >= 1.0 else "#64b5f6")) for v in _cd_q]
        fig_page5.add_trace(
            go.Bar(x=kospi_df.index, y=_cd_k, name="코스피 일별 순거래대금", marker_color=_cc_k, showlegend=False),
            row=3,
            col=1,
        )
        fig_page5.add_trace(
            go.Bar(x=kosdaq_df.index, y=_cd_q, name="코스닥 일별 순거래대금", marker_color=_cc_q, showlegend=False),
            row=3,
            col=2,
        )

        _apply_common_layout(fig_page5, "Page 5: CVI (20d adv/decl trade-value ratio)", layout_height=1280, max_xaxis_row=3)
        fig_page5.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page5.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page5.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page5.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page5.update_yaxes(title_text="CVI (20일 비)", row=2, col=1)
        fig_page5.update_yaxes(title_text="CVI (20일 비)", row=2, col=2)
        fig_page5.update_yaxes(title_text="일별 비 (상승÷하락)", row=3, col=1)
        fig_page5.update_yaxes(title_text="일별 비 (상승÷하락)", row=3, col=2)

        # Page 6
        def compute_close_above_sma_ratio(universe_set: set, target_index: pd.DatetimeIndex, windows=(5, 10, 20)) -> pd.DataFrame:
            if target_index is None or len(target_index) == 0:
                cols = [f"above_sma{w}_pct" for w in windows]
                return pd.DataFrame(index=pd.DatetimeIndex([]), columns=cols)

            idx = pd.DatetimeIndex(target_index)
            true_cnt = {w: np.zeros(len(idx), dtype=float) for w in windows}
            valid_cnt = {w: np.zeros(len(idx), dtype=float) for w in windows}

            for t in universe_set:
                df = ohlcv_data.get(t)
                if df is None or df.empty or "close" not in df.columns:
                    continue

                if getattr(df.index, "dtype", None) is not None and not np.issubdtype(df.index.dtype, np.datetime64):
                    df = df.copy()
                    df.index = pd.to_datetime(df.index, errors="coerce")
                    df = df[~df.index.isna()].sort_index()
                else:
                    df = df.sort_index()

                if df.empty:
                    continue

                close = pd.to_numeric(df["close"], errors="coerce")
                if close.isna().all():
                    continue

                for w in windows:
                    sma = close.rolling(w, min_periods=w).mean()
                    aligned_close = close.reindex(idx)
                    aligned_sma = sma.reindex(idx)
                    m = aligned_close.notna() & aligned_sma.notna()
                    if not m.any():
                        continue
                    v = (aligned_close[m] > aligned_sma[m]).to_numpy(dtype=float)
                    pos = np.flatnonzero(m.to_numpy())
                    true_cnt[w][pos] += v
                    valid_cnt[w][pos] += 1.0

            out = pd.DataFrame(index=idx)
            for w in windows:
                out[f"above_sma{w}_pct"] = np.where(valid_cnt[w] > 0, true_cnt[w] / valid_cnt[w] * 100.0, np.nan)
            return out

        def compute_close_below_sma_ratio(universe_set: set, target_index: pd.DatetimeIndex, windows=(10,)) -> pd.DataFrame:
            if target_index is None or len(target_index) == 0:
                cols = [f"below_sma{w}_pct" for w in windows]
                return pd.DataFrame(index=pd.DatetimeIndex([]), columns=cols)

            idx = pd.DatetimeIndex(target_index)
            true_cnt = {w: np.zeros(len(idx), dtype=float) for w in windows}
            valid_cnt = {w: np.zeros(len(idx), dtype=float) for w in windows}

            for t in universe_set:
                df = ohlcv_data.get(t)
                if df is None or df.empty or "close" not in df.columns:
                    continue

                if getattr(df.index, "dtype", None) is not None and not np.issubdtype(df.index.dtype, np.datetime64):
                    df = df.copy()
                    df.index = pd.to_datetime(df.index, errors="coerce")
                    df = df[~df.index.isna()].sort_index()
                else:
                    df = df.sort_index()

                if df.empty:
                    continue

                close = pd.to_numeric(df["close"], errors="coerce")
                if close.isna().all():
                    continue

                for w in windows:
                    sma = close.rolling(w, min_periods=w).mean()
                    aligned_close = close.reindex(idx)
                    aligned_sma = sma.reindex(idx)
                    m = aligned_close.notna() & aligned_sma.notna()
                    if not m.any():
                        continue
                    v = (aligned_close[m] < aligned_sma[m]).to_numpy(dtype=float)
                    pos = np.flatnonzero(m.to_numpy())
                    true_cnt[w][pos] += v
                    valid_cnt[w][pos] += 1.0

            out = pd.DataFrame(index=idx)
            for w in windows:
                out[f"below_sma{w}_pct"] = np.where(valid_cnt[w] > 0, true_cnt[w] / valid_cnt[w] * 100.0, np.nan)
            return out

        kospi_above = compute_close_above_sma_ratio(kospi_set, kospi_df.index, windows=(5, 10, 20))
        kosdaq_above = compute_close_above_sma_ratio(kosdaq_set, kosdaq_df.index, windows=(5, 10, 20))
        kospi_below10 = compute_close_below_sma_ratio(kospi_set, kospi_df.index, windows=(10,))
        kosdaq_below10 = compute_close_below_sma_ratio(kosdaq_set, kosdaq_df.index, windows=(10,))

        fig_page6 = make_subplots(
            rows=4,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.07,
            horizontal_spacing=0.07,
            specs=[
                [{"secondary_y": True}, {"secondary_y": True}],
                [{}, {}],
                [{}, {}],
                [{}, {}],
            ],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피: 종가 > SMA5 비중(%)",
                "코스닥: 종가 > SMA5 비중(%)",
                "코스피: 종가 > SMA10 비중(%)",
                "코스닥: 종가 > SMA10 비중(%)",
                "코스피: 종가 > SMA20 비중(%)",
                "코스닥: 종가 > SMA20 비중(%)",
            ],
        )
        fig_page6.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page6.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page6.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page6.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page6.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page6.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page6.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page6.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page6.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page6.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        ix_k2 = kospi_above.index
        ix_q2 = kosdaq_above.index
        fig_page6.add_trace(
            go.Scatter(x=ix_k2, y=kospi_above["above_sma5_pct"], mode="lines", name="코스피 > SMA5 (%)", line=dict(color="#2ECC71", width=2.2)),
            row=2,
            col=1,
        )
        fig_page6.add_trace(
            go.Scatter(x=ix_q2, y=kosdaq_above["above_sma5_pct"], mode="lines", name="코스닥 > SMA5 (%)", line=dict(color="#2ECC71", width=2.2)),
            row=2,
            col=2,
        )
        fig_page6.add_trace(
            go.Scatter(x=ix_k2, y=kospi_above["above_sma10_pct"], mode="lines", name="코스피 > SMA10 (%)", line=dict(color="#F39C12", width=2.2)),
            row=3,
            col=1,
        )
        fig_page6.add_trace(
            go.Scatter(x=ix_q2, y=kosdaq_above["above_sma10_pct"], mode="lines", name="코스닥 > SMA10 (%)", line=dict(color="#F39C12", width=2.2)),
            row=3,
            col=2,
        )
        fig_page6.add_trace(
            go.Scatter(x=ix_k2, y=kospi_above["above_sma20_pct"], mode="lines", name="코스피 > SMA20 (%)", line=dict(color="#E74C3C", width=2.2)),
            row=4,
            col=1,
        )
        fig_page6.add_trace(
            go.Scatter(x=ix_q2, y=kosdaq_above["above_sma20_pct"], mode="lines", name="코스닥 > SMA20 (%)", line=dict(color="#E74C3C", width=2.2)),
            row=4,
            col=2,
        )

        _apply_common_layout(fig_page6, "Page 6: Close > SMA breadth (%) — by window", layout_height=1680, max_xaxis_row=4)
        fig_page6.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page6.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page6.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page6.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page6.update_yaxes(title_text="비중(%)", row=2, col=1, range=[0, 100])
        fig_page6.update_yaxes(title_text="비중(%)", row=2, col=2, range=[0, 100])
        fig_page6.update_yaxes(title_text="비중(%)", row=3, col=1, range=[0, 100])
        fig_page6.update_yaxes(title_text="비중(%)", row=3, col=2, range=[0, 100])
        fig_page6.update_yaxes(title_text="비중(%)", row=4, col=1, range=[0, 100])
        fig_page6.update_yaxes(title_text="비중(%)", row=4, col=2, range=[0, 100])

        # Page 7: 종가>SMA5 비중 vs 종가<SMA10 비중 (종목수/전체 = %와 동일 스케일)
        fig_page7 = make_subplots(
            rows=2,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.10,
            horizontal_spacing=0.07,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피: 종가>SMA5 · 종가<SMA10 비중(%)",
                "코스닥: 종가>SMA5 · 종가<SMA10 비중(%)",
            ],
        )
        fig_page7.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page7.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page7.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page7.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page7.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page7.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page7.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page7.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page7.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page7.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        ix_k7 = kospi_above.index
        fig_page7.add_trace(
            go.Scatter(x=ix_k7, y=kospi_above["above_sma5_pct"], mode="lines", name="코스피 종가>SMA5 (%)", line=dict(color="#2ECC71", width=2.2)),
            row=2,
            col=1,
        )
        fig_page7.add_trace(
            go.Scatter(x=kospi_below10.index, y=kospi_below10["below_sma10_pct"], mode="lines", name="코스피 종가<SMA10 (%)", line=dict(color="#9B59B6", width=2.2)),
            row=2,
            col=1,
        )

        ix_q7 = kosdaq_above.index
        fig_page7.add_trace(
            go.Scatter(x=ix_q7, y=kosdaq_above["above_sma5_pct"], mode="lines", name="코스닥 종가>SMA5 (%)", line=dict(color="#2ECC71", width=2.2)),
            row=2,
            col=2,
        )
        fig_page7.add_trace(
            go.Scatter(x=kosdaq_below10.index, y=kosdaq_below10["below_sma10_pct"], mode="lines", name="코스닥 종가<SMA10 (%)", line=dict(color="#9B59B6", width=2.2)),
            row=2,
            col=2,
        )

        _apply_common_layout(fig_page7, "Page 7: Close>SMA5 vs Close<SMA10 breadth (%)")
        fig_page7.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page7.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page7.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page7.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page7.update_yaxes(title_text="비중(%)", row=2, col=1, range=[0, 100])
        fig_page7.update_yaxes(title_text="비중(%)", row=2, col=2, range=[0, 100])

        # Page 8: 종가 기준 120일 신고가/신저가 종목수
        def compute_120d_high_low_counts(universe_set: set, target_index: pd.DatetimeIndex, window: int = 120) -> pd.DataFrame:
            """각 날짜별로 '종가가 최근 window 거래일 최고/최저'인 종목 수를 계산."""
            idx = pd.DatetimeIndex(target_index)
            if len(idx) == 0:
                return pd.DataFrame(index=idx, columns=["high_cnt", "low_cnt"], dtype=float)

            high_cnt = np.zeros(len(idx), dtype=int)
            low_cnt = np.zeros(len(idx), dtype=int)

            for t in universe_set:
                df = ohlcv_data.get(t)
                if df is None or df.empty or "close" not in df.columns:
                    continue

                if getattr(df.index, "dtype", None) is not None and not np.issubdtype(df.index.dtype, np.datetime64):
                    df = df.copy()
                    df.index = pd.to_datetime(df.index, errors="coerce")
                    df = df[~df.index.isna()].sort_index()
                else:
                    df = df.sort_index()

                close = pd.to_numeric(df["close"], errors="coerce")
                if close.isna().all():
                    continue

                roll_max = close.rolling(window, min_periods=window).max()
                roll_min = close.rolling(window, min_periods=window).min()

                c_al = close.reindex(idx)
                mx_al = roll_max.reindex(idx)
                mn_al = roll_min.reindex(idx)

                valid = c_al.notna() & mx_al.notna() & mn_al.notna()
                if not valid.any():
                    continue

                hi = (c_al[valid] >= mx_al[valid]).to_numpy(dtype=bool)
                lo = (c_al[valid] <= mn_al[valid]).to_numpy(dtype=bool)
                pos = np.flatnonzero(valid.to_numpy())
                high_cnt[pos] += hi.astype(int)
                low_cnt[pos] += lo.astype(int)

            out = pd.DataFrame(index=idx)
            out["high_cnt"] = high_cnt
            out["low_cnt"] = low_cnt
            return out

        kospi_120hl = compute_120d_high_low_counts(kospi_set, kospi_df.index, window=120)
        kosdaq_120hl = compute_120d_high_low_counts(kosdaq_set, kosdaq_df.index, window=120)

        fig_page8 = make_subplots(
            rows=2,
            cols=2,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=("코스피 지수", "코스닥 지수", "코스피: 120일 신고가/신저가 종목수", "코스닥: 120일 신고가/신저가 종목수"),
            vertical_spacing=0.12,
            horizontal_spacing=0.06,
        )

        fig_page8.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page8.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page8.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page8.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page8.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page8.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page8.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page8.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page8.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page8.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        fig_page8.add_trace(go.Scatter(x=kospi_120hl.index, y=kospi_120hl["high_cnt"], mode="lines", name="코스피 120일 신고가 수", line=dict(color="#E74C3C", width=2.2)), row=2, col=1)
        fig_page8.add_trace(go.Scatter(x=kospi_120hl.index, y=kospi_120hl["low_cnt"], mode="lines", name="코스피 120일 신저가 수", line=dict(color="#2980B9", width=2.2)), row=2, col=1)
        fig_page8.add_trace(go.Scatter(x=kosdaq_120hl.index, y=kosdaq_120hl["high_cnt"], mode="lines", name="코스닥 120일 신고가 수", line=dict(color="#E74C3C", width=2.2)), row=2, col=2)
        fig_page8.add_trace(go.Scatter(x=kosdaq_120hl.index, y=kosdaq_120hl["low_cnt"], mode="lines", name="코스닥 120일 신저가 수", line=dict(color="#2980B9", width=2.2)), row=2, col=2)

        _apply_common_layout(fig_page8, "Page 8: 120-day New High / New Low counts (Close 기준)")
        fig_page8.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page8.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page8.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page8.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page8.update_yaxes(title_text="종목 수", row=2, col=1)
        fig_page8.update_yaxes(title_text="종목 수", row=2, col=2)

        fig_page9 = make_subplots(
            rows=2,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.10,
            horizontal_spacing=0.07,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 ADR (20일 누적 상승÷하락 × 100) + SMA10",
                "코스닥 ADR (20일 누적 상승÷하락 × 100) + SMA10",
            ],
        )
        fig_page9.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page9.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page9.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page9.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page9.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page9.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page9.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page9.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page9.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page9.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        ix_adr_k = kospi_df.index
        nk = len(ix_adr_k)
        adr_k_s = pd.to_numeric(kospi_df["adr"], errors="coerce")
        adr_k_sma10 = adr_k_s.rolling(10, min_periods=1).mean()
        fig_page9.add_trace(go.Scatter(x=ix_adr_k, y=adr_k_s, mode="lines", name="코스피 ADR", line=dict(color="#F39C12", width=2.2)), row=2, col=1)
        fig_page9.add_trace(go.Scatter(x=ix_adr_k, y=adr_k_sma10, mode="lines", name="코스피 ADR SMA10", line=dict(color="#E74C3C", width=2, dash="dash")), row=2, col=1)
        _ref_k100 = np.full(nk, 100.0, dtype=float)
        _ref_k125 = np.full(nk, 125.0, dtype=float)
        _ref_k75 = np.full(nk, 75.0, dtype=float)
        fig_page9.add_trace(
            go.Scatter(x=ix_adr_k, y=_ref_k100, mode="lines", name="균형 100", line=dict(color="rgba(60, 60, 60, 0.55)", width=1.2, dash="dot")),
            row=2,
            col=1,
        )
        fig_page9.add_trace(
            go.Scatter(x=ix_adr_k, y=_ref_k125, mode="lines", name="과열 125", line=dict(color="rgba(192, 57, 43, 0.45)", width=1, dash="dot")),
            row=2,
            col=1,
        )
        fig_page9.add_trace(
            go.Scatter(x=ix_adr_k, y=_ref_k75, mode="lines", name="침체 75", line=dict(color="rgba(41, 128, 185, 0.45)", width=1, dash="dot")),
            row=2,
            col=1,
        )

        ix_adr_q = kosdaq_df.index
        nq = len(ix_adr_q)
        adr_q_s = pd.to_numeric(kosdaq_df["adr"], errors="coerce")
        adr_q_sma10 = adr_q_s.rolling(10, min_periods=1).mean()
        fig_page9.add_trace(go.Scatter(x=ix_adr_q, y=adr_q_s, mode="lines", name="코스닥 ADR", line=dict(color="#F39C12", width=2.2)), row=2, col=2)
        fig_page9.add_trace(go.Scatter(x=ix_adr_q, y=adr_q_sma10, mode="lines", name="코스닥 ADR SMA10", line=dict(color="#E74C3C", width=2, dash="dash")), row=2, col=2)
        fig_page9.add_trace(
            go.Scatter(x=ix_adr_q, y=np.full(nq, 100.0, dtype=float), mode="lines", name="균형 100", line=dict(color="rgba(60, 60, 60, 0.55)", width=1.2, dash="dot"), showlegend=False),
            row=2,
            col=2,
        )
        fig_page9.add_trace(
            go.Scatter(x=ix_adr_q, y=np.full(nq, 125.0, dtype=float), mode="lines", name="과열 125", line=dict(color="rgba(192, 57, 43, 0.45)", width=1, dash="dot"), showlegend=False),
            row=2,
            col=2,
        )
        fig_page9.add_trace(
            go.Scatter(x=ix_adr_q, y=np.full(nq, 75.0, dtype=float), mode="lines", name="침체 75", line=dict(color="rgba(41, 128, 185, 0.45)", width=1, dash="dot"), showlegend=False),
            row=2,
            col=2,
        )

        _apply_common_layout(fig_page9, "Page 9: ADR (20d sum advances / declines × 100)")
        fig_page9.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page9.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page9.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page9.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page9.update_yaxes(title_text="ADR (×100)", row=2, col=1)
        fig_page9.update_yaxes(title_text="ADR (×100)", row=2, col=2)

        # Page 10: 모멘텀 속도 (Pine: ta.roc(close,N)/N)
        kospi_mom_df = _compute_momentum_speed(kospi_index_df["close"]).reindex(kospi_df.index)
        kosdaq_mom_df = _compute_momentum_speed(kosdaq_index_df["close"]).reindex(kosdaq_df.index)

        fig_page10 = make_subplots(
            rows=2,
            cols=2,
            shared_xaxes=False,
            vertical_spacing=0.10,
            horizontal_spacing=0.07,
            specs=[[{"secondary_y": True}, {"secondary_y": True}], [{}, {}]],
            subplot_titles=[
                "코스피 지수 (10/20/50 SMA + 거래량)",
                "코스닥 지수 (10/20/50 SMA + 거래량)",
                "코스피 모멘텀 속도 (ROC÷기간, %/일)",
                "코스닥 모멘텀 속도 (ROC÷기간, %/일)",
            ],
        )
        fig_page10.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["close"], mode="lines", name="코스피 지수", line=dict(color="#1F77B4", width=2.5)), row=1, col=1)
        fig_page10.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#4C9AFF", width=1.8, dash="dot")), row=1, col=1)
        fig_page10.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#1E88E5", width=1.8, dash="dot")), row=1, col=1)
        fig_page10.add_trace(go.Scatter(x=kospi_index_aligned.index, y=kospi_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#1565C0", width=1.8, dash="dot")), row=1, col=1)
        fig_page10.add_trace(go.Bar(x=kospi_index_aligned.index, y=kospi_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=1, secondary_y=True)

        fig_page10.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["close"], mode="lines", name="코스닥 지수", line=dict(color="#9467BD", width=2.5)), row=1, col=2)
        fig_page10.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma10"], mode="lines", name="SMA10", line=dict(color="#B388FF", width=1.8, dash="dot")), row=1, col=2)
        fig_page10.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma20"], mode="lines", name="SMA20", line=dict(color="#7E57C2", width=1.8, dash="dot")), row=1, col=2)
        fig_page10.add_trace(go.Scatter(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["sma50"], mode="lines", name="SMA50", line=dict(color="#5E35B1", width=1.8, dash="dot")), row=1, col=2)
        fig_page10.add_trace(go.Bar(x=kosdaq_index_aligned.index, y=kosdaq_index_aligned["volume"], name="Volume", marker=dict(color="rgba(128, 128, 128, 0.35)")), row=1, col=2, secondary_y=True)

        ix_mom_k = kospi_df.index
        ix_mom_q = kosdaq_df.index
        for _mom_len in _MOMENTUM_SPEED_PERIODS:
            _mom_col = _MOMENTUM_SPEED_COLORS[_mom_len]
            _mom_label = f"{_mom_len}일 모멘텀%/{_mom_len}"
            fig_page10.add_trace(
                go.Scatter(
                    x=ix_mom_k,
                    y=kospi_mom_df[f"mom_{_mom_len}"],
                    mode="lines",
                    name=f"코스피 {_mom_label}",
                    line=dict(color=_mom_col, width=2),
                ),
                row=2,
                col=1,
            )
            fig_page10.add_trace(
                go.Scatter(
                    x=ix_mom_q,
                    y=kosdaq_mom_df[f"mom_{_mom_len}"],
                    mode="lines",
                    name=f"코스닥 {_mom_label}",
                    line=dict(color=_mom_col, width=2),
                ),
                row=2,
                col=2,
            )
        _zero_mom_k = np.zeros(len(ix_mom_k), dtype=float)
        _zero_mom_q = np.zeros(len(ix_mom_q), dtype=float)
        fig_page10.add_trace(
            go.Scatter(x=ix_mom_k, y=_zero_mom_k, mode="lines", name="0", line=dict(color="rgba(128,128,128,0.55)", width=1, dash="dash"), showlegend=False),
            row=2,
            col=1,
        )
        fig_page10.add_trace(
            go.Scatter(x=ix_mom_q, y=_zero_mom_q, mode="lines", name="0", line=dict(color="rgba(128,128,128,0.55)", width=1, dash="dash"), showlegend=False),
            row=2,
            col=2,
        )

        _apply_common_layout(fig_page10, "Page 10: (KOSPI/KOSDAQ Index) x (Momentum Speed)")
        fig_page10.update_yaxes(title_text="지수", row=1, col=1, secondary_y=False)
        fig_page10.update_yaxes(title_text="거래량", row=1, col=1, secondary_y=True)
        fig_page10.update_yaxes(title_text="지수", row=1, col=2, secondary_y=False)
        fig_page10.update_yaxes(title_text="거래량", row=1, col=2, secondary_y=True)
        fig_page10.update_yaxes(title_text="모멘텀 (%/일)", row=2, col=1)
        fig_page10.update_yaxes(title_text="모멘텀 (%/일)", row=2, col=2)

        # Save as a single HTML with 10 pages (toggle)
        output_base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
        output_dir = os.path.join(output_base, date.today().strftime("%Y-%m-%d"))
        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "market_AD_line.html")

        div1 = pio.to_html(fig_page1, full_html=False, include_plotlyjs="cdn")
        div2 = pio.to_html(fig_page2, full_html=False, include_plotlyjs=False)
        div3 = pio.to_html(fig_page3, full_html=False, include_plotlyjs=False)
        div4 = pio.to_html(fig_page4, full_html=False, include_plotlyjs=False)
        div5 = pio.to_html(fig_page5, full_html=False, include_plotlyjs=False)
        div6 = pio.to_html(fig_page6, full_html=False, include_plotlyjs=False)
        div7 = pio.to_html(fig_page7, full_html=False, include_plotlyjs=False)
        div8 = pio.to_html(fig_page8, full_html=False, include_plotlyjs=False)
        div9 = pio.to_html(fig_page9, full_html=False, include_plotlyjs=False)
        div10 = pio.to_html(fig_page10, full_html=False, include_plotlyjs=False)

        def _dash_page_block(n: int, plot_div: str) -> str:
            desc = html.escape(MARKET_DASH_PAGE_DESCS.get(n, ""))
            return '<div class="page-desc">' + desc + '</div>' + chr(10) + plot_div

        pb1 = _dash_page_block(1, div1)
        pb2 = _dash_page_block(2, div2)
        pb3 = _dash_page_block(3, div3)
        pb4 = _dash_page_block(4, div4)
        pb5 = _dash_page_block(5, div5)
        pb6 = _dash_page_block(6, div6)
        pb7 = _dash_page_block(7, div7)
        pb8 = _dash_page_block(8, div8)
        pb9 = _dash_page_block(9, div9)
        pb10 = _dash_page_block(10, div10)

        html_doc_dash = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>KRX Market Dashboard</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background: #fff; color: #111; }}
    .topbar {{ position: sticky; top: 0; z-index: 10; background: rgba(255,255,255,0.95); border-bottom: 1px solid #e6e6e6; padding: 10px 14px; display:flex; flex-wrap: wrap; gap:10px; align-items:center; }}
    .btn {{ border: 1px solid #d0d0d0; background: #f7f7f7; padding: 8px 10px; border-radius: 8px; cursor: pointer; font-weight: 600; }}
    .btn.active {{ background: #111; color: #fff; border-color: #111; }}
    .wrap {{ padding: 10px 12px 18px 12px; }}
    .page {{ display: none; }}
    .page.active {{ display: block; }}
    .page-desc {{ font-size: 13px; color: #444; line-height: 1.5; margin: 0 0 12px 0; padding: 10px 12px; background: #f7f7f7; border-radius: 8px; border: 1px solid #eee; }}
    .hint {{ margin-left:auto; font-size: 12px; color:#555; }}
  </style>
</head>
<body>
  <div class="topbar">
    <button id="b1" class="btn active" onclick="showPage(1)">1페이지: 지수 x AD line</button>
    <button id="b2" class="btn" onclick="showPage(2)">2페이지: 지수 x 변동성</button>
    <button id="b3" class="btn" onclick="showPage(3)">3페이지: 지수 x 맥클레란</button>
    <button id="b4" class="btn" onclick="showPage(4)">4페이지: Zweig Breadth Thrust</button>
    <button id="b5" class="btn" onclick="showPage(5)">5페이지: CVI(거래대금)</button>
    <button id="b6" class="btn" onclick="showPage(6)">6페이지: 종가>SMA5/10/20 비중</button>
    <button id="b7" class="btn" onclick="showPage(7)">7페이지: 종가>SMA5 · &lt;SMA10 비중</button>
    <button id="b8" class="btn" onclick="showPage(8)">8페이지: 120일 신고가/신저가 종목수</button>
    <button id="b9" class="btn" onclick="showPage(9)">9페이지: ADR</button>
    <button id="b10" class="btn" onclick="showPage(10)">10페이지: 모멘텀 속도</button>
    <div class="hint">파일: {os.path.basename(out_path)}</div>
  </div>
  <div class="wrap">
    <div id="p1" class="page active">{pb1}</div>
    <div id="p2" class="page">{pb2}</div>
    <div id="p3" class="page">{pb3}</div>
    <div id="p4" class="page">{pb4}</div>
    <div id="p5" class="page">{pb5}</div>
    <div id="p6" class="page">{pb6}</div>
    <div id="p7" class="page">{pb7}</div>
    <div id="p8" class="page">{pb8}</div>
    <div id="p9" class="page">{pb9}</div>
    <div id="p10" class="page">{pb10}</div>
  </div>
  <script>
    function showPage(n) {{
      for (var i=1;i<=10;i++) {{
        document.getElementById('p'+i).classList.toggle('active', n===i);
        document.getElementById('b'+i).classList.toggle('active', n===i);
      }}
      window.dispatchEvent(new Event('resize'));
    }}
  </script>
</body>
</html>"""

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_doc_dash)

        if not quiet:
            try:
                import webbrowser

                webbrowser.open(out_path)
            except Exception:
                pass

            print("완료: 코스피/코스닥 지표 대시보드(10페이지, 1·5·6·9·10페이지는 다단 구성)")

    except Exception as e:
        print(f"실패: 코스피/코스닥 지수+AD line+변동성 대시보드 생성 ({type(e).__name__}: {e})")

    # 시장 판단용 별도 HTML: ATR/종가 vs 시총 분포 + 거래대금 상위 100 (코스피/코스닥)
    try:
        import talib

        def _mj_load_ohlcv_recent(tickers: list, eng, memory_cache=None, chunk_size: int = 350) -> pd.DataFrame:
            tickers = [str(t) for t in tickers]
            ref = pd.read_sql_query("SELECT MAX(date) AS d FROM krx_ohlcv", con=eng)
            ref_date = pd.to_datetime(ref.iloc[0]["d"])
            # "최근 120거래일" 확보를 위해 달력일은 여유 있게 로드 후 tail(120)로 계산한다.
            cutoff = ref_date - pd.Timedelta(days=260)
            co_str = cutoff.strftime("%Y-%m-%d")

            parts = []
            need_db = []
            if memory_cache:
                for t in tickers:
                    if t not in memory_cache:
                        need_db.append(t)
                        continue
                    df = memory_cache[t].copy()
                    df["date"] = pd.to_datetime(df["date"])
                    df = df[df["date"] >= cutoff].copy()
                    if df.empty:
                        continue
                    df = df.assign(ticker=t)
                    parts.append(df[["ticker", "date", "open", "high", "low", "close", "volume"]])
            else:
                need_db = list(tickers)

            if need_db:
                for i in range(0, len(need_db), chunk_size):
                    chunk = need_db[i : i + chunk_size]
                    ph = ",".join(["%s"] * len(chunk))
                    q = f"""
                        SELECT ticker, date, open, high, low, close, volume
                        FROM krx_ohlcv
                        WHERE date >= %s AND ticker IN ({ph})
                    """
                    bind = tuple([co_str] + [str(x) for x in chunk])
                    parts.append(pd.read_sql_query(q, con=eng, params=bind))

            n_mem = len(tickers) - len(need_db) if memory_cache else 0
            if memory_cache is not None:
                _log(f"  → 시장판단 OHLCV: 메모리 {n_mem}종목, DB {len(need_db)}종목")
            else:
                _log(f"  → 시장판단 OHLCV: DB 전량 {len(tickers)}종목")

            if not parts:
                return pd.DataFrame()
            out = pd.concat(parts, ignore_index=True)
            ohlcv_cols = ("open", "high", "low", "close", "volume")
            if "ticker" in out.columns and all(c in out.columns for c in ohlcv_cols):
                # 과거 일자의 volume=0 때문에 통째로 빼지 않음. DB 최신 거래일(ref_date) 봉만 보고 판단.
                out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
                ref_day = pd.Timestamp(ref_date).normalize()
                m_d0 = out["date"].eq(ref_day)
                if m_d0.any():
                    vol_d0 = (
                        pd.to_numeric(out.loc[m_d0, "volume"], errors="coerce").fillna(0.0).astype(float)
                    )
                    tk_d0 = out.loc[m_d0, "ticker"].astype(str)
                    bad_tickers = set(tk_d0.loc[vol_d0.eq(0.0)].unique().tolist())
                    if bad_tickers:
                        out = out[~out["ticker"].astype(str).isin(bad_tickers)].reset_index(drop=True)
                        _log(f"  → 시장판단: 최신일 volume=0·결측 종목 제외 ({len(bad_tickers)}종목)")
            return out

        def _mj_group_last_atr_tv(g: pd.DataFrame) -> pd.Series:
            g = g.sort_values("date")
            cl_s = g["close"].astype(float)
            cl = cl_s.astype(float).values
            n_b = len(g)
            sma10 = np.nan
            sma20 = np.nan
            if n_b >= 10:
                s10 = talib.SMA(cl, timeperiod=10)
                if s10 is not None and len(s10) and np.isfinite(s10[-1]):
                    sma10 = float(s10[-1])
            if n_b >= 20:
                s20 = talib.SMA(cl, timeperiod=20)
                if s20 is not None and len(s20) and np.isfinite(s20[-1]):
                    sma20 = float(s20[-1])
            if len(g) >= 2:
                prev_c = float(cl_s.iloc[-2])
                last_c0 = float(cl_s.iloc[-1])
                if np.isfinite(prev_c) and prev_c != 0 and np.isfinite(last_c0):
                    chg_pct = (last_c0 - prev_c) / prev_c * 100.0
                else:
                    chg_pct = np.nan
            else:
                chg_pct = np.nan
                last_c0 = float(cl_s.iloc[-1]) if len(g) else np.nan

            chg_pct_3d = np.nan
            if len(g) >= 4:
                _c0 = float(cl_s.iloc[-1])
                _c3 = float(cl_s.iloc[-4])
                if np.isfinite(_c3) and _c3 != 0 and np.isfinite(_c0):
                    chg_pct_3d = (_c0 - _c3) / _c3 * 100.0

            _tail3 = g.tail(min(3, max(len(g), 0)))
            _tv3 = (
                float(
                    (
                        pd.to_numeric(_tail3["close"], errors="coerce")
                        * pd.to_numeric(_tail3["volume"], errors="coerce")
                    ).sum()
                )
                if len(_tail3)
                else np.nan
            )

            _talent_120 = np.nan
            try:
                _tail120 = g.tail(min(120, max(len(g), 0)))
                if "open" in _tail120.columns and "close" in _tail120.columns:
                    _op = pd.to_numeric(_tail120["open"], errors="coerce")
                    _cl = pd.to_numeric(_tail120["close"], errors="coerce")
                    _m = _op.notna() & _cl.notna() & (_op.astype(float) > 0)
                    if _m.any():
                        _r = (_cl[_m].astype(float) / _op[_m].astype(float)) - 1.0
                        _talent_120 = float((_r >= 0.10).mean() * 100.0)
            except Exception:
                _talent_120 = np.nan

            if len(g) < 15:
                return pd.Series(
                    {
                        "last_date": g["date"].iloc[-1] if len(g) else pd.NaT,
                        "close": last_c0 if len(g) else np.nan,
                        "volume": float(pd.to_numeric(g["volume"], errors="coerce").iloc[-1]) if len(g) else np.nan,
                        "atr14": np.nan,
                        "atr_over_close": np.nan,
                        "chg_pct": chg_pct,
                        "chg_pct_3d": chg_pct_3d,
                        "tv_3d": _tv3,
                        "talent_120": _talent_120,
                        "sma10": sma10,
                        "sma20": sma20,
                    }
                )
            hi = g["high"].astype(float).values
            lo = g["low"].astype(float).values
            cl = g["close"].astype(float).values
            atr = talib.ATR(hi, lo, cl, timeperiod=14)
            last_atr = atr[-1]
            last_c = cl[-1]
            vol = float(pd.to_numeric(g["volume"], errors="coerce").iloc[-1])
            ratio = (last_atr / last_c) if last_c and not np.isnan(last_atr) and last_c != 0 else np.nan
            return pd.Series(
                {
                    "last_date": g["date"].iloc[-1],
                    "close": last_c,
                    "volume": vol,
                    "atr14": last_atr,
                    "atr_over_close": ratio,
                    "chg_pct": chg_pct,
                    "chg_pct_3d": chg_pct_3d,
                    "tv_3d": _tv3,
                    "talent_120": _talent_120,
                    "sma10": sma10,
                    "sma20": sma20,
                }
            )

        _log("\n" + "=" * 80)
        _log("시장 판단 HTML (변동성 분포 · 거래대금 상위) 생성")
        _log("=" * 80)
        _highlight_set = set([str(x) for x in (highlight_tickers or set())])
        _top100_tickers_all: set[str] = set()
        _rs_rank_map_all = _load_latest_rs_rank_map(engine)

        q_meta = """
            SELECT t.종목코드 AS ticker, t.종목명 AS name, t.시가총액 AS mcap, ts.sector_cd
            FROM krx_ticker t
            INNER JOIN krx_ticker_sector ts ON t.종목코드 = ts.ticker
            WHERE t.기준일 = (SELECT MAX(기준일) FROM krx_ticker)
              AND t.종목구분 = '보통주'
              AND ts.sector_cd IN ('1001', '2001');
        """
        meta = pd.read_sql_query(q_meta, con=engine)
        meta["ticker"] = meta["ticker"].astype(str)
        meta["mcap"] = pd.to_numeric(meta["mcap"], errors="coerce")
        try:
            q_theme = """
            SELECT ticker,
                   GROUP_CONCAT(DISTINCT theme_name ORDER BY theme_name SEPARATOR ' · ') AS theme_str
            FROM krx_theme_stock
            GROUP BY ticker
            """
            th_df = pd.read_sql_query(q_theme, con=engine)
            th_df["ticker"] = th_df["ticker"].astype(str)
            meta = meta.merge(th_df, on="ticker", how="left")
        except Exception:
            meta["theme_str"] = ""
        meta["theme_str"] = meta["theme_str"].fillna("").astype(str)
        kospi_tickers = meta.loc[meta["sector_cd"] == "1001", "ticker"].tolist()
        kosdaq_tickers = meta.loc[meta["sector_cd"] == "2001", "ticker"].tolist()
        all_tickers = kospi_tickers + kosdaq_tickers

        ohlcv_recent = _mj_load_ohlcv_recent(all_tickers, engine, memory_cache=memory_cache)
        if ohlcv_recent.empty:
            raise RuntimeError("최근 OHLCV 데이터가 없습니다.")

        ohlcv_recent["date"] = pd.to_datetime(ohlcv_recent["date"])

        # 최근 20거래일 '일별 거래대금 Top20' 표 — 코스피(1001)·코스닥(2001) 각각 해당 시장 내 상위 20
        def _fmt_tv_krw_short(v: float) -> str:
            try:
                x = float(v)
            except (TypeError, ValueError):
                return ""
            if not np.isfinite(x) or x <= 0:
                return ""
            if x >= 1e12:
                return f"{x/1e12:.2f}조"
            if x >= 1e8:
                return f"{x/1e8:.0f}억"
            if x >= 1e6:
                return f"{x/1e6:.0f}백만"
            return f"{x:,.0f}"

        def _build_daily_top20_html_for_market(tv_mkt: pd.DataFrame, dates_sorted: list) -> str:
            """dates_sorted: 코스피+코스닥 통합 OHLCV에서 뽑은 최근 20거래일(행 정렬 공통)."""
            if tv_mkt is None or tv_mkt.empty or not dates_sorted:
                return ""
            _tv_chg_map = _build_ticker_date_chg_map(
                tv_mkt.loc[:, ["ticker", "date", "close"]].copy()
            )
            cols = [f"Top{i}" for i in range(1, 21)]
            rows_20: list[dict] = []
            prev_day_set: set[str] = set()
            for d in sorted(dates_sorted):
                dkey = pd.Timestamp(d).strftime("%Y-%m-%d")
                dd = tv_mkt[tv_mkt["date"] == d].copy()
                if dd.empty:
                    row = {"date": dkey}
                    for c in cols:
                        row[c] = ""
                    rows_20.append(row)
                    prev_day_set = set()
                    continue
                dd = dd.sort_values("trading_value", ascending=False).head(20).reset_index(drop=True)
                row = {"date": dkey}
                for i in range(20):
                    if i < len(dd):
                        tk = str(dd.loc[i, "ticker"])
                        nm = str(dd.loc[i, "name"]).strip()
                        tvs = _fmt_tv_krw_short(dd.loc[i, "trading_value"])
                        label = f"{nm}({tk})" if nm else tk
                        cell_main = html.escape(label)
                        if tk in prev_day_set:
                            cell_main = f"<b>{cell_main}</b>"
                        cell_main = _krx_colored_html(cell_main, _tv_chg_map.get((tk, dkey)))
                        row[f"Top{i+1}"] = cell_main + (
                            f"<br/><span class='tv'>{html.escape(tvs)}</span>" if tvs else ""
                        )
                    else:
                        row[f"Top{i+1}"] = ""
                rows_20.append(row)
                prev_day_set = set(dd["ticker"].astype(str).tolist())
            daily_top20_df = pd.DataFrame(rows_20).set_index("date")
            return daily_top20_df.to_html(escape=False, index=True, border=0, classes="tv20 krx-sortable")

        daily_top20_k_html = ""
        daily_top20_q_html = ""
        _tv_k = pd.DataFrame()
        _tv_q = pd.DataFrame()
        _dates: list = []
        try:
            _tv = ohlcv_recent.loc[:, ["ticker", "date", "close", "volume"]].copy()
            _tv["ticker"] = _tv["ticker"].astype(str)
            _tv["close"] = pd.to_numeric(_tv["close"], errors="coerce").astype(float)
            _tv["volume"] = pd.to_numeric(_tv["volume"], errors="coerce").astype(float)
            _tv["trading_value"] = _tv["close"] * _tv["volume"]

            # 마지막 20개 거래일(데이터에 존재하는 date 기준, 코스피+코스닥 통합 캘린더)
            _dates = (
                pd.to_datetime(_tv["date"], errors="coerce")
                .dropna()
                .sort_values()
                .drop_duplicates()
                .tail(20)
                .to_list()
            )
            if _dates:
                _tv = _tv[_tv["date"].isin(_dates)].copy()
                _tv = _tv.merge(meta.loc[:, ["ticker", "name", "sector_cd"]], on="ticker", how="left")
                _tv["name"] = _tv["name"].fillna("").astype(str)
                _tv["sector_cd"] = _tv["sector_cd"].fillna("").astype(str)

                _tv_k = _tv[_tv["sector_cd"] == "1001"].copy()
                _tv_q = _tv[_tv["sector_cd"] == "2001"].copy()
                daily_top20_k_html = _build_daily_top20_html_for_market(_tv_k, _dates)
                daily_top20_q_html = _build_daily_top20_html_for_market(_tv_q, _dates)
        except Exception:
            daily_top20_k_html = ""
            daily_top20_q_html = ""
        rows = []
        for t, g in tqdm(
            ohlcv_recent.groupby("ticker"),
            desc="ATR·당일 스냅",
            total=ohlcv_recent["ticker"].nunique(),
            disable=quiet,
        ):
            r = _mj_group_last_atr_tv(g).to_dict()
            r["ticker"] = t
            rows.append(r)
        snap = pd.DataFrame(rows)
        snap = snap.merge(meta[["ticker", "name", "mcap", "sector_cd", "theme_str"]], on="ticker", how="left")
        snap = snap.dropna(subset=["sector_cd"])
        snap["ticker"] = snap["ticker"].astype(str)
        snap["sector_cd"] = snap["sector_cd"].astype(str)
        snap["rs_rank"] = snap["ticker"].map(_rs_rank_map_all)

        snap["tv_rank_prev"] = np.nan
        # 시장판단에서 쓰는 '거래대금 순위'를 외부에서 재사용할 수 있도록 저장
        _rank_df_mj = pd.DataFrame()
        try:
            snap["ticker"] = snap["ticker"].astype(str)
            snap["sector_cd"] = snap["sector_cd"].astype(str)
            snap["close"] = pd.to_numeric(snap.get("close"), errors="coerce")
            snap["volume"] = pd.to_numeric(snap.get("volume"), errors="coerce")
            snap["trading_value"] = snap["close"].astype(float) * snap["volume"].astype(float)
            snap["tv_rank"] = snap.groupby("sector_cd")["trading_value"].rank(ascending=False, method="min")

            try:
                _ud = (
                    pd.to_datetime(ohlcv_recent["date"], errors="coerce")
                    .dropna()
                    .sort_values()
                    .drop_duplicates()
                )
                if len(_ud) >= 2:
                    d_prev = pd.Timestamp(_ud.iloc[-2]).normalize()
                    _pr = ohlcv_recent[
                        pd.to_datetime(ohlcv_recent["date"], errors="coerce").dt.normalize() == d_prev
                    ].copy()
                    if not _pr.empty:
                        _pr["ticker"] = _pr["ticker"].astype(str)
                        _pr["close"] = pd.to_numeric(_pr["close"], errors="coerce")
                        _pr["volume"] = pd.to_numeric(_pr["volume"], errors="coerce")
                        _pr["tv_prev"] = _pr["close"].astype(float) * _pr["volume"].astype(float)
                        _pr = _pr.merge(meta.loc[:, ["ticker", "sector_cd"]], on="ticker", how="inner")
                        _pr["tv_rank_prev"] = _pr.groupby("sector_cd")["tv_prev"].rank(ascending=False, method="min")
                        snap["tv_rank_prev"] = snap["ticker"].map(
                            dict(zip(_pr["ticker"].astype(str), _pr["tv_rank_prev"].astype(float)))
                        )
            except Exception:
                pass

            output_base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
            out_dir = os.path.join(output_base, date.today().strftime("%Y-%m-%d"))
            os.makedirs(out_dir, exist_ok=True)
            out_rank = os.path.join(out_dir, "market_judgment_tv_rank.csv")

            _rank_df_mj = snap.loc[:, ["ticker", "sector_cd", "tv_rank", "close", "trading_value"]].copy()
            _rank_df_mj = _rank_df_mj.rename(columns={"close": "current_price", "trading_value": "trade_value"})
            _rank_df_mj.to_csv(out_rank, index=False, encoding="utf-8-sig")
        except Exception:
            pass

        def _mj_atr_scatter_mask(df: pd.DataFrame) -> pd.Series:
            ac = pd.to_numeric(df["atr_over_close"], errors="coerce")
            # 극단 변동성(저유동·스팩 등) 왜곡 완화: ATR14/종가 ≥ 0.4 는 산점도·분위선에서 제외
            return np.isfinite(ac) & (df["mcap"].fillna(0) > 0) & (ac < 0.4)

        def _mj_scatter_trace(df: pd.DataFrame, color: str) -> go.Scatter:
            d = df[_mj_atr_scatter_mask(df)].copy()
            d["mcap"] = d["mcap"].astype(float)
            return go.Scatter(
                x=d["atr_over_close"],
                y=d["mcap"],
                mode="markers",
                marker=dict(size=6, opacity=0.45, color=color),
                text=d["name"],
                customdata=np.stack([d["ticker"], d["name"], d["atr_over_close"], d["mcap"]], axis=-1).tolist(),
                hovertemplate=(
                    "티커 %{customdata[0]}<br>"
                    "종목 %{customdata[1]}<br>"
                    "ATR14/종가 %{customdata[2]:.4f}<br>"
                    "시가총액 %{customdata[3]:,.0f}<extra></extra>"
                ),
                showlegend=False,
            )

        def _mj_atr_x_range_combined(df_a: pd.DataFrame, df_b: pd.DataFrame, pad_ratio: float = 0.06):
            """코스피·코스닥 산점도에 쓰는 ATR14/종가 공통 x축 [lo, hi]."""
            xs = []
            for df in (df_a, df_b):
                m = _mj_atr_scatter_mask(df)
                if not m.any():
                    continue
                x = pd.to_numeric(df.loc[m, "atr_over_close"], errors="coerce")
                x = x[np.isfinite(x)].to_numpy(dtype=float)
                if len(x):
                    xs.extend([float(x.min()), float(x.max())])
            if not xs:
                return None
            lo, hi = min(xs), max(xs)
            if not (np.isfinite(lo) and np.isfinite(hi)):
                return None
            if hi <= lo:
                span = max(abs(lo), 1e-8) * pad_ratio
                return lo - span, hi + span
            span = (hi - lo) * pad_ratio
            return lo - span, hi + span

        def _mj_atr_ref_line_traces(df: pd.DataFrame, legend_prefix: str) -> list:
            """ATR14/종가 분포용 P25/P50/P75·평균 수직선. 범례는 짧은 이름만, 수치는 hover."""
            d = df[_mj_atr_scatter_mask(df)].copy()
            if d.empty:
                return []
            vals = pd.to_numeric(d["atr_over_close"], errors="coerce")
            vals = vals[np.isfinite(vals)].to_numpy(dtype=float)
            if len(vals) == 0:
                return []
            p25, p50, p75 = np.percentile(vals, [25, 50, 75])
            mean_v = float(np.nanmean(vals))
            y0 = float(d["mcap"].astype(float).min())
            y1 = float(d["mcap"].astype(float).max())
            if not (np.isfinite(y0) and np.isfinite(y1) and y0 > 0 and y1 > 0):
                return []
            y_pad_lo = y0 * 0.95
            y_pad_hi = y1 * 1.05
            pre = (legend_prefix or "").strip()
            n_y = 48
            y_pts = np.logspace(np.log10(y_pad_lo), np.log10(y_pad_hi), n_y)
            specs = [
                (p25, "dash", "#78909c", 1.5, "P25", p25),
                (p50, "solid", "#ef6c00", 2.0, "P50", p50),
                (p75, "dash", "#78909c", 1.5, "P75", p75),
                (mean_v, "dot", "#2e7d32", 2.0, "mean", mean_v),
            ]
            out = []
            for xv, dash, col, lw, label, stat_v in specs:
                x_pts = np.full(n_y, xv, dtype=float)
                ht = (
                    f"<b>{pre} · {label}</b><br>"
                    f"ATR14/종가 = {float(stat_v):.6f}<br>"
                    "<span style='font-size:11px'>해당 시장 종목 기준</span><extra></extra>"
                )
                out.append(
                    go.Scatter(
                        x=x_pts,
                        y=y_pts,
                        mode="lines",
                        name=f"{pre} {label}",
                        line=dict(color=col, dash=dash, width=lw),
                        hovertemplate=ht,
                        showlegend=True,
                    )
                )
            return out

        def _mj_top100_table_fig(
            df: pd.DataFrame, market_name: str, total_tv: float, total_mcap: float, total_tv_3d: float
        ) -> tuple[str, dict, pd.DataFrame]:
            df = df.copy()
            if "theme_str" not in df.columns:
                df["theme_str"] = ""
            if "chg_pct" not in df.columns:
                df["chg_pct"] = np.nan
            if "chg_pct_3d" not in df.columns:
                df["chg_pct_3d"] = np.nan
            if "tv_3d" not in df.columns:
                df["tv_3d"] = np.nan
            if "talent_120" not in df.columns:
                df["talent_120"] = np.nan
            if "sma10" not in df.columns:
                df["sma10"] = np.nan
            if "sma20" not in df.columns:
                df["sma20"] = np.nan
            if "tv_rank_prev" not in df.columns:
                df["tv_rank_prev"] = np.nan
            df["trading_value"] = df["close"].astype(float) * df["volume"].astype(float)
            # 시총순위는 시장 전체 종목 기준(거래대금 상위 100 추리기 전에 계산)
            df["mcap_rank"] = df["mcap"].rank(ascending=False, method="min")
            df = df.sort_values("trading_value", ascending=False).head(100).reset_index(drop=True)
            _top100_tickers_all.update([str(x) for x in df["ticker"].astype(str).tolist()])
            # 신고가여부(250일): D-0 종가 vs D-3 말 기준 250일 고가 rolling max
            try:
                _hf_map = _compute_250d_high_flag_map(engine, [str(x) for x in df["ticker"].astype(str).tolist()])
            except Exception:
                _hf_map = {}
            df["신고가여부"] = df["ticker"].astype(str).map(_hf_map).fillna("")
            df["tv_pct"] = np.where(total_tv > 0, df["trading_value"] / total_tv * 100.0, np.nan)
            df["mcap_pct"] = np.where((total_mcap > 0) & df["mcap"].notna(), df["mcap"].astype(float) / total_mcap * 100.0, np.nan)
            df["energy_ratio"] = np.where(
                np.isfinite(df["tv_pct"]) & np.isfinite(df["mcap_pct"]) & (df["mcap_pct"].astype(float) > 0),
                df["tv_pct"].astype(float) / df["mcap_pct"].astype(float),
                np.nan,
            )
            df["tv_3d_pct"] = np.where(
                total_tv_3d > 0,
                pd.to_numeric(df["tv_3d"], errors="coerce").astype(float) / float(total_tv_3d) * 100.0,
                np.nan,
            )
            df["energy_ratio_3d"] = np.where(
                np.isfinite(df["tv_3d_pct"])
                & np.isfinite(df["mcap_pct"])
                & (df["mcap_pct"].astype(float) > 0),
                df["tv_3d_pct"].astype(float) / df["mcap_pct"].astype(float),
                np.nan,
            )

            def _fmt_int(v):
                if pd.isna(v):
                    return ""
                return f"{int(round(v)):,}"

            def _fmt_pct(v):
                if pd.isna(v):
                    return ""
                return f"{v:.2f}%"

            def _fmt_theme_cell(th):
                th = (th or "").strip() if pd.notna(th) else ""
                if len(th) > 96:
                    return th[:95] + "…"
                return th

            def _fmt_price(v):
                if pd.isna(v):
                    return ""
                try:
                    x = float(v)
                except (TypeError, ValueError):
                    return ""
                if not np.isfinite(x):
                    return ""
                if abs(x - round(x)) < 1e-6:
                    return f"{int(round(x)):,}"
                return f"{x:,.2f}"

            def _fmt_sma_trunc(v):
                if pd.isna(v):
                    return ""
                try:
                    x = float(v)
                except (TypeError, ValueError):
                    return ""
                if not np.isfinite(x):
                    return ""
                return f"{int(np.trunc(x)):,}"

            def _mj_chg_font_color(v) -> str:
                return _krx_chg_font_color(v)

            def _mj_sma_cell_color(px: float, sma_v: float) -> str:
                """현재가 대비 SMA: 위 빨강, 아래 파랑, 동일·부족 데이터는 기본색."""
                _base = "#212121"
                if not (np.isfinite(px) and np.isfinite(sma_v)):
                    return "#757575"
                if px > sma_v:
                    return "#c62828"
                if px < sma_v:
                    return "#1565c0"
                return _base

            def _fmt_chg_pct(v):
                try:
                    if v is None or pd.isna(v):
                        return ""
                except (ValueError, TypeError):
                    pass
                try:
                    x = float(v)
                except (TypeError, ValueError):
                    return ""
                if not np.isfinite(x):
                    return ""
                return f"{x:+.2f}%"

            def _mj_lerp_hex(c0: str, c1: str, t: float) -> str:
                t = max(0.0, min(1.0, float(t)))
                c0, c1 = c0.lstrip("#"), c1.lstrip("#")
                r0, g0, b0 = int(c0[0:2], 16), int(c0[2:4], 16), int(c0[4:6], 16)
                r1, g1, b1 = int(c1[0:2], 16), int(c1[2:4], 16), int(c1[4:6], 16)
                r = int(r0 + (r1 - r0) * t)
                g = int(g0 + (g1 - g0) * t)
                b = int(b0 + (b1 - b0) * t)
                return f"#{r:02x}{g:02x}{b:02x}"

            def _mj_row_bgs_tv_vs_mcap() -> list:
                """거래대금 전체비중 > 시총 전체비중 → 붉은 톤, 반대 → 푸른 톤(상위 100 내 상대 강도)."""
                n = len(df)
                if n == 0:
                    return []
                diffs = []
                for i in range(n):
                    tv = df["tv_pct"].iloc[i]
                    mc = df["mcap_pct"].iloc[i]
                    if np.isfinite(tv) and np.isfinite(mc):
                        diffs.append(float(tv - mc))
                    else:
                        diffs.append(0.0)
                pos = [d for d in diffs if d > 0]
                neg = [-d for d in diffs if d < 0]
                dmax_pos = max(pos) if pos else 0.0
                dmax_neg = max(neg) if neg else 0.0
                if dmax_pos <= 0:
                    dmax_pos = 1.0
                if dmax_neg <= 0:
                    dmax_neg = 1.0
                neutral, red_hi, blue_hi = "#ffffff", "#ef9a9a", "#90caf9"
                out = []
                for d in diffs:
                    if d > 0:
                        out.append(_mj_lerp_hex(neutral, red_hi, min(1.0, d / dmax_pos)))
                    elif d < 0:
                        out.append(_mj_lerp_hex(neutral, blue_hi, min(1.0, (-d) / dmax_neg)))
                    else:
                        out.append(neutral)
                return out

            n_rows = len(df)
            row_bgs = _mj_row_bgs_tv_vs_mcap()
            _fc = "#212121"

            tk_raw = ["" if pd.isna(x) else str(x) for x in df["ticker"]]
            name_raw = ["" if pd.isna(x) else str(x) for x in df["name"]]
            tk_col = [f"<b>{html.escape(t)}</b>" if t in _highlight_set else html.escape(t) for t in tk_raw]
            name_col = [
                f"<b>{html.escape(n)}</b>" if tk_raw[i] in _highlight_set else html.escape(n) for i, n in enumerate(name_raw)
            ]
            rs_rank_col = [
                "" if pd.isna(x) else str(int(float(x))) for x in pd.to_numeric(df.get("rs_rank"), errors="coerce").fillna(np.nan)
            ]
            high_flag_col = ["" if pd.isna(x) else str(x) for x in df.get("신고가여부", [""] * n_rows)]
            theme_col = [_fmt_theme_cell(df["theme_str"].iloc[i]) for i in range(n_rows)]
            price_col = [_fmt_price(df["close"].iloc[i]) for i in range(n_rows)]
            atr_col = [f"{x:.4f}" if np.isfinite(x) else "" for x in df["atr_over_close"]]
            sma10_col = [_fmt_sma_trunc(df["sma10"].iloc[i]) for i in range(n_rows)]
            sma20_col = [_fmt_sma_trunc(df["sma20"].iloc[i]) for i in range(n_rows)]
            chg_col = [_fmt_chg_pct(df["chg_pct"].iloc[i]) for i in range(n_rows)]
            chg3d_col = [_fmt_chg_pct(df["chg_pct_3d"].iloc[i]) for i in range(n_rows)]
            energy_col = [
                f"{float(df['energy_ratio'].iloc[i]):.2f}" if np.isfinite(df["energy_ratio"].iloc[i]) else ""
                for i in range(n_rows)
            ]
            energy3d_col = [
                f"{float(df['energy_ratio_3d'].iloc[i]):.2f}" if np.isfinite(df["energy_ratio_3d"].iloc[i]) else ""
                for i in range(n_rows)
            ]
            talent_col = [
                f"{float(df['talent_120'].iloc[i]):.1f}" if np.isfinite(df["talent_120"].iloc[i]) else ""
                for i in range(n_rows)
            ]

            tv_prev_col = []
            rank_chg_col: list[str] = []
            rank_chg_sort: list[float | None] = []
            rank_chg_color: list[str] = []
            for i in range(n_rows):
                v = pd.to_numeric(df["tv_rank_prev"], errors="coerce").iloc[i] if "tv_rank_prev" in df.columns else np.nan
                if pd.isna(v) or not np.isfinite(float(v)):
                    tv_prev_col.append("")
                else:
                    tv_prev_col.append(str(int(float(v))))
                _rc_txt, _rc_sv, _rc_col = _krx_fmt_rank_change_cell(i + 1, v)
                rank_chg_col.append(_rc_txt)
                rank_chg_sort.append(_rc_sv)
                rank_chg_color.append(_rc_col)

            def _uf():
                return [_fc] * n_rows

            cells_font_color = [
                _uf(),
                _uf(),
                rank_chg_color,
                [_mj_chg_font_color(df["chg_pct"].iloc[i]) for i in range(n_rows)],
                [_mj_chg_font_color(df["chg_pct"].iloc[i]) for i in range(n_rows)],
                _uf(),
                _uf(),
                _uf(),
                [_mj_sma_cell_color(float(df["close"].iloc[i]), float(df["sma10"].iloc[i])) for i in range(n_rows)],
                [_mj_sma_cell_color(float(df["close"].iloc[i]), float(df["sma20"].iloc[i])) for i in range(n_rows)],
                [_mj_chg_font_color(df["chg_pct"].iloc[i]) for i in range(n_rows)],
                [_mj_chg_font_color(df["chg_pct_3d"].iloc[i]) for i in range(n_rows)],
                [_mj_energy_ratio_font_color(float(df["energy_ratio"].iloc[i])) for i in range(n_rows)],
                [_mj_energy_ratio_font_color(float(df["energy_ratio_3d"].iloc[i])) for i in range(n_rows)],
                _uf(),
                _uf(),
                _uf(),
                _uf(),
                _uf(),
                _uf(),
                _uf(),
                _uf(),
            ]

            def _sv_num_attr(x) -> str:
                try:
                    if x is None or (isinstance(x, float) and (np.isnan(x) or not np.isfinite(x))):
                        return ""
                    v = float(x)
                    if not np.isfinite(v):
                        return ""
                    return f' data-sort-value="{v}"'
                except (TypeError, ValueError):
                    return ""

            mcap_rank_str = ["" if pd.isna(x) else str(int(x)) for x in df["mcap_rank"]]
            mcap_amt_col = [_fmt_int(x) for x in df["mcap"]]
            tv_fmt_col = [_fmt_int(x) for x in df["trading_value"]]
            tv_pct_fmt = [_fmt_pct(x) for x in df["tv_pct"]]
            mcap_pct_fmt = [_fmt_pct(x) for x in df["mcap_pct"]]

            hdr_cells = [
                ("순위", "center"),
                ("전일 순위", "right"),
                ("순위 변동", "right"),
                ("종목코드", "center"),
                ("종목명", "left"),
                ("테마", "left"),
                ("현재가", "right"),
                ("ATR/종가", "right"),
                ("SMA10", "right"),
                ("SMA20", "right"),
                ("전일대비(%)", "right"),
                ("3일전 대비(%)", "right"),
                ("에너지 배율", "right"),
                ("3일 에너지 배율", "right"),
                ("Talent(%)", "right"),
                ("거래대금", "right"),
                ("거래대금 전체비중", "right"),
                ("시총순위", "right"),
                ("시가총액", "right"),
                ("시총 전체비중", "right"),
                ("RS순위", "right"),
                ("신고가여부", "center"),
            ]
            parts: list[str] = [
                f'<div class="mj-html-table-wrap"><h3 style="margin:10px 0 6px 0;font-size:1.05rem;">{html.escape(market_name)} 거래대금 상위 100</h3>',
                '<table class="krx-sortable mjtop100" border="0" cellpadding="5" cellspacing="0" '
                'style="border-collapse:collapse;width:100%;font-size:11px;background:#fff;border:1px solid #ddd;">',
                '<thead><tr style="background:#37474f;color:#fff;font-weight:600;">',
            ]
            for _h, _al in hdr_cells:
                parts.append(
                    f'<th style="text-align:{_al};padding:8px 4px;">{html.escape(_h)}</th>'
                )
            parts.append("</tr></thead><tbody>")

            for i in range(n_rows):
                bg = row_bgs[i] if i < len(row_bgs) else "#ffffff"
                parts.append(f'<tr style="background-color:{bg};">')
                parts.append(
                    f'<td style="text-align:center;color:{cells_font_color[0][i]}"{_sv_num_attr(i + 1)}>{i + 1}</td>'
                )
                _pv = (
                    pd.to_numeric(df["tv_rank_prev"], errors="coerce").iloc[i]
                    if "tv_rank_prev" in df.columns
                    else np.nan
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[1][i]}"{_sv_num_attr(_pv)}>{html.escape(tv_prev_col[i])}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[2][i]}"{_sv_num_attr(rank_chg_sort[i])}>{html.escape(rank_chg_col[i])}</td>'
                )
                parts.append(
                    f'<td style="text-align:center;color:{cells_font_color[3][i]}">{tk_col[i]}</td>'
                )
                parts.append(f'<td style="text-align:left;color:{cells_font_color[4][i]}">{name_col[i]}</td>')
                parts.append(
                    f'<td style="text-align:left;color:{cells_font_color[5][i]}">{html.escape(str(theme_col[i]))}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[6][i]}"{_sv_num_attr(df["close"].iloc[i])}>{price_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[7][i]}"{_sv_num_attr(df["atr_over_close"].iloc[i])}>{atr_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[8][i]}"{_sv_num_attr(df["sma10"].iloc[i])}>{sma10_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[9][i]}"{_sv_num_attr(df["sma20"].iloc[i])}>{sma20_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[10][i]}"{_sv_num_attr(df["chg_pct"].iloc[i])}>{chg_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[11][i]}"{_sv_num_attr(df["chg_pct_3d"].iloc[i])}>{chg3d_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[12][i]}"{_sv_num_attr(df["energy_ratio"].iloc[i])}>{energy_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[13][i]}"{_sv_num_attr(df["energy_ratio_3d"].iloc[i])}>{energy3d_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[14][i]}"{_sv_num_attr(df["talent_120"].iloc[i])}>{talent_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[15][i]}"{_sv_num_attr(df["trading_value"].iloc[i])}>{tv_fmt_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[16][i]}"{_sv_num_attr(df["tv_pct"].iloc[i])}>{tv_pct_fmt[i]}</td>'
                )
                _mr = pd.to_numeric(df["mcap_rank"], errors="coerce").iloc[i] if "mcap_rank" in df.columns else np.nan
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[17][i]}"{_sv_num_attr(_mr)}>{mcap_rank_str[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[18][i]}"{_sv_num_attr(df["mcap"].iloc[i])}>{mcap_amt_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[19][i]}"{_sv_num_attr(df["mcap_pct"].iloc[i])}>{mcap_pct_fmt[i]}</td>'
                )
                _rsv = pd.to_numeric(df.get("rs_rank"), errors="coerce").iloc[i] if "rs_rank" in df.columns else np.nan
                parts.append(
                    f'<td style="text-align:right;color:{cells_font_color[20][i]}"{_sv_num_attr(_rsv)}>{rs_rank_col[i]}</td>'
                )
                parts.append(
                    f'<td style="text-align:center;color:{cells_font_color[21][i]}">{html.escape(str(high_flag_col[i]))}</td>'
                )
                parts.append("</tr>")
            parts.append("</tbody></table></div>")
            html_tbl = "".join(parts)
            stats_out = {
                "talent_mean": float(np.nanmean(pd.to_numeric(df["talent_120"], errors="coerce").to_numpy(dtype=float)))
                if np.isfinite(pd.to_numeric(df["talent_120"], errors="coerce").to_numpy(dtype=float)).any()
                else np.nan,
                "talent_p95": float(np.nanpercentile(pd.to_numeric(df["talent_120"], errors="coerce").to_numpy(dtype=float), 95))
                if np.isfinite(pd.to_numeric(df["talent_120"], errors="coerce").to_numpy(dtype=float)).sum() >= 2
                else np.nan,
                "n": int(len(df)),
            }
            return html_tbl, stats_out, df

        sk = snap["sector_cd"] == "1001"
        sq = snap["sector_cd"] == "2001"
        df_k = snap.loc[sk].copy()
        df_q = snap.loc[sq].copy()
        if "ticker" in df_k.columns:
            df_k = df_k.drop_duplicates(subset=["ticker"], keep="first")
        if "ticker" in df_q.columns:
            df_q = df_q.drop_duplicates(subset=["ticker"], keep="first")

        total_tv_k = float((df_k["close"].astype(float) * df_k["volume"].astype(float)).sum())
        total_tv_q = float((df_q["close"].astype(float) * df_q["volume"].astype(float)).sum())
        total_mcap_k = float(df_k["mcap"].fillna(0).astype(float).sum())
        total_mcap_q = float(df_q["mcap"].fillna(0).astype(float).sum())
        total_tv_3d_k = float(pd.to_numeric(df_k["tv_3d"], errors="coerce").fillna(0).astype(float).sum())
        total_tv_3d_q = float(pd.to_numeric(df_q["tv_3d"], errors="coerce").fillna(0).astype(float).sum())

        div_tbl_k, _st_k, df_mj_k_db = _mj_top100_table_fig(df_k, "코스피", total_tv_k, total_mcap_k, total_tv_3d_k)
        div_tbl_q, _st_q, df_mj_q_db = _mj_top100_table_fig(df_q, "코스닥", total_tv_q, total_mcap_q, total_tv_3d_q)
        _prev_tv_mj: dict[str, float] = {}
        try:
            for _, _r in snap.iterrows():
                _tkp = str(_r.get("ticker", "") or "")
                _prv = _r.get("tv_rank_prev")
                if _tkp and pd.notna(_prv) and np.isfinite(float(_prv)):
                    _prev_tv_mj[_tkp] = float(_prv)
        except Exception:
            _prev_tv_mj = {}
        div_tbl_kq200_50 = _mj_html_tv200_top50_energy3d_combined(
            df_k,
            df_q,
            total_tv_k,
            total_tv_q,
            total_mcap_k,
            total_mcap_q,
            total_tv_3d_k,
            total_tv_3d_q,
            _highlight_set,
            _prev_tv_mj,
        )

        try:
            _snap_td = pd.to_datetime(snap["last_date"].max(), errors="coerce")
            _rd_db = (
                pd.Timestamp(_snap_td).normalize().date()
                if pd.notna(_snap_td)
                else _krx_max_ohlcv_trade_date(engine)
            )
            _save_krx_analysis_table(engine, "krx_analysis_mj_tv_rank", _rank_df_mj, _rd_db)
            _mj_db = pd.concat(
                [
                    _mj_top100_df_for_db(df_mj_k_db, "KOSPI"),
                    _mj_top100_df_for_db(df_mj_q_db, "KOSDAQ"),
                ],
                ignore_index=True,
            )
            _save_krx_analysis_table(engine, "krx_analysis_mj_top100", _mj_db, _rd_db)
            _mj_tv20 = pd.concat(
                [
                    _mj_daily_trade_value_top20_long_df(_tv_k, _dates, "KOSPI"),
                    _mj_daily_trade_value_top20_long_df(_tv_q, _dates, "KOSDAQ"),
                ],
                ignore_index=True,
            )
            _save_krx_analysis_table(engine, "krx_analysis_mj_daily_tv_top20", _mj_tv20, _rd_db)
        except Exception as _e_db:
            _log(f"경고: 시장판단 분석 DB 저장 실패 ({type(_e_db).__name__}: {_e_db})")

        ref_d = pd.to_datetime(snap["last_date"].max())
        output_base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
        out_dir = os.path.join(output_base, date.today().strftime("%Y-%m-%d"))
        os.makedirs(out_dir, exist_ok=True)
        out_mj = os.path.join(out_dir, "market_judgment.html")

        sub = make_subplots(
            rows=2,
            cols=1,
            subplot_titles=("코스피: ATR14/종가 vs 시가총액(로그)", "코스닥: ATR14/종가 vs 시가총액(로그)"),
            vertical_spacing=0.12,
        )
        sub.add_trace(_mj_scatter_trace(df_k, "#1f77b4"), row=1, col=1)
        for tr in _mj_atr_ref_line_traces(df_k, "코스피"):
            sub.add_trace(tr, row=1, col=1)
        sub.add_trace(_mj_scatter_trace(df_q, "#9467bd"), row=2, col=1)
        for tr in _mj_atr_ref_line_traces(df_q, "코스닥"):
            sub.add_trace(tr, row=2, col=1)
        sub.update_xaxes(title_text="ATR14/종가", row=1, col=1)
        sub.update_yaxes(type="log", title_text="시가총액 (원)", row=1, col=1)
        sub.update_xaxes(title_text="ATR14/종가", row=2, col=1)
        sub.update_yaxes(type="log", title_text="시가총액 (원)", row=2, col=1)
        _xr = _mj_atr_x_range_combined(df_k, df_q)
        if _xr is not None:
            _xlo, _xhi = _xr
            sub.update_xaxes(range=[_xlo, _xhi], row=1, col=1)
            sub.update_xaxes(range=[_xlo, _xhi], row=2, col=1)
        sub.update_layout(
            height=1040,
            template="plotly_white",
            title_text=f"시장 변동성 분포 (기준일 근접: {ref_d.date()})",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
            margin=dict(t=100),
        )

        div_scatter = pio.to_html(sub, full_html=False, include_plotlyjs="cdn")

        def _fmt_talent_stat(x):
            try:
                if x is None or (isinstance(x, float) and (np.isnan(x) or not np.isfinite(x))):
                    return "—"
            except Exception:
                return "—"
            try:
                v = float(x)
            except (TypeError, ValueError):
                return "—"
            return f"{v:.1f}%"

        def _mj_top_theme_terms_html(sub_df: pd.DataFrame, market_label: str, top_n: int = 22) -> str:
            """거래대금 상위표 '위' 요약: 테마 문자열에서 상위 테마명(불용어 제거 포함)."""
            if sub_df is None or sub_df.empty or "theme_str" not in sub_df.columns:
                return (
                    f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                    f"(본 표 <code>테마</code> 칼럼 기준): 종목 없음</p>"
                )

            _theme_stopwords_norm = {
                "등",
                "기업가치",
                "제고계획",
                "발표",
                "밸류업",
                "코리아",
                "지수",
                "주요종목",
                "value-up",
                "valueup",
            }

            def _norm_theme_token(x: str) -> str:
                s0 = (x or "").strip()
                if not s0:
                    return ""
                s0 = re.sub(r"[().,]", " ", s0)
                s0 = re.sub(r"\s+", " ", s0).strip()
                s0 = re.sub(r"\s*등\s*$", "", s0).strip()
                return s0

            def _is_stopword_token(x: str) -> bool:
                nx = _norm_theme_token(x)
                if not nx:
                    return True
                k = nx.casefold()
                if k in _theme_stopwords_norm:
                    return True
                for sw in _theme_stopwords_norm:
                    if sw and sw in k:
                        return True
                return False

            cnt: Counter[str] = Counter()
            for raw in sub_df["theme_str"].astype(str):
                s = raw.strip()
                if not s:
                    continue
                seen_row: set[str] = set()
                for part in re.split(r"\s*·\s*", s):
                    t = _norm_theme_token(part)
                    if len(t) < 1 or _is_stopword_token(t):
                        continue
                    if t not in seen_row:
                        seen_row.add(t)
                        cnt[t] += 1
                    for w in re.split(r"\s+", t):
                        w = _norm_theme_token(w)
                        if len(w) < 2 or w == t or _is_stopword_token(w):
                            continue
                        if w not in seen_row:
                            seen_row.add(w)
                            cnt[w] += 1

            if not cnt:
                return (
                    f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                    f"(본 표 <code>테마</code> 칼럼 기준): (비어 있음)</p>"
                )
            top = cnt.most_common(top_n)
            parts_esc = [f"{html.escape(name)} <span class='tc'>({c})</span>" for name, c in top]
            body = ", ".join(parts_esc)
            return (
                f'<p class="theme-summary"><strong>{html.escape(market_label)} 주요 테마</strong> '
                f"(아래 표 <code>테마</code> 칼럼에서 자주 나온 이름·단어, 괄호는 해당 시장 리스트 내 등장 종목 수):<br/>{body}</p>"
            )

        _theme_blurb_mj_k = _mj_top_theme_terms_html(df_k, "코스피 (KOSPI)")
        _theme_blurb_mj_q = _mj_top_theme_terms_html(df_q, "코스닥 (KOSDAQ)")

        html_mj = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>KRX 시장 판단</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background: #fafafa; color: #111; }}
    h1 {{ padding: 16px 20px; margin: 0; font-size: 1.25rem; background: #fff; border-bottom: 1px solid #e0e0e0; }}
    .note {{ padding: 10px 20px; font-size: 13px; color: #444; background: #fff; border-bottom: 1px solid #eee; }}
    section {{ padding: 16px 20px 24px; }}
    h2 {{ font-size: 1.05rem; margin: 20px 0 10px; }}
    .theme-summary {{ font-size: 12px; color: #333; margin: 0 0 12px 0; line-height: 1.55; max-width: 100%; }}
    .theme-summary .tc {{ color: #666; font-weight: 600; }}
    table.tv20 {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e6e6e6; }}
    table.tv20 thead th {{ position: sticky; top: 0; background: #fafafa; z-index: 1; }}
    table.tv20 th, table.tv20 td {{ border: 1px solid #eee; padding: 8px 8px; font-size: 12px; vertical-align: top; }}
    table.tv20 th {{ text-align: center; font-weight: 700; color: #222; }}
    table.tv20 td {{ min-width: 110px; }}
    table.tv20 td .tv {{ color: #666; font-size: 11px; }}
    .tv20-wrap {{ overflow: auto; max-height: 520px; border-radius: 8px; }}
    .mj-html-table-wrap {{ overflow: auto; max-height: 560px; border-radius: 8px; border: 1px solid #e6e6e6; background: #fff; }}
    table.mjtop100 th, table.mjtop100 td {{ border: 1px solid #e8e8e8; padding: 6px 5px; }}
  </style>
</head>
<body>
  <h1>시장 판단 리포트</h1>
  <div class="note">
    거래대금·ATR은 OHLCV 최신 구간 기준입니다. 시가총액·종목명은 <code>krx_ticker</code> 최신 기준일 기준입니다.
    테마는 <code>krx_theme_stock</code> 기준입니다. 거래대금 비중은 해당 시장 당일 합산 거래대금 대비, 시총 비중은 해당 시장 시가총액 합 대비입니다.
    전일대비(%)는 직전 거래일 종가 대비 최신 종가 등락률, 3일전 대비(%)는 최신 종가 대비 3거래일 이전 종가 등락률(OHLCV 최근 구간)이며, 등락 색 규칙은 전일대비와 동일합니다.
    거래대금 상위 100 표는 행마다 거래대금 전체비중이 시총 전체비중보다 크면 붉은 배경, 작으면 푸른 배경으로 강조됩니다(차이 크기는 같은 표 안에서 상대 비교).
    <strong>전일 순위</strong>는 직전 거래일 시장 내 거래대금 순위이며, <strong>RS순위·신고가여부</strong>는 표 오른쪽 끝 칼럼입니다.<br/>
    <strong>2절 요약표</strong>는 코스피·코스닥 각 당일 거래대금 상위 100종(최대 200)을 합친 뒤, 3일 에너지배율이 높은 순으로 상위 50만 표시합니다.<br/>
    SMA10·SMA20은 최근 OHLCV 종가 기준(표시는 소수점 이하 절삭)이며, 각 숫자는 현재가가 그 이동평균보다 높으면 빨간색, 낮으면 파란색입니다(같으면 검정, 데이터 부족은 회색).
    에너지배율 = 당일 거래대금 전체비중 ÷ 시총 전체비중, 3일 에너지배율 = 최근 3거래일 거래대금 합의 전체비중 ÷ 시총 전체비중(동일 시총 기준)이며, 글자색 규칙은 두 칼럼 동일(3 이상 빨강, 1.5~3 미만 노랑, 0.7~1.5 미만 초록, 0.3~0.7 미만 파랑, 0.3 미만·산출 불가는 회색)입니다.
    Talent(%) = 최근 120거래일 중 (종가 ≥ 시가×1.10) 비중이며, 거래대금 상위 100 내 요약은 코스피 평균 {_fmt_talent_stat(_st_k.get('talent_mean'))} / 상위5% {_fmt_talent_stat(_st_k.get('talent_p95'))}, 코스닥 평균 {_fmt_talent_stat(_st_q.get('talent_mean'))} / 상위5% {_fmt_talent_stat(_st_q.get('talent_p95'))} 입니다.<br/>
    변동성 분포(위 그래프): 가로축 ATR14/종가(코스피·코스닥 동일 범위), 세로축 시가총액(로그)이며, <strong>ATR14/종가 ≥ 0.4</strong>인 종목은 극단 변동성 왜곡 방지를 위해 점·분위선(P25/P50/P75)·평균 산출에서 제외합니다. 회색 점선·주황 실선·녹색 점선은 각 시장별 P25·P50·P75 및 평균 위치입니다. 구체적 수치는 해당 선에 마우스를 올리면 표시됩니다.<br/>
    일별 거래대금 Top20 표는 코스피·코스닥 각각 <strong>해당 시장 종목만</strong> 대상으로 당일 거래대금(종가×거래량) 기준 상위 20입니다. 행 날짜는 두 시장 OHLCV가 공통으로 갖는 최근 20거래일입니다.<br/>
    <strong>표 정렬</strong>: 거래대금 상위 100·일별 Top20 표에서 칼럼 헤더를 클릭하면 해당 열 기준 오름·내림차순이 번갈아 적용됩니다.<br/>
    파일: {os.path.basename(out_mj)}
  </div>
  <section>
    <h2>1. ATR14/종가 vs 시가총액 (분포)</h2>
    {div_scatter}
  </section>
  <section>
    <h2>2. 코스피·코스닥 거래대금 각 상위 100 합산 (3일 에너지배율 높은 순, 상위 50)</h2>
    <p style="margin:0 0 10px 0;font-size:12px;color:#555;line-height:1.55;">
      당일 거래대금 기준 코스피 상위 100과 코스닥 상위 100을 합친 유니버스(최대 200종)에서 3일 에너지배율이 큰 순으로 상위 50만 표시합니다. 시장·당일 거래대금 순위·전일 순위는 각 시장 보통주 전체 기준입니다.
    </p>
    {div_tbl_kq200_50}
  </section>
  <section>
    <h2>3. 코스피 — 거래대금 상위 100</h2>
    {_theme_blurb_mj_k}
    {div_tbl_k}
  </section>
  <section>
    <h2>4. 코스피 — 최근 20거래일 일별 거래대금 Top20</h2>
    <div class="note" style="margin: 0 0 10px 0;">
      코스피(보통주) 유니버스 내 당일 거래대금 상위 20입니다. 행은 최근 20거래일, 열은 Top1~Top20이며 각 칸은 <code>종목명(티커)</code>와 거래대금(조/억 단위)입니다.
      <strong>볼드</strong>: 전일 Top20에 있던 종목이 당일에도 포함된 경우(전일 대비, 시장별 표에만 적용).
    </div>
    <div class="tv20-wrap">
      {daily_top20_k_html if daily_top20_k_html else "<p style='margin:0;color:#666;font-size:12px;'>표를 만들 데이터가 부족합니다.</p>"}
    </div>
  </section>
  <section>
    <h2>5. 코스닥 — 거래대금 상위 100</h2>
    {_theme_blurb_mj_q}
    {div_tbl_q}
  </section>
  <section>
    <h2>6. 코스닥 — 최근 20거래일 일별 거래대금 Top20</h2>
    <div class="note" style="margin: 0 0 10px 0;">
      코스닥(보통주) 유니버스 내 당일 거래대금 상위 20입니다. 행·열·볼드 규칙은 위 코스피 표와 동일합니다.
    </div>
    <div class="tv20-wrap">
      {daily_top20_q_html if daily_top20_q_html else "<p style='margin:0;color:#666;font-size:12px;'>표를 만들 데이터가 부족합니다.</p>"}
    </div>
  </section>
{KRX_SORTABLE_TABLE_CSS_JS}
</body>
</html>"""

        with open(out_mj, "w", encoding="utf-8") as f:
            f.write(html_mj)

        if not quiet:
            try:
                import webbrowser

                webbrowser.open(out_mj)
            except Exception:
                pass

            print(f"완료: 시장 판단 HTML 저장: {out_mj}")
        return _top100_tickers_all, ohlcv_data

    except Exception as e:
        print(f"실패: 시장 판단 HTML 생성 ({type(e).__name__}: {e})")
        return set(), ohlcv_data


def _announce_krx_reports_from_disk(len_rs: int, len_bo: int) -> None:
    """quiet 1차 저장 직후, 세 리포트 교집합이 없을 때: 콘솔·브라우저만 한 번(재계산 없음)."""
    base = os.getenv("KRX_OUTPUT_DIR", DEFAULT_OUTPUT_BASE_DIR)
    out_dir = os.path.join(base, date.today().strftime("%Y-%m-%d"))
    p_ad = os.path.join(out_dir, "market_AD_line.html")
    p_mj = os.path.join(out_dir, "market_judgment.html")
    p_rs = os.path.join(out_dir, "rs_high_list.html")
    p_bo = os.path.join(out_dir, "breakout_120d_high_list.html")

    try:
        import webbrowser
    except Exception:
        webbrowser = None  # type: ignore[assignment]

    def _open_if_file(p: str) -> None:
        if webbrowser is None or not os.path.isfile(p):
            return
        try:
            webbrowser.open(p)
        except Exception:
            pass

    print("완료: 코스피/코스닥 지표 대시보드(10페이지, 1·5·6·10페이지는 다단 구성)")
    _open_if_file(p_ad)
    print(f"완료: 시장 판단 HTML 저장: {p_mj}")
    _open_if_file(p_mj)
    if len_rs == 0:
        print(f"완료: RS 리스트 HTML 저장(0건): {p_rs}")
    else:
        print(f"완료: RS 고분위 리스트 HTML 저장: {p_rs} (총 {len_rs}건, rs_10d>=90, RS10·20·50·120 평균 순)")
    _open_if_file(p_rs)
    if len_bo == 0:
        print(f"완료: 120일 신고가 리스트 HTML 저장(0건): {p_bo}")
    else:
        print(f"완료: 최근 5일 120일 신고가 달성 리스트 HTML 저장: {p_bo} (총 {len_bo}건)")
    _open_if_file(p_bo)
    p_vs = os.path.join(out_dir, "volatility_spread_top100.html")
    if os.path.isfile(p_vs):
        print(f"완료: 방향 우세 Top100 HTML 저장: {p_vs}")
        _open_if_file(p_vs)


def main():
    engine = _create_engine()
    ticker_list = _load_latest_ticker_list(engine)

    # 기존 코드에서는 메모리 캐시 기본 미사용
    memory_cache = None

    # 1차: 티커 교집합 산출용(콘솔·브라우저 없음). 2차만 사용자에게 보이게 함.
    print("\n[1/2] 리포트 1차(교집합 산출) 시작... (빠른 DB 쿼리 위주)")
    mj_set = _mj_fast_top100_tickers_from_db(engine, quiet=False)
    print("[1/2] 시장판단(교집합용) 산출 완료. RS/신고가 리스트 산출 중...")
    _, rs_set = write_rs_high_list_html(engine, highlight_tickers=None, quiet=True)
    _, bo_set = write_120d_breakout_list_html(engine, highlight_tickers=None, quiet=True)

    hi = {str(x) for x in (mj_set & rs_set & bo_set)}
    if hi:
        print(f"\n[2/2] 교집합 {len(hi)}종목: 최종 리포트 생성 및 브라우저 오픈 중...")
        _, ohlcv_cache = run_market_dashboard(
            engine=engine, ticker_list=ticker_list, memory_cache=memory_cache, highlight_tickers=hi, quiet=False
        )
        write_rs_high_list_html(engine, highlight_tickers=hi, quiet=False)
        write_120d_breakout_list_html(engine, highlight_tickers=hi, quiet=False)
        write_volatility_spread_top100_html(
            engine,
            ohlcv_cache,
            ticker_list,
            highlight_tickers=hi,
            quiet=False,
            memory_cache=memory_cache,
        )
    else:
        print("\n[2/2] 교집합 종목이 없어, 저장된 리포트를 안내합니다.")
        _announce_krx_reports_from_disk(len_rs=len(rs_set), len_bo=len(bo_set))


if __name__ == "__main__":
    _ap = argparse.ArgumentParser(description="KRX 마켓 분석 / HTML 리포트")
    _ap.add_argument(
        "--drop-analysis-tables",
        action="store_true",
        help="report_date 컬럼이 있는(구스키마) krx_analysis_* 테이블만 DROP IF EXISTS",
    )
    _args = _ap.parse_args()
    if _args.drop_analysis_tables:
        print("KRX_DB_URL(미설정 시 기본값)로 접속합니다.")
        print("다음 테이블 중 **report_date 컬럼이 있는 것만** DROP 합니다:")
        for _n in sorted(_KRX_ANALYSIS_TABLES):
            print(f"  - {_n}")
        drop_krx_analysis_tables(_create_engine(), quiet=False)
        sys.exit(0)
    main()

