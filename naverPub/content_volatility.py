"""
마켓 변동성 차트 (별도 market.md/html, 시장별 섹션) — Plotly + kaleido PNG.
0) 지수 캔들 + MA20/50/120 + MACD·이격도·BB Width (최근 250거래일)
1) ATR14/종가 vs 시가총액 산점도 (거래대금 상위20 라벨)
2) 시총가중 시장 변동성 + Vol SMA10/20 + 지수 캔들(우축)
3) 모멘텀 속도 (ROC(N)% / N, 20·50일) + 지수 캔들(우축)

VPS 한글: apt install fonts-nanum fonts-nanum-coding 후 FONT_FAMILY=NanumGothic
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import FONT_FAMILY, OUTPUT_WIDTH_PX, RENDER_SCALE
from db import engine

log = logging.getLogger("naverPub.content_volatility")

ATR_N = 14
VOL_WINDOW = 250
MA_WARMUP = 120
ATR_OC_MAX = 0.4
INDEX_CODES = {"KOSPI": "1001", "KOSDAQ": "2001"}
MARKETS = ("KOSPI", "KOSDAQ")
MARKET_LABELS = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}
MOMENTUM_PERIODS = (20, 50)
SCATTER_TOP_TV = 20

# 밝은 원색 팔레트
C_UP = "#E53935"
C_DOWN = "#1E88E5"
C_GREEN = "#43A047"
C_PURPLE = "#8E24AA"
C_ORANGE = "#FB8C00"
C_TEAL = "#00ACC1"
C_BLUE = "#1E88E5"
C_GRID = "#E0E0E0"
C_ZERO = "#9E9E9E"

H_CANDLE = 1100
H_SINGLE = 720
H_DUAL = 780

FONT_STACK = f"{FONT_FAMILY}, Malgun Gothic, NanumGothic, Nanum Gothic, sans-serif"


def _trading_dates(eng, end: date, n: int) -> list[date]:
    df = pd.read_sql(
        "SELECT DISTINCT date FROM ohlcv WHERE date <= %s ORDER BY date DESC LIMIT %s",
        eng,
        params=(end, n),
    )
    if df.empty:
        return []
    return sorted(pd.to_datetime(df["date"]).dt.date.tolist())


def _load_market_ohlcv(eng, market: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """
        SELECT ticker, date, name, high, low, close, mcap, trading_value
        FROM ohlcv
        WHERE market = %s AND date >= %s AND date <= %s
        """,
        eng,
        params=(market, start, end),
    )
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    for c in ("high", "low", "close", "mcap", "trading_value"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _fill_mcap(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "mcap" not in df.columns:
        return df
    out = df.sort_values(["ticker", "date"]).copy()
    before = int(out["mcap"].notna().sum())
    out["mcap"] = out.groupby("ticker", sort=False)["mcap"].bfill().ffill()
    after = int(out["mcap"].notna().sum())
    if after > before:
        log.info("mcap 보간: notnull %d → %d", before, after)
    return out


def _load_index(eng, index_ticker: str, start: date, end: date) -> pd.Series:
    df = pd.read_sql(
        """
        SELECT date, close FROM index_ohlcv
        WHERE ticker = %s AND date >= %s AND date <= %s
        ORDER BY date
        """,
        eng,
        params=(index_ticker, start, end),
    )
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    s = pd.to_numeric(df["close"], errors="coerce")
    s.index = pd.Index(df["date"].tolist(), name="date")
    return s


def _load_index_ohlc(eng, index_ticker: str, start: date, end: date) -> pd.DataFrame:
    df = pd.read_sql(
        """
        SELECT date, open, high, low, close FROM index_ohlcv
        WHERE ticker = %s AND date >= %s AND date <= %s
        ORDER BY date
        """,
        eng,
        params=(index_ticker, start, end),
    )
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    for c in ("open", "high", "low", "close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    if df.empty:
        return pd.DataFrame()
    df = df.rename(
        columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"}
    )
    return df.set_index("date").sort_index()[["Open", "High", "Low", "Close"]]


def _add_atr_over_close(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.sort_values(["ticker", "date"]).copy()
    prev = out.groupby("ticker", sort=False)["close"].shift(1)
    hi, lo, cl = out["high"], out["low"], out["close"]
    tr = pd.concat(
        [(hi - lo).abs(), (hi - prev).abs(), (lo - prev).abs()],
        axis=1,
    ).max(axis=1)
    out["atr14"] = tr.groupby(out["ticker"], sort=False).transform(
        lambda s: s.rolling(ATR_N, min_periods=ATR_N).mean()
    )
    out["atr_over_close"] = out["atr14"] / out["close"].replace(0, np.nan)
    out["atr_over_close"] = out["atr_over_close"].replace([np.inf, -np.inf], np.nan)
    return out


def _mcap_weighted_vol(df: pd.DataFrame) -> pd.Series:
    v = df[
        df["atr_over_close"].notna()
        & df["mcap"].notna()
        & (df["mcap"] > 0)
        & df["close"].notna()
        & (df["close"] > 0)
    ].copy()
    if v.empty:
        return pd.Series(dtype=float)
    v["wx"] = v["atr_over_close"] * v["mcap"]
    g = v.groupby("date", sort=True).agg(num=("wx", "sum"), den=("mcap", "sum"))
    out = (g["num"] / g["den"]).replace([np.inf, -np.inf], np.nan)
    out.index = pd.Index(out.index.tolist(), name="date")
    return out


def _scatter_snapshot(df: pd.DataFrame, as_of: date) -> pd.DataFrame:
    day = df[df["date"] == as_of].copy()
    if day.empty:
        return day
    ac = day["atr_over_close"]
    m = (
        ac.notna()
        & np.isfinite(ac)
        & (ac < ATR_OC_MAX)
        & day["mcap"].notna()
        & (day["mcap"] > 0)
    )
    cols = ["ticker", "name", "atr_over_close", "mcap", "trading_value"]
    cols = [c for c in cols if c in day.columns]
    return day.loc[m, cols].reset_index(drop=True)


def _compute_momentum_speed(
    close: pd.Series,
    periods: tuple[int, ...] = MOMENTUM_PERIODS,
) -> pd.DataFrame:
    s = pd.to_numeric(close, errors="coerce")
    out = pd.DataFrame(index=s.index)
    for length in periods:
        prev = s.shift(length)
        roc_pct = (s - prev) / prev.replace(0, np.nan) * 100.0
        out[f"mom_{length}"] = roc_pct / length
    return out


def _macd_series(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    s = pd.to_numeric(close, errors="coerce")
    ema_f = s.ewm(span=fast, adjust=False).mean()
    ema_s = s.ewm(span=slow, adjust=False).mean()
    macd = ema_f - ema_s
    sig = macd.ewm(span=signal, adjust=False).mean()
    return macd, sig, macd - sig


def _bb_width(close: pd.Series, window: int = 20, n_sigma: float = 2.0) -> pd.Series:
    s = pd.to_numeric(close, errors="coerce")
    mid = s.rolling(window, min_periods=window).mean()
    std = s.rolling(window, min_periods=window).std()
    return (2.0 * n_sigma * std) / mid.replace(0, np.nan)


def _to_dt_index(idx) -> pd.DatetimeIndex:
    return pd.to_datetime(pd.Index(idx))


def _day_close_from_ohlc(ohlc: Optional[pd.DataFrame], as_of: date) -> float:
    if ohlc is None or ohlc.empty or "Close" not in ohlc.columns:
        return float("nan")
    try:
        ts = pd.Timestamp(as_of)
        if isinstance(ohlc.index, pd.DatetimeIndex):
            m = ohlc.index.normalize() == ts.normalize()
            sub = ohlc.loc[m]
            if not sub.empty:
                return float(sub["Close"].iloc[-1])
        return float(ohlc["Close"].iloc[-1])
    except Exception:
        return float("nan")


def _base_layout(title: str, height: int) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=16, family=FONT_STACK), x=0.01, xanchor="left"),
        width=OUTPUT_WIDTH_PX,
        height=height,
        font=dict(family=FONT_STACK, size=12, color="#212121"),
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=64, r=56, t=56, b=56),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=11)),
        hovermode="x unified",
    )


def _style_axes(fig, rows: int = 1, *, secondary: bool = False) -> None:
    for r in range(1, rows + 1):
        fig.update_xaxes(
            showgrid=True,
            gridcolor=C_GRID,
            tickformat="%y-%m-%d",
            ticks="outside",
            row=r,
            col=1,
        )
        fig.update_yaxes(showgrid=True, gridcolor=C_GRID, row=r, col=1)
        if secondary:
            fig.update_yaxes(showgrid=False, row=r, col=1, secondary_y=True)


def _write_png(fig, out_path: Path, height: int) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.update_layout(width=OUTPUT_WIDTH_PX, height=height)
    try:
        fig.write_image(
            str(out_path),
            width=OUTPUT_WIDTH_PX,
            height=height,
            scale=max(1, int(RENDER_SCALE)),
            engine="kaleido",
        )
    except Exception as e:
        log.error("Plotly write_image 실패(%s): %s", out_path.name, e)
        raise
    return out_path


def _empty_fig(title: str, msg: str, height: int, out_path: Path) -> Path:
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_annotation(text=msg, xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
    fig.update_layout(**_base_layout(title, height))
    return _write_png(fig, out_path, height)


def _add_day_note(fig, text: str) -> None:
    if not text:
        return
    fig.add_annotation(
        text=text,
        xref="paper",
        yref="paper",
        x=1.0,
        y=-0.08 if fig.layout.height and fig.layout.height < 900 else -0.04,
        xanchor="right",
        yanchor="top",
        showarrow=False,
        font=dict(size=11, color="#37474F", family=FONT_STACK),
    )


def _candlestick_trace(ohlc: pd.DataFrame, name: str = "지수"):
    import plotly.graph_objects as go

    x = _to_dt_index(ohlc.index)
    return go.Candlestick(
        x=x,
        open=ohlc["Open"],
        high=ohlc["High"],
        low=ohlc["Low"],
        close=ohlc["Close"],
        name=name,
        increasing_line_color=C_UP,
        increasing_fillcolor=C_UP,
        decreasing_line_color=C_DOWN,
        decreasing_fillcolor=C_DOWN,
        showlegend=True,
    )


def _plot_index_candle(
    ohlc: pd.DataFrame,
    market: str,
    as_of: date,
    out_path: Path,
) -> Path:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    title = f"{market} 지수 · {as_of}"
    if ohlc is None or ohlc.empty or len(ohlc) < 5:
        return _empty_fig(title, "OHLC 데이터 없음 (백필 필요)", H_CANDLE, out_path)

    full = ohlc.sort_index().tail(VOL_WINDOW + MA_WARMUP)
    close = full["Close"]
    ma20 = close.rolling(20, min_periods=20).mean()
    ma50 = close.rolling(50, min_periods=50).mean()
    ma120 = close.rolling(120, min_periods=120).mean()
    macd, signal, hist = _macd_series(close)
    disp50 = (close / ma50.replace(0, np.nan)) * 100.0
    disp120 = (close / ma120.replace(0, np.nan)) * 100.0
    bbw = _bb_width(close)

    plot_df = full.tail(VOL_WINDOW).copy()
    x = _to_dt_index(plot_df.index)

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=[0.5, 0.2, 0.2, 0.1],
        subplot_titles=("지수 + MA", "MACD", "이격도", "BB Width"),
    )
    fig.add_trace(_candlestick_trace(plot_df, name=f"{market} 지수"), row=1, col=1)
    fig.add_trace(
        go.Scatter(x=x, y=ma20.reindex(plot_df.index), name="MA20", line=dict(color=C_ORANGE, width=1.6)),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x, y=ma50.reindex(plot_df.index), name="MA50", line=dict(color=C_GREEN, width=1.6)),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x, y=ma120.reindex(plot_df.index), name="MA120", line=dict(color=C_PURPLE, width=1.6)),
        row=1, col=1,
    )

    macd_p = macd.reindex(plot_df.index)
    sig_p = signal.reindex(plot_df.index)
    hist_p = hist.reindex(plot_df.index)
    fig.add_trace(
        go.Scatter(x=x, y=macd_p, name="MACD", line=dict(color=C_BLUE, width=1.4), legendgroup="macd"),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x, y=sig_p, name="Signal", line=dict(color=C_ORANGE, width=1.4), legendgroup="macd"),
        row=2, col=1,
    )
    hist_colors = [C_UP if (isinstance(v, (int, float)) and np.isfinite(v) and v >= 0) else C_DOWN for v in hist_p.tolist()]
    fig.add_trace(
        go.Bar(x=x, y=hist_p, name="Hist", marker_color=hist_colors, opacity=0.75, legendgroup="macd"),
        row=2, col=1,
    )
    fig.add_hline(y=0, line_dash="dot", line_color=C_ZERO, row=2, col=1)

    d50 = disp50.reindex(plot_df.index)
    d120 = disp120.reindex(plot_df.index)
    fig.add_trace(
        go.Scatter(x=x, y=d50, name="이격도50", line=dict(color=C_GREEN, width=1.4), legendgroup="disp"),
        row=3, col=1,
    )
    fig.add_trace(
        go.Scatter(x=x, y=d120, name="이격도120", line=dict(color=C_PURPLE, width=1.4), legendgroup="disp"),
        row=3, col=1,
    )
    fig.add_hline(y=100, line_dash="dash", line_color=C_ZERO, row=3, col=1)

    bbw_p = bbw.reindex(plot_df.index)
    fig.add_trace(
        go.Scatter(x=x, y=bbw_p, name="BB Width", line=dict(color=C_TEAL, width=1.5), legendgroup="bb"),
        row=4, col=1,
    )

    fig.update_layout(**_base_layout(title, H_CANDLE), xaxis_rangeslider_visible=False)
    fig.update_yaxes(title_text="지수", row=1, col=1)
    fig.update_yaxes(title_text="MACD", row=2, col=1)
    fig.update_yaxes(title_text="이격도", row=3, col=1)
    fig.update_yaxes(title_text="BB Width", row=4, col=1)
    _style_axes(fig, rows=4)

    def _last(s):
        try:
            v = float(pd.to_numeric(s, errors="coerce").dropna().iloc[-1])
            return v if np.isfinite(v) else float("nan")
        except Exception:
            return float("nan")

    day_c = float(plot_df["Close"].iloc[-1])
    note = (
        f"당일 지수 {day_c:,.2f} / MACD {_last(macd_p):.2f} · "
        f"이격도50 {_last(d50):.2f} · 이격도120 {_last(d120):.2f} · "
        f"BB Width {_last(bbw_p):.4f}"
    )
    _add_day_note(fig, note)
    return _write_png(fig, out_path, H_CANDLE)


def _plot_scatter(
    snap: pd.DataFrame,
    market: str,
    as_of: date,
    out_path: Path,
    day_all: Optional[pd.DataFrame] = None,
    index_close: Optional[float] = None,
    day_vol: Optional[float] = None,
) -> Path:
    import plotly.graph_objects as go

    title = f"{market} 시총 대비 변동성 분포 · {as_of}"
    if snap is None or snap.empty:
        return _empty_fig(title, "데이터 없음", H_SINGLE, out_path)

    fig = go.Figure()
    color = C_BLUE if market == "KOSPI" else C_UP
    fig.add_trace(
        go.Scatter(
            x=snap["atr_over_close"],
            y=snap["mcap"],
            mode="markers",
            name="종목",
            marker=dict(size=7, color=color, opacity=0.45),
            text=snap.get("name"),
            hovertemplate="%{text}<br>ATR/종가=%{x:.4f}<br>시총=%{y:,.0f}<extra></extra>",
        )
    )
    vals = pd.to_numeric(snap["atr_over_close"], errors="coerce").dropna()
    if len(vals):
        for name, xv, col, dash in (
            ("평균", float(vals.mean()), C_GREEN, "solid"),
            ("중앙값", float(vals.median()), C_ORANGE, "dash"),
            ("상위20%(P80)", float(vals.quantile(0.80)), C_UP, "dot"),
            ("하위20%(P20)", float(vals.quantile(0.20)), C_PURPLE, "dashdot"),
        ):
            if not np.isfinite(xv):
                continue
            fig.add_vline(
                x=xv,
                line_color=col,
                line_dash=dash,
                annotation_text=f"{name} {xv:.4f}",
                annotation_position="top",
            )

    # 거래대금 상위 20 라벨
    src = day_all if day_all is not None and not day_all.empty else snap
    if "trading_value" in src.columns:
        tv = pd.to_numeric(src["trading_value"], errors="coerce")
        ranked = src.loc[tv.notna() & (tv > 0)].sort_values("trading_value", ascending=False).head(SCATTER_TOP_TV)
        labeled = snap[snap["ticker"].astype(str).isin(set(ranked["ticker"].astype(str)))]
        for _, r in labeled.iterrows():
            nm = r.get("name") or r.get("ticker") or ""
            fig.add_annotation(
                x=float(r["atr_over_close"]),
                y=float(r["mcap"]),
                text=str(nm)[:10],
                showarrow=False,
                font=dict(size=9, color="#37474F"),
                xanchor="left",
                yanchor="middle",
            )

    fig.update_layout(**_base_layout(title, H_SINGLE))
    fig.update_xaxes(title_text="ATR14/종가", showgrid=True, gridcolor=C_GRID)
    fig.update_yaxes(title_text="시가총액", type="log", showgrid=True, gridcolor=C_GRID)
    fig.add_annotation(
        text="거래대금 상위 20개 종목",
        xref="paper",
        yref="paper",
        x=0.0,
        y=-0.08,
        xanchor="left",
        showarrow=False,
        font=dict(size=11, color="#546E7A"),
    )
    ix = float(index_close) if index_close is not None and np.isfinite(index_close) else float("nan")
    vv = float(day_vol) if day_vol is not None and np.isfinite(day_vol) else float("nan")
    note = f"당일 지수 {ix:,.2f} / 시총가중 변동성 {vv:.4f}"
    _add_day_note(fig, note)
    return _write_png(fig, out_path, H_SINGLE)


def _align_ohlc_to_dates(ohlc: pd.DataFrame, dates) -> pd.DataFrame:
    if ohlc is None or ohlc.empty:
        return pd.DataFrame()
    o = ohlc.copy()
    if not isinstance(o.index, pd.DatetimeIndex):
        o.index = pd.to_datetime(o.index)
    keys = pd.to_datetime(pd.Index(list(dates)))
    # map date-only
    o2 = o.copy()
    o2.index = o2.index.normalize()
    rows = []
    idx = []
    for d in keys:
        dn = pd.Timestamp(d).normalize()
        if dn in o2.index:
            row = o2.loc[dn]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            rows.append(row)
            idx.append(dn)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, index=pd.DatetimeIndex(idx))


def _plot_vol_trend(
    vol: pd.Series,
    index_ohlc: pd.DataFrame,
    market: str,
    as_of: date,
    out_path: Path,
) -> Path:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    title = f"{market} 변동성 추이 (ATR 기준) · {as_of}"
    day_ix = _day_close_from_ohlc(index_ohlc, as_of)
    day_vol = float("nan")

    if vol is None or vol.dropna().empty:
        return _empty_fig(title, "변동성 데이터 없음", H_DUAL, out_path)

    vol = vol.sort_index()
    x = _to_dt_index(vol.index)
    sma10 = vol.rolling(10, min_periods=1).mean()
    sma20 = vol.rolling(20, min_periods=1).mean()
    try:
        day_vol = float(vol.dropna().iloc[-1])
    except Exception:
        pass

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=x, y=vol.values, name="시장 변동성(시총가중)", line=dict(color=C_GREEN, width=2.2)),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=x, y=sma10.values, name="Vol SMA10", line=dict(color=C_PURPLE, width=1.6, dash="dash")),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(x=x, y=sma20.values, name="Vol SMA20", line=dict(color=C_ORANGE, width=1.6, dash="dot")),
        secondary_y=False,
    )
    ohlc_a = _align_ohlc_to_dates(index_ohlc, vol.index)
    if not ohlc_a.empty:
        fig.add_trace(_candlestick_trace(ohlc_a, name=f"{market} 지수(캔들)"), secondary_y=True)

    fig.update_layout(**_base_layout(title, H_DUAL), xaxis_rangeslider_visible=False)
    fig.update_yaxes(title_text="ATR14/종가 (시총가중)", secondary_y=False, showgrid=True, gridcolor=C_GRID)
    fig.update_yaxes(title_text=f"{market} 지수", secondary_y=True, showgrid=False)
    fig.update_xaxes(tickformat="%y-%m-%d", showgrid=True, gridcolor=C_GRID)
    _add_day_note(fig, f"당일 지수 {day_ix:,.2f} / 변동성 {day_vol:.4f}")
    return _write_png(fig, out_path, H_DUAL)


def _plot_momentum_speed(
    mom: pd.DataFrame,
    index_ohlc: pd.DataFrame,
    market: str,
    as_of: date,
    out_path: Path,
) -> Path:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    title = f"{market} 모멘텀 속도 · {as_of}"
    day_ix = _day_close_from_ohlc(index_ohlc, as_of)
    m20 = m50 = float("nan")

    if mom is None or mom.empty or not any(c in mom.columns for c in ("mom_20", "mom_50")):
        return _empty_fig(title, "데이터 없음", H_DUAL, out_path)

    x = _to_dt_index(mom.index)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_hline(y=0, line_color=C_ZERO, line_width=1.2, secondary_y=False)

    if "mom_20" in mom.columns:
        fig.add_trace(
            go.Scatter(
                x=x,
                y=mom["mom_20"],
                name="모멘텀 속도 20일",
                line=dict(color=C_GREEN, width=1.9),
            ),
            secondary_y=False,
        )
        try:
            m20 = float(mom["mom_20"].dropna().iloc[-1])
        except Exception:
            pass
    if "mom_50" in mom.columns:
        fig.add_trace(
            go.Scatter(
                x=x,
                y=mom["mom_50"],
                name="모멘텀 속도 50일",
                line=dict(color=C_PURPLE, width=1.9),
            ),
            secondary_y=False,
        )
        try:
            m50 = float(mom["mom_50"].dropna().iloc[-1])
        except Exception:
            pass

    ohlc_a = _align_ohlc_to_dates(index_ohlc, mom.index)
    if not ohlc_a.empty:
        fig.add_trace(_candlestick_trace(ohlc_a, name=f"{market} 지수(캔들)"), secondary_y=True)

    fig.update_layout(**_base_layout(title, H_DUAL), xaxis_rangeslider_visible=False)
    fig.update_yaxes(title_text="하루 평균 변화율 (%/일)", secondary_y=False, showgrid=True, gridcolor=C_GRID)
    fig.update_yaxes(title_text=f"{market} 지수", secondary_y=True, showgrid=False)
    fig.update_xaxes(tickformat="%y-%m-%d", showgrid=True, gridcolor=C_GRID)
    _add_day_note(fig, f"당일 지수 {day_ix:,.2f} / 모멘텀20 {m20:.3f} · 모멘텀50 {m50:.3f}")
    return _write_png(fig, out_path, H_DUAL)


def _write_volatility_docs(
    out_dir: Path,
    as_of: date,
    text: str,
    sections: list[dict],
    metrics: Optional[dict] = None,
) -> tuple[Path, Path]:
    from design_html import date_iso, date_kr, write_design_html

    title = f"마켓 변동성 {as_of}"
    md = [f"# {title}\n", f"{text}\n"]
    for sec in sections:
        sec_title = sec.get("title") or ""
        if sec_title:
            md.append(f"## {sec_title}\n")
        for art in sec.get("articles") or []:
            png = art.get("png")
            if png is None:
                continue
            name = Path(png).name
            t = art.get("title") or name
            md.append(f"### {t}\n\n![{t}]({name})\n")
    md_path = out_dir / "market.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    m = metrics or {}
    html_path = write_design_html(
        "volatility_design.html",
        out_dir / "market.html",
        {
            "DATE": date_iso(as_of),
            "DATE_KR": date_kr(as_of),
            "KOSPI_VOL": m.get("kospi_vol", "-"),
            "KOSPI_VOL_DAYS": m.get("kospi_vol_days", "-"),
            "KOSDAQ_VOL": m.get("kosdaq_vol", "-"),
            "KOSDAQ_VOL_DAYS": m.get("kosdaq_vol_days", "-"),
            "TOTAL_STOCKS": m.get("total_stocks", "-"),
            "KOSPI_STOCKS": m.get("kospi_stocks", "-"),
            "KOSDAQ_STOCKS": m.get("kosdaq_stocks", "-"),
        },
    )
    return md_path, html_path


def render_market_volatility(
    as_of: Optional[date] = None,
    out_dir: Optional[Path] = None,
) -> dict:
    """
    시장별(코스피→코스닥): 캔들 → 산점도 → 변동성추이 → 모멘텀속도 + market.md/html.
    """
    eng = engine()
    if as_of is None:
        d = pd.read_sql("SELECT MAX(date) AS d FROM ohlcv", eng)
        if d.empty or pd.isna(d.iloc[0]["d"]):
            return {"paths": [], "text": "OHLCV 없음", "articles": []}
        as_of = pd.to_datetime(d.iloc[0]["d"]).date()

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    from render import cleanup_publish_artifacts

    cleanup_publish_artifacts(out_dir)
    for old in out_dir.glob("05_volatility_*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    need = VOL_WINDOW + max(ATR_N, max(MOMENTUM_PERIODS), MA_WARMUP) + 10
    dates = _trading_dates(eng, as_of, need)
    if len(dates) < ATR_N + 5:
        return {
            "paths": [],
            "text": f"{as_of} 변동성 산출용 거래일 부족",
            "articles": [],
        }
    load_start = dates[0]
    plot_dates = [d for d in dates if d <= as_of][-VOL_WINDOW:]

    paths: list[Path] = []
    articles: list[dict] = []
    sections: list[dict] = []
    summaries: list[str] = []
    metrics: dict = {
        "kospi_vol": "-",
        "kospi_vol_days": "-",
        "kosdaq_vol": "-",
        "kosdaq_vol_days": "-",
        "kospi_stocks": "0",
        "kosdaq_stocks": "0",
        "total_stocks": "0",
    }

    for market in MARKETS:
        mkt_l = market.lower()
        sec_title = MARKET_LABELS.get(market, market)
        log.info("변동성 계산 %s (%s → %s)", market, load_start, as_of)

        ohlc = _load_index_ohlc(eng, INDEX_CODES[market], load_start, as_of)
        p_cd = _plot_index_candle(
            ohlc, market, as_of, out_dir / f"05_volatility_{mkt_l}_candle.png"
        )
        n_bars = 0 if ohlc is None or ohlc.empty else len(ohlc.tail(VOL_WINDOW))

        raw = _fill_mcap(_load_market_ohlcv(eng, market, load_start, as_of))
        if raw.empty:
            summaries.append(f"{market}: OHLCV 없음")
            sec_arts = [
                {
                    "key": f"05_volatility_{mkt_l}_candle",
                    "title": "지수 캔들",
                    "png": p_cd,
                    "text": "",
                    "df": ohlc.tail(VOL_WINDOW) if ohlc is not None else pd.DataFrame(),
                }
            ]
            paths.append(p_cd)
            articles.extend(sec_arts)
            sections.append({"title": sec_title, "articles": sec_arts})
            continue

        enriched = _add_atr_over_close(raw)
        snap = _scatter_snapshot(enriched, as_of)
        day_all = enriched[enriched["date"] == as_of].copy()
        vol = _mcap_weighted_vol(enriched)
        vol = vol.reindex(plot_dates)
        nn = int(vol.notna().sum())
        log.info(
            "%s 시총가중 변동성: 유효 %d/%d, min=%.4f max=%.4f",
            market,
            nn,
            len(vol),
            float(vol.min()) if nn else float("nan"),
            float(vol.max()) if nn else float("nan"),
        )
        idx = _load_index(eng, INDEX_CODES[market], load_start, as_of)
        mom = _compute_momentum_speed(idx).reindex(plot_dates)

        last_v = float(vol.dropna().iloc[-1]) if vol is not None and vol.dropna().size else np.nan
        day_ix = float("nan")
        try:
            if ohlc is not None and not ohlc.empty:
                day_ix = float(ohlc["Close"].iloc[-1])
        except Exception:
            day_ix = float("nan")
        p_sc = _plot_scatter(
            snap,
            market,
            as_of,
            out_dir / f"05_volatility_{mkt_l}_scatter.png",
            day_all=day_all,
            index_close=day_ix,
            day_vol=last_v,
        )
        p_tr = _plot_vol_trend(
            vol, ohlc, market, as_of, out_dir / f"05_volatility_{mkt_l}_trend.png"
        )
        p_mo = _plot_momentum_speed(
            mom, ohlc, market, as_of, out_dir / f"05_volatility_{mkt_l}_momentum.png"
        )
        paths.extend([p_cd, p_sc, p_tr, p_mo])
        sec_arts = [
            {
                "key": f"05_volatility_{mkt_l}_candle",
                "title": "지수 캔들",
                "png": p_cd,
                "text": "",
                "df": ohlc.tail(VOL_WINDOW) if ohlc is not None else pd.DataFrame(),
            },
            {
                "key": f"05_volatility_{mkt_l}_scatter",
                "title": "ATR14/종가 vs 시총",
                "png": p_sc,
                "text": "",
                "df": snap,
            },
            {
                "key": f"05_volatility_{mkt_l}_trend",
                "title": "변동성 추이",
                "png": p_tr,
                "text": "",
                "df": vol.to_frame("vol") if vol is not None else pd.DataFrame(),
            },
            {
                "key": f"05_volatility_{mkt_l}_momentum",
                "title": "모멘텀 속도",
                "png": p_mo,
                "text": "",
                "df": mom if mom is not None else pd.DataFrame(),
            },
        ]
        articles.extend(sec_arts)
        sections.append({"title": sec_title, "articles": sec_arts})
        n_pts = 0 if snap is None else len(snap)
        last_s = f"{last_v:.4f}" if np.isfinite(last_v) else "-"
        summaries.append(
            f"{market} 캔들 {n_bars}일, 산점도 {n_pts}종, 변동성 {last_s} (유효일 {nn})"
        )
        key = "kospi" if market == "KOSPI" else "kosdaq"
        metrics[f"{key}_vol"] = last_s
        metrics[f"{key}_vol_days"] = str(nn)
        metrics[f"{key}_stocks"] = str(n_pts)

    try:
        metrics["total_stocks"] = str(
            int(metrics["kospi_stocks"]) + int(metrics["kosdaq_stocks"])
        )
    except (TypeError, ValueError):
        metrics["total_stocks"] = "-"

    text = (
        f"{as_of} 마켓 변동성 (Plotly). "
        f"캔들: 지수 OHLC + MA20/50/120 + MACD·이격도·BB Width (최근 {VOL_WINDOW}거래일). "
        f"산점도: ATR14/종가≥{ATR_OC_MAX} 제외, 거래대금 상위{SCATTER_TOP_TV} 라벨. "
        f"추이: 시총가중 ATR14/종가 + Vol SMA10/20 + 지수 캔들(우축). "
        f"모멘텀 속도: ROC(N)%÷N (%/일) 20·50일 + 지수 캔들(우축). "
        + " / ".join(summaries)
    )
    md_path, html_path = _write_volatility_docs(
        out_dir, as_of, text, sections, metrics=metrics
    )
    cleanup_publish_artifacts(out_dir)
    log.info("변동성 문서: %s / %s (%s)", out_dir, md_path.name, html_path.name)
    return {
        "paths": paths,
        "text": text,
        "articles": articles,
        "sections": sections,
        "as_of": as_of,
        "md": md_path,
        "html": html_path,
        "out_dir": out_dir,
    }
