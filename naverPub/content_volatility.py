"""
마켓 변동성 차트 (별도 volatility.md/html, 시장별 섹션):
0) 코스피/코스닥 지수 캔들 + MA20/50/120 (최근 250거래일, MA 워밍업 120일)
1) ATR14/종가 vs 시가총액 산점도 (기준일, ATR14/종가 >= 0.4 제외, 거래대금 상위20 라벨)
2) 시총가중 시장 변동성 250거래일 추이 + Vol SMA20 + 지수(보조축)
3) 모멘텀 속도 (ROC(N)% / N, 20·50일 + 0선)

ATR14 = True Range의 14일 단순이동평균 (talib 미사용).
과거 mcap NULL은 티커별 bfill/ffill로 보간 (이관·수집에서 최신일만 시총이 있는 경우 대응).
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from config import FONT_FAMILY
from db import engine

log = logging.getLogger("naverPub.content_volatility")

ATR_N = 14
VOL_WINDOW = 250
MA_WARMUP = 120  # 캔들 MA120 워밍업 (표시 구간 앞)
ATR_OC_MAX = 0.4  # 산점도 극단값 제외
FIGSIZE = (10, 6.5)  # 산점도 기준 — 전 그래프 통일
FIG_DPI = 140
INDEX_CODES = {"KOSPI": "1001", "KOSDAQ": "2001"}
MARKETS = ("KOSPI", "KOSDAQ")
MARKET_LABELS = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}
MOMENTUM_PERIODS = (20, 50)
MOMENTUM_COLORS = {20: "#43A047", 50: "#1E88E5"}
DATE_FMT_SHORT = "%y-%m-%d"  # x축 라벨 (수평, 공간 절약)
X_TICK_LABELSIZE = 9
# 시총 로그축 한글 단위 (원)
MCAP_YTICKS = (
    (1e9, "십억"),
    (1e10, "백억"),
    (1e11, "천억"),
    (1e12, "1조"),
    (1e13, "10조"),
    (1e14, "100조"),
    (1e15, "1000조"),
)
SCATTER_TOP_TV = 20


def _setup_korean_font() -> str:
    """matplotlib 한글 폰트. config FONT_FAMILY → Malgun → Nanum 순."""
    import matplotlib.pyplot as plt
    from matplotlib import font_manager as fm

    candidates = [
        FONT_FAMILY,
        "Malgun Gothic",
        "NanumGothic",
        "Nanum Gothic",
        "AppleGothic",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    chosen = next((c for c in candidates if c in available), None)
    if chosen is None:
        for f in fm.fontManager.ttflist:
            low = (f.name or "").lower() + " " + (getattr(f, "fname", "") or "").lower()
            if "malgun" in low or "nanum" in low:
                chosen = f.name
                break
    if chosen:
        plt.rcParams["font.family"] = chosen
    plt.rcParams["axes.unicode_minus"] = False
    return chosen or "sans-serif"


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
    """
    과거일 mcap NULL 보간.
    이관/수집에서 최신일에만 시총이 있는 경우가 많아, 티커별 bfill→ffill.
    """
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
    """캔들용 OHLC. open/high/low/close 모두 유효한 날만."""
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
    df = df.set_index("date").sort_index()
    return df[["Open", "High", "Low", "Close"]]


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
    """날짜별 Σ(ATR14/종가 × 시총) / Σ시총."""
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


def _comma_formatter():
    from matplotlib.ticker import FuncFormatter

    return FuncFormatter(lambda x, _p: f"{x:,.0f}")


def _apply_horizontal_date_ticks(
    ax,
    fontsize: int = X_TICK_LABELSIZE,
    *,
    format_dates: bool = True,
) -> None:
    """x축 날짜: 회전 0도, YY-MM-DD, 눈금 개수는 유지하고 폰트만 축소."""
    from matplotlib.dates import DateFormatter

    if format_dates:
        try:
            ax.xaxis.set_major_formatter(DateFormatter(DATE_FMT_SHORT))
        except Exception:
            pass
    ax.tick_params(axis="x", labelrotation=0, labelsize=fontsize)
    for label in ax.get_xticklabels():
        label.set_rotation(0)
        label.set_horizontalalignment("center")
        label.set_fontsize(fontsize)
        t = (label.get_text() or "").strip()
        # mplfinance 등 문자열 라벨 YYYY-MM-DD → YY-MM-DD
        if len(t) >= 10 and t[4:5] == "-" and t[7:8] == "-":
            label.set_text(t[2:10])


def _apply_mcap_yticks(ax, mcap: pd.Series) -> None:
    """시총 로그축 → 십억·백억·천억·1조…"""
    if mcap is None or mcap.empty:
        return
    lo = float(np.nanmin(mcap.values))
    hi = float(np.nanmax(mcap.values))
    if not np.isfinite(lo) or not np.isfinite(hi) or lo <= 0:
        return
    ticks = [(v, lab) for v, lab in MCAP_YTICKS if lo * 0.5 <= v <= hi * 2.0]
    if not ticks:
        ticks = [(v, lab) for v, lab in MCAP_YTICKS if v >= lo * 0.1]
    if not ticks:
        return
    ax.set_yticks([v for v, _ in ticks])
    ax.set_yticklabels([lab for _, lab in ticks])


def _compute_momentum_speed(
    close: pd.Series,
    periods: tuple[int, ...] = MOMENTUM_PERIODS,
) -> pd.DataFrame:
    """Pine ROC(close,N)/N → 하루 평균 변화율(%/일). 21번 파일과 동일."""
    s = pd.to_numeric(close, errors="coerce")
    out = pd.DataFrame(index=s.index)
    for length in periods:
        prev = s.shift(length)
        roc_pct = (s - prev) / prev.replace(0, np.nan) * 100.0
        out[f"mom_{length}"] = roc_pct / length
    return out


def _plot_index_candle(
    ohlc: pd.DataFrame,
    market: str,
    as_of: date,
    out_path: Path,
) -> Path:
    """지수 캔들 + MA20/50/120. 상승 빨강 / 하락 파랑. MA는 워밍업 후 250일만 표시."""
    import matplotlib.pyplot as plt
    import mplfinance as mpf
    from matplotlib.lines import Line2D

    _setup_korean_font()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if ohlc is None or ohlc.empty or len(ohlc) < 5:
        fig, ax = plt.subplots(figsize=FIGSIZE, dpi=FIG_DPI)
        ax.text(0.5, 0.5, "OHLC 데이터 없음 (백필 필요)", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(f"{market} 지수 캔들 ({as_of})")
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        return out_path

    # MA 워밍업: 표시 250일 + 120일 과거에서 MA 계산 → 최근 250일만 플롯
    full = ohlc.sort_index()
    need_n = VOL_WINDOW + MA_WARMUP
    full = full.tail(need_n)
    ma20 = full["Close"].rolling(20, min_periods=20).mean()
    ma50 = full["Close"].rolling(50, min_periods=50).mean()
    ma120 = full["Close"].rolling(120, min_periods=120).mean()
    plot_df = full.tail(VOL_WINDOW).copy()
    ma20_p = ma20.reindex(plot_df.index)
    ma50_p = ma50.reindex(plot_df.index)
    ma120_p = ma120.reindex(plot_df.index)

    font_name = plt.rcParams.get("font.family", "sans-serif")
    if isinstance(font_name, (list, tuple)):
        font_name = font_name[0] if font_name else "sans-serif"
    mc = mpf.make_marketcolors(
        up="#d32f2f",
        down="#1565c0",
        edge="inherit",
        wick={"up": "#d32f2f", "down": "#1565c0"},
        ohlc="inherit",
    )
    style = mpf.make_mpf_style(
        marketcolors=mc,
        facecolor="white",
        gridstyle=":",
        y_on_right=False,
        rc={
            "font.family": font_name,
            "axes.unicode_minus": False,
            "figure.facecolor": "white",
        },
    )
    mav_colors = ["#ef6c00", "#2e7d32", "#5e35b1"]  # 20, 50, 120
    addplots = [
        mpf.make_addplot(ma20_p, color=mav_colors[0], width=1.4),
        mpf.make_addplot(ma50_p, color=mav_colors[1], width=1.4),
        mpf.make_addplot(ma120_p, color=mav_colors[2], width=1.4),
    ]
    fig, axes = mpf.plot(
        plot_df,
        type="candle",
        style=style,
        addplot=addplots,
        figsize=FIGSIZE,
        returnfig=True,
        datetime_format=DATE_FMT_SHORT,
        title=f"{market} 지수 캔들 + MA20/50/120 ({as_of})",
        ylabel="지수",
    )
    ax = axes[0] if isinstance(axes, (list, np.ndarray)) else axes
    ax.yaxis.set_major_formatter(_comma_formatter())
    # datetime_format으로 이미 YY-MM-DD — 회전만 강제 (카테고리 축에 DateFormatter 금지)
    targets = axes if isinstance(axes, (list, np.ndarray)) else [ax]
    for a in targets:
        try:
            _apply_horizontal_date_ticks(a, format_dates=False)
        except Exception:
            pass
    handles = [
        Line2D([0], [0], color=mav_colors[0], lw=1.5, label="MA20"),
        Line2D([0], [0], color=mav_colors[1], lw=1.5, label="MA50"),
        Line2D([0], [0], color=mav_colors[2], lw=1.5, label="MA120"),
    ]
    ax.legend(handles=handles, loc="upper left", fontsize=9, framealpha=0.92)
    fig.set_size_inches(*FIGSIZE)
    fig.set_dpi(FIG_DPI)
    fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _annotate_top_tv(ax, snap: pd.DataFrame, day_all: Optional[pd.DataFrame] = None) -> None:
    """
    당일 거래대금 상위 N 종목명 라벨 (산점도에 있는 점만).
    오프셋 순환으로 겹침 완화 (adjustText 미사용).
    """
    if snap is None or snap.empty:
        return
    # 시장 당일 전체 기준 상위 N → 산점도 교집합
    src = day_all if day_all is not None and not day_all.empty else snap
    if "trading_value" not in src.columns:
        return
    tv = pd.to_numeric(src["trading_value"], errors="coerce")
    ranked = src.loc[tv.notna() & (tv > 0)].copy()
    if ranked.empty:
        return
    ranked = ranked.sort_values("trading_value", ascending=False).head(SCATTER_TOP_TV)
    tickers = set(ranked["ticker"].astype(str))
    labeled = snap[snap["ticker"].astype(str).isin(tickers)].copy()
    if labeled.empty:
        return
    # 거래대금 순 유지
    order = {str(t): i for i, t in enumerate(ranked["ticker"].astype(str))}
    labeled["_ord"] = labeled["ticker"].astype(str).map(order)
    labeled = labeled.sort_values("_ord")

    offsets = [
        (6, 5),
        (6, -9),
        (-6, 5),
        (-6, -9),
        (10, 0),
        (-12, 0),
        (6, 12),
        (-6, 12),
    ]
    for i, (_, r) in enumerate(labeled.iterrows()):
        name = r.get("name")
        if name is None or (isinstance(name, float) and np.isnan(name)):
            name = r.get("ticker", "")
        name = str(name).strip()
        if not name:
            continue
        ox, oy = offsets[i % len(offsets)]
        oy += (i // len(offsets)) * 4
        ax.annotate(
            name,
            xy=(float(r["atr_over_close"]), float(r["mcap"])),
            xytext=(ox, oy),
            textcoords="offset points",
            fontsize=6.5,
            color="#37474f",
            alpha=0.92,
            ha="left" if ox >= 0 else "right",
            va="center",
            zorder=5,
        )


def _plot_scatter(
    snap: pd.DataFrame,
    market: str,
    as_of: date,
    out_path: Path,
    day_all: Optional[pd.DataFrame] = None,
) -> Path:
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    _setup_korean_font()
    fig, ax = plt.subplots(figsize=FIGSIZE, dpi=FIG_DPI)
    if snap is None or snap.empty:
        ax.text(0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax.transAxes)
    else:
        ax.scatter(
            snap["atr_over_close"],
            snap["mcap"],
            s=18,
            alpha=0.45,
            c="#1565c0" if market == "KOSPI" else "#c62828",
            edgecolors="none",
            zorder=2,
            label="종목",
        )
        ax.set_yscale("log")
        _apply_mcap_yticks(ax, snap["mcap"])
        vals = pd.to_numeric(snap["atr_over_close"], errors="coerce").dropna()
        if len(vals):
            stats = [
                ("평균", float(vals.mean()), "#2e7d32", "-"),
                ("중앙값", float(vals.median()), "#ef6c00", "--"),
                ("상위20%(P80)", float(vals.quantile(0.80)), "#c62828", "-."),
                ("하위20%(P20)", float(vals.quantile(0.20)), "#6a1b9a", ":"),
            ]
            handles = [
                Line2D(
                    [0],
                    [0],
                    marker="o",
                    color="w",
                    markerfacecolor="#1565c0" if market == "KOSPI" else "#c62828",
                    markersize=7,
                    label="종목",
                )
            ]
            for name, xv, color, ls in stats:
                if not np.isfinite(xv):
                    continue
                ax.axvline(xv, color=color, ls=ls, lw=1.4, zorder=3)
                handles.append(
                    Line2D([0], [0], color=color, ls=ls, lw=1.4, label=f"{name} {xv:.4f}")
                )
            ax.legend(handles=handles, loc="upper right", fontsize=8, framealpha=0.92)
        _annotate_top_tv(ax, snap, day_all=day_all)

    ax.set_xlabel("ATR14/종가")
    ax.set_ylabel("시가총액")
    ax.set_title(f"{market}: ATR14/종가 vs 시가총액 ({as_of})")
    ax.grid(True, which="both", alpha=0.25)
    fig.tight_layout()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _plot_vol_trend(
    vol: pd.Series,
    index_close: pd.Series,
    market: str,
    as_of: date,
    out_path: Path,
) -> Path:
    """왼쪽=변동성(주축), 오른쪽=지수(twinx)."""
    import matplotlib.pyplot as plt

    _setup_korean_font()
    fig, ax1 = plt.subplots(figsize=FIGSIZE, dpi=FIG_DPI)
    vol_nn = vol.dropna() if vol is not None else pd.Series(dtype=float)

    if vol is None or vol_nn.empty:
        ax1.text(
            0.5,
            0.5,
            "변동성 데이터 없음 (시총·ATR 유효일 확인)",
            ha="center",
            va="center",
            transform=ax1.transAxes,
        )
        log.warning("%s 변동성 시리즈 유효점 0", market)
    else:
        vol = vol.sort_index()
        sma20 = vol.rolling(20, min_periods=1).mean()
        (ln_vol,) = ax1.plot(
            vol.index,
            vol.values,
            color="#2e7d32",
            lw=2.0,
            label="시장 변동성(시총가중)",
            zorder=3,
        )
        (ln_sma,) = ax1.plot(
            sma20.index,
            sma20.values,
            color="#ef6c00",
            lw=1.5,
            label="Vol SMA20",
            zorder=3,
        )
        ax1.set_ylabel("ATR14/종가 (시총가중)", color="#2e7d32")
        ax1.tick_params(axis="y", labelcolor="#2e7d32")
        vmin = float(np.nanmin(vol.values))
        vmax = float(np.nanmax(vol.values))
        pad = max((vmax - vmin) * 0.12, 1e-4)
        ax1.set_ylim(max(0.0, vmin - pad), vmax + pad)

        handles = [ln_vol, ln_sma]
        labels = [ln_vol.get_label(), ln_sma.get_label()]

        if index_close is not None and not index_close.empty:
            ix = index_close.reindex(vol.index)
            if ix.notna().any():
                ax2 = ax1.twinx()
                (ln_ix,) = ax2.plot(
                    ix.index,
                    ix.values,
                    color="#546e7a",
                    lw=1.3,
                    alpha=0.9,
                    label=f"{market} 지수",
                    zorder=2,
                )
                ax2.set_ylabel(f"{market} 지수", color="#546e7a")
                ax2.tick_params(axis="y", labelcolor="#546e7a")
                ax2.yaxis.set_major_formatter(_comma_formatter())
                handles.append(ln_ix)
                labels.append(ln_ix.get_label())

        ax1.legend(handles, labels, loc="upper left", fontsize=9, framealpha=0.92)

    ax1.set_xlabel("날짜")
    ax1.set_title(f"{market}: 시장 변동성 추이 + 지수 (최근 {VOL_WINDOW}거래일, {as_of})")
    ax1.grid(True, alpha=0.25, zorder=0)
    _apply_horizontal_date_ticks(ax1)
    fig.tight_layout()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _plot_momentum_speed(
    mom: pd.DataFrame,
    index_close: pd.Series,
    market: str,
    as_of: date,
    out_path: Path,
) -> Path:
    """왼쪽=모멘텀 속도 20·50 + 0선, 오른쪽=지수(twinx)."""
    import matplotlib.pyplot as plt

    _setup_korean_font()
    fig, ax1 = plt.subplots(figsize=FIGSIZE, dpi=FIG_DPI)
    if mom is None or mom.empty or not any(c in mom.columns for c in ("mom_20", "mom_50")):
        ax1.text(0.5, 0.5, "데이터 없음", ha="center", va="center", transform=ax1.transAxes)
    else:
        ax1.axhline(0.0, color="#757575", lw=1.2, ls="-", zorder=1)
        ylim_probe = mom[["mom_20", "mom_50"]].stack().dropna()
        if not ylim_probe.empty:
            ylo = float(ylim_probe.min())
            yhi = float(ylim_probe.max())
            pad = max((yhi - ylo) * 0.1, 0.05)
            ax1.set_ylim(ylo - pad, yhi + pad)
            ax1.axhspan(0, ax1.get_ylim()[1], color="#e8f5e9", alpha=0.45, zorder=0)
            ax1.axhspan(ax1.get_ylim()[0], 0, color="#ffebee", alpha=0.45, zorder=0)

        handles = []
        labels = []
        if "mom_20" in mom.columns:
            (ln20,) = ax1.plot(
                mom.index,
                mom["mom_20"],
                color=MOMENTUM_COLORS[20],
                lw=1.8,
                label="모멘텀 속도 20일",
                zorder=3,
            )
            handles.append(ln20)
            labels.append(ln20.get_label())
        if "mom_50" in mom.columns:
            (ln50,) = ax1.plot(
                mom.index,
                mom["mom_50"],
                color=MOMENTUM_COLORS[50],
                lw=1.8,
                label="모멘텀 속도 50일",
                zorder=3,
            )
            handles.append(ln50)
            labels.append(ln50.get_label())

        ax1.set_ylabel("하루 평균 변화율 (%/일)")

        if index_close is not None and not index_close.empty:
            ix = index_close.reindex(mom.index)
            if ix.notna().any():
                ax2 = ax1.twinx()
                (ln_ix,) = ax2.plot(
                    ix.index,
                    ix.values,
                    color="#90a4ae",
                    lw=1.2,
                    alpha=0.75,
                    label=f"{market} 지수",
                    zorder=2,
                )
                ax2.set_ylabel(f"{market} 지수", color="#78909c")
                ax2.tick_params(axis="y", labelcolor="#78909c")
                ax2.yaxis.set_major_formatter(_comma_formatter())
                handles.append(ln_ix)
                labels.append(ln_ix.get_label())

        ax1.legend(handles, labels, loc="upper left", fontsize=9, framealpha=0.92)

    ax1.set_xlabel("날짜")
    ax1.set_title(f"{market}: 모멘텀 속도 + 지수 (ROC÷N, 최근 {VOL_WINDOW}거래일, {as_of})")
    ax1.grid(True, alpha=0.25, zorder=0)
    _apply_horizontal_date_ticks(ax1)
    fig.tight_layout()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return out_path


def _write_volatility_docs(
    out_dir: Path,
    as_of: date,
    text: str,
    sections: list[dict],
    metrics: Optional[dict] = None,
) -> tuple[Path, Path]:
    """outputs/.../volatility.md (기존) · volatility.html (디자인 템플릿)."""
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
    md_path = out_dir / "volatility.md"
    md_path.write_text("\n".join(md), encoding="utf-8")

    m = metrics or {}
    html_path = write_design_html(
        "volatility_design.html",
        out_dir / "volatility.html",
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
    시장별(코스피→코스닥): 캔들 → 산점도 → 변동성추이 → 모멘텀속도 + volatility.md/html.
    """
    eng = engine()
    if as_of is None:
        d = pd.read_sql("SELECT MAX(date) AS d FROM ohlcv", eng)
        if d.empty or pd.isna(d.iloc[0]["d"]):
            return {"paths": [], "text": "OHLCV 없음", "articles": []}
        as_of = pd.to_datetime(d.iloc[0]["d"]).date()

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("05_volatility_*.png"):
        try:
            old.unlink()
        except OSError:
            pass

    # ATR·모멘텀·캔들 MA120 워밍업 + 250거래일
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

        p_sc = _plot_scatter(
            snap,
            market,
            as_of,
            out_dir / f"05_volatility_{mkt_l}_scatter.png",
            day_all=day_all,
        )
        p_tr = _plot_vol_trend(
            vol, idx, market, as_of, out_dir / f"05_volatility_{mkt_l}_trend.png"
        )
        p_mo = _plot_momentum_speed(
            mom, idx, market, as_of, out_dir / f"05_volatility_{mkt_l}_momentum.png"
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
        last_v = float(vol.dropna().iloc[-1]) if vol is not None and vol.dropna().size else np.nan
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
        f"{as_of} 마켓 변동성. "
        f"캔들: 지수 OHLC + MA20/50/120 (최근 {VOL_WINDOW}거래일, MA 워밍업 {MA_WARMUP}일). "
        f"산점도: ATR14/종가≥{ATR_OC_MAX} 제외, 평균·중앙값·P80·P20 수직선, 거래대금 상위{SCATTER_TOP_TV} 라벨. "
        f"추이: 시총가중 ATR14/종가 + Vol SMA20 + 지수(우축). "
        f"모멘텀 속도: ROC(N)%÷N (%/일) 20·50일 + 지수(우축). "
        + " / ".join(summaries)
    )
    md_path, html_path = _write_volatility_docs(
        out_dir, as_of, text, sections, metrics=metrics
    )
    try:
        from render import capture_html_report

        capture = capture_html_report(html_path, "volatility")
    except Exception as e:
        log.warning("변동성 풀페이지 캡처 실패: %s", e)
        capture = {"full": None, "sections": [], "jpeg": None}
    log.info("변동성 문서: %s / %s (%s)", out_dir, md_path.name, html_path.name)
    return {
        "paths": paths,
        "text": text,
        "articles": articles,
        "sections": sections,
        "as_of": as_of,
        "md": md_path,
        "html": html_path,
        "capture": capture,
        "out_dir": out_dir,
    }
