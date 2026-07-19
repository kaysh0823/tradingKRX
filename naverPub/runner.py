#!/usr/bin/env python3
"""
naverPub 오케스트레이션: 수집 → 가공 → 콘텐츠 생성 → 전송
영업일 16:00 KST cron 진입점.

단독 실행 예:
  python runner.py --force --daily-only
  python runner.py --force --etf-only
  python runner.py --force   # 둘 다
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from collect_daily import collect_daily
from content_etf import build_all_etf
from content_market import build_all_market, diagnose_source_data
from content_volatility import render_market_volatility
from db import ensure_schema
from notify import notify_bundle, notify_day_outputs
from render import day_root, render_active_etf_pdf, render_daily_snapshot, render_picking

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("naverPub.runner")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="naverPub daily runner")
    parser.add_argument("--date", help="강제 기준일 YYYYMMDD (업종분류 유효일만, 아니면 최근 거래일)")
    parser.add_argument("--skip-collect", action="store_true", help="수집 생략, 콘텐츠만")
    parser.add_argument("--skip-notify", action="store_true", help="텔레그램 생략")
    parser.add_argument(
        "--force",
        action="store_true",
        help="휴장이어도 전거래일 기준으로 콘텐츠 재생성 (휴장일을 기준일로 쓰지 않음)",
    )
    parser.add_argument("--daily-only", action="store_true", help="데일리 스냅샷만 생성")
    parser.add_argument("--etf-only", action="store_true", help="액티브 ETF PDF만 생성")
    args = parser.parse_args(argv)

    do_daily = not args.etf_only or args.daily_only
    do_etf = not args.daily_only or args.etf_only
    if args.daily_only and args.etf_only:
        do_daily = do_etf = True
    if not args.daily_only and not args.etf_only:
        do_daily = do_etf = True

    ensure_schema()
    errors: list[str] = []
    biz_day = args.date
    today = date.today().strftime("%Y%m%d")

    if not args.skip_collect:
        try:
            result = collect_daily(day_str=args.date, force=args.force)
            biz_day = result["biz_day"]
            log.info(
                "오늘=%s, 기준 거래일=%s, 수집 필요 여부=%s%s",
                result.get("today", today),
                biz_day,
                "Y" if result.get("need_collect") else "N",
                " [휴장→전거래일]" if result.get("holiday") else "",
            )
            if result.get("skipped"):
                log.info("수집 스킵(이미 적재) — 콘텐츠 생성 진행")
            else:
                errors.extend(result.get("errors") or [])
                log.info(
                    "수집 완료 download=%s upsert=%s(new=%s upd=%s) pdf=%s rs=%s talent=%s",
                    result.get("ohlcv_downloaded"),
                    result.get("ohlcv_rows"),
                    result.get("ohlcv_inserted"),
                    result.get("ohlcv_updated"),
                    result.get("pdf_rows"),
                    result.get("rs_rows"),
                    result.get("talent_rows"),
                )
        except Exception as e:
            log.exception("수집 실패")
            errors.append(f"collect: {e}")
            if not biz_day:
                if not args.skip_notify:
                    notify_bundle([], f"naverPub 수집 실패: {e}", errors)
                return 1

    if not biz_day:
        from db import engine
        import pandas as pd

        d = pd.read_sql("SELECT MAX(date) AS d FROM ohlcv", engine())
        if d.empty or pd.isna(d.iloc[0]["d"]):
            log.error("ohlcv 데이터 없음")
            return 1
        biz_day = pd.to_datetime(d.iloc[0]["d"]).strftime("%Y%m%d")

    as_of_dash = f"{biz_day[:4]}-{biz_day[4:6]}-{biz_day[6:8]}"
    as_of_date = datetime.strptime(biz_day, "%Y%m%d").date()

    try:
        diagnose_source_data(as_of_date)
    except Exception as e:
        log.warning("소스진단 실패: %s", e)

    market = {}
    etf = {}
    if do_daily:
        try:
            market = build_all_market(as_of_date)
        except Exception as e:
            log.exception("시장 콘텐츠 실패")
            errors.append(f"content_market: {e}")
    if do_etf:
        try:
            etf = build_all_etf(as_of_date)
            if etf.get("note"):
                log.warning("ETF: %s", etf["note"])
        except Exception as e:
            log.exception("ETF 콘텐츠 실패")
            errors.append(f"content_etf: {e}")

    articles = []
    out_paths = []
    root = day_root(as_of_dash)
    pick_result: dict = {}
    try:
        if do_daily:
            tickers = render_daily_snapshot(as_of_dash, market, root / "tickers")
            articles.extend(tickers.get("articles") or [])
            out_paths.append(str(tickers["out_dir"]))
            try:
                vol = render_market_volatility(as_of_date, root / "martket")
                articles.extend(vol.get("articles") or [])
                out_paths.append(str(vol.get("out_dir") or (root / "martket")))
            except Exception as e:
                log.exception("마켓 변동성 실패")
                errors.append(f"martket: {e}")
            try:
                pick_result = render_picking(as_of_dash, market, root / "pick")
                articles.extend(pick_result.get("articles") or [])
                out_paths.append(str(pick_result["out_dir"]))
            except Exception as e:
                log.exception("Picking 실패")
                errors.append(f"pick: {e}")
        if do_etf:
            etf_b = render_active_etf_pdf(as_of_dash, etf, root / "etfs")
            articles.extend(etf_b.get("articles") or [])
            out_paths.append(str(etf_b["out_dir"]))
    except Exception as e:
        log.exception("렌더 실패")
        errors.append(f"render: {e}")
        if not args.skip_notify:
            notify_bundle([], f"naverPub 렌더 실패 ({biz_day}): {e}", errors)
        return 1

    screen_pass = None
    try:
        scr = (pick_result or {}).get("screen") or {}
        if "pass_count" in scr:
            screen_pass = int(scr.get("pass_count") or 0)
    except Exception:
        screen_pass = None

    from notify import count_sec_pngs

    sec_counts = count_sec_pngs(root)
    summary = (
        f"naverPub {as_of_dash} 완료\n"
        f"출력 루트: {root}\n"
        f"폴더: {', '.join(out_paths)}\n"
        f"스크리닝 통과: {screen_pass if screen_pass is not None else '-'}\n"
        f"_sec: tickers={sec_counts.get('tickers', 0)} "
        f"martket={sec_counts.get('martket', 0)} "
        f"pick={sec_counts.get('pick', 0)} "
        f"etfs={sec_counts.get('etfs', 0)}"
    )
    log.info(summary)
    if errors:
        log.warning("부분 실패: %s", errors)

    if not args.skip_notify:
        notify_day_outputs(
            root,
            as_of_dash,
            summary=summary,
            screen_pass=screen_pass,
            errors=errors or None,
        )

    return 1 if (errors and not articles) else 0


if __name__ == "__main__":
    raise SystemExit(main())
