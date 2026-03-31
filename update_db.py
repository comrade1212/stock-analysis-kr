"""
update_db.py  — 수급 DB 일괄 업데이트

사용법:
    python update_db.py              # data/ 폴더 전체 종목 업데이트
    python update_db.py 005930       # 특정 종목만
    python update_db.py --gap-only   # 빈 날짜만 채우기 (최신 추가 제외)

동작:
1. data/*_inv.parquet 스캔
2. 각 종목의 price parquet로 거래일 캘린더 구성
3. inv에 없는 날짜 탐지 (내부 빈 날짜 + 마지막 날 이후)
4. pykrx KRX 시도 → 실패 시 Naver frgn 스크래핑 (기관 + 외국인)
5. 업데이트 저장
"""

import sys
import time
import re
import math
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests
from pykrx import stock as pykrx_stock

DATA_DIR = Path(__file__).parent / "data"
TODAY = datetime.now().strftime("%Y%m%d")
TODAY_DT = pd.Timestamp(TODAY)

_NAV_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/",
}

INSTITUTION_COLS = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금등"]
INV_ORDER = ["개인", "외국인", "기관계"] + INSTITUTION_COLS + ["기타법인", "기타외국인"]


# ─────────────────────────────────────────────
# 거래일 캘린더
# ─────────────────────────────────────────────
def get_trading_days_from_price(ticker: str) -> pd.DatetimeIndex | None:
    """price parquet에서 거래일 목록 추출"""
    p = DATA_DIR / f"{ticker}_price.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    if df.empty:
        return None
    return df.index.normalize().sort_values()


def estimate_trading_days_count(from_dt: pd.Timestamp, to_dt: pd.Timestamp) -> int:
    """두 날짜 사이의 영업일 수 추정 (한국 공휴일 미반영, 근사값)"""
    bdays = pd.bdate_range(from_dt, to_dt)
    return len(bdays)


# ─────────────────────────────────────────────
# Naver frgn 스크래핑
# ─────────────────────────────────────────────
def _parse_frgn_page(html: str) -> pd.DataFrame:
    try:
        from io import StringIO
        tables = pd.read_html(StringIO(html), encoding="utf-8")
    except Exception:
        return pd.DataFrame()
    if len(tables) < 4:
        return pd.DataFrame()
    t = tables[3]
    if t.columns.nlevels > 1:
        new_cols = []
        for lv0, lv1 in zip(t.columns.get_level_values(0), t.columns.get_level_values(1)):
            if lv0 in ("기관", "외국인") and lv1 == "순매매량":
                new_cols.append(lv0)
            else:
                new_cols.append(str(lv1))
        t.columns = new_cols
    else:
        t.columns = t.columns.get_level_values(-1)

    if "날짜" not in t.columns:
        return pd.DataFrame()
    t = t.dropna(subset=["날짜"])
    t = t[t["날짜"].astype(str).str.match(r"\d{4}\.\d{2}\.\d{2}", na=False)]
    if t.empty:
        return pd.DataFrame()

    t = t.copy()
    t["date"] = pd.to_datetime(t["날짜"].str.replace(".", "-", regex=False))
    t = t.set_index("date")
    out = pd.DataFrame(index=t.index)
    for col in ("기관", "외국인"):
        if col in t.columns:
            out[col] = pd.to_numeric(t[col], errors="coerce").fillna(0).astype("int64")
    return out


def _naver_total_pages(ticker: str, session: requests.Session) -> int:
    try:
        r = session.get(
            f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page=1",
            headers=_NAV_HDR, timeout=10)
        m = re.search(r"href=['\"][^'\"]*page=(\d+)[^'\"]*['\"][^>]*>맨뒤", r.text)
        if m:
            return int(m.group(1))
        nums = re.findall(r"page=(\d+)", r.text)
        return max(int(n) for n in nums) if nums else 1
    except Exception:
        return 1


def fetch_naver_pages(ticker: str, pages: list[int], session: requests.Session) -> pd.DataFrame:
    results = []
    for p in pages:
        try:
            r = session.get(
                f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page={p}",
                headers=_NAV_HDR, timeout=12)
            df = _parse_frgn_page(r.text)
            if not df.empty:
                results.append(df)
            time.sleep(0.15)
        except Exception:
            pass
    if not results:
        return pd.DataFrame()
    df_all = pd.concat(results).sort_index()
    return df_all[~df_all.index.duplicated(keep="last")]


def pages_for_dates(missing_dates: list[pd.Timestamp], total_pages: int) -> list[int]:
    """
    누락된 날짜 목록에 해당하는 Naver 페이지 번호 계산.
    각 페이지 = 약 20 거래일. 오늘부터 역순으로 나열됨.
    """
    if not missing_dates:
        return []
    pages = set()
    for dt in missing_dates:
        days_from_today = estimate_trading_days_count(dt, TODAY_DT)
        # 해당 날짜가 몇 번째 페이지에 있는지 (1-indexed)
        page = max(1, math.ceil(days_from_today / 20))
        # 안전 버퍼: ±1 페이지 추가
        pages.update([max(1, page - 1), page, page + 1])
    return sorted(p for p in pages if 1 <= p <= total_pages)


# ─────────────────────────────────────────────
# pykrx KRX 수집 (서버 환경에서 동작)
# ─────────────────────────────────────────────
def fetch_pykrx_inv(ticker: str, from_date: str, to_date: str, timeout_sec: int = 8) -> pd.DataFrame:
    """pykrx KRX 수급 데이터 수집 (타임아웃 적용)"""
    import concurrent.futures as _cf

    def _call_detail():
        return pykrx_stock.get_market_trading_value_by_date(
            from_date, to_date, ticker, detail=True)

    def _call_basic():
        return pykrx_stock.get_market_trading_value_by_date(from_date, to_date, ticker)

    for fn in (_call_detail, _call_basic):
        with _cf.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fn)
            try:
                df = fut.result(timeout=timeout_sec)
                if df is None or df.empty:
                    continue
                if fn == _call_detail:
                    inst_present = [c for c in INSTITUTION_COLS if c in df.columns]
                    if inst_present and "기관계" not in df.columns:
                        df["기관계"] = df[inst_present].sum(axis=1)
                    cols = [c for c in INV_ORDER if c in df.columns]
                    extra = [c for c in df.columns if c not in cols and c != "전체"]
                    return df[cols + extra]
                else:
                    return df[[c for c in df.columns if c != "전체"]]
            except (_cf.TimeoutError, Exception):
                fut.cancel()
                continue
    return pd.DataFrame()


# ─────────────────────────────────────────────
# 단일 종목 업데이트
# ─────────────────────────────────────────────
def update_ticker(ticker: str, session: requests.Session, gap_only: bool = False) -> str:
    inv_file = DATA_DIR / f"{ticker}_inv.parquet"
    if not inv_file.exists():
        return f"[{ticker}] inv 파일 없음, 건너뜀"

    df_inv = pd.read_parquet(inv_file)
    if df_inv.empty:
        return f"[{ticker}] inv 비어있음, 건너뜀"

    df_inv.index = pd.to_datetime(df_inv.index).normalize()

    # 거래일 캘린더: price parquet 기준
    trading_days = get_trading_days_from_price(ticker)
    if trading_days is None or len(trading_days) == 0:
        return f"[{ticker}] price 파일 없음, 건너뜀"

    # inv 데이터가 실제로 존재하는 첫 날짜 이후만 대상 (그 전은 Naver에 데이터 없음)
    inv_start = df_inv.index.min()
    inv_end = df_inv.index.max()

    # 오늘까지의 거래일 (inv 시작일 이후만)
    all_trading = trading_days[(trading_days >= inv_start) & (trading_days <= TODAY_DT)]

    if not gap_only:
        # inv 마지막 날 이후 영업일 추가 (Naver에서 실제 있는 날만 저장됨)
        if inv_end < TODAY_DT - timedelta(days=1):
            estimated = pd.bdate_range(inv_end + timedelta(days=1), TODAY_DT)
            all_trading = pd.DatetimeIndex(
                sorted(set(all_trading) | set(pd.DatetimeIndex(estimated).normalize()))
            )

    inv_days = set(df_inv.index.normalize())
    missing = sorted(set(all_trading) - inv_days)

    if not missing:
        return f"[{ticker}] 최신 상태 (누락 없음)"

    print(f"[{ticker}] 누락 {len(missing)}일 발견: {missing[0].date()} ~ {missing[-1].date()}")

    # ── 1순위: Naver frgn (기관+외국인, 로컬에서 항상 동작)
    new_rows = pd.DataFrame()
    total_pages = _naver_total_pages(ticker, session)
    needed_pages = pages_for_dates(missing, total_pages)
    if needed_pages:
        naver_df = fetch_naver_pages(ticker, needed_pages, session)
        if not naver_df.empty:
            naver_df.index = pd.to_datetime(naver_df.index).normalize()
            new_rows = naver_df[naver_df.index.isin(missing)]

    # ── 2순위: pykrx KRX (서버 환경 / 상세 컬럼 필요 시)
    # 아직 못 채운 날짜가 있을 때 시도 (타임아웃 5초)
    missing_still = sorted(set(missing) - set(new_rows.index)) if not new_rows.empty else missing
    if missing_still:
        from_str = missing_still[0].strftime("%Y%m%d")
        to_str = missing_still[-1].strftime("%Y%m%d")
        krx_df = fetch_pykrx_inv(ticker, from_str, to_str, timeout_sec=5)
        if not krx_df.empty:
            krx_df.index = pd.to_datetime(krx_df.index).normalize()
            krx_fill = krx_df[krx_df.index.isin(missing_still)]
            if not krx_fill.empty:
                krx_fill = _align_columns(krx_fill, df_inv)
                new_rows = pd.concat([new_rows, krx_fill]) if not new_rows.empty else krx_fill

    if new_rows.empty:
        return f"[{ticker}] 데이터 수집 실패 (누락 {len(missing)}일)"

    # ── 병합 & 저장
    # 컬럼 정합성
    new_rows = _align_columns(new_rows, df_inv)
    df_updated = pd.concat([df_inv, new_rows])
    df_updated = df_updated[~df_updated.index.duplicated(keep="last")].sort_index()
    df_updated.to_parquet(inv_file)

    filled = len(new_rows)
    return f"[{ticker}] {filled}일 추가 완료 (총 {len(df_updated)}행)"


def _align_columns(new_df: pd.DataFrame, ref_df: pd.DataFrame) -> pd.DataFrame:
    """새 데이터 컬럼을 기존 df 컬럼에 맞춤 (없는 컬럼은 0으로 채움)"""
    ref_cols = list(ref_df.columns)
    for col in ref_cols:
        if col not in new_df.columns:
            new_df = new_df.copy()
            new_df[col] = 0
    # ref에 없는 컬럼은 그대로 추가 (컬럼 확장)
    return new_df


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    args = sys.argv[1:]
    gap_only = "--gap-only" in args
    args = [a for a in args if not a.startswith("--")]

    if args:
        tickers = args
    else:
        # data/ 폴더의 모든 inv parquet
        tickers = sorted(
            p.stem.replace("_inv", "")
            for p in DATA_DIR.glob("*_inv.parquet")
        )

    if not tickers:
        print("업데이트할 종목 없음")
        return

    print(f"대상 종목: {len(tickers)}개 | gap_only={gap_only} | 기준일={TODAY}")
    print("=" * 60)

    session = requests.Session()
    session.headers.update(_NAV_HDR)

    results = []
    for ticker in tickers:
        try:
            msg = update_ticker(ticker, session, gap_only=gap_only)
            results.append(msg)
            print(msg)
        except Exception as e:
            msg = f"[{ticker}] 오류: {e}"
            results.append(msg)
            print(msg)
        time.sleep(0.3)  # 서버 부하 방지

    print("=" * 60)
    print(f"완료: {len(results)}개 종목 처리")


if __name__ == "__main__":
    main()
