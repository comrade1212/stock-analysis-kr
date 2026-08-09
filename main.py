from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from io import StringIO
import asyncio
import concurrent.futures
import threading
import queue
import urllib.request
import urllib.parse
import json
import re
import os
import math
import time

UPDATE_SECRET = os.environ.get("UPDATE_SECRET", "")
# DATA_DIR: 배포 환경에서는 영구 볼륨 경로를 DATA_DIR 환경변수로 주입한다.
# (Railway Volume 마운트 경로 예: /app/data). 미설정 시 로컬 data/ 폴더.
DATA_DIR = Path(os.environ.get("DATA_DIR") or Path(__file__).parent / "data")
import requests as _requests  # Naver 스크래핑용
DATA_DIR.mkdir(parents=True, exist_ok=True)

ticker_cache: dict = {}
cache_ready = False
KRX_SEARCH_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

# 기관계 구성 세부 투자주체 (합산용)
INSTITUTION_COLS = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금등"]
# detail=True 컬럼 순서
INV_ORDER = ["개인", "외국인", "기관계"] + INSTITUTION_COLS + ["기타법인", "기타외국인"]

# 서버(Railway 등)는 UTC로 돌므로 날짜 계산은 반드시 KST 기준으로 한다.
KST = timezone(timedelta(hours=9))

def now_kst() -> datetime:
    return datetime.now(KST)

def today_kst_str() -> str:
    return now_kst().strftime("%Y%m%d")


# ─────────────────────────────────────────────
# KRX(pykrx) 호출 보호
# KRX는 해외 데이터센터 IP를 차단하는 경우가 많아, pykrx 호출이 응답 없이
# 매달리면 이벤트 루프/스레드가 잠기고 Railway CPU 과금만 쌓인다.
# 타임아웃을 강제하고, 연속 실패 시 1시간 동안 KRX 호출을 건너뛴다.
# ─────────────────────────────────────────────
krx_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
_krx_state = {"fails": 0, "disabled_until": 0.0}
_KRX_FAIL_LIMIT = 3
_KRX_COOLDOWN_SEC = 3600

def krx_call(fn, timeout_sec: float = 8.0):
    """pykrx 호출을 타임아웃과 함께 실행. 실패/타임아웃 시 None 반환."""
    if time.time() < _krx_state["disabled_until"]:
        return None
    fut = krx_pool.submit(fn)
    try:
        result = fut.result(timeout=timeout_sec)
        _krx_state["fails"] = 0
        return result
    except Exception:
        fut.cancel()
        _krx_state["fails"] += 1
        if _krx_state["fails"] >= _KRX_FAIL_LIMIT:
            _krx_state["disabled_until"] = time.time() + _KRX_COOLDOWN_SEC
            print(f"[KRX] 연속 {_krx_state['fails']}회 실패 → {_KRX_COOLDOWN_SEC // 60}분간 KRX 호출 생략 (Naver 폴백 사용)")
        return None


# ─────────────────────────────────────────────
# 종목 캐시
# ─────────────────────────────────────────────
def build_cache():
    global ticker_cache, cache_ready
    result = {}
    try:
        params = urllib.parse.urlencode({
            "bld": "dbms/comm/finder/finder_stkisu",
            "locale": "ko_KR",
            "pagePath": "/contents/COM/FinderStkIsu.jsp",
            "isuSrtCd": "",
            "mktId": "ALL",
            "sortRule": "D",
            "pageSize": "5000",
            "currentPageSize": "5000",
            "page": "1",
        })
        req = urllib.request.Request(
            KRX_SEARCH_URL, data=params.encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "User-Agent": "Mozilla/5.0",
                     "Referer": "http://data.krx.co.kr/"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        for item in data.get("block1", []):
            market = "KOSPI" if item.get("marketEngName") == "KOSPI" else "KOSDAQ"
            result[item["short_code"]] = {"name": item["codeName"], "market": market}
    except Exception as e:
        print(f"[Cache] 오류: {e}")
    ticker_cache = result
    cache_ready = True
    print(f"[Cache] 종목 {len(ticker_cache)}개 로드 완료")


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=build_cache, daemon=True).start()
    yield


app = FastAPI(title="한국 주식 수급분석", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
# CPU 스파이크(=Railway 과금)를 줄이기 위해 워커 수를 보수적으로 유지
pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)


# ─────────────────────────────────────────────
# 로컬 파일 캐시
# ─────────────────────────────────────────────
def price_path(ticker: str) -> Path:
    return DATA_DIR / f"{ticker}_price.parquet"

def inv_path(ticker: str) -> Path:
    return DATA_DIR / f"{ticker}_inv.parquet"

def load_price(ticker: str) -> pd.DataFrame:
    p = price_path(ticker)
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

def load_inv(ticker: str) -> pd.DataFrame:
    p = inv_path(ticker)
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()

def save_price(ticker: str, df: pd.DataFrame):
    df.to_parquet(price_path(ticker))

def save_inv(ticker: str, df: pd.DataFrame):
    df.to_parquet(inv_path(ticker))

def last_date(df: pd.DataFrame) -> str | None:
    if df.empty:
        return None
    idx = df.index[-1]
    return idx.strftime("%Y%m%d") if hasattr(idx, "strftime") else str(idx)[:8].replace("-", "")


# ─────────────────────────────────────────────
# OHLCV 수집
# ─────────────────────────────────────────────
def fetch_ohlcv_full_naver(ticker: str) -> pd.DataFrame:
    """Naver siseJson: 상장일~현재 전체 이력 (한 번에 수신, 가장 빠름)"""
    today = today_kst_str()
    url = (
        f"https://fchart.stock.naver.com/siseJson.nhn"
        f"?symbol={ticker}&requestType=1"
        f"&startTime=19900101&endTime={today}&timeframe=day"
    )
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"})
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read().decode("utf-8")
    rows = re.findall(r'\["(\d{8})",\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+),\s*(\d+)', raw)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "시가", "고가", "저가", "종가", "거래량"])
    df["date"] = pd.to_datetime(df["date"])
    for col in ["시가", "고가", "저가", "종가", "거래량"]:
        df[col] = pd.to_numeric(df[col])
    return df.set_index("date").sort_index()


def fetch_ohlcv_pykrx(ticker: str, from_date: str, to_date: str) -> pd.DataFrame:
    """pykrx: 지정 기간 OHLCV (증분 업데이트용)"""
    df = stock.get_market_ohlcv_by_date(from_date, to_date, ticker)
    if df is None or df.empty:
        return pd.DataFrame()
    df.index = pd.to_datetime(df.index)
    rename = {}
    for col in df.columns:
        if "시가" in col: rename[col] = "시가"
        elif "고가" in col: rename[col] = "고가"
        elif "저가" in col: rename[col] = "저가"
        elif "종가" in col: rename[col] = "종가"
        elif "거래량" in col: rename[col] = "거래량"
    if rename:
        df = df.rename(columns=rename)
    keep = [c for c in ["시가", "고가", "저가", "종가", "거래량"] if c in df.columns]
    return df[keep].sort_index()


# ─────────────────────────────────────────────
# 수급 데이터 수집 (Naver Finance 스크래핑)
# ─────────────────────────────────────────────
_NAV_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/",
}

def _parse_frgn_page(html: str) -> pd.DataFrame:
    """frgn.nhn HTML → DataFrame (날짜, 기관, 외국인)"""
    try:
        # pandas 2.1+ 는 문자열 직접 전달을 지원하지 않으므로 StringIO로 감싼다
        tables = pd.read_html(StringIO(html), flavor="lxml")
    except Exception:
        return pd.DataFrame()
    if len(tables) < 4:
        return pd.DataFrame()
    t = tables[3]
    # 멀티 헤더: level(0)=기관/외국인, level(1)=순매매량
    if t.columns.nlevels > 1:
        # 컬럼명: (기관,순매매량) → 기관, (외국인,순매매량) → 외국인
        new_cols = []
        for lv0, lv1 in zip(t.columns.get_level_values(0), t.columns.get_level_values(1)):
            if lv0 in ("기관", "외국인") and lv1 == "순매매량":
                new_cols.append(lv0)
            else:
                new_cols.append(lv1)
        t.columns = new_cols
    else:
        # 단일 헤더일 때 (날짜, 종가, ..., 순매매량, 순매매량, ...)
        t.columns = t.columns.get_level_values(-1)

    if "날짜" not in t.columns:
        return pd.DataFrame()
    t = t.dropna(subset=["날짜"])
    t = t[t["날짜"].astype(str).str.match(r"\d{4}\.\d{2}\.\d{2}", na=False)]
    if t.empty:
        return pd.DataFrame()

    t["date"] = pd.to_datetime(t["날짜"].str.replace(".", "-"))
    t = t.set_index("date")
    out = pd.DataFrame(index=t.index)
    if "기관" in t.columns:
        out["기관"] = pd.to_numeric(t["기관"], errors="coerce").fillna(0).astype("int64")
    if "외국인" in t.columns:
        out["외국인"] = pd.to_numeric(t["외국인"], errors="coerce").fillna(0).astype("int64")
    return out


def _frgn_total_pages(ticker: str) -> int:
    """frgn.nhn 마지막 페이지 번호 조회"""
    try:
        r = _requests.get(
            f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page=1",
            headers=_NAV_HDR, timeout=10)
        # "맨뒤" 링크에서 직접 추출
        m = re.search(r"href=['\"][^'\"]*page=(\d+)[^'\"]*['\"][^>]*>맨뒤", r.text)
        if m:
            return int(m.group(1))
        # fallback: 모든 page= 숫자 중 최대값
        nums = re.findall(r"page=(\d+)", r.text)
        return max(int(n) for n in nums) if nums else 1
    except Exception:
        pass
    return 1


def fetch_investor_naver_pages(ticker: str, pages: list[int]) -> pd.DataFrame:
    """지정 페이지 목록 병렬 수집 → 합쳐서 반환"""
    session = _requests.Session()
    session.headers.update(_NAV_HDR)

    def get_page(p):
        try:
            r = session.get(
                f"https://finance.naver.com/item/frgn.nhn?code={ticker}&page={p}",
                timeout=12)
            return _parse_frgn_page(r.text)
        except Exception:
            return pd.DataFrame()

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        for df in ex.map(get_page, pages):
            if not df.empty:
                results.append(df)

    if not results:
        return pd.DataFrame()
    df_all = pd.concat(results).sort_index()
    return df_all[~df_all.index.duplicated(keep="last")]


def fetch_investor_range(ticker: str, from_date: str, to_date: str) -> tuple[pd.DataFrame, list]:
    """수급 데이터 수집. KRX 상세(타임아웃 보호) 우선, Naver 폴백."""
    # 1순위: KRX 상세 시도 (타임아웃 보호 - KRX가 응답 안 하면 즉시 폴백)
    df = krx_call(lambda: stock.get_market_trading_value_by_date(
        from_date, to_date, ticker, detail=True), timeout_sec=8)
    if df is not None and not df.empty:
        inst_present = [c for c in INSTITUTION_COLS if c in df.columns]
        if inst_present and "기관계" not in df.columns:
            df["기관계"] = df[inst_present].sum(axis=1)
        cols = [c for c in INV_ORDER if c in df.columns]
        extra = [c for c in df.columns if c not in cols and c != "전체"]
        cols += extra
        return df[cols], cols
    df = krx_call(lambda: stock.get_market_trading_value_by_date(
        from_date, to_date, ticker), timeout_sec=8)
    if df is not None and not df.empty:
        cols = [c for c in df.columns if c != "전체"]
        return df[cols], cols

    # 2순위: Naver 스크래핑 (기관 + 외국인, 항상 동작)
    try:
        # 날짜 필터 대신 page=1 (최근 20일) - 증분 업데이트용
        df = fetch_investor_naver_pages(ticker, [1, 2])
        if not df.empty:
            # 날짜 범위 필터
            fd = pd.to_datetime(from_date)
            td = pd.to_datetime(to_date)
            df = df[(df.index >= fd) & (df.index <= td)]
            cols = [c for c in df.columns if c in ("기관", "외국인")]
            return df[cols], cols
    except Exception:
        pass

    return pd.DataFrame(), []


# ─────────────────────────────────────────────
# 전체 다운로드 (SSE 스트리밍)
# ─────────────────────────────────────────────
def _sse(msg: str, pct: int = -1, done: bool = False) -> str:
    payload = {"msg": msg, "done": done}
    if pct >= 0:
        payload["pct"] = pct
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def do_full_download(ticker: str):
    """제너레이터: SSE 메시지를 yield 하며 전체 이력 다운로드"""
    today = today_kst_str()

    # ── 1. OHLCV (Naver siseJson, 한 번에)
    yield _sse("📥 OHLCV 전체 이력 다운로드 중...", 5)
    try:
        df_price = fetch_ohlcv_full_naver(ticker)
        if df_price.empty:
            yield _sse("⚠️ OHLCV 데이터를 가져오지 못했습니다.", done=True)
            return
        save_price(ticker, df_price)
        yield _sse(f"✅ OHLCV {len(df_price):,}행 저장 완료 (시작: {df_price.index[0].date()})", 30)
    except Exception as e:
        yield _sse(f"❌ OHLCV 오류: {e}", done=True)
        return

    # ── 2. 수급 (Naver frgn 전체 페이지 스크래핑)
    yield _sse("📥 수급 데이터 다운로드 중 (Naver, 기관·외국인)...", 35)
    try:
        total_pages = _frgn_total_pages(ticker)
        yield _sse(f"  총 {total_pages}페이지 병렬 수집 시작...", 38)

        all_inv = []
        BATCH = 20  # 한 번에 20페이지씩
        page_batches = [list(range(i, min(i + BATCH, total_pages + 1)))
                        for i in range(1, total_pages + 1, BATCH)]

        for bi, batch in enumerate(page_batches):
            pct = 38 + int((bi / len(page_batches)) * 55)
            yield _sse(f"  페이지 {batch[0]}~{batch[-1]} / {total_pages} 수집 중...", pct)
            df_batch = fetch_investor_naver_pages(ticker, batch)
            if not df_batch.empty:
                all_inv.append(df_batch)

        if all_inv:
            df_inv = pd.concat(all_inv).sort_index()
            df_inv = df_inv[~df_inv.index.duplicated(keep="last")]
            save_inv(ticker, df_inv)
            inv_cols = list(df_inv.columns)
            yield _sse(f"✅ 수급 {len(df_inv):,}행 저장 완료 ({', '.join(inv_cols)})", 95)
        else:
            yield _sse("⚠️ 수급 데이터를 가져오지 못했습니다.", 95)
    except Exception as e:
        yield _sse(f"⚠️ 수급 오류: {e}", 95)

    yield _sse("🎉 전체 다운로드 완료!", 100, done=True)


# ─────────────────────────────────────────────
# 서버 수급 업데이트 (pykrx 13컬럼, 컬럼 업그레이드)
# ─────────────────────────────────────────────
def _upgrade_inv_columns(df_inv: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """기존 inv parquet를 pykrx 13컬럼 체계로 업그레이드.
    Naver의 '기관' → '기관계' rename, 없는 컬럼은 0으로 채움.
    """
    df_inv = df_inv.copy()
    if "기관" in df_inv.columns and "기관계" not in df_inv.columns:
        df_inv = df_inv.rename(columns={"기관": "기관계"})
    for col in new_df.columns:
        if col not in df_inv.columns:
            df_inv[col] = 0
    return df_inv


def _estimate_pages_for_dates(missing_dates: list, today_dt: pd.Timestamp) -> list[int]:
    """Naver frgn 페이지 번호 계산 (20행/페이지, 오늘 기준 역순)"""
    if not missing_dates:
        return []
    pages = set()
    for dt in missing_dates:
        bdays = len(pd.bdate_range(dt, today_dt))
        page = max(1, math.ceil(bdays / 20))
        pages.update([max(1, page - 1), page, page + 1])
    return sorted(pages)


def _server_fetch_inv(ticker: str, from_date: str, to_date: str) -> pd.DataFrame:
    """서버 전용: pykrx 13컬럼 시도(타임아웃 보호) → Naver 폴백 (기관계+외국인)"""
    # 1순위: pykrx detail
    df = krx_call(lambda: stock.get_market_trading_value_by_date(
        from_date, to_date, ticker, detail=True), timeout_sec=8)
    if df is not None and not df.empty:
        inst_present = [c for c in INSTITUTION_COLS if c in df.columns]
        if inst_present and "기관계" not in df.columns:
            df["기관계"] = df[inst_present].sum(axis=1)
        cols = [c for c in INV_ORDER if c in df.columns]
        return df[cols]
    # 2순위: pykrx 기본
    df = krx_call(lambda: stock.get_market_trading_value_by_date(
        from_date, to_date, ticker), timeout_sec=8)
    if df is not None and not df.empty:
        return df[[c for c in df.columns if c != "전체"]]
    # 3순위: Naver frgn
    try:
        today_dt = pd.Timestamp(now_kst().date())
        missing_approx = list(pd.bdate_range(from_date, to_date))
        pages = _estimate_pages_for_dates(missing_approx, today_dt)
        pages = pages[:10]  # 최대 10페이지
        naver_df = fetch_investor_naver_pages(ticker, pages)
        if not naver_df.empty:
            fd, td = pd.Timestamp(from_date), pd.Timestamp(to_date)
            return naver_df[(naver_df.index >= fd) & (naver_df.index <= td)]
    except Exception:
        pass
    return pd.DataFrame()


def server_update_ticker(ticker: str) -> tuple[str, int]:
    """서버에서 한 종목 수급 업데이트. (메시지, 추가된 행 수) 반환"""
    inv_file = inv_path(ticker)
    price_file = price_path(ticker)
    if not inv_file.exists() or not price_file.exists():
        return f"[{ticker}] 파일 없음", 0

    df_inv = load_inv(ticker)
    df_pr = load_price(ticker)
    if df_inv.empty or df_pr.empty:
        return f"[{ticker}] 데이터 없음", 0

    df_inv.index = pd.to_datetime(df_inv.index).normalize()
    df_pr.index = pd.to_datetime(df_pr.index).normalize()

    today_dt = pd.Timestamp(now_kst().date())
    inv_start = df_inv.index.min()
    inv_end = df_inv.index.max()

    # 거래일 후보: price 기준 + inv 마지막 이후 영업일 추정
    trading = df_pr.index[(df_pr.index >= inv_start) & (df_pr.index <= today_dt)]
    if inv_end < today_dt - timedelta(days=1):
        estimated = pd.DatetimeIndex(
            pd.bdate_range(inv_end + timedelta(days=1), today_dt)
        ).normalize()
        trading = pd.DatetimeIndex(sorted(set(trading) | set(estimated)))

    missing = sorted(set(trading) - set(df_inv.index.normalize()))
    if not missing:
        return f"[{ticker}] 최신 상태", 0

    from_str = missing[0].strftime("%Y%m%d")
    to_str = missing[-1].strftime("%Y%m%d")

    new_df = _server_fetch_inv(ticker, from_str, to_str)
    if new_df.empty:
        return f"[{ticker}] 수집 실패 ({len(missing)}일 누락)", 0

    new_df.index = pd.to_datetime(new_df.index).normalize()
    new_df = new_df[new_df.index.isin(missing)]
    if new_df.empty:
        return f"[{ticker}] 해당 기간 거래 없음", 0

    # 컬럼 업그레이드 (Naver 2컬럼 → pykrx 13컬럼)
    df_inv = _upgrade_inv_columns(df_inv, new_df)
    # 새 데이터도 기존 컬럼에 맞춤 (누락 컬럼 0으로)
    for col in df_inv.columns:
        if col not in new_df.columns:
            new_df = new_df.copy()
            new_df[col] = 0
    new_df = new_df[df_inv.columns]

    df_updated = pd.concat([df_inv, new_df])
    df_updated = df_updated[~df_updated.index.duplicated(keep="last")].sort_index()
    save_inv(ticker, df_updated)

    return f"[{ticker}] {len(new_df)}일 추가 (총 {len(df_updated)}행)", len(new_df)


def do_server_update_all(tickers: list[str] | None = None):
    """SSE 제너레이터: 모든 종목(또는 지정 종목) 서버 업데이트"""
    if tickers is None:
        tickers = sorted(
            p.stem.replace("_inv", "")
            for p in DATA_DIR.glob("*_inv.parquet")
        )
    if not tickers:
        yield _sse("업데이트할 종목 없음", done=True)
        return

    total = len(tickers)
    yield _sse(f"서버 수급 업데이트 시작: {total}개 종목", 0)

    added_total = 0
    for i, ticker in enumerate(tickers):
        pct = int((i / total) * 100)
        try:
            msg, added = server_update_ticker(ticker)
            added_total += added
            yield _sse(msg, pct)
        except Exception as e:
            yield _sse(f"[{ticker}] 오류: {e}", pct)
        time.sleep(0.2)

    yield _sse(f"완료: {total}개 종목, {added_total}일 추가", 100, done=True)


# ─────────────────────────────────────────────
# 수급 분석 계산
# ─────────────────────────────────────────────
def calc_supply_analysis(df: pd.DataFrame, inv_cols: list):
    summary = {}
    dist_series = {}
    for col in inv_cols:
        daily = df[col].fillna(0)
        cum = daily.cumsum()
        peak = cum.cummax()
        dist = (cum / peak.replace(0, np.nan) * 100).fillna(0).clip(0, 100)
        buy_only = daily.clip(lower=0)
        total_buy = buy_only.sum()
        avg_p = float((df["종가"] * buy_only).sum() / total_buy) if total_buy > 0 else 0
        summary[col] = {
            "current_hold": int(cum.iloc[-1]),
            "peak_accum": int(peak.iloc[-1]),
            "dist_ratio": round(float(dist.iloc[-1]), 1),
            "avg_price": round(avg_p),
        }
        dist_series[col] = dist.round(1).tolist()
    pos_total = sum(max(0, v["current_hold"]) for v in summary.values())
    for col in inv_cols:
        hold = max(0, summary[col]["current_hold"])
        summary[col]["leadership_pct"] = round(hold / pos_total * 100, 1) if pos_total > 0 else 0
    return {"summary": summary, "dist_series": dist_series}


# ─────────────────────────────────────────────
# KRX 종목 검색
# ─────────────────────────────────────────────
def krx_search_by_code(code: str) -> list:
    params = urllib.parse.urlencode({
        "bld": "dbms/comm/finder/finder_stkisu",
        "locale": "ko_KR",
        "pagePath": "/contents/COM/FinderStkIsu.jsp",
        "isuSrtCd": code,
        "mktId": "ALL",
        "sortRule": "D",
        "pageSize": "15",
        "currentPageSize": "15",
        "page": "1",
    })
    req = urllib.request.Request(
        KRX_SEARCH_URL, data=params.encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "User-Agent": "Mozilla/5.0",
                 "Referer": "http://data.krx.co.kr/"})
    with urllib.request.urlopen(req, timeout=8) as r:
        data = json.loads(r.read().decode("utf-8"))
    results = []
    q_up = code.upper()
    for item in data.get("block1", []):
        if q_up in item["short_code"] or q_up.lower() in item["codeName"].lower():
            market = "KOSPI" if item.get("marketEngName") == "KOSPI" else "KOSDAQ"
            results.append({"ticker": item["short_code"], "name": item["codeName"], "market": market})
            if len(results) >= 15:
                break
    return results


# ─────────────────────────────────────────────
# API 엔드포인트
# ─────────────────────────────────────────────
@app.get("/api/cache-status")
def cache_status():
    return {"ready": cache_ready, "count": len(ticker_cache)}


@app.get("/api/search")
def search(q: str = Query(..., min_length=1)):
    if ticker_cache:
        q_up = q.upper()
        q_lo = q.lower()
        results = []
        for ticker, info in ticker_cache.items():
            if q_up in ticker or q_lo in info["name"].lower():
                results.append({"ticker": ticker, "name": info["name"], "market": info["market"]})
                if len(results) >= 15:
                    break
        if results:
            return results
    try:
        return krx_search_by_code(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 업데이트 진행 상태 (중복 실행 방지 + 상태 조회용)
_update_state = {"running": False, "started_at": None, "finished_at": None, "last_message": None}
_update_lock = threading.Lock()


def _run_update_thread(ticker_list, q: queue.Queue):
    """백그라운드 스레드에서 전체 업데이트 실행.
    클라이언트(cron)가 연결을 끊어도 업데이트는 끝까지 진행된다."""
    try:
        for msg in do_server_update_all(ticker_list):
            _update_state["last_message"] = msg
            q.put(msg)
    except Exception as e:
        q.put(_sse(f"업데이트 스레드 오류: {e}", done=True))
    finally:
        _update_state["running"] = False
        _update_state["finished_at"] = now_kst().isoformat()
        q.put(None)


@app.get("/api/admin/update-all")
async def admin_update_all(
    tickers: str = Query(None, description="쉼표 구분 종목코드. 없으면 전체"),
    x_update_secret: str | None = Header(None),
):
    """서버에서 수급 업데이트 (SSE 스트리밍, 백그라운드 실행).
    X-Update-Secret 헤더가 UPDATE_SECRET 환경변수와 일치해야 한다.
    """
    if not UPDATE_SECRET:
        raise HTTPException(
            status_code=503,
            detail="UPDATE_SECRET 환경변수가 설정되지 않았습니다. Railway 대시보드에서 설정하세요.")
    if x_update_secret != UPDATE_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    with _update_lock:
        if _update_state["running"]:
            raise HTTPException(status_code=409, detail="이미 업데이트가 진행 중입니다")
        _update_state["running"] = True
        _update_state["started_at"] = now_kst().isoformat()
        _update_state["finished_at"] = None

    ticker_list = [t.strip() for t in tickers.split(",")] if tickers else None
    q: queue.Queue = queue.Queue()
    threading.Thread(target=_run_update_thread, args=(ticker_list, q), daemon=True).start()

    async def async_generate():
        loop = asyncio.get_event_loop()
        while True:
            msg = await loop.run_in_executor(None, q.get)
            if msg is None:
                break
            yield msg

    return StreamingResponse(
        async_generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/admin/status")
def admin_status(x_update_secret: str | None = Header(None)):
    """데이터 디렉터리/업데이트 상태 점검용 (볼륨 마운트·데이터 최신성 확인)"""
    if UPDATE_SECRET and x_update_secret != UPDATE_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    inv_files = sorted(DATA_DIR.glob("*_inv.parquet"))
    price_files = sorted(DATA_DIR.glob("*_price.parquet"))
    samples = {}
    for p in inv_files[:20]:
        try:
            idx = pd.read_parquet(p, columns=[]).index
            samples[p.stem.replace("_inv", "")] = str(pd.to_datetime(idx).max().date()) if len(idx) else None
        except Exception as e:
            samples[p.stem.replace("_inv", "")] = f"오류: {e}"
    return {
        "data_dir": str(DATA_DIR),
        "data_dir_env": os.environ.get("DATA_DIR"),
        "inv_files": len(inv_files),
        "price_files": len(price_files),
        "update_secret_set": bool(UPDATE_SECRET),
        "update_state": _update_state,
        "krx_disabled": time.time() < _krx_state["disabled_until"],
        "server_time_kst": now_kst().isoformat(),
        "inv_last_dates_sample": samples,
    }


@app.get("/api/download/status/{ticker}")
def download_status(ticker: str):
    """로컬 캐시 상태 조회"""
    p = load_price(ticker)
    i = load_inv(ticker)
    return {
        "has_price": not p.empty,
        "price_rows": len(p),
        "price_start": str(p.index[0].date()) if not p.empty else None,
        "price_end": str(p.index[-1].date()) if not p.empty else None,
        "has_inv": not i.empty,
        "inv_rows": len(i),
        "inv_cols": list(i.columns) if not i.empty else [],
    }


@app.get("/api/download/{ticker}")
async def download_stock(ticker: str):
    """SSE 스트리밍으로 전체 이력 다운로드"""
    loop = asyncio.get_event_loop()

    async def async_generate():
        gen = do_full_download(ticker)
        while True:
            # 동기 제너레이터를 스레드에서 돌려 이벤트 루프 블로킹 방지
            msg = await loop.run_in_executor(pool, lambda: next(gen, None))
            if msg is None:
                break
            yield msg

    return StreamingResponse(async_generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/stock/{ticker}")
async def get_stock(ticker: str):
    today = today_kst_str()

    def fetch():
        # 종목명: 캐시 우선 (pykrx 호출은 KRX 차단 시 매달릴 수 있음)
        name = ticker_cache.get(ticker, {}).get("name")
        if not name:
            name = krx_call(lambda: stock.get_market_ticker_name(ticker), timeout_sec=5) or ticker

        # ── OHLCV 로드/업데이트
        df_price = load_price(ticker)
        if df_price.empty:
            # 캐시 없음: Naver 전체 이력 (요청 1번, KRX 차단과 무관하게 동작)
            df_price = fetch_ohlcv_full_naver(ticker)
            if df_price is None or df_price.empty:
                df_price = krx_call(
                    lambda: fetch_ohlcv_pykrx(ticker, "19900101", today), timeout_sec=15)
            if df_price is None or df_price.empty:
                raise ValueError(f"종목 데이터 없음: {ticker}")
            save_price(ticker, df_price)
            cached = False
        else:
            cached = True
            ld = last_date(df_price)
            # 마지막 날짜 이후 증분 업데이트
            if ld and ld < today:
                next_day = (datetime.strptime(ld, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                new_p = krx_call(
                    lambda: fetch_ohlcv_pykrx(ticker, next_day, today), timeout_sec=10)
                if new_p is None or new_p.empty:
                    # KRX 실패 시 Naver 전체 이력으로 대체 (한 번의 요청으로 최신까지 확보)
                    naver_full = fetch_ohlcv_full_naver(ticker)
                    if not naver_full.empty:
                        new_p = naver_full[naver_full.index > df_price.index.max()]
                if new_p is not None and not new_p.empty:
                    df_price = pd.concat([df_price, new_p])
                    df_price = df_price[~df_price.index.duplicated(keep="last")].sort_index()
                    save_price(ticker, df_price)

        # ── 수급 데이터 로드/업데이트
        df_inv = load_inv(ticker)
        inv_cols = list(df_inv.columns) if not df_inv.empty else []
        if df_inv.empty:
            # 캐시 없음: 실시간 (최근 1년만)
            one_year_ago = (now_kst() - timedelta(days=365)).strftime("%Y%m%d")
            df_inv, inv_cols = fetch_investor_range(ticker, one_year_ago, today)
        else:
            ld = last_date(df_inv)
            if ld and ld < today:
                next_day = (datetime.strptime(ld, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                new_i, new_cols = fetch_investor_range(ticker, next_day, today)
                if not new_i.empty:
                    # 기관계 재계산 (컬럼이 맞지 않을 수 있으므로)
                    df_inv = pd.concat([df_inv, new_i])
                    df_inv = df_inv[~df_inv.index.duplicated(keep="last")].sort_index()
                    if not inv_cols:
                        inv_cols = new_cols
                    save_inv(ticker, df_inv)

        # ── 병합
        df_price["등락률"] = (df_price["종가"].pct_change() * 100).round(2)
        if not df_inv.empty and inv_cols:
            df_price = df_price.join(df_inv[inv_cols], how="left")
            for col in inv_cols:
                df_price[f"누적_{col}"] = df_price[col].fillna(0).cumsum()

        # ── 수급 분석
        supply = {}
        if inv_cols:
            supply = calc_supply_analysis(df_price, inv_cols)
            for col in inv_cols:
                df_price[f"분산비율_{col}"] = supply["dist_series"][col]

        df_price = df_price.replace([np.inf, -np.inf], 0).fillna(0)
        df_price.index = df_price.index.strftime("%Y-%m-%d")

        records = []
        for date_str, row in df_price.iterrows():
            rec = {"date": date_str}
            for k, v in row.items():
                if isinstance(v, np.integer):
                    rec[k] = int(v)
                elif isinstance(v, np.floating):
                    rec[k] = round(float(v), 2)
                else:
                    rec[k] = v
            records.append(rec)

        return {
            "ticker": ticker,
            "name": name,
            "market": ticker_cache.get(ticker, {}).get("market", ""),
            "inv_cols": inv_cols,
            "supply_summary": supply.get("summary", {}),
            "cached": cached,
            "data": records,
        }

    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(pool, fetch)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


app.mount("/", StaticFiles(directory="static", html=True), name="static")
