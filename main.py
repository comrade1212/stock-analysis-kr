from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import concurrent.futures
import threading
import urllib.request
import urllib.parse
import json
import re

KRX_API_KEY = "8B52DF8BF23543EFBAF0AD410C0C658E44FDBADD"
DATA_DIR = Path(__file__).parent / "data"
import requests as _requests  # Naver 스크래핑용
DATA_DIR.mkdir(exist_ok=True)

ticker_cache: dict = {}
cache_ready = False
KRX_SEARCH_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

# 기관계 구성 세부 투자주체 (합산용)
INSTITUTION_COLS = ["금융투자", "보험", "투신", "사모", "은행", "기타금융", "연기금등"]
# detail=True 컬럼 순서
INV_ORDER = ["개인", "외국인", "기관계"] + INSTITUTION_COLS + ["기타법인", "기타외국인"]


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
pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)


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
    today = datetime.now().strftime("%Y%m%d")
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
        tables = pd.read_html(html, flavor="lxml", encoding="utf-8")
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for df in ex.map(get_page, pages):
            if not df.empty:
                results.append(df)

    if not results:
        return pd.DataFrame()
    df_all = pd.concat(results).sort_index()
    return df_all[~df_all.index.duplicated(keep="last")]


def fetch_investor_range(ticker: str, from_date: str, to_date: str) -> tuple[pd.DataFrame, list]:
    """수급 데이터 수집. Naver(기관+외국인) 우선, KRX 상세 시도 병행."""
    # 1순위: KRX 상세 시도 (서버 환경에서 작동 가능)
    try:
        df = stock.get_market_trading_value_by_date(from_date, to_date, ticker, detail=True)
        if not df.empty:
            inst_present = [c for c in INSTITUTION_COLS if c in df.columns]
            if inst_present and "기관계" not in df.columns:
                df["기관계"] = df[inst_present].sum(axis=1)
            cols = [c for c in INV_ORDER if c in df.columns]
            extra = [c for c in df.columns if c not in cols and c != "전체"]
            cols += extra
            return df[cols], cols
    except Exception:
        pass
    try:
        df = stock.get_market_trading_value_by_date(from_date, to_date, ticker)
        if not df.empty:
            cols = [c for c in df.columns if c != "전체"]
            return df[cols], cols
    except Exception:
        pass

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
    today = datetime.now().strftime("%Y%m%d")

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
    def generate():
        for msg in do_full_download(ticker):
            yield msg

    loop = asyncio.get_event_loop()

    async def async_generate():
        for msg in do_full_download(ticker):
            yield msg
            await asyncio.sleep(0)

    return StreamingResponse(async_generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/stock/{ticker}")
async def get_stock(ticker: str):
    today = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    def fetch():
        # 종목명
        try:
            name = stock.get_market_ticker_name(ticker) or ticker
        except Exception:
            name = ticker_cache.get(ticker, {}).get("name", ticker)

        # ── OHLCV 로드/업데이트
        df_price = load_price(ticker)
        if df_price.empty:
            # 캐시 없음: 실시간 pykrx (최근 12년)
            df_price = fetch_ohlcv_pykrx(ticker, "19900101", today)
            if df_price is None or df_price.empty:
                raise ValueError(f"종목 데이터 없음: {ticker}")
            cached = False
        else:
            cached = True
            ld = last_date(df_price)
            # 마지막 날짜 이후 증분 업데이트
            if ld and ld < today:
                next_day = (datetime.strptime(ld, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
                new_p = fetch_ohlcv_pykrx(ticker, next_day, today)
                if not new_p.empty:
                    df_price = pd.concat([df_price, new_p])
                    df_price = df_price[~df_price.index.duplicated(keep="last")].sort_index()
                    save_price(ticker, df_price)

        # ── 수급 데이터 로드/업데이트
        df_inv = load_inv(ticker)
        inv_cols = list(df_inv.columns) if not df_inv.empty else []
        if df_inv.empty:
            # 캐시 없음: 실시간 pykrx (최근 1년만)
            one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
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
