from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
import concurrent.futures
import threading
import urllib.request
import urllib.parse
import json

ticker_cache: dict = {}
cache_ready = False

KRX_SEARCH_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

def krx_search_by_code(code: str) -> list:
    """KRX 파인더 API로 코드 검색 (캐시 미준비 시 폴백용)"""
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
        KRX_SEARCH_URL,
        data=params.encode(),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0",
            "Referer": "http://data.krx.co.kr/",
        },
    )
    with urllib.request.urlopen(req, timeout=8) as r:
        data = json.loads(r.read().decode("utf-8"))
    results = []
    q_up = code.upper()
    for item in data.get("block1", []):
        if q_up in item["short_code"] or q_up.lower() in item["codeName"].lower():
            market = "KOSPI" if item.get("marketEngName") == "KOSPI" else "KOSDAQ"
            results.append({
                "ticker": item["short_code"],
                "name": item["codeName"],
                "market": market,
            })
            if len(results) >= 15:
                break
    return results

def build_cache():
    """앱 시작 시 전종목 캐시 빌드 (KRX API 활용)"""
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
            KRX_SEARCH_URL,
            data=params.encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0",
                "Referer": "http://data.krx.co.kr/",
            },
        )
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
pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)


@app.get("/api/cache-status")
def cache_status():
    return {"ready": cache_ready, "count": len(ticker_cache)}


@app.get("/api/search")
def search(q: str = Query(..., min_length=1)):
    # 캐시가 준비된 경우 로컬 검색
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
    # 캐시 미준비 시 KRX API 직접 검색 (코드 검색만 가능)
    try:
        return krx_search_by_code(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def naver_frgn_data(ticker: str, max_pages: int = 20) -> pd.DataFrame:
    """Naver Finance에서 외국인 순매수 데이터 수집 (KRX 불가 시 fallback, 최근 ~300일)"""
    import re
    import requests as req_lib

    all_rows = []
    for page in range(1, max_pages + 1):
        try:
            r = req_lib.get(
                f"http://finance.naver.com/item/frgn.naver?code={ticker}&page={page}",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "http://finance.naver.com/"},
                timeout=8,
            )
            text = r.text
            tables = re.findall(r'<table[^>]*>.*?</table>', text, re.DOTALL)

            found = False
            for t in tables:
                rows = re.findall(r'<tr[^>]*>.*?</tr>', t, re.DOTALL)
                for row in rows:
                    tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                    vals = [re.sub(r'<[^>]+>', '', td).strip().replace('\xa0', '').replace('\n', '').replace('\t', '') for td in tds]
                    vals = [v for v in vals if v]
                    if vals and re.match(r'\d{4}\.\d{2}\.\d{2}', vals[0]):
                        try:
                            date_str = vals[0].replace('.', '-')
                            net_buy_str = vals[5] if len(vals) > 5 else '0'
                            net_buy = int(net_buy_str.replace(',', '').replace('+', ''))
                            all_rows.append({"date": date_str, "외국인": net_buy})
                            found = True
                        except Exception:
                            pass
            if not found:
                break
        except Exception:
            break

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    return df


def fetch_investor_data(ticker: str, today: str) -> tuple[pd.DataFrame, list]:
    """KRX 우선, 실패 시 Naver fallback으로 수급 데이터 수집"""
    df_inv = pd.DataFrame()
    try:
        df_inv = stock.get_market_trading_value_by_date("19900101", today, ticker, detail=True)
        if df_inv.empty:
            raise ValueError("KRX empty")
    except TypeError:
        try:
            df_inv = stock.get_market_trading_value_by_date("19900101", today, ticker)
            if df_inv.empty:
                raise ValueError("KRX empty")
        except Exception:
            df_inv = naver_frgn_data(ticker)
    except Exception:
        df_inv = naver_frgn_data(ticker)

    inv_cols = []
    if not df_inv.empty:
        inv_cols = [c for c in df_inv.columns if c != "전체"]
    return df_inv, inv_cols


@app.get("/api/stock/{ticker}")
async def get_stock(ticker: str):
    today = datetime.now().strftime("%Y%m%d")

    def fetch():
        try:
            name = stock.get_market_ticker_name(ticker) or ticker
        except Exception:
            name = ticker

        # OHLCV (상장일부터 현재까지)
        df_price = stock.get_market_ohlcv_by_date("19900101", today, ticker)
        if df_price is None or df_price.empty:
            raise ValueError(f"종목 데이터 없음: {ticker}")

        # 등락률 계산
        df_price["등락률"] = (df_price["종가"].pct_change() * 100).round(2)

        # 수급 데이터
        df_inv, inv_cols = fetch_investor_data(ticker, today)

        if not df_inv.empty and inv_cols:
            df_price = df_price.join(df_inv[inv_cols], how="left")
            for col in inv_cols:
                df_price[f"누적_{col}"] = df_price[col].fillna(0).cumsum()

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
            "inv_cols": inv_cols,
            "data": records,
        }

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(pool, fetch)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


app.mount("/", StaticFiles(directory="static", html=True), name="static")
