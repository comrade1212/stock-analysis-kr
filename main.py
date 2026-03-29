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
import re
import requests as req_lib

ticker_cache: dict = {}
cache_ready = False
KRX_SEARCH_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"


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
pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)


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


def naver_frgn_data(ticker: str, max_pages: int = 20) -> pd.DataFrame:
    """Naver 외국인 순매수 (KRX 차단 시 fallback)"""
    all_rows = []
    for page in range(1, max_pages + 1):
        try:
            r = req_lib.get(
                f"http://finance.naver.com/item/frgn.naver?code={ticker}&page={page}",
                headers={"User-Agent": "Mozilla/5.0", "Referer": "http://finance.naver.com/"},
                timeout=8)
            tables = re.findall(r'<table[^>]*>.*?</table>', r.text, re.DOTALL)
            found = False
            for t in tables:
                rows = re.findall(r'<tr[^>]*>.*?</tr>', t, re.DOTALL)
                for row in rows:
                    tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                    vals = [re.sub(r'<[^>]+>', '', td).strip().replace('\xa0', '').replace('\n', '').replace('\t', '') for td in tds]
                    vals = [v for v in vals if v]
                    if vals and re.match(r'\d{4}\.\d{2}\.\d{2}', vals[0]):
                        try:
                            net_buy = int(vals[5].replace(',', '').replace('+', '')) if len(vals) > 5 else 0
                            all_rows.append({"date": vals[0].replace('.', '-'), "외국인": net_buy})
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
    return df.sort_values("date").set_index("date")


def fetch_investor_data(ticker: str, today: str):
    """KRX 상세(전체투자자) → KRX 기본 → Naver 외국인 순으로 시도 (주수 기반)"""
    # 1순위: KRX 상세 거래량 (개인/외국인/기관계/금융투자/보험/투신/기타금융/은행/연기금등/사모펀드/기타법인/내외국인)
    try:
        df = stock.get_market_trading_volume_by_date("19900101", today, ticker, detail=True)
        if not df.empty:
            return df, [c for c in df.columns if c != "전체"]
    except Exception:
        pass
    # 2순위: KRX 기본 거래량 (개인/외국인/기관계/기타법인)
    try:
        df = stock.get_market_trading_volume_by_date("19900101", today, ticker)
        if not df.empty:
            return df, [c for c in df.columns if c != "전체"]
    except Exception:
        pass
    # 3순위: KRX 상세 거래대금 fallback
    try:
        df = stock.get_market_trading_value_by_date("19900101", today, ticker, detail=True)
        if not df.empty:
            return df, [c for c in df.columns if c != "전체"]
    except Exception:
        pass
    # 4순위: Naver 외국인 only
    df = naver_frgn_data(ticker)
    return df, list(df.columns) if not df.empty else []


def calc_supply_analysis(df: pd.DataFrame, inv_cols: list):
    """
    투자주체별 수급 분석 계산
    - 매집고점: 누적순매수의 역대 최고값
    - 현재보유수량: 현재 누적순매수
    - 분산비율(%): 현재보유 / 매집고점 × 100
    - 평균단가: 순매수 기준 가중평균 매수가
    - 주가선도비중: 양의 보유량 기준 상대 비중
    """
    summary = {}
    dist_series = {}

    for col in inv_cols:
        daily = df[col].fillna(0)
        cum = daily.cumsum()
        peak = cum.cummax()

        # 분산비율 시계열
        dist = (cum / peak.replace(0, np.nan) * 100).fillna(0).clip(0, 100)

        # 평균단가 (매수분만 가중평균)
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

    # 주가선도비중
    pos_total = sum(max(0, v["current_hold"]) for v in summary.values())
    for col in inv_cols:
        hold = max(0, summary[col]["current_hold"])
        summary[col]["leadership_pct"] = round(hold / pos_total * 100, 1) if pos_total > 0 else 0

    return {"summary": summary, "dist_series": dist_series}


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


@app.get("/api/stock/{ticker}")
async def get_stock(ticker: str):
    today = datetime.now().strftime("%Y%m%d")

    def fetch():
        # 종목명
        try:
            name = stock.get_market_ticker_name(ticker) or ticker
        except Exception:
            name = ticker_cache.get(ticker, {}).get("name", ticker)

        # OHLCV (상장일~현재)
        df_price = stock.get_market_ohlcv_by_date("19900101", today, ticker)
        if df_price is None or df_price.empty:
            raise ValueError(f"종목 데이터 없음: {ticker}")

        df_price["등락률"] = (df_price["종가"].pct_change() * 100).round(2)

        # 수급 데이터 (전체 투자자)
        df_inv, inv_cols = fetch_investor_data(ticker, today)

        if not df_inv.empty and inv_cols:
            df_price = df_price.join(df_inv[inv_cols], how="left")
            for col in inv_cols:
                df_price[f"누적_{col}"] = df_price[col].fillna(0).cumsum()

        # 수급 분석 계산
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
