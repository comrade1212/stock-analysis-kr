"""
cron_trigger.py — Cron 서비스에서 실행 (Railway Cron / Render Cron 공통)
웹서비스의 /api/admin/update-all 엔드포인트를 호출해 수급 업데이트를 트리거한다.

필요한 환경변수:
    SERVICE_URL    웹서비스의 공개 URL (예: https://xxx.up.railway.app)
    UPDATE_SECRET  웹서비스와 동일한 값
    TIMEOUT        (선택) 응답 대기 최대 초. 기본 1800.
"""
import os
import sys
import requests

SERVICE_URL = os.environ.get("SERVICE_URL", "").rstrip("/")
SECRET = os.environ.get("UPDATE_SECRET", "")
TIMEOUT = int(os.environ.get("TIMEOUT", "1800"))

if not SERVICE_URL:
    print("ERROR: SERVICE_URL 환경변수 없음")
    sys.exit(1)

url = f"{SERVICE_URL}/api/admin/update-all"
headers = {"X-Update-Secret": SECRET} if SECRET else {}

print(f"[cron] 업데이트 요청: {url}")

try:
    with requests.get(url, headers=headers, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        for line in r.iter_lines():
            if line:
                print(line.decode("utf-8", errors="replace"))
except requests.exceptions.RequestException as e:
    print(f"[cron] 오류: {e}")
    sys.exit(1)

print("[cron] 완료")
