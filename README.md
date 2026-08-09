# 한국 주식 수급분석 (Railway 배포)

FastAPI 기반 수급분석 서비스. Railway(`systemtrading-rest-production.up.railway.app`)에 배포.

---

## 💰 Railway 비용 구조와 월 $5 이내로 유지하는 방법

Railway는 **컨테이너가 켜져 있는 시간만큼** RAM/CPU를 분 단위로 과금한다
(약 RAM $10/GB·월, CPU $20/vCPU·월). 이 앱은 pandas + pykrx를 로드해서
**상시 약 250~400MB RAM**을 점유하므로, 24시간 켜 두면 **RAM만으로 월 $2.5~4**가
나가고, 여기에 KRX 호출이 매달리며 발생하던 CPU 사용까지 더하면 $5를 쉽게 넘는다.

### 적용된 최적화 (이 저장소)

| 항목 | 변경 | 효과 |
|---|---|---|
| **앱 슬립** | `railway.json`에 `"sleepApplication": true` | 트래픽 없으면 컨테이너 중지 → **과금 0**. 요청 오면 수 초 내 자동 기동 |
| **KRX 호출 타임아웃** | 모든 pykrx 호출에 5~15초 타임아웃 + 연속 실패 시 1시간 KRX 생략 | KRX가 해외 IP를 차단해 무한 대기하던 CPU 낭비 제거 |
| **스레드풀 축소** | 8 → 4 워커 | CPU 스파이크(과금 피크) 완화 |
| **cron을 GitHub Actions로** | `.github/workflows/supply-update.yml` | Railway에 별도 cron 서비스(추가 과금) 불필요 — GitHub Actions는 무료 |

### 사용자가 직접 해야 하는 설정 (필수)

1. **하드 사용 한도 $5 설정** — 코드로는 불가능, 대시보드에서만 가능:
   - Railway 대시보드 → 우측 상단 프로필 → **Workspace Settings** → **Usage Limits**
   - **Hard Limit: $5** 입력 (도달 시 서비스가 자동 중지되어 초과 과금이 원천 차단됨)
   - 필요하면 Soft Limit $4로 설정해 이메일 경고를 먼저 받기
2. **앱 슬립 확인**: 이 브랜치 배포 후 서비스 → Settings → **App Sleeping**이
   활성화됐는지 확인 (config-as-code가 반영되면 자동으로 켜짐)

---

## 📉 "수급 업데이트가 안 되는" 문제의 원인과 해결

### 원인

1. **cron이 아예 없었음** — `render.yaml`의 cron 정의는 **Render 전용**이라
   Railway에서는 무시된다. 즉, Railway에는 매일 수급 업데이트를 트리거하는
   장치가 존재하지 않았다.
2. **KRX(pykrx) 호출이 매달림** — KRX는 해외 데이터센터 IP를 자주 차단한다.
   기존 코드는 타임아웃 없이 pykrx를 호출해서, 업데이트를 수동 실행해도
   응답을 기다리다 멈추거나 cron 타임아웃으로 중단됐다.
3. **pandas 호환성** — `main.py`의 Naver 폴백 파서가 `pd.read_html()`에 HTML
   문자열을 직접 넘겼는데, 최신 pandas에서는 실패한다 → Naver 폴백도 조용히
   빈 결과를 반환.
4. **시간대 버그** — 서버는 UTC로 돌아서 `datetime.now()` 기반 "오늘" 계산이
   KST와 어긋났다 (한국 새벽 시간대에 하루 밀림).

### 해결 (이 브랜치)

- ✅ **GitHub Actions cron 추가**: 평일 18:40 KST에 `/api/admin/update-all` 호출
- ✅ pykrx 호출 전부 타임아웃 + 실패 누적 시 자동으로 Naver 폴백 전환
- ✅ `read_html(StringIO(...))` 로 pandas 최신 버전 호환
- ✅ 모든 날짜 계산 KST 고정
- ✅ 업데이트를 **백그라운드 스레드**로 실행 — cron 연결이 끊겨도 끝까지 완료
- ✅ OHLCV 갱신도 KRX 실패 시 Naver로 자동 대체
- ✅ `/api/admin/status` 진단 엔드포인트 추가

### 설정 체크리스트 (수급 업데이트가 돌려면 전부 필요)

| # | 설정 | 위치 | 내용 |
|---|---|---|---|
| 1 | `UPDATE_SECRET` | Railway → 서비스 → **Variables** | 임의의 긴 문자열. 미설정 시 업데이트 엔드포인트가 503으로 거부함 (무단 호출로 인한 과금 방지) |
| 2 | `UPDATE_SECRET` | GitHub 저장소 → Settings → **Secrets and variables → Actions → Secrets** | Railway와 **동일한 값** |
| 3 | `SERVICE_URL` (선택) | GitHub → Settings → Secrets and variables → Actions → **Variables** | 기본값이 `https://systemtrading-rest-production.up.railway.app` 이므로 URL이 바뀐 경우만 |
| 4 | Volume + `DATA_DIR` | Railway → 서비스 → 우클릭 **Attach Volume** (예: `/app/data`) → Variables에 `DATA_DIR=/app/data` | **볼륨이 없으면 재배포 때마다 데이터가 사라져** 업데이트할 대상 자체가 없어짐 |

### 동작 확인

```bash
# 상태 진단 (볼륨 마운트, 파일 수, 마지막 수급 날짜 확인)
curl -H "X-Update-Secret: <시크릿>" \
  https://systemtrading-rest-production.up.railway.app/api/admin/status

# 수동으로 수급 업데이트 실행 (SSE로 진행 상황 출력)
curl -N -H "X-Update-Secret: <시크릿>" \
  https://systemtrading-rest-production.up.railway.app/api/admin/update-all
```

GitHub Actions 탭에서 **"수급 데이터 일일 업데이트"** 워크플로를 **Run workflow**
버튼으로 수동 실행해 볼 수도 있다.

---

## 구조

```
main.py          FastAPI 서버 (검색/조회/다운로드/수급 업데이트 API + 정적 프런트)
update_db.py     로컬 일괄 업데이트 스크립트
cron_trigger.py  외부 cron에서 update-all 을 호출하는 헬퍼 (Actions 워크플로가 동일 역할)
railway.json     Railway 배포 설정 (앱 슬립 포함)
render.yaml      Render 전용 설정 (Railway에서는 사용되지 않음)
static/          프런트엔드
```
