# 배포 가이드 — Streamlit Community Cloud

## 사전 준비

### 1. GCP Billing Alert 설정 (반드시 먼저)

[Console → Billing → Budgets & alerts](https://console.cloud.google.com/billing/budgets) → CREATE BUDGET

- 월 한도: $5 (또는 본인 기준)
- 알림 임계값: 50% / 90% / 100%
- 이메일 알림 ON

AI 코멘트는 호출당 ~$0.001이지만 abuse 시 누적될 수 있음. 알림은 무료니 무조건 켤 것.

### 2. (선택) Vertex AI Quota 제한

[Console → IAM & Admin → Quotas](https://console.cloud.google.com/iam-admin/quotas) → "Vertex AI" 검색

일일 prediction 호출 cap을 보수적으로 (예: 1000/일).

---

## 배포 절차

### Step 1. Git 저장소 초기화

```bash
cd D:\project\steam_service
git init
git add .
git status   # credentials.json, .env, venv/ 가 포함되지 않는지 반드시 확인
git commit -m "Initial commit"
```

**확인 포인트** — `git status`에 다음이 보이면 안 됨:
- `credentials.json` (.gitignore에 `*.json` 있어 자동 제외)
- `.env`
- `venv/`
- `output/`, `_*.txt`, `_*.json` (임시 파일들)

### Step 2. GitHub repo 생성 + push

```bash
# GitHub에서 빈 repo 생성 후
git remote add origin https://github.com/<USERNAME>/<REPO>.git
git branch -M main
git push -u origin main
```

### Step 3. Streamlit Community Cloud 연결

1. [share.streamlit.io](https://share.streamlit.io/) 접속 → GitHub 계정 연결
2. **New app** → 방금 push한 repo 선택
3. Branch: `main`, Main file path: `dashboard/app.py`
4. **Advanced settings** → **Secrets** 탭에서 GCP 서비스 계정 키 입력:

   `.streamlit/secrets.toml.example` 형식 그대로 — `credentials.json`의 내용을 TOML로 변환해서 붙여넣기.

   **변환 팁** — `credentials.json`의 키들을 `[gcp_service_account]` 섹션 아래 한 줄씩 옮기되:
   - `private_key`는 triple-quote `"""..."""`로 감싸기 (개행 보존)
   - 나머지 문자열 값은 일반 따옴표

5. **Deploy** 클릭

### Step 4. 배포 후 확인

배포된 URL에서:
- 게임 검색 정상 동작
- 🤖 popover 열기 → "▶ 분석 실행" → 3 카드 출력 확인
- BQ 쿼리/Gemini 호출이 Cloud 환경에서도 통과되는지

---

## 비용 모니터링

배포 후 1~2주 동안 [GCP Billing Dashboard](https://console.cloud.google.com/billing/) 모니터링:
- 월 $1 이내: 정상 (자연 트래픽)
- 월 $5 이상: 비정상, 추가 통제 필요

### 비용 폭증 시 대응 옵션

1. **세션당 클릭 제한** — `dashboard/app.py`의 popover 버튼에 `st.session_state` 카운터 추가 (예: 세션당 5회 제한)
2. **AI 버튼 hide** — `selected_app_id` 화이트리스트만 AI 활성화 (Top 10 인기 게임만)
3. **Pre-compute 캐시** — 매일 1회 Top 30 게임 AI 코멘트 미리 계산해서 BQ에 저장, 사용자에겐 캐시만 서빙
4. **인증 게이트** — Streamlit auth 또는 Cloudflare Access로 본인+초대받은 사람만 접근

---

## Cloud Functions 별도

Streamlit Cloud는 대시보드만 호스팅. 데이터 수집은 별도로 GCP Cloud Functions에서 동작 중 (이미 배포됨).

업데이트 시: `bash cloud_functions/deploy.sh functions`

---

## 트러블슈팅

| 증상 | 원인 | 해결 |
|------|------|------|
| `db-dtypes 패키지 필요` | requirements.txt 누락 | 이미 추가됨, 재배포 |
| `DefaultCredentialsError` | Secrets 미설정 또는 잘못 입력 | Streamlit Cloud Secrets에 `[gcp_service_account]` 블록 확인 |
| Gemini 호출 실패 (403) | 서비스 계정에 Vertex AI User 롤 없음 | IAM에서 권한 추가 |
| 한글 깨짐 | streamlit 페이지 인코딩 (브라우저) | 발생 안 함 (UTF-8 기본). 만약 발생 시 `<meta charset>` 확인 |
