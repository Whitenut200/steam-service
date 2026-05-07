# Steam 게임 분석 서비스

> Steam 게임 데이터를 실시간 수집·분석해 **그로스 마케팅 인사이트**로 변환하는 데이터 서비스. BigQuery + Cloud Functions + Vertex AI Gemini 기반 end-to-end 파이프라인.

## 핵심 기능

- **자동 데이터 수집** — Cloud Scheduler + Cloud Functions로 매일/3시간 단위 Steam API 데이터 적재 (게임 메타·리뷰·뉴스·가격·동접)
- **3탭 대시보드** — Steam 다크 테마 Streamlit. 게임 검색 → 종합/리뷰&플레이타임/가격&동접 3개 탭 분석
- **AI 코멘트 (3섹션 9항목)** — Gemini 2.5 Flash로 통계 검정 결과를 자연어 인사이트로 변환
  - ① 감성·키워드 요약 (TF-IDF 차별화 키워드 + 4주 추세)
  - ② 유저 세그먼트 시그널 (플레이타임 코호트 + 언어 갭)
  - ③ 변화 알림 (패치 임팩트 + 세일 효과 + 동접 z-score 이상치)
- **통계적 유의성 적용** — 두 비율 z-test, baseline z-score로 단순 % 비교의 함정 회피

## 라이브 데모

배포 URL: (Streamlit Cloud 배포 후 추가 예정)

## 기술 스택

| 레이어 | 기술 |
|--------|------|
| 데이터 수집 | Python, functions-framework, Steam API, IsThereAnyDeal API |
| 인프라 | GCP Cloud Functions, Cloud Scheduler, Cloud Storage |
| 데이터 저장/분석 | BigQuery (SQL + Views) |
| NLP | Kiwi (한국어 형태소 분석), Deep Translator |
| AI | Vertex AI Gemini 2.5 Flash |
| 대시보드 | Streamlit, Plotly |
| 호스팅 | Streamlit Community Cloud |

## 아키텍처

```
┌─────────────────────┐
│ Cloud Scheduler     │ 매일 / 3시간 트리거
└──────────┬──────────┘
           ↓
┌─────────────────────┐    Steam API
│ Cloud Functions × 5 │ ←  IsThereAnyDeal API
│ (collect / process /│    Google Translate
│  translate / analyze│
│  / player_counts)   │
└──────────┬──────────┘
           ↓ insert_rows_json
┌─────────────────────┐
│ BigQuery steam_data │  games / reviews / news /
│ (8 테이블 + Views)   │  price_history / player_counts /
└──────────┬──────────┘  review_summary / category_analysis /
           ↓             keyword_analysis
┌─────────────────────┐
│ Streamlit Dashboard │  + Vertex AI Gemini (AI 코멘트)
└─────────────────────┘
```

## 프로젝트 구조

```
steam_service/
├── analyses/              # AI 코멘트 분석 모듈 (3섹션 9항목)
│   ├── sentiment_summary.py    # ① 감성 요약 통합
│   ├── tfidf_keywords.py       # ① TF-IDF 차별화 키워드
│   ├── segment_signals.py      # ② 코호트 + 언어 갭
│   ├── event_signals.py        # ③ 패치/세일/키워드 surge/동접 이상치
│   └── gemini_comment.py       # 3섹션 프롬프트 + Gemini 호출
├── collectors/            # Steam/ITAD API 수집기
├── cloud_functions/       # GCP Cloud Functions 엔트리포인트
│   ├── main.py            # 5개 함수 정의
│   └── deploy.sh          # 함수 + 스케줄러 배포 스크립트
├── dashboard/
│   └── app.py             # Streamlit 대시보드 (3탭 + AI popover)
├── bigquery/
│   ├── schema.sql         # 8개 테이블 스키마
│   └── views.sql          # 분석용 뷰
├── utils/                 # 공통 헬퍼 (BQ client, 환경, 번역)
├── first/                 # 초기 데이터 적재 스크립트
└── config/                # 설정 (KST, GCS bucket)
```

## 로컬 실행

```bash
# 1. 의존성 설치
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux
pip install -r requirements.txt

# 2. 인증 — credentials.json (GCP 서비스 계정 키) 프로젝트 루트에 배치
# 3. 환경변수 설정 (.env 파일 또는 utils/env.py에서 자동 로드)

# 4. 대시보드 실행
streamlit run dashboard/app.py
```

## 구현 하이라이트

### 1. 통계 기반 변화 감지

단순 % 비교 대신 게임별 자체 변동성을 baseline으로 두고 z-score로 판단.

> **사례**: 게임 A의 세일 기간 동접 +22% — 단순 비교로는 "세일 효과 있음"으로 보이지만, 게임 평소 변동성 σ=7,000명 기준 z=+0.37(평소 변동 범위 안)으로 통계적 유의성 없음 판정. 단편 비교의 함정 자동 회피.

`event_signals.py`에 `_two_proportion_z`, `_categorize_z` 등 통계 헬퍼 + 5단계 카테고리(뚜렷한_증가/약한_증가/평소_변동_범위/...)로 일관된 라벨링.

### 2. Gemini 프롬프트 설계 — "구체 숫자 → 통계 결론 → 액션 시그널"

3개 섹션 각각 전용 프롬프트. 통계 용어(z-score, 표준편차)는 금지하되 "통계적 검정 결과 유의미한 변화" 같은 평이한 표현은 허용. 출력 패턴 예시:

> "60% 할인 동안 동접이 직전 대비 22.1% 늘었지만, 통계적 검정 결과 평소 변동 범위 안이라 세일 효과로 단정하긴 어렵습니다. **따라서** 가격 인하 자체보다 다른 요인 영향일 가능성도 봐야 합니다."

### 3. 단일 트리거 + 병렬 실행

popover 제목 옆 단일 "▶ 분석 실행" 버튼 클릭 → `ThreadPoolExecutor(max_workers=3)`로 3 섹션 동시 호출. 직렬 ~15초 → 병렬 ~5-8초.

### 4. 이중 캐싱으로 비용 통제

- `@st.cache_data(ttl=3600)` — 프로세스 레벨 (모든 사용자 공유, 같은 게임 재호출 시 BQ/Gemini 안 탐)
- `st.session_state` — 브라우저 세션 (popover 열고 닫아도 결과 유지)
- 자연 트래픽 기준 월 AI 비용 < $1

### 5. TF-IDF 차별화 키워드 — 전체 vs 같은 장르 두 코퍼스 비교

`genres` 필드의 첫 2 토큰이 일치하는 게임을 "같은 장르 코퍼스"로 정의해 각 게임만의 차별화 키워드를 두 관점에서 추출. 마케팅 메시지 발굴용.

## 문서

- [PROJECT_PLAN.md](PROJECT_PLAN.md) — 상세 설계, 분석 방법론, AI 컨셉, MVP 단계
- [DEPLOY.md](DEPLOY.md) — Streamlit Community Cloud 배포 가이드 + 비용 통제
- [SETUP_GUIDE.md](SETUP_GUIDE.md) — 초기 GCP 환경 셋업

## 라이선스

MIT (또는 본인 선택)

---

**Author**: [Whitenut200](https://github.com/Whitenut200) · **Email**: dtbldus34@gmail.com
