# GCP 설정 & 실행 가이드

## 1. 로컬 테스트

### 패키지 설치
```bash
cd D:/project/steam_service
pip install -r requirements.txt
```

### Steam API 수집 테스트
```bash
# 게임 정보 수집 테스트
python -m collectors.game_info

# 리뷰 수집 테스트
python -m collectors.reviews

# 가격 스냅샷 테스트
python -m collectors.price_snapshot

# 뉴스 수집 테스트
python -m collectors.news
```

---

## 2. GCP 프로젝트 설정

### 2-1. 프로젝트 ID 확인
```bash
gcloud projects list
```

### 2-2. 프로젝트 선택
```bash
gcloud config set project YOUR_PROJECT_ID
```

### 2-3. 필요한 API 활성화
```bash
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    bigquery.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com
```

---

## 3. BigQuery 데이터셋 & 테이블 생성

### 3-1. 데이터셋 생성
```bash
bq mk --dataset --location=asia-northeast3 YOUR_PROJECT_ID:steam_data
```

### 3-2. 테이블 생성 (BigQuery 콘솔에서 SQL 실행)
`bigquery/schema.sql` 파일의 CREATE TABLE 문을 BigQuery 콘솔에서 실행

또는 bq 명령어로:
```bash
bq query --use_legacy_sql=false < bigquery/schema.sql
```

---

## 4. Cloud Functions 배포

### 4-1. 환경변수 설정
```bash
export GCP_PROJECT_ID=your-project-id
```

### 4-2. 배포
```bash
cd cloud_functions
bash deploy.sh
```

---

## 5. 동작 확인

### 수동 트리거 테스트
```bash
# 매일 수집 함수 테스트
curl https://asia-northeast3-YOUR_PROJECT_ID.cloudfunctions.net/collect-daily

# 리뷰 수집 함수 테스트
curl https://asia-northeast3-YOUR_PROJECT_ID.cloudfunctions.net/collect-reviews
```

### BigQuery에서 데이터 확인
```sql
SELECT * FROM steam_data.games LIMIT 10;
SELECT * FROM steam_data.price_history LIMIT 10;
SELECT * FROM steam_data.review_summary LIMIT 10;
```

---

## 수집 스케줄

| 함수 | 주기 | 시간 (KST) | 내용 |
|---|---|---|---|
| collect-daily | 매일 | 06:00 | 게임 정보 + 리뷰 요약 + 가격 + 뉴스 |
| collect-reviews | 매주 월 | 07:00 | 상세 리뷰 텍스트 (NLP용) |
