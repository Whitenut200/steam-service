#!/bin/bash
# Cloud Functions + Scheduler 배포 스크립트
# 사용법: bash cloud_functions/deploy.sh [functions|schedulers|all]
#   functions  - 함수만 재배포
#   schedulers - 스케줄러만 재생성/업데이트 (idempotent)
#   all        - 둘 다 (기본값)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_DIR="$SCRIPT_DIR"

PROJECT_ID="${GCP_PROJECT_ID:-steam-service-492701}"
REGION="asia-northeast3"
GCS_BUCKET="${GCS_BUCKET:-${PROJECT_ID}-data}"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"
BASE_URI="https://${REGION}-${PROJECT_ID}.cloudfunctions.net"

MODE="${1:-all}"

# ── 헬퍼 ──────────────────────────────────────────────────

# 함수 배포
deploy_fn() {
    local name=$1 entry=$2 memory=$3 timeout=$4 extra_env=$5
    echo ">>> deploy function: $name"
    local env_vars="GCP_PROJECT_ID=$PROJECT_ID,BQ_DATASET=steam_data,GCS_BUCKET=$GCS_BUCKET"
    [ -n "$extra_env" ] && env_vars="$env_vars,$extra_env"

    gcloud functions deploy "$name" \
        --runtime=python311 \
        --region="$REGION" \
        --source="$SOURCE_DIR" \
        --entry-point="$entry" \
        --trigger-http \
        --no-allow-unauthenticated \
        --memory="$memory" \
        --timeout="$timeout" \
        --set-env-vars="$env_vars" \
        --project="$PROJECT_ID"
}

# 스케줄러 생성/업데이트 (idempotent)
upsert_scheduler() {
    local name=$1 schedule=$2 uri_path=$3 deadline="${4:-1800s}"
    local uri="${BASE_URI}/${uri_path}"
    # query string 제거한 audience (Cloud Run은 base URL로 검증)
    local audience="${BASE_URI}/${uri_path%%\?*}"

    if gcloud scheduler jobs describe "$name" --location="$REGION" --project="$PROJECT_ID" >/dev/null 2>&1; then
        echo ">>> update scheduler: $name"
        gcloud scheduler jobs update http "$name" \
            --schedule="$schedule" \
            --time-zone="Asia/Seoul" \
            --uri="$uri" \
            --http-method=POST \
            --oidc-service-account-email="$SERVICE_ACCOUNT" \
            --oidc-token-audience="$audience" \
            --location="$REGION" \
            --attempt-deadline="$deadline" \
            --project="$PROJECT_ID"
    else
        echo ">>> create scheduler: $name"
        gcloud scheduler jobs create http "$name" \
            --schedule="$schedule" \
            --time-zone="Asia/Seoul" \
            --uri="$uri" \
            --http-method=POST \
            --oidc-service-account-email="$SERVICE_ACCOUNT" \
            --oidc-token-audience="$audience" \
            --location="$REGION" \
            --attempt-deadline="$deadline" \
            --project="$PROJECT_ID"
    fi
}

# ── 함수 배포 ─────────────────────────────────────────────

if [ "$MODE" = "functions" ] || [ "$MODE" = "all" ]; then
    echo "=== Cloud Functions 배포 ==="
    deploy_fn collect-daily        collect_daily         512MB  3600s
    deploy_fn process-new-games    process_new_games     512MB  3600s  "ITAD_API_KEY=${ITAD_API_KEY}"
    deploy_fn translate-daily      translate_daily       1024MB 3600s
    deploy_fn analyze-daily        analyze_daily         2048MB 3600s
    deploy_fn collect-player-counts collect_player_counts 256MB  300s
fi

# ── 스케줄러 설정 ─────────────────────────────────────────

if [ "$MODE" = "schedulers" ] || [ "$MODE" = "all" ]; then
    echo "=== Cloud Scheduler 설정 ==="
    # 매일 01:00 KST — 전날 데이터 수집
    upsert_scheduler collect-daily-job          "0 1 * * *"   collect-daily
    # 매일 02:00 KST — 신규 게임 ITAD+뉴스
    upsert_scheduler process-new-games-job      "0 2 * * *"   process-new-games
    # 매일 03,09,13,17 KST — 최신일 수집분 번역 (4회, 2000×4=8000 capacity)
    upsert_scheduler translate-daily-job        "0 3,9,13,17 * * *" translate-daily
    # 매일 20:00 KST — 최신일 번역완료 리뷰 텍스트 분석
    upsert_scheduler analyze-daily-job          "0 20 * * *"  analyze-daily
    # 3시간마다 (00:30 오프셋) — 동시접속자수
    upsert_scheduler collect-player-counts-job  "30 */3 * * *" collect-player-counts  "300s"
fi

echo "=== 완료 ==="
