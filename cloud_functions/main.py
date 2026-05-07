"""Cloud Functions 엔트리포인트 — Steam 데이터 수집 자동화

함수 4개:
1. collect_daily: 매일 실행 — 트렌딩 신규 게임 등록 + 전체 게임 일별 데이터 수집
2. process_new_games: 매일 실행 — GCS에 저장된 신규 게임의 ITAD 가격이력 + 전체 뉴스 수집
3. translate_daily: 매일 실행 — 번역 안 된 리뷰 번역
4. collect_player_counts: 3시간마다 — 전체 게임 동시접속자수 수집
"""
from __future__ import annotations
import json
import time
import functions_framework
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, timedelta, timezone

import os

from collectors.game_info import get_trending_games, get_game_detail, get_existing_app_ids
from collectors.reviews import collect_yesterday_reviews
from collectors.price_snapshot import get_price_snapshot
from collectors.price_history import collect_price_history
from collectors.news import get_game_news, get_yesterday_news
from collectors.player_count import get_player_count
from config.settings import KST, GCS_BUCKET

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")
bq_client = bigquery.Client(project=PROJECT_ID)

NEW_GAMES_GCS_PREFIX = "new_games"

# 함수별로만 쓰이는 클라이언트는 지연 초기화
_translator = None
_storage_client = None


def _get_translator():
    global _translator
    if _translator is None:
        from deep_translator import GoogleTranslator
        _translator = GoogleTranslator(source="auto", target="ko")
    return _translator


def _get_storage_client():
    global _storage_client
    if _storage_client is None:
        from google.cloud import storage
        _storage_client = storage.Client(project=PROJECT_ID)
    return _storage_client


# ── GCS 헬퍼 ──────────────────────────────────────────────

def _get_today_blob():
    """오늘 날짜의 신규 게임 GCS blob 반환"""
    date_str = datetime.now(KST).strftime("%Y-%m-%d")
    blob_path = f"{NEW_GAMES_GCS_PREFIX}/{date_str}.json"
    bucket = _get_storage_client().bucket(GCS_BUCKET)
    return bucket.blob(blob_path), blob_path


def _save_new_game_ids(app_ids: list[int]):
    """신규 게임 ID 목록을 GCS에 저장 (날짜별)"""
    if not app_ids:
        return
    blob, blob_path = _get_today_blob()
    blob.upload_from_string(json.dumps(app_ids), content_type="application/json")
    print(f"GCS 저장: gs://{GCS_BUCKET}/{blob_path} ({len(app_ids)}개)")


def _load_new_game_ids() -> list[int]:
    """GCS에서 오늘 날짜의 신규 게임 ID 목록 읽기"""
    blob, blob_path = _get_today_blob()
    try:
        data = json.loads(blob.download_as_text())
        print(f"GCS 로드: gs://{GCS_BUCKET}/{blob_path} ({len(data)}개)")
        return data
    except NotFound:
        print(f"GCS 파일 없음: gs://{GCS_BUCKET}/{blob_path}")
        return []


def _delete_new_game_ids():
    """처리 완료 후 GCS 파일 삭제"""
    blob, blob_path = _get_today_blob()
    try:
        blob.delete()
        print(f"GCS 삭제: gs://{GCS_BUCKET}/{blob_path}")
    except NotFound:
        pass


# ── 헬퍼 ──────────────────────────────────────────────────

def translate_to_ko(text: str, language: str = "") -> str | None:
    """번역 성공 시 한국어 반환, 실패 시 None (BQ에서 NULL로 남아 재시도됨)"""
    if not text or not text.strip():
        return None
    if language and language.lower() in ("korean", "koreana"):
        return text
    # deep-translator는 요청당 최대 5000자 제한
    snippet = text[:4900]
    for attempt in range(3):
        try:
            result = _get_translator().translate(snippet)
            if result:
                return result
        except Exception as e:
            if attempt == 2:
                print(f"  번역 실패: {e}")
            else:
                time.sleep(1.5)
    return None


def insert_rows(table_name: str, rows: list[dict]):
    if not rows:
        return
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    for i in range(0, len(rows), 500):
        batch = rows[i:i + 500]
        errors = bq_client.insert_rows_json(table_id, batch)
        if errors:
            print(f"BigQuery insert errors ({table_name}): {errors[:2]}")
            raise RuntimeError(f"Failed to insert into {table_name}")
    print(f"Inserted {len(rows)} rows into {table_name}")


# ── 중복 방지 헬퍼 ───────────────────────────────────────

def _dedup_news(rows: list[dict]) -> list[dict]:
    """기존 gid와 겹치는 뉴스 제거"""
    if not rows:
        return rows
    gids = [r["gid"] for r in rows if r.get("gid")]
    if not gids:
        return rows
    gid_list = ", ".join(f"'{g}'" for g in gids)
    query = f"SELECT DISTINCT gid FROM `{PROJECT_ID}.{DATASET}.news` WHERE gid IN ({gid_list})"
    existing = {row.gid for row in bq_client.query(query).result()}
    filtered = [r for r in rows if r.get("gid") not in existing]
    if len(filtered) < len(rows):
        print(f"  뉴스 중복 제거: {len(rows)} → {len(filtered)}개")
    return filtered


def _dedup_reviews(rows: list[dict]) -> list[dict]:
    """기존 recommendation_id와 겹치는 리뷰 제거"""
    if not rows:
        return rows
    rids = [r["recommendation_id"] for r in rows if r.get("recommendation_id")]
    if not rids:
        return rows
    rid_list = ", ".join(f"'{r}'" for r in rids)
    query = f"SELECT DISTINCT recommendation_id FROM `{PROJECT_ID}.{DATASET}.reviews` WHERE recommendation_id IN ({rid_list})"
    existing = {row.recommendation_id for row in bq_client.query(query).result()}
    filtered = [r for r in rows if r.get("recommendation_id") not in existing]
    if len(filtered) < len(rows):
        print(f"  리뷰 중복 제거: {len(rows)} → {len(filtered)}개")
    return filtered


def _dedup_price_history(rows: list[dict]) -> list[dict]:
    """기존 (app_id, snapshot_date)와 겹치는 가격 스냅샷 제거"""
    if not rows:
        return rows
    dates = list({r["snapshot_date"] for r in rows if r.get("snapshot_date")})
    if not dates:
        return rows
    date_list = ", ".join(f"'{d}'" for d in dates)
    query = f"SELECT DISTINCT app_id, CAST(snapshot_date AS STRING) as sd FROM `{PROJECT_ID}.{DATASET}.price_history` WHERE snapshot_date IN ({date_list})"
    existing = {(row.app_id, row.sd) for row in bq_client.query(query).result()}
    filtered = [r for r in rows if (r["app_id"], str(r["snapshot_date"])) not in existing]
    if len(filtered) < len(rows):
        print(f"  가격 중복 제거: {len(rows)} → {len(filtered)}개")
    return filtered


def _append_review_summary(rows: list[dict]):
    """review_summary를 일별 누적. 같은 날(KST) (app_id) 중복은 제외해 멱등성 유지."""
    if not rows:
        return
    today_kst = datetime.now(KST).strftime("%Y-%m-%d")
    query = (
        f"SELECT DISTINCT app_id FROM `{PROJECT_ID}.{DATASET}.review_summary` "
        f"WHERE DATE(collected_at, 'Asia/Seoul') = DATE '{today_kst}'"
    )
    existing = {r.app_id for r in bq_client.query(query).result()}
    filtered = [r for r in rows if r["app_id"] not in existing]
    if len(filtered) < len(rows):
        print(f"  review_summary 중복 제거: {len(rows)} → {len(filtered)}")
    insert_rows("review_summary", filtered)


# ── 1. collect_daily ───────────────────────────────────────

@functions_framework.http
def collect_daily(request):
    """매일 실행: 트렌딩 신규 게임 등록 + KST 전날 데이터 수집"""
    try:
        now = datetime.now(KST).isoformat()

        # 1. 트렌딩 100개에서 신규 게임만 추가
        trending = get_trending_games()
        existing_ids = get_existing_app_ids(bq_client, PROJECT_ID, DATASET)
        new_ids = [g["app_id"] for g in trending if g["app_id"] not in existing_ids]
        print(f"트렌딩 {len(trending)}개 중 신규: {len(new_ids)}개")

        new_games = []
        for app_id in new_ids:
            detail = get_game_detail(app_id)
            if detail:
                detail["collected_at"] = now
                new_games.append(detail)
            time.sleep(1.2)

        insert_rows("games", new_games)

        # 1-1. 신규 게임 ID를 GCS에 저장 → process_new_games에서 처리
        if new_games:
            _save_new_game_ids([g["app_id"] for g in new_games])

        # 2. DB에 있는 전체 게임 대상으로 일별 데이터 수집
        all_app_ids = list(existing_ids | {g["app_id"] for g in new_games})
        print(f"일별 수집 대상: {len(all_app_ids)}개 게임")

        # 3. 가격 스냅샷 (KST 전날 기준)
        price_snapshots = []
        for app_id in all_app_ids:
            try:
                snapshot = get_price_snapshot(app_id)
                if snapshot:
                    price_snapshots.append(snapshot)
            except Exception as e:
                print(f"  가격 수집 실패 (app_id={app_id}): {e}")
            time.sleep(1.2)

        price_snapshots = _dedup_price_history(price_snapshots)
        insert_rows("price_history", price_snapshots)

        # 4. 전날 뉴스 (KST 기준)
        yesterday_news = []
        for app_id in all_app_ids:
            try:
                news = get_yesterday_news(app_id)
                for n in news:
                    n["collected_at"] = now
                yesterday_news.extend(news)
            except Exception as e:
                print(f"  뉴스 수집 실패 (app_id={app_id}): {e}")
            time.sleep(0.3)

        yesterday_news = _dedup_news(yesterday_news)
        insert_rows("news", yesterday_news)

        # 5. 전날 리뷰 + 리뷰 요약 (KST 기준, 번역은 translate_daily에서)
        yesterday_reviews = []
        review_summaries = []
        for app_id in all_app_ids:
            try:
                reviews, query_summary = collect_yesterday_reviews(app_id, max_reviews=100)
                for r in reviews:
                    r["collected_at"] = now
                yesterday_reviews.extend(reviews)

                total = query_summary.get("total_reviews", 0)
                if total > 0:
                    review_summaries.append({
                        "app_id": app_id,
                        "total_reviews": total,
                        "total_positive": query_summary.get("total_positive", 0),
                        "total_negative": query_summary.get("total_negative", 0),
                        "review_score": query_summary.get("review_score", 0),
                        "review_score_desc": query_summary.get("review_score_desc", ""),
                        "positive_ratio": query_summary.get("total_positive", 0) / total,
                        "collected_at": now,
                    })
            except Exception as e:
                print(f"  리뷰 수집 실패 (app_id={app_id}): {e}")
            time.sleep(0.5)

        yesterday_reviews = _dedup_reviews(yesterday_reviews)
        insert_rows("reviews", yesterday_reviews)
        _append_review_summary(review_summaries)

        result = {
            "status": "success",
            "new_games": len(new_games),
            "price_snapshots": len(price_snapshots),
            "yesterday_news": len(yesterday_news),
            "yesterday_reviews": len(yesterday_reviews),
            "review_summaries": len(review_summaries),
        }
        print(f"수집 완료: {result}")
        return json.dumps(result), 200

    except Exception as e:
        print(f"수집 실패: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500


# ── 2. process_new_games ──────────────────────────────────

@functions_framework.http
def process_new_games(request):
    """신규 게임 전용: GCS에서 목록 읽어 ITAD 가격이력 + 전체 뉴스 수집"""
    try:
        now = datetime.now(KST).isoformat()
        new_app_ids = _load_new_game_ids()

        if not new_app_ids:
            return json.dumps({"status": "success", "message": "신규 게임 없음"}), 200

        print(f"신규 게임 처리 시작: {len(new_app_ids)}개")

        # 1. ITAD 과거 가격 이력 수집 (배치 insert)
        all_history = []
        for app_id in new_app_ids:
            history = collect_price_history(app_id)
            if history:
                all_history.extend(history)
            time.sleep(0.5)

        all_history = _dedup_price_history(all_history)
        insert_rows("price_history", all_history)

        # 2. 전체 뉴스 수집 (전날만이 아닌 과거 전체)
        all_news = []
        for app_id in new_app_ids:
            news = get_game_news(app_id, count=50)
            for n in news:
                n["collected_at"] = now
            all_news.extend(news)
            time.sleep(0.3)

        all_news = _dedup_news(all_news)
        insert_rows("news", all_news)

        # 3. 처리 완료 후 GCS 파일 삭제
        _delete_new_game_ids()

        result = {
            "status": "success",
            "new_games": len(new_app_ids),
            "price_history_rows": len(all_history),
            "news_rows": len(all_news),
        }
        print(f"신규 게임 처리 완료: {result}")
        return json.dumps(result), 200

    except Exception as e:
        print(f"신규 게임 처리 실패: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500


# ── 3. translate_daily ─────────────────────────────────────

@functions_framework.http
def translate_daily(request):
    """최신일(KST collected_at 기준) 리뷰 중 미번역 건을 한국어로 번역

    - 스케줄: 00:00, 03:00, 06:00 KST (하루 3회)
    - 실행당 최대 2000건
    - 연속 실패 10회 시 조기 종료 후 성공분만 MERGE
    - 기존 translate_text_ko IS NULL 백로그는 대상 아님 (최신일만 처리)
    """
    MAX_PER_RUN = 2000
    MAX_CONSECUTIVE_FAILS = 10
    try:
        # 1. 최신일 파악 (reviews 테이블 내 collected_at의 KST 최대 날짜)
        max_date_query = f"""
            SELECT MAX(DATE(TIMESTAMP(collected_at), "Asia/Seoul")) AS max_date
            FROM `{PROJECT_ID}.{DATASET}.reviews`
            WHERE review_text IS NOT NULL AND TRIM(review_text) != ''
        """
        max_date_row = list(bq_client.query(max_date_query).result())
        if not max_date_row or max_date_row[0].max_date is None:
            print("리뷰 데이터 없음")
            return json.dumps({"status": "success", "translated": 0}), 200
        target_date = max_date_row[0].max_date  # datetime.date
        print(f"번역 대상 날짜 (KST): {target_date}")

        # 2. 해당 날짜 미번역 개수
        count_query = f"""
            SELECT COUNT(*) AS c
            FROM `{PROJECT_ID}.{DATASET}.reviews`
            WHERE review_text_ko IS NULL
              AND review_text IS NOT NULL
              AND TRIM(review_text) != ''
              AND DATE(TIMESTAMP(collected_at), "Asia/Seoul") = DATE '{target_date}'
        """
        total_null = list(bq_client.query(count_query).result())[0].c
        if total_null == 0:
            print(f"{target_date} 미번역 리뷰 없음")
            return json.dumps({"status": "success", "translated": 0, "target_date": str(target_date)}), 200

        limit = min(MAX_PER_RUN, total_null)
        print(f"총 미번역 {total_null}개 / 이번 실행 {limit}개 처리")

        # 3. 번역 대상 조회
        query = f"""
            SELECT app_id, recommendation_id, review_text, language
            FROM `{PROJECT_ID}.{DATASET}.reviews`
            WHERE review_text_ko IS NULL
            AND review_text IS NOT NULL
            AND TRIM(review_text) != ''
            AND DATE(TIMESTAMP(collected_at), "Asia/Seoul") = DATE '{target_date}'
            ORDER BY collected_at DESC
            LIMIT {limit}
        """
        rows = list(bq_client.query(query).result())
        print(f"번역 대상: {len(rows)}개 리뷰")

        if not rows:
            return json.dumps({"status": "success", "translated": 0}), 200

        # 번역 (실패한 건 None → 다음 실행 때 재시도)
        translated_rows = []
        fail_count = 0
        consecutive_fails = 0
        aborted = False
        for i, row in enumerate(rows):
            ko_text = translate_to_ko(row.review_text, row.language)
            if ko_text is None:
                fail_count += 1
                consecutive_fails += 1
                if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                    print(f"  연속 실패 {consecutive_fails}회 → 조기 종료 (Google Translate 차단 의심)")
                    aborted = True
                    break
                continue
            consecutive_fails = 0
            translated_rows.append({
                "app_id": row.app_id,
                "recommendation_id": row.recommendation_id,
                "review_text_ko": ko_text,
            })
            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(rows)}] 번역 완료 (실패 {fail_count})")
            time.sleep(0.3)

        print(f"번역 성공 {len(translated_rows)}개 / 실패 {fail_count}개{' (조기종료)' if aborted else ''}")
        if not translated_rows:
            return json.dumps({"status": "aborted" if aborted else "success", "translated": 0, "failed": fail_count}), 200

        # load job으로 임시 테이블 적재 (스트리밍 버퍼 없음 → 즉시 MERGE 가능)
        temp_table_id = f"{PROJECT_ID}.{DATASET}.reviews_translated_temp"
        bq_client.query(f"DROP TABLE IF EXISTS `{temp_table_id}`").result()

        table_ref = bq_client.dataset(DATASET).table("reviews_translated_temp")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("app_id", "INT64"),
                bigquery.SchemaField("recommendation_id", "STRING"),
                bigquery.SchemaField("review_text_ko", "STRING"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        load_job = bq_client.load_table_from_json(translated_rows, table_ref, job_config=job_config)
        load_job.result()
        print(f"임시 테이블 적재 완료: {len(translated_rows)}개")

        # MERGE (스트리밍 버퍼 대기 불필요)
        merge_query = f"""
            MERGE `{PROJECT_ID}.{DATASET}.reviews` AS target
            USING `{temp_table_id}` AS source
            ON target.app_id = source.app_id
               AND target.recommendation_id = source.recommendation_id
            WHEN MATCHED THEN
                UPDATE SET review_text_ko = source.review_text_ko
        """
        bq_client.query(merge_query).result()

        # 임시 테이블 삭제
        bq_client.query(f"DROP TABLE IF EXISTS `{temp_table_id}`").result()

        result = {
            "status": "aborted" if aborted else "success",
            "translated": len(translated_rows),
            "failed": fail_count,
            "target_date": str(target_date),
        }
        print(f"번역 완료: {result}")
        return json.dumps(result), 200

    except Exception as e:
        print(f"번역 실패: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500


# ── 4. collect_player_counts ──────────────────────────────

@functions_framework.http
def collect_player_counts(request):
    """3시간마다 실행: 전체 게임 동시접속자수 수집"""
    try:
        now = datetime.now(KST).isoformat()
        existing_ids = get_existing_app_ids(bq_client, PROJECT_ID, DATASET)
        all_app_ids = list(existing_ids)
        print(f"동시접속자 수집 대상: {len(all_app_ids)}개 게임")

        rows = []
        for app_id in all_app_ids:
            count = get_player_count(app_id)
            if count is not None:
                rows.append({
                    "app_id": app_id,
                    "player_count": count,
                    "collected_at": now,
                })
            time.sleep(0.3)

        insert_rows("player_counts", rows)

        result = {
            "status": "success",
            "games": len(all_app_ids),
            "collected": len(rows),
        }
        print(f"동시접속자 수집 완료: {result}")
        return json.dumps(result), 200

    except Exception as e:
        print(f"동시접속자 수집 실패: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500


# ── 5. analyze_daily ──────────────────────────────────────

def _ensure_analysis_tables():
    dataset_ref = f"{PROJECT_ID}.{DATASET}"
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{dataset_ref}.category_analysis` (
            app_id INT64,
            polarity STRING,
            category STRING,
            subcategory STRING,
            count INT64,
            collected_date DATE
        )
    """).result()
    bq_client.query(f"""
        CREATE TABLE IF NOT EXISTS `{dataset_ref}.keyword_analysis` (
            app_id INT64,
            polarity STRING,
            keyword STRING,
            pos_tag STRING,
            count INT64,
            ratio FLOAT64,
            collected_date DATE
        )
    """).result()


@functions_framework.http
def analyze_daily(request):
    """최신일(KST collected_at 기준) 번역완료 리뷰 분석 — 카테고리/키워드 집계

    - 스케줄: 09:00 KST
    - 대상: 최신일 수집 리뷰 중 review_text_ko IS NOT NULL
    - 출력: category_analysis, keyword_analysis (collected_date = 최신일)
    - 재실행 시 동일 날짜 row 중복 누적되지 않도록 DELETE 후 INSERT
    """
    try:
        from analyzers.text_analysis import analyze_reviews

        # 최신일 파악 (번역 완료된 리뷰 기준)
        max_date_query = f"""
            SELECT MAX(DATE(TIMESTAMP(collected_at), "Asia/Seoul")) AS max_date
            FROM `{PROJECT_ID}.{DATASET}.reviews`
            WHERE review_text_ko IS NOT NULL
              AND TRIM(review_text_ko) != ''
        """
        row = list(bq_client.query(max_date_query).result())
        if not row or row[0].max_date is None:
            print("번역된 리뷰 없음")
            return json.dumps({"status": "success", "analyzed_games": 0}), 200
        target_date = row[0].max_date
        print(f"분석 대상 날짜 (KST): {target_date}")

        _ensure_analysis_tables()

        # 대상 게임 목록
        games_query = f"""
            SELECT DISTINCT app_id
            FROM `{PROJECT_ID}.{DATASET}.reviews`
            WHERE review_text_ko IS NOT NULL
              AND TRIM(review_text_ko) != ''
              AND DATE(TIMESTAMP(collected_at), "Asia/Seoul") = DATE '{target_date}'
        """
        app_ids = [r.app_id for r in bq_client.query(games_query).result()]
        print(f"분석 대상 게임: {len(app_ids)}개")
        if not app_ids:
            return json.dumps({"status": "success", "target_date": str(target_date), "analyzed_games": 0}), 200

        # 재실행 안전: 동일 날짜 기존 분석 결과 삭제
        bq_client.query(f"""
            DELETE FROM `{PROJECT_ID}.{DATASET}.category_analysis`
            WHERE collected_date = DATE '{target_date}'
        """).result()
        bq_client.query(f"""
            DELETE FROM `{PROJECT_ID}.{DATASET}.keyword_analysis`
            WHERE collected_date = DATE '{target_date}'
        """).result()

        cat_rows = []
        kw_rows = []
        today_str = str(target_date)

        for i, app_id in enumerate(app_ids):
            review_query = f"""
                SELECT voted_up, review_text_ko
                FROM `{PROJECT_ID}.{DATASET}.reviews`
                WHERE app_id = {app_id}
                  AND review_text_ko IS NOT NULL
                  AND TRIM(review_text_ko) != ''
                  AND DATE(TIMESTAMP(collected_at), "Asia/Seoul") = DATE '{target_date}'
                LIMIT 300
            """
            reviews = list(bq_client.query(review_query).result())
            if not reviews:
                continue

            result = analyze_reviews(reviews)
            total_pos = result["total_positive"]
            total_neg = result["total_negative"]

            for (top, sub), cnt in result["pos_categories"].items():
                cat_rows.append({
                    "app_id": app_id, "polarity": "positive",
                    "category": top, "subcategory": sub,
                    "count": cnt, "collected_date": today_str,
                })
            for (top, sub), cnt in result["neg_categories"].items():
                cat_rows.append({
                    "app_id": app_id, "polarity": "negative",
                    "category": top, "subcategory": sub,
                    "count": cnt, "collected_date": today_str,
                })
            for (word, tag), cnt in result["pos_keywords"]:
                ratio = cnt / total_pos if total_pos > 0 else 0
                kw_rows.append({
                    "app_id": app_id, "polarity": "positive",
                    "keyword": word, "pos_tag": tag, "count": cnt,
                    "ratio": round(ratio, 4), "collected_date": today_str,
                })
            for (word, tag), cnt in result["neg_keywords"]:
                ratio = cnt / total_neg if total_neg > 0 else 0
                kw_rows.append({
                    "app_id": app_id, "polarity": "negative",
                    "keyword": word, "pos_tag": tag, "count": cnt,
                    "ratio": round(ratio, 4), "collected_date": today_str,
                })

            if (i + 1) % 20 == 0:
                print(f"  [{i+1}/{len(app_ids)}] 분석 완료")

        if cat_rows:
            insert_rows("category_analysis", cat_rows)
        if kw_rows:
            insert_rows("keyword_analysis", kw_rows)

        result = {
            "status": "success",
            "target_date": today_str,
            "analyzed_games": len(app_ids),
            "category_rows": len(cat_rows),
            "keyword_rows": len(kw_rows),
        }
        print(f"분석 완료: {result}")
        return json.dumps(result), 200

    except Exception as e:
        print(f"분석 실패: {e}")
        return json.dumps({"status": "error", "message": str(e)}), 500
