"""번역만 실행: BQ에서 미번역 리뷰 조회 → 번역 → 업데이트

사용법:
  python first/run_translate.py
  python first/run_translate.py --limit 1000
"""
import sys
import os
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import argparse
import time
import json

from utils.env import init_env
init_env()

from utils.bq_helpers import get_bq_client, PROJECT_ID, DATASET
from utils.translate import translate_to_ko


def main():
    parser = argparse.ArgumentParser(description="미번역 리뷰 번역")
    parser.add_argument("--limit", type=int, default=2000, help="최대 번역 수 (기본: 2000)")
    args = parser.parse_args()

    MAX_CONSECUTIVE_FAILS = 10

    client = get_bq_client()

    # 1. 미번역 리뷰 조회
    query = f"""
        SELECT app_id, recommendation_id, review_text, language
        FROM `{PROJECT_ID}.{DATASET}.reviews`
        WHERE review_text_ko IS NULL
        AND review_text IS NOT NULL
        AND TRIM(review_text) != ''
        ORDER BY collected_at DESC
        LIMIT {args.limit}
    """
    rows = list(client.query(query).result())
    print(f"번역 대상: {len(rows)}개 리뷰")

    if not rows:
        print("번역할 리뷰 없음")
        return

    # 2. 번역 (실패한 건 None → 다음 실행 때 재시도)
    translated = []
    fail_count = 0
    consecutive_fails = 0
    aborted = False
    for i, row in enumerate(rows):
        ko = translate_to_ko(row.review_text, row.language)
        if ko is None:
            fail_count += 1
            consecutive_fails += 1
            if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                print(f"  연속 실패 {consecutive_fails}회 → 조기 종료 (Google Translate 차단 의심)")
                aborted = True
                break
            continue
        consecutive_fails = 0
        translated.append({
            "app_id": row.app_id,
            "recommendation_id": row.recommendation_id,
            "review_text_ko": ko,
        })
        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(rows)}] 번역 완료 (누적 실패 {fail_count})")
        time.sleep(0.3)

    print(f"번역 성공 {len(translated)}개 / 실패 {fail_count}개{' (조기종료)' if aborted else ''}")
    if not translated:
        return

    # 3. load job으로 임시 테이블 적재 (스트리밍 버퍼 없음)
    temp_table = f"{PROJECT_ID}.{DATASET}.reviews_translated_temp"
    client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()
    client.query(f"""
        CREATE TABLE `{temp_table}` (
            app_id INT64,
            recommendation_id STRING,
            review_text_ko STRING
        )
    """).result()

    from google.cloud import bigquery as bq
    table_ref = client.dataset(DATASET).table("reviews_translated_temp")
    job_config = bq.LoadJobConfig(
        source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bq.SchemaField("app_id", "INT64"),
            bq.SchemaField("recommendation_id", "STRING"),
            bq.SchemaField("review_text_ko", "STRING"),
        ],
    )
    load_job = client.load_table_from_json(translated, table_ref, job_config=job_config)
    load_job.result()
    print(f"  임시 테이블 적재 완료: {len(translated)}개")

    # 4. MERGE (스트리밍 버퍼 대기 불필요)
    client.query(f"""
        MERGE `{PROJECT_ID}.{DATASET}.reviews` AS target
        USING `{temp_table}` AS source
        ON target.app_id = source.app_id
           AND target.recommendation_id = source.recommendation_id
        WHEN MATCHED THEN
            UPDATE SET review_text_ko = source.review_text_ko
    """).result()

    # 5. 정리
    client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()
    print(f"\n=== 번역 완료: {len(translated)}개 ===")


if __name__ == "__main__":
    main()
