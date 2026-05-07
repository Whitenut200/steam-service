"""특정 날짜의 뉴스 + 리뷰(번역, 요약 포함) 백필 스크립트

사용법:
  python first/run_backfill.py 2026-04-08
  python first/run_backfill.py 2026-04-08 --only news
  python first/run_backfill.py 2026-04-08 --only reviews
"""
import sys
import os
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import argparse
import time
from datetime import datetime, timedelta, timezone

from utils.env import init_env
init_env()

from utils.bq_helpers import get_bq_client, insert_rows, get_all_app_ids
from utils.translate import translate_to_ko
from collectors.news import get_game_news
from collectors.reviews import collect_reviews_since
from config.settings import KST


def parse_date(date_str: str) -> datetime:
    """YYYY-MM-DD 문자열 → KST 자정 datetime"""
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=KST)


def backfill_news(client, app_ids: list[int], target_date: datetime, now_str: str):
    """특정 날짜의 뉴스 수집"""
    since_ts = int(target_date.timestamp())
    until_ts = int((target_date + timedelta(days=1)).timestamp())
    date_str = target_date.strftime("%Y-%m-%d")

    print(f"\n=== 뉴스 수집: {date_str} ({len(app_ids)}개 게임) ===")
    all_news = []
    for i, app_id in enumerate(app_ids):
        news = get_game_news(app_id, count=20)
        day_news = [n for n in news if since_ts <= n["date"] < until_ts]
        for n in day_news:
            n["collected_at"] = now_str
        all_news.extend(day_news)

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(app_ids)}] 누적 {len(all_news)}개 뉴스")
        time.sleep(0.3)

    print(f"  총 {len(all_news)}개 뉴스")
    insert_rows(client, "news", all_news)
    return len(all_news)


def backfill_reviews(client, app_ids: list[int], target_date: datetime, now_str: str):
    """특정 날짜의 리뷰 수집 + 번역 + 요약"""
    since_ts = int(target_date.timestamp())
    until_ts = int((target_date + timedelta(days=1)).timestamp())
    date_str = target_date.strftime("%Y-%m-%d")

    print(f"\n=== 리뷰 수집: {date_str} ({len(app_ids)}개 게임) ===")
    all_reviews = []
    all_summaries = []

    for i, app_id in enumerate(app_ids):
        reviews, query_summary = collect_reviews_since(app_id, since_ts, max_reviews=100)
        # 해당 날짜 범위만 필터
        reviews = [r for r in reviews if r["timestamp_created"] < until_ts]

        for r in reviews:
            r["collected_at"] = now_str
        all_reviews.extend(reviews)

        total = query_summary.get("total_reviews", 0)
        if total > 0:
            all_summaries.append({
                "app_id": app_id,
                "total_reviews": total,
                "total_positive": query_summary.get("total_positive", 0),
                "total_negative": query_summary.get("total_negative", 0),
                "review_score": query_summary.get("review_score", 0),
                "review_score_desc": query_summary.get("review_score_desc", ""),
                "positive_ratio": query_summary.get("total_positive", 0) / total,
                "collected_at": now_str,
            })

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(app_ids)}] 누적 {len(all_reviews)}개 리뷰")
        time.sleep(0.5)

    print(f"  총 {len(all_reviews)}개 리뷰, {len(all_summaries)}개 요약")
    insert_rows(client, "reviews", all_reviews)
    insert_rows(client, "review_summary", all_summaries)

    # 번역
    if all_reviews:
        print(f"\n=== 리뷰 번역: {len(all_reviews)}개 ===")
        translated = 0
        for i, r in enumerate(all_reviews):
            ko = translate_to_ko(r.get("review_text", ""), r.get("language", ""))
            if ko != r.get("review_text", ""):
                translated += 1
            r["review_text_ko"] = ko

            if (i + 1) % 50 == 0:
                print(f"  [{i+1}/{len(all_reviews)}] 번역 완료")
            time.sleep(0.05)

        # 번역 결과 업데이트 (임시 테이블 → MERGE)
        _merge_translations(client, all_reviews)
        print(f"  번역 완료: {translated}개 번역됨")

    return len(all_reviews)


def _merge_translations(client, reviews: list[dict]):
    """번역된 리뷰를 BQ에 MERGE"""
    from utils.bq_helpers import PROJECT_ID, DATASET

    temp_table = f"{PROJECT_ID}.{DATASET}.reviews_translated_temp"
    client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()
    client.query(f"""
        CREATE TABLE `{temp_table}` (
            app_id INT64,
            recommendation_id STRING,
            review_text_ko STRING
        )
    """).result()

    rows = [
        {
            "app_id": r["app_id"],
            "recommendation_id": r["recommendation_id"],
            "review_text_ko": r.get("review_text_ko", ""),
        }
        for r in reviews if r.get("review_text_ko")
    ]

    for i in range(0, len(rows), 500):
        batch = rows[i:i + 500]
        errors = client.insert_rows_json(temp_table, batch)
        if errors:
            print(f"  temp insert error: {errors[:2]}")

    print("  스트리밍 버퍼 대기 (30초)...")
    time.sleep(30)

    client.query(f"""
        MERGE `{PROJECT_ID}.{DATASET}.reviews` AS target
        USING `{temp_table}` AS source
        ON target.app_id = source.app_id
           AND target.recommendation_id = source.recommendation_id
        WHEN MATCHED THEN
            UPDATE SET review_text_ko = source.review_text_ko
    """).result()

    client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()


def main():
    parser = argparse.ArgumentParser(description="특정 날짜 뉴스/리뷰 백필")
    parser.add_argument("date", help="수집할 날짜 (YYYY-MM-DD)")
    parser.add_argument("--only", choices=["news", "reviews"], help="뉴스 또는 리뷰만 수집")
    args = parser.parse_args()

    target_date = parse_date(args.date)
    date_str = args.date
    now_str = datetime.now(KST).isoformat()

    print(f"백필 대상 날짜: {date_str} (KST)")

    client = get_bq_client()
    app_ids = list(get_all_app_ids(client))
    print(f"게임 수: {len(app_ids)}개")

    news_count = 0
    review_count = 0

    if args.only != "reviews":
        news_count = backfill_news(client, app_ids, target_date, now_str)

    if args.only != "news":
        review_count = backfill_reviews(client, app_ids, target_date, now_str)

    print(f"\n=== 백필 완료: {date_str} ===")
    print(f"  뉴스: {news_count}개")
    print(f"  리뷰: {review_count}개 (번역 포함)")


if __name__ == "__main__":
    main()
