"""초기 수집: games 테이블의 전체 게임 대상으로 뉴스 + 리뷰(번역, 요약) 일괄 수집

사용법:
  python first/run_initial_news_reviews.py
  python first/run_initial_news_reviews.py --only news
  python first/run_initial_news_reviews.py --only reviews
  python first/run_initial_news_reviews.py --only reviews --start 2026-04-01 --end 2026-04-08
  python first/run_initial_news_reviews.py --news-count 50 --max-reviews 200
"""
import sys
import os
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import argparse
import time
from datetime import datetime, timedelta

from utils.env import init_env
init_env()

from utils.bq_helpers import get_bq_client, insert_rows, get_all_app_ids, PROJECT_ID, DATASET
from utils.translate import translate_to_ko
from collectors.news import get_game_news
from collectors.reviews import collect_reviews_since, get_review_summary
from config.settings import KST


def collect_all_news(client, app_ids: list[int], now_str: str, count: int = 30):
    """전체 게임의 최근 뉴스 수집"""
    print(f"\n=== 뉴스 수집 ({len(app_ids)}개 게임, 게임당 최대 {count}개) ===")
    all_news = []

    for i, app_id in enumerate(app_ids):
        try:
            news = get_game_news(app_id, count=count)
            for n in news:
                n["collected_at"] = now_str
            all_news.extend(news)
        except Exception as e:
            print(f"  app_id={app_id} 뉴스 수집 실패: {e}")

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(app_ids)}] 누적 {len(all_news)}개 뉴스")
        time.sleep(0.3)

    print(f"  총 {len(all_news)}개 뉴스")
    insert_rows(client, "news", all_news)
    return len(all_news)


def collect_all_reviews(client, app_ids: list[int], now_str: str,
                        max_reviews: int = 200, start_date: str = None, end_date: str = None):
    """전체 게임의 리뷰 수집 + 요약 + 번역. start/end_date로 기간 지정 가능."""
    since_ts = 0
    until_ts = None

    if start_date:
        since_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=KST).timestamp())
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=KST) + timedelta(days=1)
        until_ts = int(end_dt.timestamp())

    date_label = ""
    if start_date and end_date:
        date_label = f" ({start_date} ~ {end_date})"
    elif start_date:
        date_label = f" ({start_date} ~)"
    elif end_date:
        date_label = f" (~ {end_date})"

    print(f"\n=== 리뷰 수집{date_label} ({len(app_ids)}개 게임, 게임당 최대 {max_reviews}개) ===")
    all_reviews = []
    all_summaries = []

    for i, app_id in enumerate(app_ids):
        try:
            reviews, _ = collect_reviews_since(app_id, since_timestamp=since_ts, max_reviews=max_reviews)
            # end_date 지정 시 해당 날짜 이후 리뷰 제거
            if until_ts:
                reviews = [r for r in reviews if r["timestamp_created"] < until_ts]
            for r in reviews:
                r["collected_at"] = now_str
            all_reviews.extend(reviews)

            # 리뷰 요약 (API에서 별도 조회)
            summary = get_review_summary(app_id)
            total = summary.get("total_reviews", 0)
            if total > 0:
                summary["positive_ratio"] = summary["total_positive"] / total
                summary["collected_at"] = now_str
                all_summaries.append(summary)
        except Exception as e:
            print(f"  app_id={app_id} 리뷰 수집 실패: {e}")

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(app_ids)}] 누적 {len(all_reviews)}개 리뷰")
        time.sleep(0.5)

    print(f"  총 {len(all_reviews)}개 리뷰, {len(all_summaries)}개 요약")

    # 번역 먼저 → review_text_ko 채운 후 한 번에 insert
    if all_reviews:
        print(f"\n=== 리뷰 번역: {len(all_reviews)}개 ===")
        translated_count = 0
        for i, r in enumerate(all_reviews):
            ko = translate_to_ko(r.get("review_text", ""), r.get("language", ""))
            r["review_text_ko"] = ko
            if ko != r.get("review_text", ""):
                translated_count += 1
            if (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(all_reviews)}] 번역 완료")
            time.sleep(0.05)
        print(f"  번역 완료: {translated_count}개 번역됨")

    insert_rows(client, "reviews", all_reviews)
    insert_rows(client, "review_summary", all_summaries)

    return len(all_reviews)



def main():
    parser = argparse.ArgumentParser(description="초기 수집: 전체 게임 뉴스 + 리뷰")
    parser.add_argument("--only", choices=["news", "reviews"], help="뉴스 또는 리뷰만 수집")
    parser.add_argument("--news-count", type=int, default=50, help="게임당 최대 뉴스 수 (기본: 50)")
    parser.add_argument("--max-reviews", type=int, default=200, help="게임당 최대 리뷰 수 (기본: 200)")
    parser.add_argument("--start", help="리뷰 시작 날짜 (YYYY-MM-DD, KST)")
    parser.add_argument("--end", help="리뷰 종료 날짜 (YYYY-MM-DD, KST, 해당일 포함)")
    args = parser.parse_args()

    now_str = datetime.now(KST).isoformat()
    client = get_bq_client()
    app_ids = list(get_all_app_ids(client))
    print(f"게임 수: {len(app_ids)}개")

    news_count = 0
    review_count = 0

    if args.only != "reviews":
        news_count = collect_all_news(client, app_ids, now_str, count=args.news_count)

    if args.only != "news":
        review_count = collect_all_reviews(client, app_ids, now_str,
                                          max_reviews=args.max_reviews,
                                          start_date=args.start, end_date=args.end)

    print(f"\n=== 초기 수집 완료 ===")
    print(f"  뉴스: {news_count}개")
    print(f"  리뷰: {review_count}개 (번역 포함)")


if __name__ == "__main__":
    main()
