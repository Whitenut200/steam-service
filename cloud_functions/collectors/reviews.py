"""Steam 유저 리뷰 수집"""
import requests
import time
from datetime import datetime, timezone, timedelta
from config.settings import STEAM_REVIEW_API, REVIEW_BATCH_SIZE, MAX_REVIEWS_PER_GAME, KST


def get_reviews(
    app_id: int,
    cursor: str = "*",
    num_per_page: int = REVIEW_BATCH_SIZE,
    review_type: str = "all",
    language: str = "all",
    purchase_type: str = "all",
) -> dict:
    """리뷰 한 페이지 가져오기"""
    resp = requests.get(
        f"{STEAM_REVIEW_API}/{app_id}",
        params={
            "json": 1,
            "cursor": cursor,
            "num_per_page": num_per_page,
            "review_type": review_type,
            "language": language,
            "purchase_type": purchase_type,
            "filter": "recent",
        },
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()


def collect_reviews_since(app_id: int, since_timestamp: int, max_reviews: int = MAX_REVIEWS_PER_GAME) -> tuple[list, dict]:
    """특정 시간 이후 작성된 리뷰만 수집. (reviews, query_summary) 튜플 반환"""
    all_reviews = []
    cursor = "*"
    query_summary = {}

    while len(all_reviews) < max_reviews:
        data = get_reviews(app_id, cursor=cursor)

        if not data.get("success"):
            break

        # 첫 페이지에서 query_summary 캡처
        if not query_summary and "query_summary" in data:
            query_summary = data["query_summary"]

        reviews = data.get("reviews", [])
        if not reviews:
            break

        found_old = False
        for r in reviews:
            # since_timestamp 이전 리뷰가 나오면 중단
            if r.get("timestamp_created", 0) < since_timestamp:
                found_old = True
                break

            author = r.get("author", {})
            all_reviews.append({
                "app_id": app_id,
                "recommendation_id": r.get("recommendationid"),
                "steam_id": author.get("steamid"),
                "playtime_forever": author.get("playtime_forever", 0),
                "playtime_at_review": author.get("playtime_at_review", 0),
                "voted_up": r.get("voted_up", False),
                "language": r.get("language", ""),
                "review_text": r.get("review", ""),
                "timestamp_created": r.get("timestamp_created", 0),
                "timestamp_updated": r.get("timestamp_updated", 0),
                "votes_up": r.get("votes_up", 0),
                "votes_funny": r.get("votes_funny", 0),
                "weighted_vote_score": float(r.get("weighted_vote_score", 0)),
            })

        if found_old:
            break

        cursor = data.get("cursor", "")
        if not cursor or cursor == "*":
            break

        time.sleep(0.5)

    return all_reviews[:max_reviews], query_summary


def collect_yesterday_reviews(app_id: int, max_reviews: int = MAX_REVIEWS_PER_GAME) -> tuple[list, dict]:
    """KST 기준 전날 리뷰만 수집. (reviews, query_summary) 튜플 반환"""
    kst_today = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0)
    kst_yesterday = kst_today - timedelta(days=1)

    since_ts = int(kst_yesterday.timestamp())
    until_ts = int(kst_today.timestamp())

    reviews, summary = collect_reviews_since(app_id, since_ts, max_reviews)
    # KST 오늘 이후 리뷰 제거
    reviews = [r for r in reviews if r["timestamp_created"] < until_ts]
    return reviews, summary


def get_review_summary(app_id: int) -> dict:
    """리뷰 요약 통계 가져오기"""
    data = get_reviews(app_id, num_per_page=0)
    summary = data.get("query_summary", {})
    return {
        "app_id": app_id,
        "total_reviews": summary.get("total_reviews", 0),
        "total_positive": summary.get("total_positive", 0),
        "total_negative": summary.get("total_negative", 0),
        "review_score": summary.get("review_score", 0),
        "review_score_desc": summary.get("review_score_desc", ""),
    }


if __name__ == "__main__":
    summary = get_review_summary(730)
    print(f"리뷰 요약: {summary}")

    reviews, summary = collect_yesterday_reviews(730, max_reviews=10)
    print(f"\n전날 리뷰 {len(reviews)}개 (summary: {summary}):")
    for r in reviews:
        vote = "추천" if r["voted_up"] else "비추천"
        text = r['review_text'][:80].encode('ascii', 'ignore').decode()
        print(f"  [{vote}] 플레이 {r['playtime_forever']//60}시간 | {text}...")
