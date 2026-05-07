"""Steam 게임 뉴스/패치노트 수집"""
import requests
from datetime import datetime, timedelta
from config.settings import STEAM_NEWS_API, KST


def get_game_news(app_id: int, count: int = 20) -> list:
    """게임 뉴스/패치노트 가져오기"""
    resp = requests.get(
        STEAM_NEWS_API,
        params={
            "appid": app_id,
            "count": count,
            "maxlength": 500,
        },
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()

    news_items = data.get("appnews", {}).get("newsitems", [])

    return [
        {
            "app_id": app_id,
            "gid": item.get("gid"),
            "title": item.get("title", ""),
            "author": item.get("author", ""),
            "contents": item.get("contents", ""),
            "feed_label": item.get("feedlabel", ""),
            "date": item.get("date", 0),
            "url": item.get("url", ""),
        }
        for item in news_items
    ]


def get_yesterday_news(app_id: int, count: int = 20) -> list:
    """KST 기준 전날 뉴스만 필터링"""
    kst_today = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0)
    kst_yesterday = kst_today - timedelta(days=1)

    since_ts = int(kst_yesterday.timestamp())
    until_ts = int(kst_today.timestamp())

    all_news = get_game_news(app_id, count=count)
    return [n for n in all_news if since_ts <= n["date"] < until_ts]


if __name__ == "__main__":
    news = get_game_news(730, count=5)
    print(f"CS2 최근 뉴스 {len(news)}개:")
    for n in news:
        print(f"  [{n['feed_label']}] {n['title']}")

    yesterday = get_yesterday_news(730)
    print(f"\n전날 뉴스 {len(yesterday)}개")
