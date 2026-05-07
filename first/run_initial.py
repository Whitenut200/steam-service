"""초기 수집: 소유자순 상위 500개 게임 → BigQuery 적재"""
import sys
import os
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import time
from utils.env import init_env
init_env()

from datetime import datetime
from config.settings import KST
from utils.bq_helpers import get_bq_client, insert_rows
from collectors.game_info import get_top_games_by_owners
from collectors.reviews import get_review_summary
from collectors.price_snapshot import get_price_snapshot
from collectors.news import get_game_news

client = get_bq_client()
now = datetime.now(KST).isoformat()

# 1. 상위 500개 게임 목록
print("=== 1. 소유자순 상위 500개 게임 조회 ===")
top_games = get_top_games_by_owners(500)
app_ids = [g["app_id"] for g in top_games]
print(f"  조회 완료: {len(app_ids)}개")

# 2. 게임 상세 정보 수집
print("\n=== 2. 게임 상세 정보 수집 (약 10분 소요) ===")
from collectors.game_info import get_game_detail

game_details = []
for i, app_id in enumerate(app_ids):
    detail = get_game_detail(app_id)
    if detail:
        detail["collected_at"] = now
        game_details.append(detail)
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(app_ids)}] {detail['name']} ... ({len(game_details)}개 수집)")
    time.sleep(1.2)

print(f"\n  총 {len(game_details)}개 게임 수집 완료")
insert_rows(client, "games", game_details)

# 3. 리뷰 요약
print("\n=== 3. 리뷰 요약 수집 ===")
summaries = []
collected_ids = [g["app_id"] for g in game_details]
for i, app_id in enumerate(collected_ids):
    s = get_review_summary(app_id)
    if s["total_reviews"] > 0:
        s["positive_ratio"] = s["total_positive"] / s["total_reviews"]
        s["collected_at"] = now
        summaries.append(s)
    if (i + 1) % 100 == 0:
        print(f"  [{i+1}/{len(collected_ids)}] {len(summaries)}개 수집")
    time.sleep(0.3)

print(f"  총 {len(summaries)}개 리뷰 요약")
insert_rows(client, "review_summary", summaries)

# 4. 가격 스냅샷
print("\n=== 4. 가격 스냅샷 수집 ===")
snapshots = []
for i, app_id in enumerate(collected_ids):
    snap = get_price_snapshot(app_id)
    if snap:
        snapshots.append(snap)
    if (i + 1) % 100 == 0:
        print(f"  [{i+1}/{len(collected_ids)}] {len(snapshots)}개 수집")
    time.sleep(1.2)

print(f"  총 {len(snapshots)}개 가격 스냅샷")
insert_rows(client, "price_history", snapshots)

# 5. 뉴스
print("\n=== 5. 뉴스 수집 ===")
all_news = []
for i, app_id in enumerate(collected_ids):
    news = get_game_news(app_id, count=5)
    for n in news:
        n["collected_at"] = now
    all_news.extend(news)
    if (i + 1) % 100 == 0:
        print(f"  [{i+1}/{len(collected_ids)}] {len(all_news)}개 수집")
    time.sleep(0.3)

print(f"  총 {len(all_news)}개 뉴스")
insert_rows(client, "news", all_news)

print("\n=== 초기 수집 완료! ===")
print(f"  games: {len(game_details)}")
print(f"  review_summary: {len(summaries)}")
print(f"  price_history: {len(snapshots)}")
print(f"  news: {len(all_news)}")
print("\nBigQuery에서 확인: SELECT COUNT(*) FROM steam_data.games;")
