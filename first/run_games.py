"""게임 목록만 수집: 소유자순 상위 500개 → BigQuery games 테이블 적재

사용법:
  python first/run_games.py
  python first/run_games.py --count 100
"""
import sys
import os
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import argparse
import time
from datetime import datetime

from utils.env import init_env
init_env()

from utils.bq_helpers import get_bq_client, insert_rows
from collectors.game_info import get_top_games_by_owners, get_game_detail
from config.settings import KST


def main():
    parser = argparse.ArgumentParser(description="게임 목록 수집")
    parser.add_argument("--count", type=int, default=200, help="수집할 게임 수 (기본: 500)")
    args = parser.parse_args()

    now = datetime.now(KST).isoformat()
    client = get_bq_client()

    print(f"=== 소유자순 상위 {args.count}개 게임 조회 ===")
    top_games = get_top_games_by_owners(args.count)
    app_ids = [g["app_id"] for g in top_games]
    print(f"  조회 완료: {len(app_ids)}개")

    print(f"\n=== 게임 상세 정보 수집 ===")
    game_details = []
    for i, app_id in enumerate(app_ids):
        detail = get_game_detail(app_id)
        if detail:
            detail["collected_at"] = now
            game_details.append(detail)
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(app_ids)}] {len(game_details)}개 수집")
        time.sleep(1.2)

    print(f"\n  총 {len(game_details)}개 게임")
    insert_rows(client, "games", game_details)

    print(f"\n=== 완료! games: {len(game_details)}개 ===")


if __name__ == "__main__":
    main()
