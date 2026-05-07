"""과거 가격 이력 수집 (ITAD) → BigQuery 적재
게임이 DB에 처음 추가될 때 1회만 실행
"""
import sys
import os
_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import time
from utils.env import init_env
init_env()

from utils.bq_helpers import get_bq_client, insert_rows, get_all_app_ids
from collectors.price_history import collect_price_history

client = get_bq_client()

app_ids = list(get_all_app_ids(client))
print(f"=== 과거 가격 이력 수집: {len(app_ids)}개 게임 ===")

total = 0
for i, app_id in enumerate(app_ids):
    history = collect_price_history(app_id)
    if history:
        insert_rows(client, "price_history", history)
        total += len(history)
    if (i + 1) % 20 == 0:
        print(f"  [{i+1}/{len(app_ids)}] 누적 {total}개 이력")
    time.sleep(0.5)

print(f"\n=== 완료! 총 {total}개 가격 이력 적재 ===")
