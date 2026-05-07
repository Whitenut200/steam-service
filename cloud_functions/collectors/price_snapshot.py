"""가격 스냅샷 수집 (할인 이력 구축용)"""
from typing import Optional
import requests
from datetime import datetime, timedelta
from config.settings import STEAM_API_BASE, KST


def get_price_snapshot(app_id: int) -> Optional[dict]:
    """현재 가격 정보 스냅샷"""
    resp = requests.get(
        f"{STEAM_API_BASE}/appdetails",
        params={"appids": app_id, "filters": "price_overview", "cc": "kr"},
        timeout=15
    )
    resp.raise_for_status()
    data = resp.json()

    app_data = data.get(str(app_id), {})
    if not app_data.get("success"):
        return None

    inner = app_data.get("data")
    if not isinstance(inner, dict):
        return None

    price = inner.get("price_overview")
    if not price:
        return None

    kst_now = datetime.now(KST)
    kst_yesterday = (kst_now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return {
        "app_id": app_id,
        "snapshot_date": kst_yesterday.strftime("%Y-%m-%d"),
        "snapshot_timestamp": kst_now.isoformat(),
        "currency": price.get("currency", ""),
        "initial_price": price.get("initial", 0) // 100,
        "final_price": price.get("final", 0) // 100,
        "discount_percent": price.get("discount_percent", 0),
    }


if __name__ == "__main__":
    test_ids = [1623730, 1091500, 1245620]  # Palworld, Cyberpunk 2077, Elden Ring
    for app_id in test_ids:
        s = get_price_snapshot(app_id)
        if s:
            print(f"  {s['app_id']} | {s['final_price']:,}{s['currency']} (할인 {s['discount_percent']}%)")
        else:
            print(f"  {app_id} | 가격 정보 없음")
