"""IsThereAnyDeal API로 과거 가격 이력 수집"""
import requests
import time
from datetime import datetime
from config.settings import ITAD_API_KEY, ITAD_API_BASE, KST


def lookup_itad_id(app_id: int) -> str:
    """Steam app_id → ITAD 게임 ID 변환"""
    try:
        resp = requests.get(
            f"{ITAD_API_BASE}/games/lookup/v1",
            params={"key": ITAD_API_KEY, "appid": app_id},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("found"):
            return data["game"]["id"]
    except (requests.RequestException, ValueError):
        pass
    return ""


def get_price_history(itad_id: str, country: str = "KR", shops: str = "steam") -> list:
    """ITAD에서 과거 가격 이력 가져오기"""
    try:
        resp = requests.get(
            f"{ITAD_API_BASE}/games/history/v2",
            params={
                "key": ITAD_API_KEY,
                "id": itad_id,
                "country": country,
                "shops": shops,
            },
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError):
        return []


def collect_price_history(app_id: int) -> list:
    """Steam app_id로 과거 가격 이력 수집 (Steam 스토어만, KRW)"""
    itad_id = lookup_itad_id(app_id)
    if not itad_id:
        return []

    data = get_price_history(itad_id)
    if not data:
        return []

    rows = []
    for entry in data:
        # Steam 스토어 데이터만 필터
        shop = entry.get("shop", {})
        if shop.get("name", "").lower() != "steam":
            continue

        deal = entry.get("deal", {})
        price = deal.get("price", {})
        regular = deal.get("regular", {})

        # ITAD 타임스탬프를 KST로 변환
        raw_ts = entry.get("timestamp", "")
        try:
            dt_kst = datetime.fromisoformat(raw_ts).astimezone(KST)
            snapshot_date = dt_kst.strftime("%Y-%m-%d")
            snapshot_timestamp = dt_kst.isoformat()
        except (ValueError, TypeError):
            snapshot_date = raw_ts[:10]
            snapshot_timestamp = raw_ts

        rows.append({
            "app_id": app_id,
            "snapshot_date": snapshot_date,
            "snapshot_timestamp": snapshot_timestamp,
            "currency": price.get("currency", "KRW"),
            "initial_price": regular.get("amountInt", 0),
            "final_price": price.get("amountInt", 0),
            "discount_percent": deal.get("cut", 0),
        })

    return rows


if __name__ == "__main__":
    # 테스트: Elden Ring
    history = collect_price_history(1245620)
    print(f"Elden Ring 가격 이력: {len(history)}개")
    for h in history[:10]:
        discount = f" (-{h['discount_percent']}%)" if h['discount_percent'] > 0 else ""
        print(f"  {h['snapshot_date']} | {h['final_price']:,}{h['currency']}{discount}")
