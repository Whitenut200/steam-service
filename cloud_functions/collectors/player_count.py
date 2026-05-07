"""Steam 동시접속자수 수집"""
import requests
from typing import Optional

PLAYER_COUNT_API = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"


def get_player_count(app_id: int) -> Optional[int]:
    """게임의 현재 동시접속자수 조회"""
    try:
        resp = requests.get(
            PLAYER_COUNT_API,
            params={"appid": app_id},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", {}).get("player_count")
    except (requests.RequestException, ValueError):
        return None


if __name__ == "__main__":
    test_ids = [730, 570, 1245620]  # CS2, Dota2, Elden Ring
    for app_id in test_ids:
        count = get_player_count(app_id)
        print(f"  app_id={app_id} | 동시접속: {count:,}명" if count else f"  app_id={app_id} | 조회 실패")
