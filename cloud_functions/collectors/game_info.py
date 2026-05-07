"""Steam 게임 기본 정보 수집"""
import re
from typing import Optional
import requests
import time
from config.settings import STEAM_API_BASE, STEAM_SPY_API, TOP_GAMES_COUNT

STEAM_SEARCH_RENDER = "https://store.steampowered.com/search/render/"
TRENDING_COUNT = 50


def get_top_games_by_owners(count: int = TOP_GAMES_COUNT) -> list:
    """[초기 수집용] SteamSpy에서 소유자 수 기준 상위 게임 목록"""
    all_games = []

    for page in range(0, (count // 1000) + 1):
        for attempt in range(3):
            try:
                resp = requests.get(STEAM_SPY_API, params={"request": "all", "page": page}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except (requests.RequestException, ValueError):
                print(f"  SteamSpy page {page} 재시도 ({attempt+1}/3)...")
                time.sleep(5)
        else:
            print(f"  SteamSpy page {page} 실패, 건너뜀")
            continue

        for app_id, info in data.items():
            all_games.append({
                "app_id": int(app_id),
                "name": info.get("name", ""),
                "owners": info.get("owners", "0"),
            })

        if len(data) < 1000:
            break
        time.sleep(2)

    def parse_owners(owners_str):
        try:
            return int(owners_str.split("..")[0].strip().replace(",", ""))
        except (ValueError, IndexError):
            return 0

    all_games.sort(key=lambda g: parse_owners(g["owners"]), reverse=True)

    return [{"app_id": g["app_id"], "name": g["name"]} for g in all_games[:count]]


def get_trending_games(count: int = TRENDING_COUNT) -> list:
    """[매일 수집용] Steam 한국 스토어 탑셀러 상위 N개 (판매 기준, 단기)

    Steam 공식 검색 render 엔드포인트(`store.steampowered.com/search/render/`)에서
    filter=topsellers, cc=kr 로 한국 지역 탑셀러 HTML을 받아 app_id만 파싱.
    번들(comma-separated data-ds-appid)은 자동 제외.
    """
    resp = requests.get(
        STEAM_SEARCH_RENDER,
        params={
            "filter": "topsellers",
            "cc": "kr",
            "l": "koreana",
            "count": count,
            "start": 0,
            "ignore_preferences": 1,
            "infinite": 1,
        },
        headers={
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()
    try:
        data = resp.json()
        html = data.get("results_html", "")
    except ValueError:
        html = resp.text

    games = []
    seen = set()
    pattern = re.compile(
        r'data-ds-appid="(\d+)".*?<span class="title">([^<]+)</span>',
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        app_id = int(m.group(1))
        if app_id in seen:
            continue
        seen.add(app_id)
        games.append({"app_id": app_id, "name": m.group(2).strip()})
        if len(games) >= count:
            break

    return games


def get_existing_app_ids(client, project_id: str, dataset: str) -> set:
    """BigQuery에서 이미 저장된 app_id 목록 조회"""
    query = f"SELECT DISTINCT app_id FROM `{project_id}.{dataset}.games`"
    result = client.query(query).result()
    return {row.app_id for row in result}



def get_game_detail(app_id: int) -> Optional[dict]:
    """게임 상세 정보 가져오기"""
    try:
        resp = requests.get(
            f"{STEAM_API_BASE}/appdetails",
            params={"appids": app_id, "l": "english", "cc": "kr"},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError):
        return None

    app_data = data.get(str(app_id), {})
    if not app_data.get("success"):
        return None

    info = app_data["data"]
    price_info = info.get("price_overview", {})

    return {
        "app_id": app_id,
        "name": info.get("name"),
        "type": info.get("type"),
        "required_age": info.get("required_age", 0),
        "is_free": info.get("is_free", False),
        "short_description": info.get("short_description", ""),
        "developers": ", ".join(info.get("developers", [])),
        "publishers": ", ".join(info.get("publishers", [])),
        "currency": price_info.get("currency", ""),
        "genres": ", ".join([g["description"] for g in info.get("genres", [])]),
        "categories": ", ".join([c["description"] for c in info.get("categories", [])]),
        "release_date": info.get("release_date", {}).get("date", ""),
        "metacritic_score": info.get("metacritic", {}).get("score", 0),
        "recommendations": info.get("recommendations", {}).get("total", 0),
        "header_image": info.get("header_image", ""),
    }


if __name__ == "__main__":
    top_games = get_top_games_by_owners(5)
    print(f"상위 게임: {top_games}")

    for g in top_games:
        detail = get_game_detail(g["app_id"])
        if detail:
            print(f"  {detail['name']} | {detail['genres']}")
        time.sleep(1.2)
