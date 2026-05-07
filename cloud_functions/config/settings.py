"""프로젝트 설정"""
import os
from datetime import timezone, timedelta

# 한국 시간대 (UTC+9)
KST = timezone(timedelta(hours=9))

# GCS (신규 게임 목록 전달용)
GCS_BUCKET = os.getenv("GCS_BUCKET", "steam-service-492701-data")

# Steam API
STEAM_API_BASE = "https://store.steampowered.com/api"
STEAM_REVIEW_API = "https://store.steampowered.com/appreviews"
STEAM_NEWS_API = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2"
STEAM_SPY_API = "https://steamspy.com/api.php"

# IsThereAnyDeal API (과거 가격 이력)
ITAD_API_KEY = os.getenv("ITAD_API_KEY", "")
ITAD_API_BASE = "https://api.isthereanydeal.com"

# 수집 설정
REVIEW_BATCH_SIZE = 100  # 한 번에 가져올 리뷰 수
MAX_REVIEWS_PER_GAME = 300  # 게임당 최대 리뷰 수
TOP_GAMES_COUNT = 500  # 수집 대상 상위 게임 수
