"""프로젝트 설정"""
import os

# GCP 설정
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_DATASET = os.getenv("BQ_DATASET", "steam_data")

# Steam API
STEAM_API_BASE = "https://store.steampowered.com/api"
STEAM_REVIEW_API = "https://store.steampowered.com/appreviews"
STEAM_NEWS_API = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2"
STEAM_SPY_API = "https://steamspy.com/api.php"

# IsThereAnyDeal API (과거 가격 이력)
ITAD_API_KEY = os.getenv("ITAD_API_KEY", "06e638df0cb1db63eb482341a6149e3b723254ec")
ITAD_API_BASE = "https://api.isthereanydeal.com"

# 수집 설정
REVIEW_BATCH_SIZE = 100  # 한 번에 가져올 리뷰 수
MAX_REVIEWS_PER_GAME = 500  # 게임당 최대 리뷰 수
TOP_GAMES_COUNT = 500  # 수집 대상 상위 게임 수


