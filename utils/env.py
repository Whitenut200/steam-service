"""환경변수 초기화 (로컬 실행용)"""
import os
import sys

def init_env():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, base_dir)
    os.environ.setdefault("GCP_PROJECT_ID", "steam-service-492701")
    os.environ.setdefault("BQ_DATASET", "steam_data")
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS",
                          os.path.join(base_dir, "credentials.json"))
