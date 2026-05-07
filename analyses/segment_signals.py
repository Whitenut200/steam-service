"""유저 세그먼트 시그널 (AI 코멘트 섹션 ②)

- 항목 4: 플레이타임 코호트별 추천률 — 진입장벽/이탈 시그널
- 항목 5: 언어별 추천률 갭 — 현지화/시장진입 시그널
"""
from __future__ import annotations
import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")
TABLE = f"{PROJECT_ID}.{DATASET}"

MIN_COHORT_N = 30
MIN_LANG_N = 30
TOP_LANGS = 5


def get_playtime_cohorts(client, app_id: int) -> dict | None:
    """플레이타임 코호트별 추천률.

    Returns:
        {
            "버킷별": [{"bucket": "0-10h", "n": 335, "pos_ratio": 83.3}, ...],
            "전체_pos_ratio": 85.4,
            "전체_n": 2344,
        } 또는 None (데이터 부족)
    """
    query = f"""
        WITH base AS (
            SELECT
                CASE
                    WHEN playtime_at_review < 600 THEN '0-10h'
                    WHEN playtime_at_review < 3000 THEN '10-50h'
                    ELSE '50h+'
                END AS bucket,
                voted_up
            FROM `{TABLE}.reviews`
            WHERE app_id = {app_id} AND playtime_at_review IS NOT NULL
        )
        SELECT
            bucket,
            COUNT(*) AS n,
            ROUND(COUNTIF(voted_up) / COUNT(*) * 100, 1) AS pos_ratio
        FROM base
        GROUP BY bucket
        ORDER BY bucket
    """
    rows = list(client.query(query).result())
    if not rows:
        return None

    buckets = [
        {"bucket": r.bucket, "n": r.n, "pos_ratio": r.pos_ratio}
        for r in rows if r.n >= MIN_COHORT_N
    ]
    if not buckets:
        return None

    total_n = sum(b["n"] for b in buckets)
    total_pos = sum(b["n"] * b["pos_ratio"] / 100 for b in buckets)
    return {
        "버킷별": buckets,
        "전체_pos_ratio": round(total_pos / total_n * 100, 1) if total_n else None,
        "전체_n": total_n,
    }


def get_language_gap(client, app_id: int) -> dict | None:
    """언어별 추천률 갭. Top N 언어 중 샘플 충분한 것만.

    Returns:
        {
            "언어별": [{"language": "english", "n": 1052, "pos_ratio": 85.2}, ...],
            "전체_pos_ratio": 84.5,
            "최고": {"language": "brazilian", "pos_ratio": 95.4},
            "최저": {"language": "schinese", "pos_ratio": 74.1},
            "갭_pp": 21.3,  # 최고 - 최저
        } 또는 None
    """
    query = f"""
        SELECT
            language,
            COUNT(*) AS n,
            ROUND(COUNTIF(voted_up) / COUNT(*) * 100, 1) AS pos_ratio
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id} AND language IS NOT NULL
        GROUP BY language
        HAVING n >= {MIN_LANG_N}
        ORDER BY n DESC
        LIMIT {TOP_LANGS}
    """
    rows = list(client.query(query).result())
    if len(rows) < 2:
        return None

    langs = [
        {"language": r.language, "n": r.n, "pos_ratio": r.pos_ratio}
        for r in rows
    ]
    total_n = sum(l["n"] for l in langs)
    total_pos = sum(l["n"] * l["pos_ratio"] / 100 for l in langs)
    avg = round(total_pos / total_n * 100, 1) if total_n else None

    high = max(langs, key=lambda x: x["pos_ratio"])
    low = min(langs, key=lambda x: x["pos_ratio"])
    return {
        "언어별": langs,
        "전체_pos_ratio": avg,
        "최고": {"language": high["language"], "pos_ratio": high["pos_ratio"]},
        "최저": {"language": low["language"], "pos_ratio": low["pos_ratio"]},
        "갭_pp": round(high["pos_ratio"] - low["pos_ratio"], 1),
    }


def get_segment_signals(client, app_id: int) -> dict:
    """섹션 ② 통합 입력 dict."""
    return {
        "플레이타임_코호트": get_playtime_cohorts(client, app_id),
        "언어_갭": get_language_gap(client, app_id),
    }
