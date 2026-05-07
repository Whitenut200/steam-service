"""감성 분석 요약 (AI 코멘트 섹션 ①)

BQ 분석 테이블에서 게임의 최신 감성/카테고리/키워드 데이터 추출 → dict 반환.
TF-IDF 차별화 키워드(전체/장르)도 함께 결합.
"""
from __future__ import annotations
import os

from analyses.tfidf_keywords import get_tfidf_keywords

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")
TABLE = f"{PROJECT_ID}.{DATASET}"


def _get_pos_ratio_trend(client, app_id: int) -> dict | None:
    """누적 review_summary에서 최근 4주 주별 긍정률 + 최근주 vs 직전3주 비교.

    각 주의 마지막 스냅샷을 뽑은 뒤 인접 주 사이 delta로 "그 주에 새로 들어온 리뷰의
    긍정률"을 계산. 누적 스냅샷이 2주 미만이면 None.
    """
    query = f"""
        WITH weekly AS (
            SELECT
                DATE_TRUNC(DATE(collected_at, "Asia/Seoul"), WEEK(MONDAY)) AS week,
                collected_at, total_reviews, total_positive,
                ROW_NUMBER() OVER (
                    PARTITION BY DATE_TRUNC(DATE(collected_at, "Asia/Seoul"), WEEK(MONDAY))
                    ORDER BY collected_at DESC
                ) AS rn
            FROM `{TABLE}.review_summary`
            WHERE app_id = {app_id}
              AND collected_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 35 DAY)
        )
        SELECT week, total_reviews, total_positive
        FROM weekly
        WHERE rn = 1
        ORDER BY week
    """
    rows = list(client.query(query).result())
    if len(rows) < 2:
        return None

    weekly = []
    for i in range(1, len(rows)):
        new_total = (rows[i].total_reviews or 0) - (rows[i - 1].total_reviews or 0)
        new_pos = (rows[i].total_positive or 0) - (rows[i - 1].total_positive or 0)
        if new_total <= 0:
            continue
        weekly.append({
            "week": str(rows[i].week),
            "new_reviews": new_total,
            "pos_ratio": round(new_pos / new_total * 100, 1),
        })
    if not weekly:
        return None

    weekly = weekly[-4:]
    recent = weekly[-1]
    prior = weekly[:-1]
    out = {
        "주별": weekly,
        "최근주_긍정률": recent["pos_ratio"],
    }
    if prior:
        prior_new = sum(w["new_reviews"] for w in prior)
        prior_pos = sum(w["new_reviews"] * w["pos_ratio"] / 100 for w in prior)
        if prior_new > 0:
            prior_avg = round(prior_pos / prior_new * 100, 1)
            out["직전3주_평균"] = prior_avg
            out["변화_pp"] = round(recent["pos_ratio"] - prior_avg, 1)
    return out


def get_sentiment_summary(client, app_id: int) -> dict | None:
    """게임의 감성 요약 데이터 추출.

    Returns:
        dict 또는 None (분석 데이터 없으면 None)
        {
            "긍정률": 72.3,
            "부정률": 27.7,
            "총_리뷰수": 523,
            "긍정_top_카테고리": [{"name": "재미/몰입", "count": 152, "share_pct": 32.5}, ...],
            "부정_top_카테고리": [{"name": "성능/서버", "count": 88, "share_pct": 41.2}, ...],
            "긍정_키워드": ["협동", "재미", "그래픽"],
            "부정_키워드": ["서버", "렉", "핑"],
            "차별화_키워드_전체": {"긍정": [...x3], "부정": [...x3], "corpus_size": int},
            "차별화_키워드_장르": {"장르": "...", "긍정": [...], "부정": [...], "corpus_size": int} | None,
            "긍정률_4주추세": {"주별": [...], "최근주_긍정률": float, "직전3주_평균": float, "변화_pp": float} | None,
        }
    """
    # 최신 collected_date의 카테고리 분석
    cat_query = f"""
        WITH latest AS (
            SELECT MAX(collected_date) AS d
            FROM `{PROJECT_ID}.{DATASET}.category_analysis`
            WHERE app_id = {app_id}
        )
        SELECT polarity, subcategory, count
        FROM `{PROJECT_ID}.{DATASET}.category_analysis`
        WHERE app_id = {app_id}
          AND collected_date = (SELECT d FROM latest)
    """
    cat_rows = list(client.query(cat_query).result())

    # 최신 collected_date의 키워드 분석
    kw_query = f"""
        WITH latest AS (
            SELECT MAX(collected_date) AS d
            FROM `{PROJECT_ID}.{DATASET}.keyword_analysis`
            WHERE app_id = {app_id}
        )
        SELECT polarity, keyword, count
        FROM `{PROJECT_ID}.{DATASET}.keyword_analysis`
        WHERE app_id = {app_id}
          AND collected_date = (SELECT d FROM latest)
        ORDER BY count DESC
    """
    kw_rows = list(client.query(kw_query).result())

    # 리뷰 요약 (Steam API 전체 통계, 최신)
    sum_query = f"""
        SELECT total_reviews, total_positive, total_negative
        FROM `{PROJECT_ID}.{DATASET}.review_summary`
        WHERE app_id = {app_id}
        ORDER BY collected_at DESC
        LIMIT 1
    """
    sum_rows = list(client.query(sum_query).result())

    if not sum_rows or not cat_rows:
        return None

    s = sum_rows[0]
    total = s.total_reviews or 0
    if total <= 0:
        return None
    pos_ratio = round((s.total_positive or 0) / total * 100, 1)

    all_pos_cats = [(r.subcategory, r.count) for r in cat_rows if r.polarity == "positive"]
    all_neg_cats = [(r.subcategory, r.count) for r in cat_rows if r.polarity == "negative"]
    pos_total = sum(c[1] for c in all_pos_cats) or 1
    neg_total = sum(c[1] for c in all_neg_cats) or 1

    def _top_with_share(cats, denom):
        top = sorted(cats, key=lambda x: -x[1])[:3]
        return [
            {"name": name, "count": cnt, "share_pct": round(cnt / denom * 100, 1)}
            for name, cnt in top
        ]

    pos_cats_top = _top_with_share(all_pos_cats, pos_total)
    neg_cats_top = _top_with_share(all_neg_cats, neg_total)
    pos_kws = [r.keyword for r in kw_rows if r.polarity == "positive"][:5]
    neg_kws = [r.keyword for r in kw_rows if r.polarity == "negative"][:5]

    tfidf = get_tfidf_keywords(client, app_id)
    trend = _get_pos_ratio_trend(client, app_id)

    return {
        "긍정률": pos_ratio,
        "부정률": round(100 - pos_ratio, 1),
        "총_리뷰수": total,
        "긍정_top_카테고리": pos_cats_top,
        "부정_top_카테고리": neg_cats_top,
        "긍정_키워드": pos_kws,
        "부정_키워드": neg_kws,
        "차별화_키워드_전체": tfidf["전체비교"],
        "차별화_키워드_장르": tfidf["장르비교"],
        "긍정률_4주추세": trend,
    }
