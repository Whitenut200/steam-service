"""TF-IDF 차별화 키워드 (AI 코멘트 섹션 ① 항목 2)

게임별로 "그 게임만의 특징적인" 긍정/부정 키워드를 추출.
- TF  = keyword_analysis.ratio
- IDF = ln(코퍼스_게임수 / df)
- 코퍼스: 전체 게임 + 같은 장르 게임 (장르 prefix 2단어 일치)

필터: df >= 2, count >= 3 (노이즈/오타 제거).
같은 장르 코퍼스 < 10이면 장르비교는 None 반환.
"""
from __future__ import annotations
import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")
TABLE = f"{PROJECT_ID}.{DATASET}"

TOP_N = 3
MIN_DF = 2
MIN_COUNT = 3
MIN_GENRE_CORPUS = 10


def _compute_tfidf(client, app_id: int, corpus_filter: str = "") -> dict:
    """TF-IDF 계산. corpus_filter는 latest_kw에 추가되는 SQL 조각 (예: 'AND k.app_id IN (1,2,3)')."""
    query = f"""
        WITH latest_per_game AS (
            SELECT app_id, MAX(collected_date) AS d
            FROM `{TABLE}.keyword_analysis`
            GROUP BY app_id
        ),
        latest_kw AS (
            SELECT k.app_id, k.polarity, k.keyword, k.count, k.ratio
            FROM `{TABLE}.keyword_analysis` k
            JOIN latest_per_game l USING (app_id)
            WHERE k.collected_date = l.d
              {corpus_filter}
        ),
        df_table AS (
            SELECT polarity, keyword, COUNT(DISTINCT app_id) AS df
            FROM latest_kw
            GROUP BY polarity, keyword
        ),
        n AS (SELECT COUNT(DISTINCT app_id) AS total FROM latest_kw),
        target AS (
            SELECT * FROM latest_kw WHERE app_id = {app_id}
        )
        SELECT
            t.polarity,
            t.keyword,
            t.count,
            t.ratio,
            df_table.df,
            ROUND(t.ratio * LN((SELECT total FROM n) / df_table.df), 4) AS tfidf,
            (SELECT total FROM n) AS corpus_size
        FROM target t
        JOIN df_table USING (polarity, keyword)
        WHERE df_table.df >= {MIN_DF} AND t.count >= {MIN_COUNT}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY t.polarity ORDER BY tfidf DESC) <= {TOP_N}
        ORDER BY t.polarity, tfidf DESC
    """
    rows = list(client.query(query).result())

    pos, neg = [], []
    corpus_size = 0
    for r in rows:
        corpus_size = r.corpus_size
        item = {
            "keyword": r.keyword,
            "tfidf": r.tfidf,
            "ratio_pct": round((r.ratio or 0) * 100, 1),
            "df_games": r.df,
        }
        if r.polarity == "positive":
            pos.append(item)
        elif r.polarity == "negative":
            neg.append(item)
    return {"긍정": pos, "부정": neg, "corpus_size": corpus_size}


def _genre_prefix_expr(col: str = "genres") -> str:
    """장르 문자열에서 첫 2 토큰 추출 (콤마 구분)."""
    return (
        "ARRAY_TO_STRING("
        f"  ARRAY(SELECT TRIM(g) FROM UNNEST(SPLIT({col}, ',')) g WITH OFFSET pos "
        "        WHERE pos < 2 ORDER BY pos), ', ')"
    )


def get_tfidf_keywords(client, app_id: int) -> dict:
    """전체비교 + 같은장르비교 TF-IDF dict 반환.

    Returns:
        {
            "전체비교": {"긍정": [...x3], "부정": [...x3], "corpus_size": int},
            "장르비교": {"장르": "Action, Adventure", "긍정": [...], "부정": [...], "corpus_size": int} | None,
        }
    """
    overall = _compute_tfidf(client, app_id)

    # 타겟 게임의 장르 prefix
    prefix_query = f"""
        SELECT {_genre_prefix_expr()} AS prefix
        FROM `{TABLE}.games`
        WHERE app_id = {app_id}
    """
    rows = list(client.query(prefix_query).result())
    target_prefix = (rows[0].prefix or "").strip() if rows else ""

    genre = None
    if target_prefix:
        # 같은 장르 prefix를 가진 app_id 목록
        same_query = f"""
            SELECT app_id
            FROM `{TABLE}.games`
            WHERE {_genre_prefix_expr()} = @prefix
        """
        from google.cloud import bigquery as bq
        cfg = bq.QueryJobConfig(
            query_parameters=[bq.ScalarQueryParameter("prefix", "STRING", target_prefix)]
        )
        same_apps = [r.app_id for r in client.query(same_query, job_config=cfg).result()]

        if len(same_apps) >= MIN_GENRE_CORPUS:
            ids_str = ",".join(str(a) for a in same_apps)
            g = _compute_tfidf(client, app_id, corpus_filter=f"AND k.app_id IN ({ids_str})")
            genre = {"장르": target_prefix, **g}

    return {"전체비교": overall, "장르비교": genre}
