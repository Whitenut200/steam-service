"""리뷰 텍스트 분석 로컬 CLI — cloud_functions/analyzers/text_analysis.py 사용

사용법:
  pip install kiwipiepy
  python first/run_analyze.py
  python first/run_analyze.py --app-id 1245620
  python first/run_analyze.py --limit 500
"""
import sys
import os

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.join(_root, "cloud_functions"))

import argparse

from utils.env import init_env
init_env()

from utils.bq_helpers import get_bq_client, insert_rows, PROJECT_ID, DATASET
from analyzers.text_analysis import analyze_reviews


TAG_LABEL = {"NNG": "명사", "NNP": "고유", "VA": "형용사"}


def get_target_date(client) -> str | None:
    """번역 완료된 리뷰의 최신 KST 수집일 반환 (analyze_daily와 동일 로직)"""
    query = f"""
        SELECT MAX(DATE(TIMESTAMP(collected_at), "Asia/Seoul")) AS max_date
        FROM `{PROJECT_ID}.{DATASET}.reviews`
        WHERE review_text_ko IS NOT NULL
          AND TRIM(review_text_ko) != ''
    """
    row = list(client.query(query).result())
    if not row or row[0].max_date is None:
        return None
    return str(row[0].max_date)


def analyze_game(client, app_id: int, target_date: str, limit: int = 300) -> dict:
    """게임 1개 분석 — 최신일 수집 리뷰만 로드 후 analyze_reviews 호출"""
    query = f"""
        SELECT recommendation_id, voted_up, review_text_ko, language
        FROM `{PROJECT_ID}.{DATASET}.reviews`
        WHERE app_id = {app_id}
          AND review_text_ko IS NOT NULL
          AND TRIM(review_text_ko) != ''
          AND DATE(TIMESTAMP(collected_at), "Asia/Seoul") = DATE '{target_date}'
        LIMIT {limit}
    """
    rows = list(client.query(query).result())
    if not rows:
        return {"app_id": app_id, "total": 0}
    result = analyze_reviews(rows)
    result["app_id"] = app_id
    return result


def print_result(result: dict):
    print(f"\n{'='*60}")
    print(f"app_id: {result['app_id']} | 리뷰 {result['total']}개 "
          f"(추천 {result.get('total_positive',0)} / 비추천 {result.get('total_negative',0)})")
    print(f"분석 문장: 긍정 {result.get('pos_sentences',0)}개 / 부정 {result.get('neg_sentences',0)}개")

    print(f"\n--- 긍정 카테고리 ---")
    for (top, sub), cnt in sorted(result.get("pos_categories", {}).items(), key=lambda x: -x[1]):
        print(f"  [{top}] {sub}: {cnt}회")

    print(f"\n--- 부정 카테고리 ---")
    for (top, sub), cnt in sorted(result.get("neg_categories", {}).items(), key=lambda x: -x[1]):
        print(f"  [{top}] {sub}: {cnt}회")

    print(f"\n--- 긍정 키워드 TOP 20 ---")
    for (word, tag), cnt in result.get("pos_keywords", [])[:20]:
        print(f"  {word} [{TAG_LABEL.get(tag, tag)}]: {cnt}회")

    print(f"\n--- 부정 키워드 TOP 20 ---")
    for (word, tag), cnt in result.get("neg_keywords", [])[:20]:
        print(f"  {word} [{TAG_LABEL.get(tag, tag)}]: {cnt}회")


def ensure_tables(client):
    dataset_ref = f"{PROJECT_ID}.{DATASET}"
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{dataset_ref}.category_analysis` (
            app_id INT64, polarity STRING, category STRING, subcategory STRING,
            count INT64, collected_date DATE
        )
    """).result()
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{dataset_ref}.keyword_analysis` (
            app_id INT64, polarity STRING, keyword STRING, pos_tag STRING,
            count INT64, ratio FLOAT64, collected_date DATE
        )
    """).result()
    print("테이블 확인 완료 (category_analysis, keyword_analysis)")


def save_to_bq(client, result: dict, today_str: str):
    app_id = result["app_id"]
    total_pos = result.get("total_positive", 0)
    total_neg = result.get("total_negative", 0)

    cat_rows = []
    for (top, sub), cnt in result.get("pos_categories", {}).items():
        cat_rows.append({"app_id": app_id, "polarity": "positive",
                         "category": top, "subcategory": sub,
                         "count": cnt, "collected_date": today_str})
    for (top, sub), cnt in result.get("neg_categories", {}).items():
        cat_rows.append({"app_id": app_id, "polarity": "negative",
                         "category": top, "subcategory": sub,
                         "count": cnt, "collected_date": today_str})
    insert_rows(client, "category_analysis", cat_rows)

    kw_rows = []
    for (word, tag), cnt in result.get("pos_keywords", []):
        ratio = cnt / total_pos if total_pos > 0 else 0
        kw_rows.append({"app_id": app_id, "polarity": "positive",
                        "keyword": word, "pos_tag": tag, "count": cnt,
                        "ratio": round(ratio, 4), "collected_date": today_str})
    for (word, tag), cnt in result.get("neg_keywords", []):
        ratio = cnt / total_neg if total_neg > 0 else 0
        kw_rows.append({"app_id": app_id, "polarity": "negative",
                        "keyword": word, "pos_tag": tag, "count": cnt,
                        "ratio": round(ratio, 4), "collected_date": today_str})
    insert_rows(client, "keyword_analysis", kw_rows)


def main():
    parser = argparse.ArgumentParser(description="리뷰 텍스트 분석 로컬 CLI")
    parser.add_argument("--app-id", type=int, help="특정 게임만 분석")
    parser.add_argument("--limit", type=int, default=300, help="게임당 최대 리뷰 수 (기본: 300)")
    args = parser.parse_args()

    client = get_bq_client()
    ensure_tables(client)

    target_date = get_target_date(client)
    if target_date is None:
        print("번역된 리뷰 없음 — 분석 중단")
        return
    print(f"분석 대상 날짜 (KST 최신 수집일): {target_date}")

    if args.app_id:
        app_ids = [args.app_id]
    else:
        # 그 날짜에 수집된 리뷰가 있는 게임만 추림
        games_query = f"""
            SELECT DISTINCT app_id
            FROM `{PROJECT_ID}.{DATASET}.reviews`
            WHERE review_text_ko IS NOT NULL
              AND TRIM(review_text_ko) != ''
              AND DATE(TIMESTAMP(collected_at), "Asia/Seoul") = DATE '{target_date}'
        """
        app_ids = [r.app_id for r in client.query(games_query).result()]

    print(f"분석 대상: {len(app_ids)}개 게임")
    for i, app_id in enumerate(app_ids):
        result = analyze_game(client, app_id, target_date, limit=args.limit)
        if result["total"] == 0:
            continue
        print_result(result)
        save_to_bq(client, result, target_date)
        if (i + 1) % 10 == 0:
            print(f"\n  [{i+1}/{len(app_ids)}] 게임 처리 완료")

    print(f"\n=== 분석 완료: {len(app_ids)}개 게임 ===")


if __name__ == "__main__":
    main()
