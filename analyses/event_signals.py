"""변화 알림 (AI 코멘트 섹션 ③) — 이벤트 시그널

4 항목:
- 항목 6: 패치 임팩트 — 최근 뉴스 ±7일 추천률·동접 변화
- 항목 7: 세일 효과 — 최근 할인 윈도 동안 동접 변화
- 항목 8: 부정 키워드 급증 — 최근 1주 vs 직전 4주 평균
- 항목 9: 동접 z-score 이상치 + news/price cross-ref
"""
from __future__ import annotations
import os
import math
from statistics import mean, stdev

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "steam-service-492701")
DATASET = os.getenv("BQ_DATASET", "steam_data")
TABLE = f"{PROJECT_ID}.{DATASET}"

# 임계값
MIN_REVIEWS_PER_WINDOW = 5      # 패치 전후 표본 최소
MIN_SALE_DISCOUNT = 10           # %, 이 이하면 세일로 안 침
ANOMALY_Z = 2.0                  # |z| 이상 이상치
SURGE_RATIO = 2.0                # 키워드 급증 배수
MIN_BASELINE_DAYS = 7            # baseline 동접 최소 일수

# 유의성 라벨 — 두 변형 (동접용/추천률용). 변경 시 SECTION3_PROMPT 본문도 동시에 갱신해야 함.
Z_LABELS = ("뚜렷한_증가", "약한_증가", "평소_변동_범위", "약한_감소", "뚜렷한_감소")
PROP_LABELS = ("뚜렷한_상승", "약한_상승", "유의미한_변화_없음", "약한_하락", "뚜렷한_하락")


def _categorize_z(z: float | None, labels: tuple[str, ...] = Z_LABELS) -> str | None:
    """z-score를 5단계 유의성 카테고리로 (|z|=2, |z|=1 기준)."""
    if z is None:
        return None
    if z >= 2:
        return labels[0]
    if z >= 1:
        return labels[1]
    if z > -1:
        return labels[2]
    if z > -2:
        return labels[3]
    return labels[4]


def _two_proportion_z(pos1: int, n1: int, pos2: int, n2: int) -> float | None:
    """두 비율 차이 z-test (pooled SE). pos2/n2 - pos1/n1 의 z 반환."""
    if n1 == 0 or n2 == 0:
        return None
    p1 = pos1 / n1
    p2 = pos2 / n2
    p_pool = (pos1 + pos2) / (n1 + n2)
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n1 + 1 / n2))
    if se == 0:
        return None
    return (p2 - p1) / se


# ── 항목 6: 패치 임팩트 ────────────────────────────────────

def get_patch_impact(client, app_id: int) -> dict | None:
    """최근 뉴스 1건을 이벤트로 잡고 ±7일 동안 추천률·평균 동접 비교."""
    news_q = f"""
        SELECT TIMESTAMP_SECONDS(date) AS event_ts, title
        FROM `{TABLE}.news`
        WHERE app_id = {app_id}
          AND TIMESTAMP_SECONDS(date) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
        ORDER BY date DESC
        LIMIT 1
    """
    news_rows = list(client.query(news_q).result())
    if not news_rows:
        return None
    event_ts = news_rows[0].event_ts
    title = news_rows[0].title

    # 추천률 비교 (timestamp_created 기준)
    rev_q = f"""
        SELECT
            CASE WHEN TIMESTAMP_SECONDS(timestamp_created) < TIMESTAMP('{event_ts.isoformat()}')
                 THEN 'before' ELSE 'after' END AS phase,
            COUNT(*) AS n,
            COUNTIF(voted_up) AS pos
        FROM `{TABLE}.reviews`
        WHERE app_id = {app_id}
          AND TIMESTAMP_SECONDS(timestamp_created)
              BETWEEN TIMESTAMP_SUB(TIMESTAMP('{event_ts.isoformat()}'), INTERVAL 7 DAY)
                  AND TIMESTAMP_ADD(TIMESTAMP('{event_ts.isoformat()}'), INTERVAL 7 DAY)
        GROUP BY phase
    """
    rev_rows = {r.phase: r for r in client.query(rev_q).result()}

    # 동접 일별 평균 (baseline = 60일 중 패치 ±7일 제외)
    event_date = event_ts.date().isoformat()
    pc_q = f"""
        WITH daily AS (
          SELECT
            DATE(TIMESTAMP(collected_at), "Asia/Seoul") AS d,
            AVG(player_count) AS avg_players
          FROM `{TABLE}.player_counts`
          WHERE app_id = {app_id}
            AND TIMESTAMP(collected_at)
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
          GROUP BY d
        )
        SELECT
          CASE
            WHEN d BETWEEN DATE_SUB(DATE '{event_date}', INTERVAL 7 DAY)
                       AND DATE_SUB(DATE '{event_date}', INTERVAL 1 DAY) THEN 'before'
            WHEN d BETWEEN DATE '{event_date}'
                       AND DATE_ADD(DATE '{event_date}', INTERVAL 7 DAY) THEN 'after'
            ELSE 'baseline'
          END AS phase,
          d, avg_players
        FROM daily
    """
    pc_phases = {"before": [], "after": [], "baseline": []}
    for r in client.query(pc_q).result():
        if r.phase in pc_phases:
            pc_phases[r.phase].append(float(r.avg_players))

    # review delta + 두 비율 z-test
    review_delta = None
    if (
        "before" in rev_rows and "after" in rev_rows
        and rev_rows["before"].n >= MIN_REVIEWS_PER_WINDOW
        and rev_rows["after"].n >= MIN_REVIEWS_PER_WINDOW
    ):
        b_n, b_pos = rev_rows["before"].n, rev_rows["before"].pos
        a_n, a_pos = rev_rows["after"].n, rev_rows["after"].pos
        before_pos = b_pos / b_n * 100
        after_pos = a_pos / a_n * 100
        z = _two_proportion_z(b_pos, b_n, a_pos, a_n)
        review_delta = {
            "before_pos_ratio": round(before_pos, 1),
            "after_pos_ratio": round(after_pos, 1),
            "delta_pp": round(after_pos - before_pos, 1),
            "before_n": b_n,
            "after_n": a_n,
            "z_score": round(z, 2) if z is not None else None,
            "유의성": _categorize_z(z, PROP_LABELS),
        }

    # player delta + baseline z-score
    player_delta = None
    if pc_phases["before"] and pc_phases["after"]:
        b = mean(pc_phases["before"])
        a = mean(pc_phases["after"])
        bl = pc_phases["baseline"]
        z_score = None
        if len(bl) >= MIN_BASELINE_DAYS and stdev(bl) > 0:
            mu, sd = mean(bl), stdev(bl)
            z_score = round((a - mu) / sd, 2)
        player_delta = {
            "before_avg": int(b),
            "after_avg": int(a),
            "delta_pct": round((a - b) / b * 100, 1) if b > 0 else None,
            "baseline_avg": int(mean(bl)) if bl else None,
            "baseline_std": int(stdev(bl)) if len(bl) >= MIN_BASELINE_DAYS and stdev(bl) > 0 else None,
            "z_score": z_score,
            "유의성": _categorize_z(z_score),
        }

    if review_delta is None and player_delta is None:
        return None

    return {
        "이벤트_제목": title,
        "이벤트_일자": event_ts.date().isoformat(),
        "추천률_변화": review_delta,
        "동접_변화": player_delta,
    }


# ── 항목 7: 세일 효과 ─────────────────────────────────────

def get_sale_impact(client, app_id: int) -> dict | None:
    """최근 60일 내 가장 최근 할인 윈도(연속 할인일) → 세일 중 동접 vs 직전 7일 비교."""
    sale_q = f"""
        SELECT snapshot_date, discount_percent
        FROM `{TABLE}.price_history`
        WHERE app_id = {app_id}
          AND snapshot_date >= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 60 DAY)
        ORDER BY snapshot_date
    """
    rows = list(client.query(sale_q).result())
    if not rows:
        return None

    # 가장 최근의 연속 할인 윈도 찾기 (back-scan)
    sale_end = None
    sale_start = None
    max_pct = 0
    for r in reversed(rows):
        if r.discount_percent and r.discount_percent >= MIN_SALE_DISCOUNT:
            if sale_end is None:
                sale_end = r.snapshot_date
            sale_start = r.snapshot_date
            max_pct = max(max_pct, r.discount_percent)
        elif sale_end is not None:
            break  # 윈도 끝에서 할인 없는 날 만남
    if sale_end is None:
        return None

    # 세일 기간/직전 7일/비-세일 baseline(60일 중 세일일자 제외) 일별 동접 평균
    pc_q = f"""
        WITH daily AS (
          SELECT
            DATE(TIMESTAMP(collected_at), "Asia/Seoul") AS d,
            AVG(player_count) AS avg_players
          FROM `{TABLE}.player_counts`
          WHERE app_id = {app_id}
            AND TIMESTAMP(collected_at)
                >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
          GROUP BY d
        )
        SELECT
          CASE
            WHEN d BETWEEN DATE '{sale_start}' AND DATE '{sale_end}' THEN 'during'
            WHEN d BETWEEN DATE_SUB(DATE '{sale_start}', INTERVAL 7 DAY)
                       AND DATE_SUB(DATE '{sale_start}', INTERVAL 1 DAY) THEN 'before'
            ELSE 'baseline'
          END AS phase,
          d, avg_players
        FROM daily
        ORDER BY d
    """
    daily_rows = list(client.query(pc_q).result())
    by_phase = {"during": [], "before": [], "baseline": []}
    for r in daily_rows:
        # baseline = 비-세일 일자 (during, before 모두 제외)
        if r.phase in by_phase:
            by_phase[r.phase].append(float(r.avg_players))

    if not by_phase["during"] or not by_phase["before"]:
        return None

    b = mean(by_phase["before"])
    d = mean(by_phase["during"])

    # 통계적 유의성 — 비-세일 baseline의 일별 평균 분포 기준
    z_score = None
    bl = by_phase["baseline"]
    if len(bl) >= MIN_BASELINE_DAYS and stdev(bl) > 0:
        mu, sd = mean(bl), stdev(bl)
        z_score = round((d - mu) / sd, 2)
    significance = _categorize_z(z_score)

    return {
        "할인율": int(max_pct),
        "세일_시작": sale_start.isoformat(),
        "세일_종료": sale_end.isoformat(),
        "직전_avg": int(b),
        "세일중_avg": int(d),
        "변화율_pct": round((d - b) / b * 100, 1) if b > 0 else None,
        "baseline_avg": int(mean(bl)) if bl else None,
        "baseline_std": int(stdev(bl)) if len(bl) >= MIN_BASELINE_DAYS and stdev(bl) > 0 else None,
        "z_score": z_score,
        "유의성": significance,
    }


# ── 항목 8: 부정 키워드 급증 ──────────────────────────────

def get_keyword_surge(client, app_id: int) -> dict | None:
    """최근 1주(가장 최근 collected_date 기준) vs 직전 4주 평균 — 부정 키워드 surge 감지."""
    q = f"""
        WITH kw AS (
          SELECT collected_date, keyword, count
          FROM `{TABLE}.keyword_analysis`
          WHERE app_id = {app_id}
            AND polarity = 'negative'
            AND collected_date >= DATE_SUB(CURRENT_DATE("Asia/Seoul"), INTERVAL 40 DAY)
        ),
        max_d AS (SELECT MAX(collected_date) AS d FROM kw)
        SELECT
          keyword,
          SUM(CASE WHEN collected_date >= DATE_SUB((SELECT d FROM max_d), INTERVAL 6 DAY)
                   THEN count ELSE 0 END) AS recent_cnt,
          SUM(CASE WHEN collected_date < DATE_SUB((SELECT d FROM max_d), INTERVAL 6 DAY)
                    AND collected_date >= DATE_SUB((SELECT d FROM max_d), INTERVAL 34 DAY)
                   THEN count ELSE 0 END) AS prior_cnt,
          COUNT(DISTINCT CASE WHEN collected_date < DATE_SUB((SELECT d FROM max_d), INTERVAL 6 DAY)
                              AND collected_date >= DATE_SUB((SELECT d FROM max_d), INTERVAL 34 DAY)
                              THEN collected_date END) AS prior_days
        FROM kw
        GROUP BY keyword
    """
    rows = list(client.query(q).result())
    if not rows:
        return None

    surged, new = [], []
    for r in rows:
        recent = r.recent_cnt or 0
        prior = r.prior_cnt or 0
        prior_days = r.prior_days or 0
        if recent < 3:
            continue  # 최소 빈도
        if prior == 0 and prior_days >= 7:
            new.append({"keyword": r.keyword, "recent_cnt": recent})
        elif prior_days >= 7:
            # 직전기간 일평균 → 1주 환산
            prior_per_week = prior * 7 / prior_days
            if prior_per_week > 0 and recent >= prior_per_week * SURGE_RATIO:
                surged.append({
                    "keyword": r.keyword,
                    "recent_cnt": recent,
                    "prior_per_week": round(prior_per_week, 1),
                    "ratio": round(recent / prior_per_week, 1),
                })
    if not surged and not new:
        return None

    surged.sort(key=lambda x: -x["ratio"])
    new.sort(key=lambda x: -x["recent_cnt"])
    return {"급증": surged[:5], "신규": new[:5]}


# ── 항목 9: 동접 z-score 이상치 ───────────────────────────

def get_player_anomaly(client, app_id: int) -> dict | None:
    """최근 30일 일별 동접 평균 → 처음 21일 baseline → 최근 7일 z-score 이상치 탐지."""
    q = f"""
        SELECT DATE(TIMESTAMP(collected_at), "Asia/Seoul") AS d,
               ROUND(AVG(player_count), 0) AS avg_players
        FROM `{TABLE}.player_counts`
        WHERE app_id = {app_id}
          AND TIMESTAMP(collected_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        GROUP BY d ORDER BY d
    """
    rows = list(client.query(q).result())
    if len(rows) < 14:
        return None
    baseline = [float(r.avg_players) for r in rows[:-7]]
    recent = rows[-7:]
    if stdev(baseline) == 0:
        return None
    mu, sd = mean(baseline), stdev(baseline)

    anomalies = []
    for r in recent:
        z = (float(r.avg_players) - mu) / sd
        if abs(z) >= ANOMALY_Z:
            anomalies.append({
                "date": r.d.isoformat(),
                "avg_players": int(r.avg_players),
                "z_score": round(z, 2),
                "기준_avg": int(mu),
            })
    if not anomalies:
        return None

    # cross-ref: 이상치 일자 ±2일에 news 또는 price_history 변화 있나
    from datetime import date as _date, timedelta
    anomaly_dates = [_date.fromisoformat(a["date"]) for a in anomalies]
    win_start = (min(anomaly_dates) - timedelta(days=2)).isoformat()
    win_end = (max(anomaly_dates) + timedelta(days=2)).isoformat()

    cr_news_q = f"""
        SELECT DATE(TIMESTAMP_SECONDS(date), "Asia/Seoul") AS news_d, title
        FROM `{TABLE}.news`
        WHERE app_id = {app_id}
          AND DATE(TIMESTAMP_SECONDS(date), "Asia/Seoul")
              BETWEEN DATE '{win_start}' AND DATE '{win_end}'
    """
    news_hits = list(client.query(cr_news_q).result())

    cr_price_q = f"""
        SELECT snapshot_date, discount_percent
        FROM `{TABLE}.price_history`
        WHERE app_id = {app_id} AND discount_percent > 0
          AND snapshot_date BETWEEN DATE '{win_start}' AND DATE '{win_end}'
    """
    price_hits = list(client.query(cr_price_q).result())

    for a in anomalies:
        d = _date.fromisoformat(a["date"])
        a["근처_뉴스"] = [
            {"date": n.news_d.isoformat(), "title": n.title}
            for n in news_hits if abs((n.news_d - d).days) <= 2
        ][:2]
        a["근처_세일"] = [
            {"date": p.snapshot_date.isoformat(), "할인율": p.discount_percent}
            for p in price_hits if abs((p.snapshot_date - d).days) <= 2
        ][:2]

    return {"이상치": anomalies, "기준_avg": int(mu), "기준_std": int(sd)}


# ── 통합 ──────────────────────────────────────────────────

def get_event_signals(client, app_id: int) -> dict:
    return {
        "패치_임팩트": get_patch_impact(client, app_id),
        "세일_효과": get_sale_impact(client, app_id),
        "부정_키워드_급증": get_keyword_surge(client, app_id),
        "동접_이상치": get_player_anomaly(client, app_id),
    }
