-- ================================================
-- Steam Service BigQuery Schema
-- Dataset: steam_data
-- 모든 collected_at, snapshot_timestamp는 STRING (KST)
-- ================================================

-- 1. 게임 기본 정보
CREATE TABLE IF NOT EXISTS `steam_data.games` (
    app_id          INT64 NOT NULL,
    name            STRING,
    type            STRING,
    required_age    INT64,
    is_free         BOOL,
    short_description STRING,
    developers      STRING,
    publishers      STRING,
    currency        STRING,
    genres          STRING,
    categories      STRING,
    release_date    STRING,
    metacritic_score INT64,
    recommendations INT64,
    header_image    STRING,
    collected_at    STRING
);

-- 2. 유저 리뷰
CREATE TABLE IF NOT EXISTS `steam_data.reviews` (
    app_id              INT64 NOT NULL,
    recommendation_id   STRING,
    steam_id            STRING,
    playtime_forever    INT64,
    playtime_at_review  INT64,
    voted_up            BOOL,
    language            STRING,
    review_text         STRING,
    timestamp_created   INT64,
    timestamp_updated   INT64,
    votes_up            INT64,
    votes_funny         INT64,
    weighted_vote_score FLOAT64,
    review_text_ko      STRING,
    collected_at        STRING
);

-- 3. 가격 히스토리 (일별 스냅샷 + ITAD 과거 이력)
CREATE TABLE IF NOT EXISTS `steam_data.price_history` (
    app_id           INT64 NOT NULL,
    snapshot_date    DATE NOT NULL,
    snapshot_timestamp STRING,
    currency         STRING,
    initial_price    INT64,
    final_price      INT64,
    discount_percent INT64
);

-- 4. 리뷰 요약 통계 (일별 누적 — 같은 날 (app_id) 중복은 적재 시점에 dedup)
CREATE TABLE IF NOT EXISTS `steam_data.review_summary` (
    app_id              INT64 NOT NULL,
    total_reviews       INT64,
    total_positive      INT64,
    total_negative      INT64,
    review_score        INT64,
    review_score_desc   STRING,
    positive_ratio      FLOAT64,
    collected_at        TIMESTAMP
);

-- 5. 뉴스/패치노트
CREATE TABLE IF NOT EXISTS `steam_data.news` (
    app_id      INT64 NOT NULL,
    gid         STRING,
    title       STRING,
    author      STRING,
    contents    STRING,
    feed_label  STRING,
    date        INT64,
    url         STRING,
    collected_at STRING
);

-- 6. 동시접속자수 (3시간마다 수집)
CREATE TABLE IF NOT EXISTS `steam_data.player_counts` (
    app_id       INT64 NOT NULL,
    player_count INT64,
    collected_at STRING
);
