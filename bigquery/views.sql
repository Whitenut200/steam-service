-- ================================================
-- Steam Service - Looker용 VIEW
-- 원본 테이블의 INT64/STRING 날짜 컬럼을 TIMESTAMP/DATE로 변환
-- 모든 뷰에 game_name 포함 (games 테이블과 LEFT JOIN USING)
-- 사용: BigQuery 콘솔에 붙여넣고 실행 (또는 bq query)
-- ================================================

-- 1. games_v
CREATE OR REPLACE VIEW `steam_data.games_v` AS
SELECT
  * EXCEPT (release_date, collected_at),
  COALESCE(
    SAFE.PARSE_DATE('%Y-%m-%d', release_date),
    SAFE.PARSE_DATE('%d %b, %Y', release_date),
    SAFE.PARSE_DATE('%b %d, %Y', release_date)
  ) AS release_date,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', collected_at) AS collected_at
FROM `steam_data.games`;


-- 2. reviews_v
CREATE OR REPLACE VIEW `steam_data.reviews_v` AS
SELECT
  r.* EXCEPT (timestamp_created, timestamp_updated, collected_at),
  g.name AS game_name,
  TIMESTAMP_SECONDS(r.timestamp_created) AS created_at,
  TIMESTAMP_SECONDS(r.timestamp_updated) AS updated_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', r.collected_at) AS collected_at
FROM `steam_data.reviews` r
LEFT JOIN `steam_data.games` g USING (app_id);


-- 3. price_history_v
CREATE OR REPLACE VIEW `steam_data.price_history_v` AS
SELECT
  p.* EXCEPT (snapshot_timestamp),
  g.name AS game_name,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', p.snapshot_timestamp) AS snapshot_timestamp
FROM `steam_data.price_history` p
LEFT JOIN `steam_data.games` g USING (app_id);


-- 4. review_summary_v
CREATE OR REPLACE VIEW `steam_data.review_summary_v` AS
SELECT
  s.*,
  g.name AS game_name
FROM `steam_data.review_summary` s
LEFT JOIN `steam_data.games` g USING (app_id);


-- 5. news_v
CREATE OR REPLACE VIEW `steam_data.news_v` AS
SELECT
  n.* EXCEPT (date, collected_at),
  g.name AS game_name,
  TIMESTAMP_SECONDS(n.date) AS published_at,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', n.collected_at) AS collected_at
FROM `steam_data.news` n
LEFT JOIN `steam_data.games` g USING (app_id);


-- 6. player_counts_v
CREATE OR REPLACE VIEW `steam_data.player_counts_v` AS
SELECT
  p.* EXCEPT (collected_at),
  g.name AS game_name,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', p.collected_at) AS collected_at
FROM `steam_data.player_counts` p
LEFT JOIN `steam_data.games` g USING (app_id);
