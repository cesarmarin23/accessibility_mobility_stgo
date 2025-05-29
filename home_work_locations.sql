
-- Step-by-step:
-- 1. Joins cell connection data with cell tower information to associate each connection with a bts.
-- 2. Calculates weighted counts of connections during "home" hours (midnight-6am and 11pm) and "work" hours (9am-5pm, weekdays).
-- 3. Aggregates weights monthly per user and selects the site (bts) with the highest weight as the likely home/work location.
-- 4. Outputs each user's work_site_id and home_site_id for each month, alongside weights for these locations.


create or replace table `xdr.sanhel_home_work` as
WITH home_location AS (
  WITH home_raw AS (
    SELECT 
      x.user_id,
      x.cell_id,
      y.site_id,
      EXTRACT(MONTH FROM x.timestamp) AS month,
      EXTRACT(HOUR FROM x.timestamp) AS hour
    FROM `xdr.sanhel_metroregion` AS x
    JOIN `xdr.cells2023` AS y
    ON x.cell_id = y.cell_id
  ),
  home_weighted AS (
    SELECT
      user_id,
      site_id,
      month,
      CASE 
        WHEN hour BETWEEN 0 AND 1 THEN 2.0
        WHEN hour BETWEEN 2 AND 3 THEN 3.0
        WHEN hour BETWEEN 4 AND 5 THEN 2.0
        WHEN hour = 6 THEN 1.0
        WHEN hour = 23 THEN 1.0
        ELSE 0.0
      END AS weight
    FROM home_raw
  ),
  home_aggregated AS (
    SELECT
      user_id,
      site_id,
      month,
      SUM(weight) AS total_weight
    FROM home_weighted
    GROUP BY user_id, site_id, month
  )
  SELECT
    user_id,
    month,
    site_id AS home_site_id,
    total_weight AS home_weight,
    ROW_NUMBER() OVER (PARTITION BY user_id, month ORDER BY total_weight DESC) AS rank
  FROM home_aggregated
),
home_selected AS (
  SELECT
    user_id,
    month,
    home_site_id,
    home_weight
  FROM home_location
  WHERE rank = 1
),

work_location AS (
  WITH work_raw AS (
    SELECT 
      x.user_id,
      x.cell_id,
      y.site_id,
      EXTRACT(MONTH FROM x.timestamp) AS month,
      EXTRACT(HOUR FROM x.timestamp) AS hour,
      FORMAT_TIMESTAMP('%A', x.timestamp) AS weekday
    FROM `xdr.sanhel_metroregion` AS x
    JOIN `xdr.cells2023` AS y
    ON x.cell_id = y.cell_id
  ),
  work_weighted AS (
    SELECT
      user_id,
      site_id,
      month,
      CASE 
        WHEN hour BETWEEN 9 AND 11 THEN 2.0
        WHEN hour BETWEEN 12 AND 13 THEN 1.0
        WHEN hour BETWEEN 14 AND 17 THEN 2.0
        ELSE 0.0
      END AS weight
    FROM work_raw
    WHERE weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
  ),
  work_aggregated AS (
    SELECT
      user_id,
      site_id,
      month,
      SUM(weight) AS total_weight
    FROM work_weighted
    GROUP BY user_id, site_id, month
  )
  SELECT
    user_id,
    month,
    site_id AS work_site_id,
    total_weight AS work_weight,
    ROW_NUMBER() OVER (PARTITION BY user_id, month ORDER BY total_weight DESC) AS rank
  FROM work_aggregated
),
work_selected AS (
  SELECT
    user_id,
    month,
    work_site_id,
    work_weight
  FROM work_location
  WHERE rank = 1
)

SELECT 
  h.user_id,
  h.month,
  h.home_site_id,
  h.home_weight,
  w.work_site_id,
  w.work_weight
FROM home_selected h
LEFT JOIN work_selected w
ON h.user_id = w.user_id AND h.month = w.month
ORDER BY h.user_id, h.month;
