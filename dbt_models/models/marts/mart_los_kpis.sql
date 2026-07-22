-- LOS statistics per encounter class: percentiles, outlier count, tier distribution,
-- and weekday vs weekend split.

{{ config(materialized='table') }}

WITH encounters AS (
    SELECT * FROM {{ ref('stg_encounters') }}
),

with_percentile AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY encounter_class
            ORDER BY los_hours
        ) AS los_percentile_within_class
    FROM encounters
)

SELECT
    encounter_class,
    encounter_class_label,
    COUNT(*)                                                                    AS encounter_count,
    ROUND(AVG(los_hours),    2)                                                 AS avg_los_hours,
    ROUND(MEDIAN(los_hours), 2)                                                 AS median_los_hours,
    ROUND(MIN(los_hours),    2)                                                 AS min_los_hours,
    ROUND(MAX(los_hours),    2)                                                 AS max_los_hours,
    ROUND(STDDEV(los_hours), 2)                                                 AS stddev_los_hours,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY los_hours), 2)          AS p25_los_hours,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY los_hours), 2)          AS p75_los_hours,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY los_hours), 2)          AS p90_los_hours,
    COUNT(CASE WHEN los_percentile_within_class > 0.90 THEN 1 END)             AS long_stay_outlier_count,
    ROUND(AVG(CASE WHEN     is_weekend_admission THEN los_hours END), 2)        AS avg_los_weekend,
    ROUND(AVG(CASE WHEN NOT is_weekend_admission THEN los_hours END), 2)        AS avg_los_weekday,
    COUNT(CASE WHEN los_tier = '< 4h'      THEN 1 END)                         AS tier_lt_4h,
    COUNT(CASE WHEN los_tier = '4-24h'     THEN 1 END)                         AS tier_4_24h,
    COUNT(CASE WHEN los_tier = '1-3 days'  THEN 1 END)                         AS tier_1_3d,
    COUNT(CASE WHEN los_tier = '3-7 days'  THEN 1 END)                         AS tier_3_7d,
    COUNT(CASE WHEN los_tier = '1-4 weeks' THEN 1 END)                         AS tier_1_4w,
    COUNT(CASE WHEN los_tier = '> 1 month' THEN 1 END)                         AS tier_gt_1m
FROM with_percentile
GROUP BY encounter_class, encounter_class_label
