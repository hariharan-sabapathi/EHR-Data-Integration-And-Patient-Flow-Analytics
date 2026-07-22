-- Population-level statistics for each LOINC code observed in the cohort.
-- Normal ranges (normal_low / normal_high) come from Dim_LOINC_Reference,
-- which uses P2.5 / P97.5 of the cohort distribution as a proxy.

{{ config(materialized='table') }}

WITH obs AS (
    SELECT
        o.observation_id,
        o.patient_id,
        o.encounter_id,
        o.loinc_code,
        o.observation_name,
        o.obs_category,
        o.effective_datetime,
        o.result_value,
        o.result_unit,
        o.result_text,
        o.is_abnormal,
        lr.normal_low,
        lr.normal_high
    FROM Dim_Clinical_Observations o
    LEFT JOIN Dim_LOINC_Reference lr ON o.loinc_code = lr.loinc_code
    WHERE o.result_value IS NOT NULL
)

SELECT
    loinc_code,
    observation_name,
    obs_category,
    result_unit,
    COUNT(*)                                                              AS total_observations,
    COUNT(DISTINCT patient_id)                                            AS patient_count,
    ROUND(AVG(result_value),    3)                                        AS mean_value,
    ROUND(MEDIAN(result_value), 3)                                        AS median_value,
    ROUND(STDDEV(result_value), 3)                                        AS stddev_value,
    ROUND(MIN(result_value),    3)                                        AS min_value,
    ROUND(MAX(result_value),    3)                                        AS max_value,
    ROUND(PERCENTILE_CONT(0.025) WITHIN GROUP (ORDER BY result_value), 3) AS p2_5,
    ROUND(PERCENTILE_CONT(0.975) WITHIN GROUP (ORDER BY result_value), 3) AS p97_5,
    SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END)                          AS abnormal_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_abnormal THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                                                     AS abnormal_rate_pct
FROM obs
GROUP BY loinc_code, observation_name, obs_category, result_unit
ORDER BY total_observations DESC
