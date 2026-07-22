-- Staging view over Fact_Patient_Encounters.
-- Filters out data quality outliers, adds human-readable class labels,
-- shift classification, and LOS tier buckets used by downstream marts.

WITH base AS (
    SELECT
        encounter_id,
        patient_id,
        status,
        encounter_class,
        encounter_type,
        admit_timestamp,
        discharge_timestamp,
        los_hours,
        los_days,
        admit_year,
        admit_month,
        admit_hour,
        admit_dow,
        service_provider,
        location_display,
        reason_for_visit,
        is_inpatient,
        is_emergency,
        episode_of_care_ref
    FROM Fact_Patient_Encounters
    WHERE admit_timestamp IS NOT NULL
      AND discharge_timestamp IS NOT NULL
      AND los_hours >= 0
      AND los_hours < 8760   -- drop obvious data artifacts (> 1 year)
),

classified AS (
    SELECT
        *,
        CASE encounter_class
            WHEN 'IMP'  THEN 'Inpatient'
            WHEN 'EMER' THEN 'Emergency'
            WHEN 'AMB'  THEN 'Ambulatory'
            WHEN 'HH'   THEN 'Home Health'
            WHEN 'VR'   THEN 'Virtual'
            ELSE 'Other'
        END AS encounter_class_label,

        CASE
            WHEN admit_hour BETWEEN 7  AND 14 THEN 'Day shift (07-15)'
            WHEN admit_hour BETWEEN 15 AND 22 THEN 'Evening shift (15-23)'
            ELSE 'Night shift (23-07)'
        END AS admit_shift,

        admit_dow IN ('Saturday', 'Sunday') AS is_weekend_admission,

        CASE
            WHEN los_hours < 4   THEN '< 4h'
            WHEN los_hours < 24  THEN '4-24h'
            WHEN los_hours < 72  THEN '1-3 days'
            WHEN los_hours < 168 THEN '3-7 days'
            WHEN los_hours < 720 THEN '1-4 weeks'
            ELSE '> 1 month'
        END AS los_tier
    FROM base
)

SELECT * FROM classified
