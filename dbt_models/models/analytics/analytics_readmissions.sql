-- Flags each encounter as a 30-day readmission using LAG/LEAD window functions
-- over each patient's ordered encounter history. Excludes Home Health and Virtual
-- encounters, which don't represent acute admissions.
--
-- is_30d_readmission: this encounter started within 30 days of the prior discharge.
-- index_discharge_with_readmission: the patient returned within 30 days after this discharge.
-- readmission_type: 7-day / 30-day / 90-day bucket, or 'Not a readmission'.

{{ config(materialized='table') }}

WITH clinical_encounters AS (
    SELECT
        encounter_id,
        patient_id,
        encounter_class,
        encounter_class_label,
        admit_timestamp,
        discharge_timestamp,
        los_hours,
        los_days,
        service_provider,
        location_display,
        reason_for_visit,
        is_inpatient,
        is_emergency,
        admit_year,
        admit_month
    FROM {{ ref('stg_encounters') }}
    WHERE encounter_class IN ('IMP', 'EMER', 'AMB')
),

sequenced AS (
    SELECT
        *,
        LAG(encounter_id)        OVER w AS prev_encounter_id,
        LAG(encounter_class)     OVER w AS prev_encounter_class,
        LAG(admit_timestamp)     OVER w AS prev_admit_timestamp,
        LAG(discharge_timestamp) OVER w AS prev_discharge_timestamp,
        LAG(service_provider)    OVER w AS prev_service_provider,

        LEAD(encounter_id)       OVER w AS next_encounter_id,
        LEAD(encounter_class)    OVER w AS next_encounter_class,
        LEAD(admit_timestamp)    OVER w AS next_admit_timestamp,
        LEAD(service_provider)   OVER w AS next_service_provider,

        ROW_NUMBER() OVER w                        AS encounter_seq_num,
        COUNT(*)     OVER (PARTITION BY patient_id) AS total_patient_encounters
    FROM clinical_encounters
    WINDOW w AS (
        PARTITION BY patient_id
        ORDER BY admit_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)

SELECT
    *,
    ROUND(
        (EPOCH(admit_timestamp) - EPOCH(prev_discharge_timestamp)) / 86400.0, 2
    ) AS days_since_prev_discharge,

    ROUND(
        (EPOCH(next_admit_timestamp) - EPOCH(discharge_timestamp)) / 86400.0, 2
    ) AS days_until_next_admit,

    CASE
        WHEN prev_discharge_timestamp IS NOT NULL
         AND EPOCH(admit_timestamp) - EPOCH(prev_discharge_timestamp) BETWEEN 0 AND 30 * 86400
        THEN TRUE ELSE FALSE
    END AS is_30d_readmission,

    CASE
        WHEN next_admit_timestamp IS NOT NULL
         AND EPOCH(next_admit_timestamp) - EPOCH(discharge_timestamp) BETWEEN 0 AND 30 * 86400
        THEN TRUE ELSE FALSE
    END AS index_discharge_with_readmission,

    CASE
        WHEN prev_discharge_timestamp IS NULL
         THEN 'Initial admission'
        WHEN EPOCH(admit_timestamp) - EPOCH(prev_discharge_timestamp) <=  7 * 86400
         THEN '7-day readmission'
        WHEN EPOCH(admit_timestamp) - EPOCH(prev_discharge_timestamp) <= 30 * 86400
         THEN '30-day readmission'
        WHEN EPOCH(admit_timestamp) - EPOCH(prev_discharge_timestamp) <= 90 * 86400
         THEN '90-day readmission'
        ELSE 'Not a readmission'
    END AS readmission_type
FROM sequenced
