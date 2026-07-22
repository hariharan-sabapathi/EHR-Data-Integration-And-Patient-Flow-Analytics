-- Links EpisodeOfCare records to their constituent encounters, producing one row
-- per encounter with episode context and sequential position within the episode.
-- Used for patient flow / Gantt visualisations.
--
-- hours_since_prev_encounter: gap between the end of the previous encounter and
-- the start of this one, within the same episode. NULL for the first encounter.

{{ config(materialized='table') }}

WITH encounters AS (
    SELECT * FROM {{ ref('stg_encounters') }}
)

SELECT
    ep.episode_id,
    ep.patient_id,
    ep.episode_type,
    ep.episode_start,
    ep.episode_end,
    ep.episode_los_hours,
    e.encounter_id,
    e.encounter_class,
    e.encounter_class_label,
    e.admit_timestamp,
    e.discharge_timestamp,
    e.los_hours,
    e.location_display,
    e.service_provider,
    ROW_NUMBER() OVER (
        PARTITION BY ep.episode_id
        ORDER BY e.admit_timestamp
    )                                                        AS encounter_seq_in_episode,
    COUNT(*) OVER (PARTITION BY ep.episode_id)              AS total_encounters_in_episode,
    ROUND(
        (
            EPOCH(e.admit_timestamp) -
            EPOCH(LAG(e.discharge_timestamp) OVER (
                PARTITION BY ep.episode_id ORDER BY e.admit_timestamp
            ))
        ) / 3600.0, 2
    )                                                        AS hours_since_prev_encounter
FROM Dim_Episodes ep
JOIN encounters e
    ON REPLACE(e.episode_of_care_ref, 'EpisodeOfCare/', '') = ep.episode_id
