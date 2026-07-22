# Power BI Dashboard Build Guide
## EHR Patient Flow Analytics — Operational Control Tower

This guide walks through connecting Power BI Desktop to the warehouse,
building the data model, writing DAX measures, and recreating all five
report pages.

---

## 1. Connect to the data source

### Local (DuckDB — while testing)
Power BI doesn't have a native DuckDB connector. Export the five
analytical tables to CSV first:

```python
# run this after dbt models complete
python export_for_powerbi.py
```

This writes five CSVs to `data/powerbi/`. In Power BI Desktop:
`Home → Get Data → Text/CSV` → import each file.

### Snowflake (production)
`Home → Get Data → Snowflake`

| Field | Value |
|---|---|
| Server | `xy12345.us-east-1.snowflakecomputing.com` |
| Warehouse | `EHR_WH` |
| Database | `EHR_DB` |
| Schema | `ANALYTICS` |

Import mode (not DirectQuery) for datasets under 1M rows.

Tables to import:
- `FACT_PATIENT_ENCOUNTERS`
- `DIM_PATIENTS`
- `DIM_DIAGNOSES`
- `DIM_CLINICAL_OBSERVATIONS`
- `DIM_LOINC_REFERENCE`
- `ANALYTICS_READMISSIONS`
- `ANALYTICS_THROUGHPUT`
- `ANALYTICS_LAB_KPIS`
- `MART_LOS_KPIS`

---

## 2. Data model (relationships)

In `Model view`, create these relationships (all single-directional,
many-to-one):

| From table | From column | To table | To column |
|---|---|---|---|
| FACT_PATIENT_ENCOUNTERS | patient_id | DIM_PATIENTS | patient_id |
| DIM_DIAGNOSES | encounter_id | FACT_PATIENT_ENCOUNTERS | encounter_id |
| DIM_CLINICAL_OBSERVATIONS | encounter_id | FACT_PATIENT_ENCOUNTERS | encounter_id |
| ANALYTICS_READMISSIONS | encounter_id | FACT_PATIENT_ENCOUNTERS | encounter_id |
| ANALYTICS_THROUGHPUT | encounter_id | FACT_PATIENT_ENCOUNTERS | encounter_id |

---

## 3. DAX measures

Create a dedicated `_Measures` table (`Modeling → New Table → _Measures = {}`),
then add each measure below.

### Core KPIs

```dax
Total Encounters =
COUNTROWS(FACT_PATIENT_ENCOUNTERS)

Total Patients =
DISTINCTCOUNT(FACT_PATIENT_ENCOUNTERS[patient_id])

Avg LOS Hours =
AVERAGE(FACT_PATIENT_ENCOUNTERS[los_hours])

Avg LOS Days =
AVERAGE(FACT_PATIENT_ENCOUNTERS[los_days])

Median LOS Hours =
MEDIANX(FACT_PATIENT_ENCOUNTERS, FACT_PATIENT_ENCOUNTERS[los_hours])
```

### Length of Stay by class

```dax
Avg LOS Inpatient =
CALCULATE(
    AVERAGE(FACT_PATIENT_ENCOUNTERS[los_hours]),
    FACT_PATIENT_ENCOUNTERS[encounter_class] = "IMP"
)

Avg LOS Emergency =
CALCULATE(
    AVERAGE(FACT_PATIENT_ENCOUNTERS[los_hours]),
    FACT_PATIENT_ENCOUNTERS[encounter_class] = "EMER"
)

Avg LOS Home Health =
CALCULATE(
    AVERAGE(FACT_PATIENT_ENCOUNTERS[los_hours]),
    FACT_PATIENT_ENCOUNTERS[encounter_class] = "HH"
)

LOS P90 =
PERCENTILEX.INC(FACT_PATIENT_ENCOUNTERS, FACT_PATIENT_ENCOUNTERS[los_hours], 0.9)

Long Stay Outliers =
CALCULATE(
    COUNTROWS(FACT_PATIENT_ENCOUNTERS),
    FACT_PATIENT_ENCOUNTERS[los_hours] >= [LOS P90]
)
```

### 30-day readmissions

```dax
Readmission Count 30D =
CALCULATE(
    COUNTROWS(ANALYTICS_READMISSIONS),
    ANALYTICS_READMISSIONS[is_30d_readmission] = TRUE()
)

Readmission Rate 30D % =
DIVIDE(
    [Readmission Count 30D],
    COUNTROWS(ANALYTICS_READMISSIONS),
    0
) * 100

Readmission Count 7D =
CALCULATE(
    COUNTROWS(ANALYTICS_READMISSIONS),
    ANALYTICS_READMISSIONS[readmission_type] = "7-day readmission"
)

Avg Days to Readmission =
CALCULATE(
    AVERAGE(ANALYTICS_READMISSIONS[days_since_prev_discharge]),
    ANALYTICS_READMISSIONS[is_30d_readmission] = TRUE()
)
```

### Throughput

```dax
Admissions This Hour =
CALCULATE(
    COUNTROWS(FACT_PATIENT_ENCOUNTERS),
    FACT_PATIENT_ENCOUNTERS[admit_hour] = SELECTEDVALUE(FACT_PATIENT_ENCOUNTERS[admit_hour])
)

Peak Hour Flag =
IF(
    [Admissions This Hour] >=
        PERCENTILEX.INC(
            ALL(FACT_PATIENT_ENCOUNTERS[admit_hour]),
            CALCULATE(COUNTROWS(FACT_PATIENT_ENCOUNTERS)),
            0.75
        ),
    "Peak", "Normal"
)

Weekend Admissions % =
DIVIDE(
    CALCULATE(
        COUNTROWS(FACT_PATIENT_ENCOUNTERS),
        FACT_PATIENT_ENCOUNTERS[is_weekend_admission] = TRUE()
    ),
    COUNTROWS(FACT_PATIENT_ENCOUNTERS),
    0
) * 100
```

### Lab KPIs

```dax
Total LOINC Observations =
COUNTROWS(DIM_CLINICAL_OBSERVATIONS)

Abnormal Result Rate % =
DIVIDE(
    CALCULATE(
        COUNTROWS(DIM_CLINICAL_OBSERVATIONS),
        DIM_CLINICAL_OBSERVATIONS[is_abnormal] = TRUE()
    ),
    COUNTROWS(DIM_CLINICAL_OBSERVATIONS),
    0
) * 100

Avg Pain Score =
CALCULATE(
    AVERAGE(DIM_CLINICAL_OBSERVATIONS[result_value]),
    DIM_CLINICAL_OBSERVATIONS[loinc_code] = "72514-3"
)
```

---

## 4. Report pages

### Page 1 — Length of Stay

**Visuals:**

| Visual | Type | Fields |
|---|---|---|
| Avg LOS (Inpatient) | Card | `[Avg LOS Inpatient]` |
| Avg LOS (Emergency) | Card | `[Avg LOS Emergency]` |
| LOS P90 | Card | `[LOS P90]` |
| Long-stay outliers | Card | `[Long Stay Outliers]` |
| Avg LOS by class | Horizontal bar | Axis: `encounter_class_label`, Value: `avg_los_hours` (from `MART_LOS_KPIS`) |
| LOS tier distribution | Donut | Legend: LOS tier columns from `MART_LOS_KPIS`, Values: count |
| LOS weekday vs weekend | Clustered bar | Axis: `encounter_class_label`, Values: `avg_los_weekday`, `avg_los_weekend` |

**Filters:** Encounter class slicer (AMB / IMP / EMER / HH / VR)

---

### Page 2 — Readmissions

**Visuals:**

| Visual | Type | Fields |
|---|---|---|
| 30-day readmit rate | Card | `[Readmission Rate 30D %]` |
| 7-day readmit count | Card | `[Readmission Count 7D]` |
| Avg days to readmit | Card | `[Avg Days to Readmission]` |
| Readmission rate by year | Line chart | X: `admit_year`, Y: `[Readmission Rate 30D %]`, Legend: `encounter_class_label` |
| Readmission type breakdown | Stacked bar | Axis: `encounter_class_label`, Legend: `readmission_type`, Value: count |
| Patient encounter sequence | Table | `patient_id`, `admit_timestamp`, `encounter_class`, `encounter_seq_num`, `is_30d_readmission` |

**Filters:** Year slicer (2010–2023), encounter class slicer

---

### Page 3 — Throughput & Flow

**Visuals:**

| Visual | Type | Fields |
|---|---|---|
| Peak hour | Card | Top `admit_hour` by count |
| Night shift % | Card | Custom DAX for hours 23–06 |
| Monthly volume | Line chart | X: `month_start`, Y: encounter count |
| Hourly admissions | Column chart | X: `admit_hour`, Y: count — conditional formatting: red > 600, amber > 400 |
| Patient episode flow | Gantt (custom visual) | Use "Gantt Chart" from AppSource. Task: `encounter_class_label`, Start: `admit_timestamp`, End: `discharge_timestamp`, Legend: `episode_type` |

**AppSource visual for Gantt:**
`Insert → More visuals → AppSource` → search "Gantt Chart by MAQ Software"

---

### Page 4 — Lab KPIs

**Visuals:**

| Visual | Type | Fields |
|---|---|---|
| Total observations | Card | `[Total LOINC Observations]` |
| Abnormal rate | Card | `[Abnormal Result Rate %]` |
| Avg pain score | Card | `[Avg Pain Score]` |
| Top LOINC codes by volume | Horizontal bar | Axis: `observation_name`, Value: `total_observations` (from `ANALYTICS_LAB_KPIS`) |
| Mean vs P97.5 | Clustered bar | Axis: `observation_name`, Values: `mean_value`, `p97_5` |
| Lab stats table | Table | `observation_name`, `loinc_code`, `total_observations`, `mean_value`, `p2_5`, `p97_5`, `abnormal_rate_pct` |

---

### Page 5 — Providers

**Visuals:**

| Visual | Type | Fields |
|---|---|---|
| Top provider by volume | Card | Top `service_provider` by count |
| Provider comparison | Scatter chart | X: encounter count, Y: avg LOS, Size: unique patient count, Legend: `service_provider` |
| Provider table | Table | `service_provider`, encounter count, unique patients, avg LOS |

---

## 5. Formatting

- **Theme:** `View → Themes → Executive` or import a custom JSON theme
- **Colors:**
  - Inpatient: `#2a78d6`
  - Emergency: `#d29922`
  - Home Health: `#0ea5e9`
  - Ambulatory: `#1baf7a`
  - Virtual: `#888780`
  - Readmission alert: `#e34948`
- **Canvas size:** 1280 × 720 (16:9) for all pages
- **Page navigation:** Add buttons with page navigation action for the tab row

---

## 6. Publishing

`File → Publish → Publish to Power BI` → select your workspace.

For scheduled refresh from Snowflake:
- Install the On-premises data gateway (if Snowflake is behind a VPN)
- In Power BI Service: `Dataset → Settings → Scheduled refresh → Add Snowflake credentials`

---

## 7. Switching from local CSV to Snowflake

`Home → Transform data → Data source settings → Change source`

Update each table source from CSV file path to the Snowflake table name.
All measures and relationships persist — only the source connection changes.
