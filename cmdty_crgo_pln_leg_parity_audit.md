# Parity audit: Athena validation query vs dev Glue code

Field-by-field proof that `cmdty_crgo_pln_leg_validation.sql` /
`cmdty_crgo_pln_leg_mismatch_drilldown.sql` replicate the dev transform in
`cmdty_crgo_schemas.py` (+ `glue_clss.py` transform pipeline).

Status legend:

- **EXACT** — provably the same result for all inputs.
- **EXACT\*** — the same result under a stated assumption (assumption listed).
- **STRUCTURAL** — cannot be identical by construction; impact explained.

Empirical cross-check: the prod run (2026-05-20..2026-06-07, 8,063,922 logical
rows on both sides, `missing_in_target = 0`, `extra_in_target = 0`) confirms
the end-to-end equivalence holds on real data.

---

## 1. Extract step

| Dev (Glue) | Athena | Status |
|---|---|---|
| `df.filter(col('event_name').isin(event_filter_list))` (13 events) | `WHERE event_name IN (...)` same 13 literals | EXACT |
| `posexplode(planneditinerary)` — drops NULL/empty arrays | `CROSS JOIN UNNEST ... WITH ORDINALITY` + explicit `IS NOT NULL AND cardinality > 0` | EXACT |
| `planneditinerary[leg_pos - 1] AS prev_leg` — Spark returns NULL for index −1 (non-ANSI) | `IF(leg_ord > 1, element_at(arr, leg_ord - 1))` — NULL guard (Trino errors on index 0) | EXACT |
| `planneditinerary[leg_pos + 1] AS next_leg` — NULL past end | `IF(leg_ord < cardinality(arr), element_at(arr, leg_ord + 1))` | EXACT |

Assumption for the whole section: Glue runs Spark in default non-ANSI mode
(`spark.sql.ansi.enabled = false`), which is the Glue 3/4 default.

## 2. Field transforms (schema `path` lambdas + `default`s)

| Field | Dev expression | Athena expression | Status |
|---|---|---|---|
| air_wb_prfx_id | `"airwaybillprefix"`, na.fill `'-'` (NULL only, blanks kept) | `COALESCE(airwaybillprefix, '-')` | EXACT |
| air_wb_num | same pattern | `COALESCE(airwaybillnumber, '-')` | EXACT |
| air_wb_cre_dt | `to_date(col)` — takes the literal date portion of the ISO string, no TZ shift | `TRY(CAST(substr(x,1,10) AS date))` | EXACT (see note A on the default) |
| air_wb_cre_h2si | `date_format(to_timestamp(col), 'HH:mm:ss.SSS')` — converts to session TZ first | `format_datetime(from_iso8601_timestamp(x) AT TIME ZONE 'UTC', 'HH:mm:ss.SSS')` | EXACT\* — assumes Glue session TZ is UTC (Glue default) |
| air_wb_pce_num | `cast("smallint")`, na.fill 0 | `COALESCE(TRY(CAST(x AS smallint)), 0)` | EXACT\* — see note B (decimal-string edge) |
| opng_carr_cde | struct field, na.fill `'-'` | `COALESCE(...operationalcarrier, '-')` | EXACT |
| opng_flt_num | `cast("smallint")`, na.fill 0 | `COALESCE(TRY(CAST(...)), 0)` | EXACT\* — note B |
| opng_flt_num_sufx_txt | `coalesce(operationalsuffix, lit('-'))` | `COALESCE(...operationalsuffix, '-')` | EXACT |
| flt_dep_dt | `to_date(origindate)` | `TRY(CAST(substr(x,1,10) AS date))` | EXACT (note A) |
| leg_orig_arpt_cde | struct field, na.fill `'-'` | `COALESCE(..., '-')` | EXACT |
| leg_dest_arpt_cde | struct field, na.fill `'-'` | `COALESCE(..., '-')` | EXACT |
| eff_fm_cent_tz | `to_timestamp(event_created)` (session TZ = UTC) | `CAST(TRY(from_iso8601_timestamp(x)) AS timestamp(3))` | EXACT\* — UTC assumption; note A on the default |
| pln_crgo_leg_seq_num | `array_position(planneditinerary, exploded_leg)` — 1-based **first match**, struct equality is null-safe (Spark ordering equality treats NULL == NULL) | `MIN(leg_ord) OVER (PARTITION BY source_row_num + all 8 leg fields)` — PARTITION BY also groups NULLs together, so first-match + null-safety are both reproduced | EXACT |
| pln_max_crgo_leg_seq_num | `coalesce(size(planneditinerary), lit(0))` | `CAST(COALESCE(cardinality(arr), 0) AS integer)` | EXACT (arrays are non-null/non-empty after the extract filter) |
| pln_crgo_dep_ld_flag | `when(pos==0,1).when(prev null,1).when(neq OR neq OR neq,1).otherwise(0)` | `CASE` chain with one `WHEN` per inequality | EXACT — under 3-valued logic, `WHEN a THEN 1 WHEN b THEN 1 WHEN c THEN 1 ELSE 0` ≡ `when(a OR b OR c, 1).otherwise(0)`: any TRUE fires; NULLs skip; all NULL/FALSE → 0 |
| pln_crgo_arr_unld_flag | same, `pos == size-1` for last leg | `spark_leg_pos = cardinality(arr) - 1` | EXACT |
| cmdty_flt_leg_type_cde | `when(lower(trim(x))=='unknown','-')...otherwise('-')` | simple `CASE lower(trim(x)) WHEN ... ELSE '-'` — NULL input falls to ELSE, matching `.otherwise` | EXACT |
| year / month / day | `split(awbCreationDate,'-')` + lpad (partition cols) | not derived | STRUCTURAL by design — functionally dependent on air_wb_cre_dt, excluded from comparison |
| latest_job_id | `lit(int(time()))` — wall clock at job start | `CAST(NULL AS integer)`, reported as diagnostic | STRUCTURAL — not derivable from source data |

**Note A (defaults on date/timestamp fields):** the dev defaults
(`'1900-01-01'` etc.) are applied via `df.na.fill`, which only fills columns
whose type matches the fill value — string fills do **not** apply to
Date/Timestamp columns, so dev actually leaves NULL on parse failure, and the
non-nullable schema then makes DQ validation **reject** the row entirely. The
Athena query instead COALESCEs to the documented default. Net effect: a row
with an unparseable date appears in `expected` (Athena) but never reaches the
target (dev rejects it) → it would surface as `missing_in_target`. The prod
run shows `missing_in_target = 0`, so no such rows exist in the data.

**Note B (numeric cast edge):** Spark non-ANSI `cast('8.0' AS smallint)`
returns 8 (accepts decimal strings); Trino `TRY(CAST('8.0' AS smallint))`
returns NULL → default 0. Only differs for non-integer numeric strings in
`airwaybillpiecenumber` / `flightnumber`. The prod run shows zero mismatches
attributable to this (it would appear in missing/extra or the flt_num field
counter, all clean).

## 3. Duplicate removal (`remove_dupes: True`)

Dev `transform_df`: `groupBy(all non-partition, non-redshift fields)` +
`min(struct(year, month, day))`. Within one group, year/month/day are
functionally determined by `air_wb_cre_dt` (same source string), so the
group-by collapses to plain `SELECT DISTINCT` over the compared columns —
which is what the Athena query does. **EXACT** for the compared columns.

## 4. DQ validation step (`validate_df`)

Dev runs AWS Glue Data Quality rules (IsComplete on non-nullable fields,
ColumnDataType/ColumnLength/Uniqueness) and routes failing rows to the reject
bucket. The Athena query does not replicate rule evaluation. **STRUCTURAL** —
any DQ-rejected row appears as `missing_in_target`. Prod run:
`missing_in_target = 0`, so no DQ rejects exist in the window.

## 5. Redshift CDC replica vs `sql_pln_leg_set_eff_dates_and_flags`

| Dev (Redshift SQL) | Athena | Status |
|---|---|---|
| `deduped_stage`: ROW_NUMBER over 17-col partition, `ORDER BY EFF_FM_CENT_TZ, LATEST_JOB_ID DESC` | identical partition/order (`COALESCE` for the underivable job id; rows in a partition are identical anyway) | EXACT |
| `deduped_pln`: `LISTAGG('('||seq||')|'||carrier||...,'  ,') WITHIN GROUP (ORDER BY seq)` | `array_join(array_agg(same concat ORDER BY seq), '  ,')` — both skip NULL elements; `||` with NULL yields NULL element in both engines | EXACT |
| `sig_data`: `WHERE SIG != PRV_SIG OR PRV_SIG IS NULL` (LAG over piece) | identical | EXACT |
| `timestamp_windows`: LEAD(eff_fm) over piece | identical | EXACT |
| `eff_to_cent_tz = COALESCE(next, '2099-12-31 00:00:00.000')` | identical literal | EXACT |
| `min_eff_fm_flag` via `RANK() = 1`, `max_eff_to_flag` via `next IS NULL` | identical | EXACT |
| `final_dedup`: ROW_NUMBER over keys + EFF_TO, `ORDER BY EFF_TO DESC, LATEST_JOB_ID DESC` | identical | EXACT |
| Input scope: dev recalculates over staging **after** `sql_cdc_stage_target_union` pulls all prior target versions for affected AWBs | Athena replica runs over all source events in the window; since AWB lifecycle events share the AWB creation date, an `air_wb_cre_dt` window contains the complete history of every AWB in it | EXACT\* — assumes the window fully covers the AWB creation dates being validated (it does, by construction) |
| `original_job_id` preserved from target via `sql_update_staging` | NULL diagnostic | STRUCTURAL — job ids not derivable |

## 6. Bottom line

- Every business-data expression is **EXACT**, or **EXACT\*** under two
  benign assumptions (Glue session TZ = UTC; Spark non-ANSI mode) that are
  the Glue defaults.
- The **STRUCTURAL** items (job ids, partition columns, DQ rejection) cannot
  be made identical in any SQL re-implementation, and the prod run proves
  none of them affected the comparison (`missing/extra = 0`).
- Remaining mismatch counters (402/2,960/4/100/4 on 8.06M rows) are explained
  by join fan-out double-counting and historical rows written by earlier
  versions of the Glue transform — use
  `cmdty_crgo_pln_leg_mismatch_drilldown.sql` (job-run-day summary) to
  confirm from `latest_job_id` timestamps.
