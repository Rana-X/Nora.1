-- ============================================================================
-- cmdty_crgo_pln_leg : Athena replication + validation query
--
-- Re-derives CMDTY_CRGO_PLN_LEG rows directly from the curated source using
-- the same logic as the dev Glue job (cmdty_crgo_schemas.py), then compares
-- the result against the dev Athena table cmdty_crgo_pln_leg.
--
-- Flow:  1) EXTRACT   - pull events from the curated source table
--        2) TRANSFORM - explode plannedItinerary and derive the leg columns
--        3) VALIDATE  - EXCEPT-diff against the dev table in both directions
--
-- Before running:
--   * replace <SOURCE_DB> with the curated database (e.g. datalake_dev1_xmpl)
--   * replace <TARGET_DB> with the database holding cmdty_crgo_pln_leg
--   * uncomment + adjust the year/month/day partition filters in BOTH the
--     'source' and 'actual' CTEs (full scans are slow and expensive)
--
-- Output: one row per mismatch
--   diff_type = 'missing_in_dev' -> derived from source, absent in dev table
--   diff_type = 'extra_in_dev'   -> present in dev table, not re-derivable
--   Empty result = the dev table matches the source-derived data exactly.
-- ============================================================================

WITH

-- ----------------------------------------------------------------------------
-- 1) EXTRACT: same event filter as the dev job
--    (metadata['cmdty_crgo_pln_leg'].extract_filter -> event_filter_list)
-- ----------------------------------------------------------------------------
source AS (
    SELECT
        airwaybillprefix,
        airwaybillnumber,
        airwaybillcreationdate,
        airwaybillpiecenumber,
        event_created,
        planneditinerary
    FROM <SOURCE_DB>.cmdty_crgo_curated
    WHERE event_name IN (
        'piece-created', 'piece-itinerary-changed', 'piece-wab-added',
        'piece-wab-changed', 'piece-loaded-on-flight', 'piece-unloaded-from-flight',
        'piece-offloaded-from-flight', 'piece-undeleted', 'piece-deleted',
        'piece-updated', 'piece-tracking', 'piece-loaded-by-backup-mode',
        'piece-not-loaded-by-backup-mode'
    )
    AND planneditinerary IS NOT NULL
    AND cardinality(planneditinerary) > 0
    -- partition pruning (keep in sync with 'actual' below):
    -- AND year = '2025' AND month = '09' AND day = '05'
),

-- ----------------------------------------------------------------------------
-- 2a) EXPLODE: Athena equivalent of the dev job's
--     posexplode(planneditinerary) + prev_leg / next_leg lookups.
--     NOTE: ordinality (leg_pos) is 1-based; Spark posexplode is 0-based.
-- ----------------------------------------------------------------------------
exploded AS (
    SELECT
        s.airwaybillprefix,
        s.airwaybillnumber,
        s.airwaybillcreationdate,
        s.airwaybillpiecenumber,
        s.event_created,
        s.planneditinerary,
        leg.exploded_leg,
        CAST(leg.leg_pos AS integer) AS leg_pos,
        IF(leg.leg_pos > 1,
           element_at(s.planneditinerary, CAST(leg.leg_pos AS integer) - 1)) AS prev_leg,
        IF(leg.leg_pos < cardinality(s.planneditinerary),
           element_at(s.planneditinerary, CAST(leg.leg_pos AS integer) + 1)) AS next_leg
    FROM source s
    CROSS JOIN UNNEST(s.planneditinerary) WITH ORDINALITY AS leg (exploded_leg, leg_pos)
),

-- ----------------------------------------------------------------------------
-- 2b) TRANSFORM: mirrors each field's 'path' lambda + 'default' value from the
--     cmdty_crgo_pln_leg schema. DISTINCT mirrors remove_dupes=True.
--     (year/month/day partitions and latest_job_id are intentionally not
--     derived: partitions are functionally dependent on air_wb_cre_dt, and
--     job ids differ per run, so neither is comparable.)
-- ----------------------------------------------------------------------------
expected AS (
    SELECT DISTINCT
        COALESCE(airwaybillprefix, '-')                                          AS air_wb_prfx_id,
        COALESCE(airwaybillnumber, '-')                                          AS air_wb_num,
        COALESCE(TRY(CAST(substr(airwaybillcreationdate, 1, 10) AS date)),
                 DATE '1900-01-01')                                              AS air_wb_cre_dt,
        COALESCE(format_datetime(TRY(from_iso8601_timestamp(airwaybillcreationdate))
                                 AT TIME ZONE 'UTC', 'HH:mm:ss.SSS'), '-')       AS air_wb_cre_h2si,
        COALESCE(TRY(CAST(airwaybillpiecenumber AS smallint)),
                 CAST(0 AS smallint))                                            AS air_wb_pce_num,
        COALESCE(exploded_leg.flightleg.operationalcarrier, '-')                 AS opng_carr_cde,
        COALESCE(TRY(CAST(exploded_leg.flightleg.flightnumber AS smallint)),
                 CAST(0 AS smallint))                                            AS opng_flt_num,
        COALESCE(exploded_leg.flightleg.operationalsuffix, '-')                  AS opng_flt_num_sufx_txt,
        COALESCE(TRY(CAST(substr(exploded_leg.flightleg.origindate, 1, 10) AS date)),
                 DATE '1900-01-01')                                              AS flt_dep_dt,
        COALESCE(exploded_leg.flightleg.departureairportiatacode, '-')           AS leg_orig_arpt_cde,
        COALESCE(exploded_leg.flightleg.arrivalairportiatacode, '-')             AS leg_dest_arpt_cde,
        CAST(TRY(from_iso8601_timestamp(event_created)) AS timestamp(3))         AS eff_fm_cent_tz,

        -- array_position(planneditinerary, exploded_leg) == ordinality position
        leg_pos                                                                  AS pln_crgo_leg_seq_num,
        CAST(cardinality(planneditinerary) AS integer)                           AS pln_max_crgo_leg_seq_num,

        -- get_dep_ld_flag: 1 on first leg or any connection break vs prev leg.
        -- CASE skips NULL comparisons exactly like Spark's when() chain.
        CAST(CASE
            WHEN leg_pos = 1                                                                       THEN 1
            WHEN prev_leg IS NULL                                                                  THEN 1
            WHEN prev_leg.flightleg.flightnumber       <> exploded_leg.flightleg.flightnumber      THEN 1
            WHEN prev_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier THEN 1
            WHEN prev_leg.flightleg.arrivalairportiatacode
                 <> exploded_leg.flightleg.departureairportiatacode                                THEN 1
            ELSE 0
        END AS smallint)                                                         AS pln_crgo_dep_ld_flag,

        -- get_arr_unld_flag: 1 on last leg or any connection break vs next leg
        CAST(CASE
            WHEN leg_pos = cardinality(planneditinerary)                                           THEN 1
            WHEN next_leg IS NULL                                                                  THEN 1
            WHEN next_leg.flightleg.flightnumber       <> exploded_leg.flightleg.flightnumber      THEN 1
            WHEN next_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier THEN 1
            WHEN next_leg.flightleg.departureairportiatacode
                 <> exploded_leg.flightleg.arrivalairportiatacode                                  THEN 1
            ELSE 0
        END AS smallint)                                                         AS pln_crgo_arr_unld_flag,

        -- get_pln_leg_type_code
        CASE lower(trim(exploded_leg.flightlegtypeid))
            WHEN 'unknown'      THEN '-'
            WHEN 'originating'  THEN '0'
            WHEN 'transfer'     THEN '1'
            WHEN 'thru'         THEN '2'
            WHEN 'standbyearly' THEN '3'
            ELSE '-'
        END                                                                      AS cmdty_flt_leg_type_cde
    FROM exploded
),

-- ----------------------------------------------------------------------------
-- 3a) ACTUAL: the dev table as it exists in Athena
-- ----------------------------------------------------------------------------
actual AS (
    SELECT DISTINCT
        air_wb_prfx_id,
        air_wb_num,
        air_wb_cre_dt,
        air_wb_cre_h2si,
        air_wb_pce_num,
        opng_carr_cde,
        opng_flt_num,
        opng_flt_num_sufx_txt,
        flt_dep_dt,
        leg_orig_arpt_cde,
        leg_dest_arpt_cde,
        CAST(eff_fm_cent_tz AS timestamp(3))                                     AS eff_fm_cent_tz,
        pln_crgo_leg_seq_num,
        pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag,
        pln_crgo_arr_unld_flag,
        cmdty_flt_leg_type_cde
    FROM <TARGET_DB>.cmdty_crgo_pln_leg
    -- partition pruning (keep in sync with 'source' above):
    -- WHERE year = '2025' AND month = '09' AND day = '05'
),

-- ----------------------------------------------------------------------------
-- 3b) VALIDATE: full-row diff in both directions
-- ----------------------------------------------------------------------------
missing_in_dev AS (
    SELECT * FROM expected
    EXCEPT
    SELECT * FROM actual
),
extra_in_dev AS (
    SELECT * FROM actual
    EXCEPT
    SELECT * FROM expected
)

SELECT 'missing_in_dev' AS diff_type, * FROM missing_in_dev
UNION ALL
SELECT 'extra_in_dev'   AS diff_type, * FROM extra_in_dev
ORDER BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
         air_wb_pce_num, pln_crgo_leg_seq_num, diff_type;


-- ============================================================================
-- OPTIONAL: quick summary instead of the row-level diff. Swap the final
-- SELECT above for this block to get just the counts.
-- ============================================================================
-- SELECT
--     (SELECT count(*) FROM expected)                       AS expected_cnt,
--     (SELECT count(*) FROM actual)                         AS dev_cnt,
--     (SELECT count(*) FROM missing_in_dev)                 AS missing_in_dev_cnt,
--     (SELECT count(*) FROM extra_in_dev)                   AS extra_in_dev_cnt;
