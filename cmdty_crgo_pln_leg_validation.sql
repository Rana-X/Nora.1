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
-- Output: ONE summary row
--   source_count       -> rows re-derived from the curated source (expected)
--   target_count       -> rows in the dev Athena table (actual)
--   matched_on_keys    -> rows joined 1:1 on the business keys
--   missing_in_target  -> source-derived keys with no row in the dev table
--   extra_in_target    -> dev-table keys that could not be re-derived
--   mismatch_<field>   -> per field: +1 for every key-matched row where the
--                         source-derived value differs from the dev value
--                         (0 everywhere = perfect match)
--
-- Rows are matched on the schema's business/primary keys (KEYS_PRIMARY):
--   air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
--   air_wb_pce_num, opng_carr_cde, opng_flt_num, opng_flt_num_sufx_txt,
--   flt_dep_dt, leg_orig_arpt_cde, leg_dest_arpt_cde
-- plus eff_fm_cent_tz, because the datalake table keeps one row per event
-- version; without it the join would multiply versions against each other.
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
-- 3b) JOIN source-derived rows to dev rows on the business keys
-- ----------------------------------------------------------------------------
joined AS (
    SELECT
        (s.air_wb_prfx_id IS NOT NULL) AS s_present,
        (t.air_wb_prfx_id IS NOT NULL) AS t_present,
        s.pln_crgo_leg_seq_num         AS s_pln_crgo_leg_seq_num,
        t.pln_crgo_leg_seq_num         AS t_pln_crgo_leg_seq_num,
        s.pln_max_crgo_leg_seq_num     AS s_pln_max_crgo_leg_seq_num,
        t.pln_max_crgo_leg_seq_num     AS t_pln_max_crgo_leg_seq_num,
        s.pln_crgo_dep_ld_flag         AS s_pln_crgo_dep_ld_flag,
        t.pln_crgo_dep_ld_flag         AS t_pln_crgo_dep_ld_flag,
        s.pln_crgo_arr_unld_flag       AS s_pln_crgo_arr_unld_flag,
        t.pln_crgo_arr_unld_flag       AS t_pln_crgo_arr_unld_flag,
        s.cmdty_flt_leg_type_cde       AS s_cmdty_flt_leg_type_cde,
        t.cmdty_flt_leg_type_cde       AS t_cmdty_flt_leg_type_cde
    FROM expected s
    FULL OUTER JOIN actual t
        ON  s.air_wb_prfx_id        = t.air_wb_prfx_id
        AND s.air_wb_num            = t.air_wb_num
        AND s.air_wb_cre_dt         = t.air_wb_cre_dt
        AND s.air_wb_cre_h2si       = t.air_wb_cre_h2si
        AND s.air_wb_pce_num        = t.air_wb_pce_num
        AND s.opng_carr_cde         = t.opng_carr_cde
        AND s.opng_flt_num          = t.opng_flt_num
        AND s.opng_flt_num_sufx_txt = t.opng_flt_num_sufx_txt
        AND s.flt_dep_dt            = t.flt_dep_dt
        AND s.leg_orig_arpt_cde     = t.leg_orig_arpt_cde
        AND s.leg_dest_arpt_cde     = t.leg_dest_arpt_cde
        AND s.eff_fm_cent_tz IS NOT DISTINCT FROM t.eff_fm_cent_tz
)

-- ----------------------------------------------------------------------------
-- 3c) VALIDATE: one summary row. Per non-key field: +1 per key-matched row
--     where source-derived and dev values differ, +0 when they match.
--     IS DISTINCT FROM counts NULL vs non-NULL as a mismatch, NULL vs NULL
--     as a match.
-- ----------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM expected)                  AS source_count,
    (SELECT count(*) FROM actual)                    AS target_count,
    count_if(s_present AND t_present)                AS matched_on_keys,
    count_if(s_present AND NOT t_present)            AS missing_in_target,
    count_if(NOT s_present AND t_present)            AS extra_in_target,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_crgo_leg_seq_num     IS DISTINCT FROM t_pln_crgo_leg_seq_num
             THEN 1 ELSE 0 END)                      AS mismatch_pln_crgo_leg_seq_num,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_max_crgo_leg_seq_num IS DISTINCT FROM t_pln_max_crgo_leg_seq_num
             THEN 1 ELSE 0 END)                      AS mismatch_pln_max_crgo_leg_seq_num,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_crgo_dep_ld_flag     IS DISTINCT FROM t_pln_crgo_dep_ld_flag
             THEN 1 ELSE 0 END)                      AS mismatch_pln_crgo_dep_ld_flag,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_crgo_arr_unld_flag   IS DISTINCT FROM t_pln_crgo_arr_unld_flag
             THEN 1 ELSE 0 END)                      AS mismatch_pln_crgo_arr_unld_flag,
    sum(CASE WHEN s_present AND t_present
              AND s_cmdty_flt_leg_type_cde   IS DISTINCT FROM t_cmdty_flt_leg_type_cde
             THEN 1 ELSE 0 END)                      AS mismatch_cmdty_flt_leg_type_cde
FROM joined;


-- ============================================================================
-- OPTIONAL: row-level drill-down. Once the summary above shows a non-zero
-- counter, swap the final SELECT for this EXCEPT diff to see the actual
-- offending rows (keep the CTEs as-is; 'joined' becomes unused).
-- ============================================================================
-- SELECT 'missing_in_dev' AS diff_type, * FROM (
--     SELECT * FROM expected EXCEPT SELECT * FROM actual
-- )
-- UNION ALL
-- SELECT 'extra_in_dev' AS diff_type, * FROM (
--     SELECT * FROM actual EXCEPT SELECT * FROM expected
-- )
-- ORDER BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
--          air_wb_pce_num, pln_crgo_leg_seq_num, diff_type;
