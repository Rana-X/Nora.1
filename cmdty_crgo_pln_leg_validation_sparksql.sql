-- ============================================================================
-- cmdty_crgo_pln_leg validation : ONE big Spark SQL query
--
-- Dialect: Spark SQL (NOT Athena/Trino SQL). Run it where Spark runs:
--   * Athena Spark workgroup notebook: paste into a %%sql cell, or
--     spark.sql(open('this file').read()).show(vertical=True)
--   * Glue job / Glue interactive session: spark.sql(...)
--
-- Because it executes on Spark, every expression uses the dev job's exact
-- engine semantics (posexplode, array_position null-safe struct equality,
-- to_date/to_timestamp parsing, <=> null-safe comparison).
--
-- Generated from cmdty_crgo_pln_leg_validation_pyspark.py - edit there.
-- Output: ONE summary row (source/target counts, matched/missing/extra, and
-- one mismatch_<field> counter per non-identity field: +1 per differing
-- key-matched row, +0 on match).
-- ============================================================================
WITH
-- ----------------------------------------------------------------------------
-- 1) EXTRACT: same event filter as metadata['cmdty_crgo_pln_leg'].extract_filter
-- ----------------------------------------------------------------------------
source AS (
    SELECT *
    FROM datalake_prod1_entp_ctds.cmdty_crgo_curated
    WHERE event_name IN (
        'piece-created', 'piece-itinerary-changed', 'piece-wab-added',
        'piece-wab-changed', 'piece-loaded-on-flight', 'piece-unloaded-from-flight',
        'piece-offloaded-from-flight', 'piece-undeleted', 'piece-deleted',
        'piece-updated', 'piece-tracking', 'piece-loaded-by-backup-mode',
        'piece-not-loaded-by-backup-mode'
    )
    AND planneditinerary IS NOT NULL
    AND size(planneditinerary) > 0
    AND to_date(airwaybillcreationdate) BETWEEN DATE '2026-05-20' AND DATE '2026-06-07'
),

-- ----------------------------------------------------------------------------
-- 2) EXPLODE: dev posexplode (leg_pos is ZERO-based, exactly like the dev job)
--    plus the dev prev_leg / next_leg array lookups.
-- ----------------------------------------------------------------------------
exploded AS (
    SELECT
        s.*,
        leg_pos,
        exploded_leg,
        planneditinerary[leg_pos - 1] AS prev_leg,
        planneditinerary[leg_pos + 1] AS next_leg
    FROM source s
    LATERAL VIEW posexplode(planneditinerary) pe AS leg_pos, exploded_leg
),

-- ----------------------------------------------------------------------------
-- 3) TRANSFORM: the schema 'path' expressions verbatim, with the schema
--    defaults applied the same way na.fill does (strings/numerics only;
--    dates/timestamps stay NULL on parse failure, like dev).
-- ----------------------------------------------------------------------------
transformed AS (
    SELECT
        COALESCE(airwaybillprefix, '-')                                       AS air_wb_prfx_id,
        COALESCE(airwaybillnumber, '-')                                       AS air_wb_num,
        to_date(airwaybillcreationdate)                                       AS air_wb_cre_dt,
        COALESCE(date_format(to_timestamp(airwaybillcreationdate),
                             'HH:mm:ss.SSS'), '-')                            AS air_wb_cre_h2si,
        COALESCE(CAST(airwaybillpiecenumber AS smallint),
                 CAST(0 AS smallint))                                         AS air_wb_pce_num,
        COALESCE(exploded_leg.flightleg.operationalcarrier, '-')              AS opng_carr_cde,
        COALESCE(CAST(exploded_leg.flightleg.flightnumber AS smallint),
                 CAST(0 AS smallint))                                         AS opng_flt_num,
        COALESCE(exploded_leg.flightleg.operationalsuffix, '-')               AS opng_flt_num_sufx_txt,
        to_date(exploded_leg.flightleg.origindate)                            AS flt_dep_dt,
        COALESCE(exploded_leg.flightleg.departureairportiatacode, '-')        AS leg_orig_arpt_cde,
        COALESCE(exploded_leg.flightleg.arrivalairportiatacode, '-')          AS leg_dest_arpt_cde,
        to_timestamp(event_created)                                           AS eff_fm_cent_tz,
        CAST(COALESCE(array_position(planneditinerary, exploded_leg), 0)
             AS int)                                                          AS pln_crgo_leg_seq_num,
        CAST(COALESCE(size(planneditinerary), 0) AS int)                      AS pln_max_crgo_leg_seq_num,

        -- get_dep_ld_flag, verbatim when() chain
        CAST(CASE
            WHEN leg_pos = 0 THEN 1
            WHEN prev_leg IS NULL THEN 1
            WHEN (prev_leg.flightleg.flightnumber <> exploded_leg.flightleg.flightnumber)
              OR (prev_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier)
              OR (prev_leg.flightleg.arrivalairportiatacode
                  <> exploded_leg.flightleg.departureairportiatacode) THEN 1
            ELSE 0
        END AS smallint)                                                      AS pln_crgo_dep_ld_flag,

        -- get_arr_unld_flag, verbatim when() chain
        CAST(CASE
            WHEN leg_pos = size(planneditinerary) - 1 THEN 1
            WHEN next_leg IS NULL THEN 1
            WHEN (next_leg.flightleg.flightnumber <> exploded_leg.flightleg.flightnumber)
              OR (next_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier)
              OR (next_leg.flightleg.departureairportiatacode
                  <> exploded_leg.flightleg.arrivalairportiatacode) THEN 1
            ELSE 0
        END AS smallint)                                                      AS pln_crgo_arr_unld_flag,

        -- get_pln_leg_type_code, verbatim when() chain
        CASE
            WHEN lower(trim(exploded_leg.flightlegtypeid)) = 'unknown'      THEN '-'
            WHEN lower(trim(exploded_leg.flightlegtypeid)) = 'originating'  THEN '0'
            WHEN lower(trim(exploded_leg.flightlegtypeid)) = 'transfer'     THEN '1'
            WHEN lower(trim(exploded_leg.flightlegtypeid)) = 'thru'         THEN '2'
            WHEN lower(trim(exploded_leg.flightlegtypeid)) = 'standbyearly' THEN '3'
            ELSE '-'
        END                                                                   AS cmdty_flt_leg_type_cde
    FROM exploded
),

-- remove_dupes=True
expected AS (
    SELECT DISTINCT * FROM transformed
),

-- ----------------------------------------------------------------------------
-- 4) ACTUAL: the dev table, logical rows (latest_job_id excluded)
-- ----------------------------------------------------------------------------
actual AS (
    SELECT DISTINCT
        air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
        air_wb_pce_num, opng_carr_cde, opng_flt_num, opng_flt_num_sufx_txt,
        flt_dep_dt, leg_orig_arpt_cde, leg_dest_arpt_cde,
        eff_fm_cent_tz,
        pln_crgo_leg_seq_num, pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag, pln_crgo_arr_unld_flag, cmdty_flt_leg_type_cde
    FROM datalake_prod1_entp_ctds.cmdty_crgo_pln_leg
    WHERE air_wb_cre_dt BETWEEN DATE '2026-05-20' AND DATE '2026-06-07'
),

-- ----------------------------------------------------------------------------
-- 5) JOIN on row identity (AWB piece + event version + leg slot) using
--    Spark's null-safe equality <=>. Every other field is compared below.
-- ----------------------------------------------------------------------------
joined AS (
    SELECT
        (s.air_wb_prfx_id IS NOT NULL) AS s_present,
        (t.air_wb_prfx_id IS NOT NULL) AS t_present,
        s.air_wb_prfx_id           AS k_air_wb_prfx_id,
        s.air_wb_num               AS k_air_wb_num,
        s.air_wb_cre_dt            AS k_air_wb_cre_dt,
        s.air_wb_cre_h2si          AS k_air_wb_cre_h2si,
        s.air_wb_pce_num           AS k_air_wb_pce_num,
        s.eff_fm_cent_tz           AS k_eff_fm_cent_tz,
        s.pln_crgo_leg_seq_num     AS k_pln_crgo_leg_seq_num,
        s.opng_carr_cde            AS s_opng_carr_cde,            t.opng_carr_cde            AS t_opng_carr_cde,
        s.opng_flt_num             AS s_opng_flt_num,             t.opng_flt_num             AS t_opng_flt_num,
        s.opng_flt_num_sufx_txt    AS s_opng_flt_num_sufx_txt,    t.opng_flt_num_sufx_txt    AS t_opng_flt_num_sufx_txt,
        s.flt_dep_dt               AS s_flt_dep_dt,               t.flt_dep_dt               AS t_flt_dep_dt,
        s.leg_orig_arpt_cde        AS s_leg_orig_arpt_cde,        t.leg_orig_arpt_cde        AS t_leg_orig_arpt_cde,
        s.leg_dest_arpt_cde        AS s_leg_dest_arpt_cde,        t.leg_dest_arpt_cde        AS t_leg_dest_arpt_cde,
        s.pln_max_crgo_leg_seq_num AS s_pln_max_crgo_leg_seq_num, t.pln_max_crgo_leg_seq_num AS t_pln_max_crgo_leg_seq_num,
        s.pln_crgo_dep_ld_flag     AS s_pln_crgo_dep_ld_flag,     t.pln_crgo_dep_ld_flag     AS t_pln_crgo_dep_ld_flag,
        s.pln_crgo_arr_unld_flag   AS s_pln_crgo_arr_unld_flag,   t.pln_crgo_arr_unld_flag   AS t_pln_crgo_arr_unld_flag,
        s.cmdty_flt_leg_type_cde   AS s_cmdty_flt_leg_type_cde,   t.cmdty_flt_leg_type_cde   AS t_cmdty_flt_leg_type_cde
    FROM expected s
    FULL OUTER JOIN actual t
        ON  s.air_wb_prfx_id       <=> t.air_wb_prfx_id
        AND s.air_wb_num           <=> t.air_wb_num
        AND s.air_wb_cre_dt        <=> t.air_wb_cre_dt
        AND s.air_wb_cre_h2si      <=> t.air_wb_cre_h2si
        AND s.air_wb_pce_num       <=> t.air_wb_pce_num
        AND s.eff_fm_cent_tz       <=> t.eff_fm_cent_tz
        AND s.pln_crgo_leg_seq_num <=> t.pln_crgo_leg_seq_num
)

-- ----------------------------------------------------------------------------
-- 6) SUMMARY: counts + one mismatch counter per non-identity field
--    (match adds 0, mismatch adds 1)
-- ----------------------------------------------------------------------------
SELECT
    DATE '2026-05-20' AS beg_dt,
    DATE '2026-06-07' AS end_dt,
    SUM(CASE WHEN s_present THEN 1 ELSE 0 END) AS source_logical_count,
    SUM(CASE WHEN t_present THEN 1 ELSE 0 END) AS target_logical_count,
    SUM(CASE WHEN s_present AND t_present THEN 1 ELSE 0 END) AS matched_on_keys,
    SUM(CASE WHEN s_present AND NOT t_present THEN 1 ELSE 0 END) AS missing_in_target,
    SUM(CASE WHEN NOT s_present AND t_present THEN 1 ELSE 0 END) AS extra_in_target,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_opng_carr_cde <=> t_opng_carr_cde)
             THEN 1 ELSE 0 END) AS mismatch_opng_carr_cde,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_opng_flt_num <=> t_opng_flt_num)
             THEN 1 ELSE 0 END) AS mismatch_opng_flt_num,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_opng_flt_num_sufx_txt <=> t_opng_flt_num_sufx_txt)
             THEN 1 ELSE 0 END) AS mismatch_opng_flt_num_sufx_txt,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_flt_dep_dt <=> t_flt_dep_dt)
             THEN 1 ELSE 0 END) AS mismatch_flt_dep_dt,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_leg_orig_arpt_cde <=> t_leg_orig_arpt_cde)
             THEN 1 ELSE 0 END) AS mismatch_leg_orig_arpt_cde,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_leg_dest_arpt_cde <=> t_leg_dest_arpt_cde)
             THEN 1 ELSE 0 END) AS mismatch_leg_dest_arpt_cde,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_pln_max_crgo_leg_seq_num <=> t_pln_max_crgo_leg_seq_num)
             THEN 1 ELSE 0 END) AS mismatch_pln_max_crgo_leg_seq_num,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_pln_crgo_dep_ld_flag <=> t_pln_crgo_dep_ld_flag)
             THEN 1 ELSE 0 END) AS mismatch_pln_crgo_dep_ld_flag,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_pln_crgo_arr_unld_flag <=> t_pln_crgo_arr_unld_flag)
             THEN 1 ELSE 0 END) AS mismatch_pln_crgo_arr_unld_flag,
    SUM(CASE WHEN s_present AND t_present
              AND NOT (s_cmdty_flt_leg_type_cde <=> t_cmdty_flt_leg_type_cde)
             THEN 1 ELSE 0 END) AS mismatch_cmdty_flt_leg_type_cde
FROM joined
