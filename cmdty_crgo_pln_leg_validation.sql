-- ============================================================================
-- cmdty_crgo_pln_leg : Athena (Trino) validation — per non-PK field
--
-- Source -> transform -> compare vs datalake_prod1_entp_ctds.cmdty_crgo_pln_leg.
-- Runs in the regular Athena (Trino) query editor.
--
-- Grain / join keys = the 11 PRIMARY KEY fields + eff_fm_cent_tz (event version):
--   air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num,
--   opng_carr_cde, opng_flt_num, opng_flt_num_sufx_txt, flt_dep_dt,
--   leg_orig_arpt_cde, leg_dest_arpt_cde, eff_fm_cent_tz
--
-- Mismatch counter for EVERY non-PK field of the datalake table:
--   pln_crgo_leg_seq_num, pln_max_crgo_leg_seq_num,
--   pln_crgo_dep_ld_flag, pln_crgo_arr_unld_flag, cmdty_flt_leg_type_cde
-- (the redshift-only fields eff_to_cent_tz/min_eff_fm_flag/max_eff_to_flag and
--  the runtime metadata latest_job_id do not exist in / are not derivable for
--  the datalake table, so they are not compared.)
--
-- A difference in a PK/join-key field (e.g. opng_flt_num, leg airports) shows
-- up as missing_in_target + extra_in_target, not as a field counter.
--
-- Fix vs the earlier draft: date/timestamp fields are NOT defaulted to
-- 1900-01-01. The dev na.fill leaves Date/Timestamp NULL on parse failure, so
-- the 1900-01-01 default was manufacturing false flt_dep_dt / eff_fm diffs.
-- ============================================================================

WITH
params AS (
    SELECT
        DATE '2026-06-09' AS beg_dt,
        DATE '2026-06-13' AS end_dt
),
source AS (
    SELECT
        row_number() OVER (
            ORDER BY
                c.id,
                c.event_created,
                c.airwaybillprefix,
                c.airwaybillnumber,
                c.airwaybillcreationdate,
                c.airwaybillpiecenumber
        ) AS source_row_num,
        c.airwaybillprefix,
        c.airwaybillnumber,
        c.airwaybillcreationdate,
        c.airwaybillpiecenumber,
        c.event_created,
        c.planneditinerary
    FROM datalake_prod1_entp_ctds.cmdty_crgo_curated c
    CROSS JOIN params p
    WHERE c.event_name IN (
        'piece-created',
        'piece-itinerary-changed',
        'piece-wab-added',
        'piece-wab-changed',
        'piece-loaded-on-flight',
        'piece-unloaded-from-flight',
        'piece-offloaded-from-flight',
        'piece-undeleted',
        'piece-deleted',
        'piece-updated',
        'piece-tracking',
        'piece-loaded-by-backup-mode',
        'piece-not-loaded-by-backup-mode'
    )
    AND c.planneditinerary IS NOT NULL
    AND cardinality(c.planneditinerary) > 0
    AND TRY(CAST(substr(c.airwaybillcreationdate, 1, 10) AS date))
        BETWEEN p.beg_dt AND p.end_dt
),
exploded_base AS (
    SELECT
        s.source_row_num,
        s.airwaybillprefix,
        s.airwaybillnumber,
        s.airwaybillcreationdate,
        s.airwaybillpiecenumber,
        s.event_created,
        s.planneditinerary,
        leg.exploded_leg,
        CAST(leg.leg_ord AS integer) AS leg_ord,
        CAST(leg.leg_ord - 1 AS integer) AS spark_leg_pos,
        leg.exploded_leg.flightleg.departureairportiatacode AS leg_departureairportiatacode,
        leg.exploded_leg.flightleg.arrivalairportiatacode AS leg_arrivalairportiatacode,
        leg.exploded_leg.flightleg.origindate AS leg_origindate,
        leg.exploded_leg.flightleg.flightnumber AS leg_flightnumber,
        leg.exploded_leg.flightleg.operationalcarrier AS leg_operationalcarrier,
        leg.exploded_leg.flightleg.operationalsuffix AS leg_operationalsuffix,
        leg.exploded_leg.flightleg.repeatnumber AS leg_repeatnumber,
        leg.exploded_leg.flightlegtypeid AS leg_flightlegtypeid
    FROM source s
    CROSS JOIN UNNEST(s.planneditinerary) WITH ORDINALITY AS leg (exploded_leg, leg_ord)
),
exploded AS (
    SELECT
        eb.*,
        -- null-safe replica of Spark array_position(planneditinerary, exploded_leg):
        -- first ordinal among elements with identical struct values.
        MIN(leg_ord) OVER (
            PARTITION BY
                source_row_num,
                leg_departureairportiatacode,
                leg_arrivalairportiatacode,
                leg_origindate,
                leg_flightnumber,
                leg_operationalcarrier,
                leg_operationalsuffix,
                leg_repeatnumber,
                leg_flightlegtypeid
        ) AS first_matching_leg_ord,
        IF(leg_ord > 1, element_at(planneditinerary, leg_ord - 1)) AS prev_leg,
        IF(leg_ord < cardinality(planneditinerary), element_at(planneditinerary, leg_ord + 1)) AS next_leg
    FROM exploded_base eb
),
transformed AS (
    SELECT
        COALESCE(airwaybillprefix, '-') AS air_wb_prfx_id,
        COALESCE(airwaybillnumber, '-') AS air_wb_num,
        -- dates/timestamps: NULL on parse failure (matches dev na.fill, which
        -- does not default Date/Timestamp columns). NO 1900-01-01 default.
        TRY(CAST(substr(airwaybillcreationdate, 1, 10) AS date)) AS air_wb_cre_dt,
        COALESCE(format_datetime(TRY(from_iso8601_timestamp(airwaybillcreationdate)) AT TIME ZONE 'UTC', 'HH:mm:ss.SSS'), '-') AS air_wb_cre_h2si,
        COALESCE(TRY(CAST(airwaybillpiecenumber AS smallint)), CAST(0 AS smallint)) AS air_wb_pce_num,
        COALESCE(exploded_leg.flightleg.operationalcarrier, '-') AS opng_carr_cde,
        COALESCE(TRY(CAST(exploded_leg.flightleg.flightnumber AS smallint)), CAST(0 AS smallint)) AS opng_flt_num,
        COALESCE(exploded_leg.flightleg.operationalsuffix, '-') AS opng_flt_num_sufx_txt,
        TRY(CAST(substr(exploded_leg.flightleg.origindate, 1, 10) AS date)) AS flt_dep_dt,
        COALESCE(exploded_leg.flightleg.departureairportiatacode, '-') AS leg_orig_arpt_cde,
        COALESCE(exploded_leg.flightleg.arrivalairportiatacode, '-') AS leg_dest_arpt_cde,
        CAST(TRY(from_iso8601_timestamp(event_created)) AS timestamp(3)) AS eff_fm_cent_tz,
        CASE
            WHEN exploded_leg IS NULL THEN 0
            ELSE COALESCE(first_matching_leg_ord, 0)
        END AS pln_crgo_leg_seq_num,
        CAST(COALESCE(cardinality(planneditinerary), 0) AS integer) AS pln_max_crgo_leg_seq_num,
        CAST(CASE
            WHEN spark_leg_pos = 0 THEN 1
            WHEN prev_leg IS NULL THEN 1
            WHEN prev_leg.flightleg.flightnumber <> exploded_leg.flightleg.flightnumber THEN 1
            WHEN prev_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier THEN 1
            WHEN prev_leg.flightleg.arrivalairportiatacode <> exploded_leg.flightleg.departureairportiatacode THEN 1
            ELSE 0
        END AS smallint) AS pln_crgo_dep_ld_flag,
        CAST(CASE
            WHEN spark_leg_pos = cardinality(planneditinerary) - 1 THEN 1
            WHEN next_leg IS NULL THEN 1
            WHEN next_leg.flightleg.flightnumber <> exploded_leg.flightleg.flightnumber THEN 1
            WHEN next_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier THEN 1
            WHEN next_leg.flightleg.departureairportiatacode <> exploded_leg.flightleg.arrivalairportiatacode THEN 1
            ELSE 0
        END AS smallint) AS pln_crgo_arr_unld_flag,
        CASE lower(trim(exploded_leg.flightlegtypeid))
            WHEN 'unknown' THEN '-'
            WHEN 'originating' THEN '0'
            WHEN 'transfer' THEN '1'
            WHEN 'thru' THEN '2'
            WHEN 'standbyearly' THEN '3'
            ELSE '-'
        END AS cmdty_flt_leg_type_cde
    FROM exploded
),
expected_datalake AS (
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
        eff_fm_cent_tz,
        pln_crgo_leg_seq_num,
        pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag,
        pln_crgo_arr_unld_flag,
        cmdty_flt_leg_type_cde
    FROM transformed
),
actual_athena_raw AS (
    SELECT
        t.air_wb_prfx_id,
        t.air_wb_num,
        t.air_wb_cre_dt,
        t.air_wb_cre_h2si,
        t.air_wb_pce_num,
        t.opng_carr_cde,
        t.opng_flt_num,
        t.opng_flt_num_sufx_txt,
        t.flt_dep_dt,
        t.leg_orig_arpt_cde,
        t.leg_dest_arpt_cde,
        CAST(t.eff_fm_cent_tz AS timestamp(3)) AS eff_fm_cent_tz,
        t.pln_crgo_leg_seq_num,
        t.pln_max_crgo_leg_seq_num,
        t.pln_crgo_dep_ld_flag,
        t.pln_crgo_arr_unld_flag,
        t.cmdty_flt_leg_type_cde
    FROM datalake_prod1_entp_ctds.cmdty_crgo_pln_leg t
    CROSS JOIN params p
    WHERE t.air_wb_cre_dt BETWEEN p.beg_dt AND p.end_dt
),
actual_athena_logical AS (
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
        eff_fm_cent_tz,
        pln_crgo_leg_seq_num,
        pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag,
        pln_crgo_arr_unld_flag,
        cmdty_flt_leg_type_cde
    FROM actual_athena_raw
),
target_duplicate_groups AS (
    -- physical rows in the target that share a full logical row (reprocessing)
    SELECT count(*) AS physical_row_count
    FROM actual_athena_raw
    GROUP BY
        air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num,
        opng_carr_cde, opng_flt_num, opng_flt_num_sufx_txt, flt_dep_dt,
        leg_orig_arpt_cde, leg_dest_arpt_cde, eff_fm_cent_tz,
        pln_crgo_leg_seq_num, pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag, pln_crgo_arr_unld_flag, cmdty_flt_leg_type_cde
    HAVING count(*) > 1
),
joined_datalake AS (
    SELECT
        (s.air_wb_prfx_id IS NOT NULL) AS s_present,
        (t.air_wb_prfx_id IS NOT NULL) AS t_present,
        s.pln_crgo_leg_seq_num AS s_pln_crgo_leg_seq_num,
        t.pln_crgo_leg_seq_num AS t_pln_crgo_leg_seq_num,
        s.pln_max_crgo_leg_seq_num AS s_pln_max_crgo_leg_seq_num,
        t.pln_max_crgo_leg_seq_num AS t_pln_max_crgo_leg_seq_num,
        s.pln_crgo_dep_ld_flag AS s_pln_crgo_dep_ld_flag,
        t.pln_crgo_dep_ld_flag AS t_pln_crgo_dep_ld_flag,
        s.pln_crgo_arr_unld_flag AS s_pln_crgo_arr_unld_flag,
        t.pln_crgo_arr_unld_flag AS t_pln_crgo_arr_unld_flag,
        s.cmdty_flt_leg_type_cde AS s_cmdty_flt_leg_type_cde,
        t.cmdty_flt_leg_type_cde AS t_cmdty_flt_leg_type_cde
    FROM expected_datalake s
    FULL OUTER JOIN actual_athena_logical t
        ON  s.air_wb_prfx_id IS NOT DISTINCT FROM t.air_wb_prfx_id
        AND s.air_wb_num IS NOT DISTINCT FROM t.air_wb_num
        AND s.air_wb_cre_dt IS NOT DISTINCT FROM t.air_wb_cre_dt
        AND s.air_wb_cre_h2si IS NOT DISTINCT FROM t.air_wb_cre_h2si
        AND s.air_wb_pce_num IS NOT DISTINCT FROM t.air_wb_pce_num
        AND s.opng_carr_cde IS NOT DISTINCT FROM t.opng_carr_cde
        AND s.opng_flt_num IS NOT DISTINCT FROM t.opng_flt_num
        AND s.opng_flt_num_sufx_txt IS NOT DISTINCT FROM t.opng_flt_num_sufx_txt
        AND s.flt_dep_dt IS NOT DISTINCT FROM t.flt_dep_dt
        AND s.leg_orig_arpt_cde IS NOT DISTINCT FROM t.leg_orig_arpt_cde
        AND s.leg_dest_arpt_cde IS NOT DISTINCT FROM t.leg_dest_arpt_cde
        AND s.eff_fm_cent_tz IS NOT DISTINCT FROM t.eff_fm_cent_tz
)
SELECT
    (SELECT beg_dt FROM params) AS beg_dt,
    (SELECT end_dt FROM params) AS end_dt,
    (SELECT count(*) FROM transformed) AS source_transformed_physical_count,
    (SELECT count(*) FROM expected_datalake) AS source_logical_count,
    (SELECT count(*) FROM actual_athena_raw) AS target_physical_count,
    (SELECT count(*) FROM actual_athena_logical) AS target_logical_count,
    (SELECT COALESCE(sum(physical_row_count - 1), 0) FROM target_duplicate_groups) AS target_extra_physical_duplicate_rows,
    (SELECT count(*) FROM target_duplicate_groups) AS target_duplicate_logical_groups,
    count(*) AS joined_pairs,
    count_if(s_present AND t_present) AS matched_on_keys,
    count_if(s_present AND NOT t_present) AS missing_in_target,
    count_if(NOT s_present AND t_present) AS extra_in_target,
    -- one counter per NON-PK field (+1 per key-matched row that differs)
    sum(CASE WHEN s_present AND t_present
              AND s_pln_crgo_leg_seq_num IS DISTINCT FROM t_pln_crgo_leg_seq_num
             THEN 1 ELSE 0 END) AS mismatch_pln_crgo_leg_seq_num,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_max_crgo_leg_seq_num IS DISTINCT FROM t_pln_max_crgo_leg_seq_num
             THEN 1 ELSE 0 END) AS mismatch_pln_max_crgo_leg_seq_num,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_crgo_dep_ld_flag IS DISTINCT FROM t_pln_crgo_dep_ld_flag
             THEN 1 ELSE 0 END) AS mismatch_pln_crgo_dep_ld_flag,
    sum(CASE WHEN s_present AND t_present
              AND s_pln_crgo_arr_unld_flag IS DISTINCT FROM t_pln_crgo_arr_unld_flag
             THEN 1 ELSE 0 END) AS mismatch_pln_crgo_arr_unld_flag,
    sum(CASE WHEN s_present AND t_present
              AND s_cmdty_flt_leg_type_cde IS DISTINCT FROM t_cmdty_flt_leg_type_cde
             THEN 1 ELSE 0 END) AS mismatch_cmdty_flt_leg_type_cde
FROM joined_datalake;
