-- ============================================================================
-- cmdty_crgo_pln_leg : mismatch drill-down (companion to
-- cmdty_crgo_pln_leg_validation.sql)
--
-- Purpose: list the actual rows behind the non-zero mismatch_* counters from
-- the summary run, side by side (s_ = derived from source, t_ = prod table),
-- with two diagnostics per row:
--
--   pair_count        > 1 means the key group fans out (several rows share the
--                     same 11 business keys + eff_fm_cent_tz and differ only in
--                     the compared fields) -- the summary counters count every
--                     cross-pair, so these inflate the totals.
--
--   t_max_latest_job_id  the newest Glue job id that wrote the target logical
--                     row. If mismatching rows cluster on OLD job ids, the
--                     difference comes from rows loaded by a previous version
--                     of the transform (e.g. the old window-based
--                     pln_max_crgo_leg_seq_num logic that is still visible,
--                     commented out, in cmdty_crgo_schemas.py) rather than
--                     from a bug in the current logic.
--
-- Same window/database as the summary run; adjust params as needed.
-- ============================================================================

WITH
params AS (
    SELECT
        DATE '2026-05-20' AS beg_dt,
        DATE '2026-06-07' AS end_dt
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
        IF(leg_ord > 1,
           element_at(planneditinerary, leg_ord - 1)) AS prev_leg,
        IF(leg_ord < cardinality(planneditinerary),
           element_at(planneditinerary, leg_ord + 1)) AS next_leg
    FROM exploded_base eb
),

transformed AS (
    SELECT
        COALESCE(airwaybillprefix, '-') AS air_wb_prfx_id,
        COALESCE(airwaybillnumber, '-') AS air_wb_num,
        COALESCE(
            TRY(CAST(substr(airwaybillcreationdate, 1, 10) AS date)),
            DATE '1900-01-01'
        ) AS air_wb_cre_dt,
        COALESCE(
            format_datetime(
                TRY(from_iso8601_timestamp(airwaybillcreationdate)) AT TIME ZONE 'UTC',
                'HH:mm:ss.SSS'
            ),
            '-'
        ) AS air_wb_cre_h2si,
        COALESCE(
            TRY(CAST(airwaybillpiecenumber AS smallint)),
            CAST(0 AS smallint)
        ) AS air_wb_pce_num,
        COALESCE(exploded_leg.flightleg.operationalcarrier, '-') AS opng_carr_cde,
        COALESCE(
            TRY(CAST(exploded_leg.flightleg.flightnumber AS smallint)),
            CAST(0 AS smallint)
        ) AS opng_flt_num,
        COALESCE(exploded_leg.flightleg.operationalsuffix, '-') AS opng_flt_num_sufx_txt,
        COALESCE(
            TRY(CAST(substr(exploded_leg.flightleg.origindate, 1, 10) AS date)),
            DATE '1900-01-01'
        ) AS flt_dep_dt,
        COALESCE(exploded_leg.flightleg.departureairportiatacode, '-') AS leg_orig_arpt_cde,
        COALESCE(exploded_leg.flightleg.arrivalairportiatacode, '-') AS leg_dest_arpt_cde,
        COALESCE(
            CAST(TRY(from_iso8601_timestamp(event_created)) AS timestamp(3)),
            TIMESTAMP '1900-01-01 00:00:00.000'
        ) AS eff_fm_cent_tz,
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
            WHEN prev_leg.flightleg.arrivalairportiatacode
                 <> exploded_leg.flightleg.departureairportiatacode THEN 1
            ELSE 0
        END AS smallint) AS pln_crgo_dep_ld_flag,
        CAST(CASE
            WHEN spark_leg_pos = cardinality(planneditinerary) - 1 THEN 1
            WHEN next_leg IS NULL THEN 1
            WHEN next_leg.flightleg.flightnumber <> exploded_leg.flightleg.flightnumber THEN 1
            WHEN next_leg.flightleg.operationalcarrier <> exploded_leg.flightleg.operationalcarrier THEN 1
            WHEN next_leg.flightleg.departureairportiatacode
                 <> exploded_leg.flightleg.arrivalairportiatacode THEN 1
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
    SELECT DISTINCT *
    FROM transformed
),

-- Target logical rows, keeping the newest job id per logical row so we can
-- see WHEN the mismatching value was written.
actual_athena_logical AS (
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
        t.cmdty_flt_leg_type_cde,
        max(t.latest_job_id) AS t_max_latest_job_id,
        min(t.latest_job_id) AS t_min_latest_job_id
    FROM datalake_prod1_entp_ctds.cmdty_crgo_pln_leg t
    CROSS JOIN params p
    WHERE t.air_wb_cre_dt BETWEEN p.beg_dt AND p.end_dt
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
),

mismatch_pairs AS (
    SELECT
        s.air_wb_prfx_id,
        s.air_wb_num,
        s.air_wb_cre_dt,
        s.air_wb_cre_h2si,
        s.air_wb_pce_num,
        s.opng_carr_cde,
        s.opng_flt_num,
        s.opng_flt_num_sufx_txt,
        s.flt_dep_dt,
        s.leg_orig_arpt_cde,
        s.leg_dest_arpt_cde,
        s.eff_fm_cent_tz,
        s.pln_crgo_leg_seq_num     AS s_pln_crgo_leg_seq_num,
        t.pln_crgo_leg_seq_num     AS t_pln_crgo_leg_seq_num,
        s.pln_max_crgo_leg_seq_num AS s_pln_max_crgo_leg_seq_num,
        t.pln_max_crgo_leg_seq_num AS t_pln_max_crgo_leg_seq_num,
        s.pln_crgo_dep_ld_flag     AS s_pln_crgo_dep_ld_flag,
        t.pln_crgo_dep_ld_flag     AS t_pln_crgo_dep_ld_flag,
        s.pln_crgo_arr_unld_flag   AS s_pln_crgo_arr_unld_flag,
        t.pln_crgo_arr_unld_flag   AS t_pln_crgo_arr_unld_flag,
        s.cmdty_flt_leg_type_cde   AS s_cmdty_flt_leg_type_cde,
        t.cmdty_flt_leg_type_cde   AS t_cmdty_flt_leg_type_cde,
        t.t_max_latest_job_id,
        t.t_min_latest_job_id,
        from_unixtime(t.t_max_latest_job_id) AS t_max_job_run_ts,
        count(*) OVER (
            PARTITION BY
                s.air_wb_prfx_id, s.air_wb_num, s.air_wb_cre_dt,
                s.air_wb_cre_h2si, s.air_wb_pce_num,
                s.opng_carr_cde, s.opng_flt_num, s.opng_flt_num_sufx_txt,
                s.flt_dep_dt, s.leg_orig_arpt_cde, s.leg_dest_arpt_cde,
                s.eff_fm_cent_tz
        ) AS pair_count,
        array_join(filter(ARRAY[
            IF(s.pln_crgo_leg_seq_num     IS DISTINCT FROM t.pln_crgo_leg_seq_num,     'pln_crgo_leg_seq_num'),
            IF(s.pln_max_crgo_leg_seq_num IS DISTINCT FROM t.pln_max_crgo_leg_seq_num, 'pln_max_crgo_leg_seq_num'),
            IF(s.pln_crgo_dep_ld_flag     IS DISTINCT FROM t.pln_crgo_dep_ld_flag,     'pln_crgo_dep_ld_flag'),
            IF(s.pln_crgo_arr_unld_flag   IS DISTINCT FROM t.pln_crgo_arr_unld_flag,   'pln_crgo_arr_unld_flag'),
            IF(s.cmdty_flt_leg_type_cde   IS DISTINCT FROM t.cmdty_flt_leg_type_cde,   'cmdty_flt_leg_type_cde')
        ], x -> x IS NOT NULL), ',') AS mismatched_fields
    FROM expected_datalake s
    JOIN actual_athena_logical t
        ON  s.air_wb_prfx_id = t.air_wb_prfx_id
        AND s.air_wb_num = t.air_wb_num
        AND s.air_wb_cre_dt = t.air_wb_cre_dt
        AND s.air_wb_cre_h2si = t.air_wb_cre_h2si
        AND s.air_wb_pce_num = t.air_wb_pce_num
        AND s.opng_carr_cde = t.opng_carr_cde
        AND s.opng_flt_num = t.opng_flt_num
        AND s.opng_flt_num_sufx_txt = t.opng_flt_num_sufx_txt
        AND s.flt_dep_dt = t.flt_dep_dt
        AND s.leg_orig_arpt_cde = t.leg_orig_arpt_cde
        AND s.leg_dest_arpt_cde = t.leg_dest_arpt_cde
        AND s.eff_fm_cent_tz IS NOT DISTINCT FROM t.eff_fm_cent_tz
)

SELECT *
FROM mismatch_pairs
WHERE mismatched_fields <> ''
ORDER BY mismatched_fields, t_max_latest_job_id,
         air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
         air_wb_pce_num, eff_fm_cent_tz, s_pln_crgo_leg_seq_num
LIMIT 1000;

-- ============================================================================
-- Alternative summary: do mismatches cluster on old job runs?
-- Swap the final SELECT above for this one. If mismatch rows concentrate on
-- old t_max_job_run dates, the differences were written by a previous version
-- of the Glue transform, not produced by the current logic.
-- ============================================================================
-- SELECT
--     mismatched_fields,
--     date_trunc('day', from_unixtime(t_max_latest_job_id)) AS t_job_run_day,
--     count(*) AS pair_cnt,
--     count_if(pair_count > 1) AS fanout_pair_cnt
-- FROM mismatch_pairs
-- WHERE mismatched_fields <> ''
-- GROUP BY 1, 2
-- ORDER BY 1, 2;
