-- ============================================================================
-- cmdty_crgo_pln_leg : full Athena validation + Redshift CDC replica
--
-- Purpose:
--   1) Re-derive the datalake/Athena rows from the source events using the same
--      schema-driven logic in cmdty_crgo_schemas.py.
--   2) Compare those logical rows to datalake_qa1_entp_ctds.cmdty_crgo_pln_leg,
--      field by field: rows are matched on row identity only (AWB piece keys +
--      eff_fm_cent_tz version + pln_crgo_leg_seq_num leg slot) and EVERY other
--      field gets its own mismatch counter (+1 per differing row, +0 on match).
--   3) Reproduce the Redshift-only CDC/effectivity logic from the redshift load
--      path so the complete target shape is available for comparison.
--   4) Explain physical target duplicates: Athena contains latest_job_id, and
--      repeated loads can leave multiple physical rows for the same logical row.
--
-- Source implementation references:
--   - cmdty_crgo_schemas.py: cmdty_crgo_pln_leg StructType
--   - cmdty_crgo_schemas.py: metadata['cmdty_crgo_pln_leg']
--   - cmdty_crgo_schemas.py: get_dep_ld_flag/get_arr_unld_flag/get_pln_leg_type_code
--   - cmdty_crgo_schemas.py: sql_pln_leg_set_eff_dates_and_flags
--   - redshift_clss.py: build_datatype_expr/get_cdc_rs_query/load
--
-- Scope: adjust beg_dt/end_dt in 'params' for the AWB-creation-date window.
--   To compare ALL records from source to target, widen the window to cover
--   the full data range (e.g. DATE '1900-01-01' .. DATE '2099-12-31') -- both
--   tables are scanned in full in that case, so expect cost/runtime.
--
-- Important:
--   The Athena datalake table has latest_job_id, but it does not have the
--   Redshift-only fields eff_to_cent_tz, min_eff_fm_flag, max_eff_to_flag, or
--   original_job_id. Those fields are derived below in expected_redshift_cdc.
--   If the Redshift target is exposed through Athena, add its table in the
--   optional actual_redshift section at the bottom and compare against
--   expected_redshift_cdc.
-- ============================================================================

WITH
params AS (
    SELECT
        DATE '2026-05-20' AS beg_dt,
        DATE '2026-06-07' AS end_dt
),

-- ----------------------------------------------------------------------------
-- 1) Raw/source events. This mirrors the dev extract_filter:
--    df.filter(col('event_name').isin(event_filter_list))
--      .select("*", posexplode(planneditinerary), prev_leg, next_leg)
-- ----------------------------------------------------------------------------
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
    FROM datalake_qa1_entp_ctds.cmdty_crgo_curated c
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

-- ----------------------------------------------------------------------------
-- 2) Datalake transform before duplicate removal.
--    Mirrors every non-partition datalake field in cmdty_crgo_pln_leg.
--
--    Note on sequence number:
--      Dev Spark uses array_position(planneditinerary, exploded_leg), not
--      posexplode's zero-based pos + 1. Athena cannot run array_position on an
--      array<struct> when any struct contains null fields, so first_matching_leg_ord
--      reproduces Spark array_position with a null-safe first-match window.
-- ----------------------------------------------------------------------------
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
        END AS cmdty_flt_leg_type_cde,
        -- latest_job_id is generated at runtime in Python as int(time()).
        -- It is not derivable from source event data, so validation treats it
        -- as a physical-run diagnostic rather than a business-data comparison.
        CAST(NULL AS integer) AS latest_job_id
    FROM exploded
),

-- Mirrors remove_dupes=True for the business columns. The actual Spark job also
-- carries latest_job_id, but one Glue run has a single generated latest_job_id,
-- so it does not change logical duplicate removal.
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

-- ----------------------------------------------------------------------------
-- 3) Actual Athena/datalake target. Keep raw physical rows and logical rows.
-- ----------------------------------------------------------------------------
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
        t.cmdty_flt_leg_type_cde,
        t.latest_job_id
    FROM datalake_qa1_entp_ctds.cmdty_crgo_pln_leg t
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
    SELECT
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
        cmdty_flt_leg_type_cde,
        count(*) AS physical_row_count,
        count(DISTINCT latest_job_id) AS latest_job_id_count
    FROM actual_athena_raw
    GROUP BY
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
    HAVING count(*) > 1
),

-- ----------------------------------------------------------------------------
-- 4) Logical comparison against Athena/datalake.
--
--    Rows are matched on ROW IDENTITY ONLY:
--      which AWB piece     -> air_wb_prfx_id, air_wb_num, air_wb_cre_dt,
--                             air_wb_cre_h2si, air_wb_pce_num
--      which event version -> eff_fm_cent_tz
--      which leg slot      -> pln_crgo_leg_seq_num
--
--    EVERY other field is then compared 1:1 and gets its own mismatch counter
--    in the final output (match adds 0, mismatch adds 1). A difference in an
--    identity field cannot be counted per-field by definition; it surfaces as
--    a missing_in_target + extra_in_target pair instead.
-- ----------------------------------------------------------------------------
joined_datalake AS (
    SELECT
        (s.air_wb_prfx_id IS NOT NULL) AS s_present,
        (t.air_wb_prfx_id IS NOT NULL) AS t_present,
        s.opng_carr_cde AS s_opng_carr_cde,
        t.opng_carr_cde AS t_opng_carr_cde,
        s.opng_flt_num AS s_opng_flt_num,
        t.opng_flt_num AS t_opng_flt_num,
        s.opng_flt_num_sufx_txt AS s_opng_flt_num_sufx_txt,
        t.opng_flt_num_sufx_txt AS t_opng_flt_num_sufx_txt,
        s.flt_dep_dt AS s_flt_dep_dt,
        t.flt_dep_dt AS t_flt_dep_dt,
        s.leg_orig_arpt_cde AS s_leg_orig_arpt_cde,
        t.leg_orig_arpt_cde AS t_leg_orig_arpt_cde,
        s.leg_dest_arpt_cde AS s_leg_dest_arpt_cde,
        t.leg_dest_arpt_cde AS t_leg_dest_arpt_cde,
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
        ON  s.air_wb_prfx_id = t.air_wb_prfx_id
        AND s.air_wb_num = t.air_wb_num
        AND s.air_wb_cre_dt = t.air_wb_cre_dt
        AND s.air_wb_cre_h2si = t.air_wb_cre_h2si
        AND s.air_wb_pce_num = t.air_wb_pce_num
        AND s.eff_fm_cent_tz IS NOT DISTINCT FROM t.eff_fm_cent_tz
        AND s.pln_crgo_leg_seq_num = t.pln_crgo_leg_seq_num
),

-- ----------------------------------------------------------------------------
-- 5) Redshift CDC/effectivity replica.
--    This mirrors sql_pln_leg_set_eff_dates_and_flags. It starts from the
--    source-derived logical rows because Redshift stage is loaded from the
--    datalake table after schema casting.
-- ----------------------------------------------------------------------------
redshift_stage AS (
    SELECT
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
        TIMESTAMP '2099-12-31 00:00:00.000' AS eff_to_cent_tz,
        CAST(0 AS smallint) AS min_eff_fm_flag,
        CAST(0 AS smallint) AS max_eff_to_flag,
        pln_crgo_leg_seq_num,
        pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag,
        pln_crgo_arr_unld_flag,
        cmdty_flt_leg_type_cde,
        CAST(NULL AS integer) AS original_job_id,
        CAST(NULL AS integer) AS latest_job_id
    FROM expected_datalake
),

deduped_stage AS (
    SELECT *
    FROM (
        SELECT
            rs.*,
            row_number() OVER (
                PARTITION BY
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
                ORDER BY eff_fm_cent_tz, COALESCE(latest_job_id, -2147483648) DESC
            ) AS rn
        FROM redshift_stage rs
    ) ranked
    WHERE rn = 1
),

deduped_pln AS (
    SELECT
        air_wb_prfx_id,
        air_wb_num,
        air_wb_cre_dt,
        air_wb_cre_h2si,
        air_wb_pce_num,
        eff_fm_cent_tz,
        array_join(
            array_agg(
                '(' || CAST(pln_crgo_leg_seq_num AS varchar) || ')|'
                    || opng_carr_cde || '|'
                    || CAST(opng_flt_num AS varchar) || '|'
                    || opng_flt_num_sufx_txt || '|'
                    || CAST(flt_dep_dt AS varchar) || '|'
                    || leg_orig_arpt_cde || '|'
                    || leg_dest_arpt_cde
                ORDER BY pln_crgo_leg_seq_num
            ),
            '  ,'
        ) AS sig
    FROM deduped_stage
    GROUP BY
        air_wb_prfx_id,
        air_wb_num,
        air_wb_cre_dt,
        air_wb_cre_h2si,
        air_wb_pce_num,
        eff_fm_cent_tz
),

sig_data AS (
    SELECT
        air_wb_prfx_id,
        air_wb_num,
        air_wb_cre_dt,
        air_wb_cre_h2si,
        air_wb_pce_num,
        eff_fm_cent_tz,
        sig,
        prv_sig
    FROM (
        SELECT
            dp.*,
            lag(sig) OVER (
                PARTITION BY
                    air_wb_prfx_id,
                    air_wb_num,
                    air_wb_cre_dt,
                    air_wb_cre_h2si,
                    air_wb_pce_num
                ORDER BY eff_fm_cent_tz
            ) AS prv_sig
        FROM deduped_pln dp
    ) subq
    WHERE sig <> prv_sig OR prv_sig IS NULL
),

fnl_dup AS (
    SELECT ds.*
    FROM deduped_stage ds
    JOIN sig_data sd
        ON  ds.air_wb_prfx_id = sd.air_wb_prfx_id
        AND ds.air_wb_num = sd.air_wb_num
        AND ds.air_wb_cre_dt = sd.air_wb_cre_dt
        AND ds.air_wb_cre_h2si = sd.air_wb_cre_h2si
        AND ds.air_wb_pce_num = sd.air_wb_pce_num
        AND ds.eff_fm_cent_tz = sd.eff_fm_cent_tz
),

distinct_timestamps AS (
    SELECT DISTINCT
        air_wb_prfx_id,
        air_wb_num,
        air_wb_cre_dt,
        air_wb_cre_h2si,
        air_wb_pce_num,
        eff_fm_cent_tz
    FROM fnl_dup
),

timestamp_windows AS (
    SELECT
        dt.*,
        lead(eff_fm_cent_tz) OVER (
            PARTITION BY
                air_wb_prfx_id,
                air_wb_num,
                air_wb_cre_dt,
                air_wb_cre_h2si,
                air_wb_pce_num
            ORDER BY eff_fm_cent_tz
        ) AS next_eff_tz
    FROM distinct_timestamps dt
),

enriched_data AS (
    SELECT
        n.air_wb_prfx_id,
        n.air_wb_num,
        n.air_wb_cre_dt,
        n.air_wb_cre_h2si,
        n.air_wb_pce_num,
        n.opng_carr_cde,
        n.opng_flt_num,
        n.opng_flt_num_sufx_txt,
        n.flt_dep_dt,
        n.leg_orig_arpt_cde,
        n.leg_dest_arpt_cde,
        n.eff_fm_cent_tz,
        COALESCE(tw.next_eff_tz, TIMESTAMP '2099-12-31 00:00:00.000') AS eff_to_cent_tz,
        CAST(CASE
            WHEN rank() OVER (
                PARTITION BY
                    n.air_wb_prfx_id,
                    n.air_wb_num,
                    n.air_wb_cre_dt,
                    n.air_wb_cre_h2si,
                    n.air_wb_pce_num
                ORDER BY n.eff_fm_cent_tz
            ) = 1 THEN 1
            ELSE 0
        END AS smallint) AS min_eff_fm_flag,
        CAST(CASE
            WHEN tw.next_eff_tz IS NULL THEN 1
            ELSE 0
        END AS smallint) AS max_eff_to_flag,
        n.pln_crgo_leg_seq_num,
        n.pln_max_crgo_leg_seq_num,
        n.pln_crgo_dep_ld_flag,
        n.pln_crgo_arr_unld_flag,
        n.cmdty_flt_leg_type_cde,
        n.original_job_id,
        n.latest_job_id
    FROM fnl_dup n
    INNER JOIN timestamp_windows tw
        ON  n.air_wb_prfx_id = tw.air_wb_prfx_id
        AND n.air_wb_num = tw.air_wb_num
        AND n.air_wb_cre_dt = tw.air_wb_cre_dt
        AND n.air_wb_cre_h2si = tw.air_wb_cre_h2si
        AND n.air_wb_pce_num = tw.air_wb_pce_num
        AND n.eff_fm_cent_tz = tw.eff_fm_cent_tz
),

expected_redshift_cdc AS (
    SELECT
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
        eff_to_cent_tz,
        min_eff_fm_flag,
        max_eff_to_flag,
        pln_crgo_leg_seq_num,
        pln_max_crgo_leg_seq_num,
        pln_crgo_dep_ld_flag,
        pln_crgo_arr_unld_flag,
        cmdty_flt_leg_type_cde,
        original_job_id,
        latest_job_id
    FROM (
        SELECT
            ed.*,
            row_number() OVER (
                PARTITION BY
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
                    eff_to_cent_tz
                ORDER BY eff_to_cent_tz DESC, COALESCE(latest_job_id, -2147483648) DESC
            ) AS rn_final
        FROM enriched_data ed
    ) final_dedup
    WHERE rn_final = 1
)

-- ----------------------------------------------------------------------------
-- Main datalake validation output: every record in the window is compared in
-- both directions, and every non-identity field has its own mismatch counter.
-- ----------------------------------------------------------------------------
SELECT
    (SELECT beg_dt FROM params) AS beg_dt,
    (SELECT end_dt FROM params) AS end_dt,
    (SELECT count(*) FROM transformed) AS source_transformed_physical_count,
    (SELECT count(*) FROM expected_datalake) AS source_logical_count,
    (SELECT count(*) FROM actual_athena_raw) AS target_physical_count,
    (SELECT count(*) FROM actual_athena_logical) AS target_logical_count,
    (SELECT COALESCE(sum(physical_row_count - 1), 0) FROM target_duplicate_groups)
        AS target_extra_physical_duplicate_rows,
    (SELECT count(*) FROM target_duplicate_groups) AS target_duplicate_logical_groups,
    (SELECT COALESCE(max(physical_row_count), 0) FROM target_duplicate_groups)
        AS target_max_physical_rows_per_logical_row,
    (SELECT COALESCE(max(latest_job_id_count), 0) FROM target_duplicate_groups)
        AS target_max_latest_job_ids_per_logical_row,
    count_if(s_present AND t_present) AS matched_on_keys,
    count_if(s_present AND NOT t_present) AS missing_in_target,
    count_if(NOT s_present AND t_present) AS extra_in_target,
    sum(CASE WHEN s_present AND t_present
              AND s_opng_carr_cde IS DISTINCT FROM t_opng_carr_cde
             THEN 1 ELSE 0 END) AS mismatch_opng_carr_cde,
    sum(CASE WHEN s_present AND t_present
              AND s_opng_flt_num IS DISTINCT FROM t_opng_flt_num
             THEN 1 ELSE 0 END) AS mismatch_opng_flt_num,
    sum(CASE WHEN s_present AND t_present
              AND s_opng_flt_num_sufx_txt IS DISTINCT FROM t_opng_flt_num_sufx_txt
             THEN 1 ELSE 0 END) AS mismatch_opng_flt_num_sufx_txt,
    sum(CASE WHEN s_present AND t_present
              AND s_flt_dep_dt IS DISTINCT FROM t_flt_dep_dt
             THEN 1 ELSE 0 END) AS mismatch_flt_dep_dt,
    sum(CASE WHEN s_present AND t_present
              AND s_leg_orig_arpt_cde IS DISTINCT FROM t_leg_orig_arpt_cde
             THEN 1 ELSE 0 END) AS mismatch_leg_orig_arpt_cde,
    sum(CASE WHEN s_present AND t_present
              AND s_leg_dest_arpt_cde IS DISTINCT FROM t_leg_dest_arpt_cde
             THEN 1 ELSE 0 END) AS mismatch_leg_dest_arpt_cde,
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
             THEN 1 ELSE 0 END) AS mismatch_cmdty_flt_leg_type_cde,
    (SELECT count(*) FROM redshift_stage) AS source_redshift_stage_count,
    (SELECT count(*) FROM deduped_stage) AS source_redshift_deduped_stage_count,
    (SELECT count(*) FROM deduped_pln) AS source_redshift_itinerary_signature_count,
    (SELECT count(*) FROM sig_data) AS source_redshift_changed_signature_count,
    (SELECT count(*) FROM fnl_dup) AS source_redshift_rows_after_signature_filter_count,
    (SELECT count(*) FROM expected_redshift_cdc) AS source_expected_redshift_cdc_count
FROM joined_datalake;

-- ============================================================================
-- Optional diagnostics:
-- Copy the WITH block above and replace the main SELECT with one of these.
-- ============================================================================
-- 1) See which logical rows are physically duplicated in Athena.
--
-- SELECT *
-- FROM target_duplicate_groups
-- ORDER BY physical_row_count DESC, latest_job_id_count DESC,
--          air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
--          air_wb_pce_num, eff_fm_cent_tz, pln_crgo_leg_seq_num;
--
-- 2) See exact datalake row differences.
--
-- SELECT 'missing_in_athena' AS diff_type, * FROM (
--     SELECT * FROM expected_datalake EXCEPT SELECT * FROM actual_athena_logical
-- )
-- UNION ALL
-- SELECT 'extra_in_athena' AS diff_type, * FROM (
--     SELECT * FROM actual_athena_logical EXCEPT SELECT * FROM expected_datalake
-- )
-- ORDER BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
--          air_wb_pce_num, eff_fm_cent_tz, pln_crgo_leg_seq_num, diff_type;
--
-- 3) Inspect the fully derived Redshift-style CDC rows.
--
-- SELECT *
-- FROM expected_redshift_cdc
-- ORDER BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si,
--          air_wb_pce_num, eff_fm_cent_tz, pln_crgo_leg_seq_num;
--
-- 4) If the Redshift target is exposed through Athena, add an actual_redshift
--    CTE with the same columns as expected_redshift_cdc, then compare:
--
-- SELECT 'missing_in_redshift' AS diff_type, * FROM (
--     SELECT * FROM expected_redshift_cdc EXCEPT SELECT * FROM actual_redshift
-- )
-- UNION ALL
-- SELECT 'extra_in_redshift' AS diff_type, * FROM (
--     SELECT * FROM actual_redshift EXCEPT SELECT * FROM expected_redshift_cdc
-- );
