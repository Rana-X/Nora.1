"""Table schemas from the 'Commodity Cargo' data domain."""


from time import time

from glue_clss import KEYS_PRIMARY, PRPS_PARTITION, PRPS_REDSHIFT_ONLY # type: ignore
from pyspark.sql import Column, DataFrame # type: ignore
from pyspark.sql.functions import coalesce, col, lit, split, when, to_date, to_timestamp, date_format, get_json_object, trim, lower, expr, regexp_replace, size, transform, filter, translate, array, element_at, posexplode, posexplode_outer, length, max as spark_max # type: ignore
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType, ShortType, DecimalType, DateType, TimestampType # type: ignore
from pyspark.sql.window import Window # type: ignore

PRPS_DATALAKE_ONLY = 'd'

latest_job_id = int(time())
event_filter_list = ['piece-created', 'piece-itinerary-changed', 'piece-wab-added', 'piece-wab-changed', 'piece-loaded-on-flight', 'piece-unloaded-from-flight', 'piece-offloaded-from-flight', 'piece-undeleted', 'piece-deleted', 'piece-updated', 'piece-tracking', 'piece-loaded-by-backup-mode', 'piece-not-loaded-by-backup-mode']
scan_leg_filter_list =['piece-loaded-on-flight', 'piece-unloaded-from-flight', 'piece-offloaded-from-flight','piece-loaded-by-backup-mode','piece-not-loaded-by-backup-mode','piece-tracking']


def add_max_seq_num(_, df) -> DataFrame:
    """Add max_seq_num column for latest instance identification"""
    window = Window.partitionBy("air_wb_prfx_id", "air_wb_num", "air_wb_cre_dt", "air_wb_cre_h2si", "air_wb_pce_num")
    return df.withColumn("max_seq_num", spark_max(col("pos") + 1).over(window))


def map_event_name_to_code(_) -> Column:
    """Maps cargo event names to standardized 2-character codes."""
    event_col = lower(trim(col("event_name")))

    return (
        when(event_col == "piece-created",               lit("CR"))
        .when(event_col == "piece-itinerary-changed",    lit("IC"))
        .when(event_col == "piece-wab-added",            lit("WA"))
        .when(event_col == "piece-wab-changed",          lit("WC"))
        .when(event_col == "piece-loaded-on-flight",     lit("DL"))
        .when(event_col == "piece-offloaded-from-flight", lit("DU"))
        .when(event_col == "piece-unloaded-from-flight",  lit("AU"))
        .when(event_col == "piece-undeleted",             lit("CZ"))
        .when(event_col == "piece-deleted",               lit("CD"))
        .when(event_col == "piece-updated",               lit("CC"))
        .when(event_col == "piece-tracking",              lit("TS"))
        .when(event_col == "piece-loaded-by-backup-mode",    lit("BL"))
        .when(event_col == "piece-not-loaded-by-backup-mode",lit("BN"))
        .otherwise(lit("-"))
    )


def get_evnt_asgd_arpt_cde(_):
    """Determines the assigned airport code for an event in the EVENT TABLE.

    - Uses the exploded_scan_code directly (no array indexing needed).
    - Else, fetches the departure IATA code from the originating flight leg.
    - Defaults to '-' if neither is available.
    """
    return (
        when(
            col("exploded_scan_code").isNotNull() & col("exploded_scan_code").scaniatastationcode.isNotNull(),
            col("exploded_scan_code").scaniatastationcode
        )
        .when(
             (col("planneditinerary").isNotNull()) & (size(col("planneditinerary")) > 0),
            expr("filter(planneditinerary, x -> x.flightlegtypeid = 'originating')[0].flightleg.departureairportiatacode")
        )
        .otherwise(lit('-'))
    )


def default_if_null_or_empty(column_name, default_value):
    """
    Returns default value if column is null or empty, otherwise returns column value.

    Args:
        column_name (str): Name of the column to check
        default_value: Default value to return if column is null or empty

    Returns:
        pyspark.sql.Column: Column expression with default handling
    """
    return when((col(column_name).isNull()) | (trim(col(column_name)) == ""), lit(default_value)).otherwise(col(column_name))


def add_matched_scan_codes_column(_, df) -> DataFrame:
    """Adds matched_scan_codes column containing scans that match event creation time.

    Filters commodityscans array to include only scans where scantime equals
    event_created timestamp. Returns empty array if no commodity scans exist.
    """
    return df.withColumn("matched_scan_codes",
                        when(col("commodityscans").isNotNull() & (size(col("commodityscans")) > 0),
                            filter(
                                col("commodityscans"),
                                lambda x: to_timestamp(x["scantime"]) == to_timestamp(col("event_created"))
                            )
                        ).otherwise(array()))


def add_matched_scan_codes_and_explode(_, df) -> DataFrame:
    """Adds matched_scan_codes column containing scans that match event creation time AND event type, then explodes them into separate rows."""
    # First add the matched_scan_codes column
    df_with_matched = df.withColumn("matched_scan_codes",
                        when(col("commodityscans").isNotNull() & (size(col("commodityscans")) > 0),
                            filter(
                                col("commodityscans"),
                                lambda x: (
                                    (to_timestamp(x["scantime"]) == to_timestamp(col("event_created"))) &
                                    (
                                        # Match scan type to event name
                                        (x["scantype"] == col("event_name")) |
                                        # OR for tracking events, match PieceTracking* pattern
                                        ((col("event_name") == "piece-tracking") & (x["scantype"].like("PieceTracking%")))
                                    )
                                )
                            )
                        ).otherwise(array()))

    return df_with_matched.select(
        "*",
        posexplode_outer(col("matched_scan_codes")).alias("scan_code_pos", "exploded_scan_code")
    ).drop("matched_scan_codes")  # Remove the original array column


def get_flight_leg_type_code(_) -> Column:
    """Returns flight leg type code by matching commodity scan with planned itinerary.

    Maps flight leg type IDs to numeric codes:
    - 'originating' -> '0'
    - 'transfer' -> '1'
    - 'thru' -> '2'
    - 'unknown' or other -> '-'
    """
    filtered_scan = get_first_matched_scan_code().flightleg
    return when(
        col("planneditinerary").isNotNull() &
        col("commodityscans").isNotNull() &
        (size(col("commodityscans")) > 0),
            element_at(
                transform(
                    filter(
                        col("planneditinerary"),
                        lambda planned: (
                            (planned['flightleg']['departureairportiatacode'] == filtered_scan['departureairportiatacode']) &
                            (planned['flightleg']['arrivalairportiatacode'] == filtered_scan['arrivalairportiatacode']) &
                            (planned['flightleg']['flightnumber'] == filtered_scan['flightnumber']) &
                            (planned['flightleg']['operationalcarrier'] == filtered_scan['operationalcarrier'])
                        )
                    ),
                    lambda planned: when(lower(trim(planned['flightlegtypeid'])) == 'unknown', lit('-'))
                                   .when(lower(trim(planned['flightlegtypeid'])) == 'originating', lit('0'))
                                   .when(lower(trim(planned['flightlegtypeid'])) == 'transfer', lit('1'))
                                   .when(lower(trim(planned['flightlegtypeid'])) == 'thru', lit('2'))
                                   .when(lower(trim(planned['flightlegtypeid'])) == 'standbyearly', lit('3'))
                                   .otherwise(lit('-'))
                ),
                1
            )
    ).otherwise(lit('-'))


def get_first_matched_scan_code() -> Column:
    """Returns the first element from matched_scan_codes array if it exists and is not empty."""
    return when(
        (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0),
        col("matched_scan_codes")[0]
    ).otherwise(lit(None))


def get_tracking_location_info(column_name) -> Column:
    """Returns tracking location field value for piece-tracking events using exploded scan code.

    Args:
        column_name (str): Field name from trackingLocation object to extract

    Returns:
        Column: Tracking location field value or '-' if not piece-tracking event or null/blank
    """
    trackinglocation = when(
        (col("event_name") == 'piece-tracking') &
        col("exploded_scan_code").isNotNull() &
        col("exploded_scan_code").scantype.like("PieceTracking%"),
        col("exploded_scan_code").trackinglocation
    ).otherwise(lit(None))

    return when(
        (trackinglocation.isNotNull()) &
        (trackinglocation[column_name].isNotNull()) &
        (trim(trackinglocation[column_name]) != ''),
        trackinglocation[column_name]
    ).otherwise(lit("-"))


def get_exploded_scan_code_field(field_name, default="-") -> Column:
    """Returns a field from exploded_scan_code column, or default if null.

    Args:
        field_name (str): Field name to extract from the exploded scan code struct
        default: Default value if exploded_scan_code is null

    Returns:
        Column: The field value or default
    """
    return when(
        col("exploded_scan_code").isNotNull(),
        col("exploded_scan_code")[field_name]
    ).otherwise(lit(default))


def create_sql_select_cols(self, exclude_cols=[]) -> str:
    """Creates comma-separated SQL column list from schema fields.

    Filters out fields with PRPS_PARTITION or PRPS_DATALAKE_ONLY purposes
    and sanitizes field names for SQL compatibility.

    Args:
        exclude_cols: List of column names to exclude from selection

    Returns:
        str: Comma-separated list of sanitized column names for SQL SELECT
    """
    return ', '.join([
        fldnm
        for fld in self.get_schema().fields
        if fld.metadata.get('purpose') not in (PRPS_PARTITION, PRPS_DATALAKE_ONLY)
        if fld.name not in exclude_cols
        if (fldnm := self.sanitize_sql(fld.name))
    ])


def get_evnt_scan_leg_seq_num(_) -> Column:
    """Returns the sequence number of the commodity scan matching the event creation time.
    This function finds the position (1-based index) of the commodity scan whose scantime
    matches the event_created timestamp within the commodityscans array
    """
    return when(
        col("commodityscans").isNotNull() & (size(col("commodityscans")) > 0),
        coalesce(
            expr("""
                array_position(
                    transform(commodityscans, x -> to_timestamp(x.scantime)),
                    to_timestamp(event_created)
                )
            """),
            lit(0)
        )
    ).otherwise(lit(0))


def sql_cust_sql_distinct_stage(self, **kwargs):
    """Custom SQL to remove duplicates from staging table while preserving latest_job_id"""
    src_schema = kwargs['src_schema']
    tbl_nm = kwargs['tbl_nm']

    ord_keys = self.tbl_metadata.get("orderby_keys", [])
    pks = self.get_keys({KEYS_PRIMARY})

    # Exclude system columns from partitioning
    excluded_cols = ['original_job_id', 'latest_job_id', 'eff_fm_cent_tz',
                     'eff_to_cent_tz', 'min_eff_fm_flag', 'max_eff_to_flag']

    # Build ORDER BY clause
    order_parts = []
    if ord_keys:
        order_parts.extend([f"{key} desc" for key in ord_keys])
    if tbl_nm == 'cmdty_crgo_evnt':
        order_parts.append("scan_leg_ct desc")
    order_parts.append("latest_job_id desc")

    return f"""
        CREATE TEMP TABLE {tbl_nm}_uniquerecs AS
        SELECT {create_sql_select_cols(self)}
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {', '.join(pks + ord_keys)}
                       ORDER BY {', '.join(order_parts)}
                   ) as rn
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {create_sql_select_cols(self, excluded_cols)}
                           ORDER BY {', '.join(ord_keys)}
                       ) as rn_dup
                FROM {src_schema}.{tbl_nm}
            ) dedup
            WHERE rn_dup = 1
        ) ranked
        WHERE rn = 1;

        DELETE FROM {src_schema}.{tbl_nm};

        INSERT INTO {src_schema}.{tbl_nm}
        SELECT * FROM {tbl_nm}_uniquerecs;

        DROP TABLE {tbl_nm}_uniquerecs;
    """


def sql_cust_delete_unchanged_from_staging(self, src_schema, tgt_schema, tbl_nm, **_kwargs):
    """Custom SQL to delete unchanged records from staging"""
    exclude_cols = ['original_job_id', 'latest_job_id']

    # Tables that should include eff columns in join conditions
    tables_with_eff_cols = ['cmdty_crgo', 'cmdty_crgo_pln_leg', 'cmdty_crgo_hndlg']

    def create_sql_join_cols(self, excl_cols_list) -> str:
        return ' and '.join([
                f"({tgt_schema}.{tbl_nm}.{fldnm} = {src_schema}.{tbl_nm}.{fldnm}" + \
                  (f" OR {tgt_schema}.{tbl_nm}.{fldnm} is null and {src_schema}.{tbl_nm}.{fldnm} is null " if fld.nullable else "") +
                ")"
                for fld in self.get_schema().fields
                if fld.name not in excl_cols_list
                if ((tbl_nm in tables_with_eff_cols and 'eff' in fld.name) or fld.metadata.get('purpose') not in (PRPS_PARTITION, PRPS_REDSHIFT_ONLY, PRPS_DATALAKE_ONLY))
                if (fldnm := self.sanitize_sql(fld.name))
            ])

    return f"""
        delete from {src_schema}.{tbl_nm}
        using {tgt_schema}.{tbl_nm}
        where {create_sql_join_cols(self, exclude_cols)}
    """


def sql_delete_older_records_from_target(self, src_schema, tgt_schema, tbl_nm, **_kwargs):
    """Hard delete existing records that will be replaced.
    Deletes based on primary keys + orderby keys (includes eff_fm_cent_tz for CDC tables)."""

    pks = self.get_keys({KEYS_PRIMARY})
    ord_keys = pks + self.tbl_metadata.get("orderby_keys", [])
    col_list = [pk for pk in ord_keys if pk != 'eff_fm_cent_tz']

    if tbl_nm == 'cmdty_crgo_leg':
        col_list = ['air_wb_prfx_id', 'air_wb_num', 'air_wb_cre_dt', 'air_wb_cre_h2si', 'air_wb_pce_num']

    return f"""
    DELETE FROM {tgt_schema}.{tbl_nm}
    WHERE EXISTS (
        SELECT 1 FROM {src_schema}.{tbl_nm} s
        WHERE {' and '.join([
                f"({tgt_schema}.{tbl_nm}.{fld} = s.{fld} )"
                for fld in col_list
            ])}
    );"""



def sql_cdc_stage_target_union(_self, src_schema, tgt_schema, tbl_nm, **_kwargs):
    """Efficient CDC logic that appends matching target records to staging table.
    Only adds target records that match staging keys for change detection.

    For CDC tables with eff_fm_cent_tz, this pulls ALL versions of matching records
    so they can be reprocessed together in staging."""
    return f"""
        INSERT INTO {src_schema}.{tbl_nm}
        SELECT t.*
        FROM {tgt_schema}.{tbl_nm} t
        WHERE EXISTS (
            SELECT 1 FROM {src_schema}.{tbl_nm} s
            WHERE t.air_wb_prfx_id = s.air_wb_prfx_id
              AND t.air_wb_num = s.air_wb_num
              AND t.air_wb_cre_dt = s.air_wb_cre_dt
              AND t.air_wb_cre_h2si = s.air_wb_cre_h2si
              AND t.air_wb_pce_num = s.air_wb_pce_num
        )
    """

def sql_delete_outdated_staging_records(_self, src_schema, tgt_schema, tbl_nm, **_kwargs):
    """Remove staging records with lower scan_leg_ct than existing target records."""
    return f"""
        DELETE FROM {src_schema}.{tbl_nm}
        WHERE EXISTS (
            SELECT 1 FROM {tgt_schema}.{tbl_nm} t
            WHERE t.air_wb_prfx_id = {src_schema}.{tbl_nm}.air_wb_prfx_id
              AND t.air_wb_num = {src_schema}.{tbl_nm}.air_wb_num
              AND t.air_wb_cre_dt = {src_schema}.{tbl_nm}.air_wb_cre_dt
              AND t.air_wb_cre_h2si = {src_schema}.{tbl_nm}.air_wb_cre_h2si
              AND t.air_wb_pce_num = {src_schema}.{tbl_nm}.air_wb_pce_num
              AND t.cmdty_crgo_evnt_type_cde = {src_schema}.{tbl_nm}.cmdty_crgo_evnt_type_cde
              AND t.evnt_asgd_arpt_cde = {src_schema}.{tbl_nm}.evnt_asgd_arpt_cde
              AND t.scan_leg_ct > {src_schema}.{tbl_nm}.scan_leg_ct
        );
    """

# Custom SQL to Apply on CMDTY_CRGO_EVNT to derive MAX_EVNT_FLAG
def sql_update_max_evnt_flag(_self, **kwargs):
    """Simple, efficient max_evnt_flag update for high-volume production"""
    src_schema = kwargs['src_schema']
    tbl_nm = kwargs['tbl_nm']
    return f"""
        WITH ranked_events AS (
            SELECT air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num,
                   evnt_tz, cmdty_crgo_evnt_type_cde, evnt_asgd_arpt_cde,
                   ROW_NUMBER() OVER (
                       PARTITION BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num
                       ORDER BY evnt_tz DESC
                   ) as rn
            FROM {src_schema}.{tbl_nm}
        )
        UPDATE {src_schema}.{tbl_nm}
        SET max_evnt_flag = CASE WHEN r.rn = 1 THEN 1 ELSE 0 END
        FROM ranked_events r
        WHERE {src_schema}.{tbl_nm}.air_wb_prfx_id = r.air_wb_prfx_id
          AND {src_schema}.{tbl_nm}.air_wb_num = r.air_wb_num
          AND {src_schema}.{tbl_nm}.air_wb_cre_dt = r.air_wb_cre_dt
          AND {src_schema}.{tbl_nm}.air_wb_cre_h2si = r.air_wb_cre_h2si
          AND {src_schema}.{tbl_nm}.air_wb_pce_num = r.air_wb_pce_num
          AND {src_schema}.{tbl_nm}.evnt_tz = r.evnt_tz
          AND {src_schema}.{tbl_nm}.cmdty_crgo_evnt_type_cde = r.cmdty_crgo_evnt_type_cde
          AND {src_schema}.{tbl_nm}.evnt_asgd_arpt_cde = r.evnt_asgd_arpt_cde;
    """

def sql_update_scan_reqr_crgo_arr_unld_flag(_self, **kwargs):
    """Update scan_reqr_bag_arr_unld_flag based on event name and next leg comparison"""
    src_schema = kwargs['src_schema']
    tbl_nm = kwargs['tbl_nm']
    return f"""
        WITH windowed_data AS (
            SELECT *,
                LEAD(leg_orig_arpt_cde) OVER (
                    PARTITION BY air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num, evnt_tz, opng_carr_cde, opng_flt_num, opng_flt_num_sufx_txt, flt_dep_dt
                    ORDER BY evnt_scan_leg_seq_num ASC
                ) AS next_leg_orig_arpt_cde
            FROM {src_schema}.{tbl_nm}
        )
        UPDATE {src_schema}.{tbl_nm}
        SET scan_reqr_crgo_arr_unld_flag = CASE
            WHEN w.scan_crgo_arr_unld_flag = 1 THEN 1
            WHEN w.leg_dest_arpt_cde = w.next_leg_orig_arpt_cde THEN 0
            ELSE 1
        END
        FROM windowed_data w
        WHERE {src_schema}.{tbl_nm}.air_wb_num = w.air_wb_num
          AND {src_schema}.{tbl_nm}.air_wb_cre_dt = w.air_wb_cre_dt
          AND {src_schema}.{tbl_nm}.air_wb_cre_h2si = w.air_wb_cre_h2si
          AND {src_schema}.{tbl_nm}.air_wb_pce_num = w.air_wb_pce_num
          AND {src_schema}.{tbl_nm}.evnt_tz = w.evnt_tz
          AND {src_schema}.{tbl_nm}.evnt_scan_leg_seq_num = w.evnt_scan_leg_seq_num;
    """


def sql_cust_sql_cmdty_crgo_leg(_self, src_schema, tgt_schema, tbl_nm, **_kwargs):
    """
    V2 Custom SQL for cmdty_crgo_leg table combining planned and scanned cargo legs.

    The logic flow is:
    1. deleted_awbs: Identify 'D'eleted AWB pieces to exclude from processing.
    2. staged_awbs: Select the unique AWB pieces present in the event table.
    3. scan_leg_at_max_evnt: For each scan leg partition (AWB keys + flight leg keys
       + derived asgn_arpt_cde + leg_type), find the latest event using ROW_NUMBER.
    4. scan_leg_latest: Keep only the latest event per partition (rn=1) and derive
       scan flags (dep_ld, dep_thru, arr_unld) and their corresponding timestamps.
    5. scan_leg_agg: Aggregate scan leg data per leg key (11 cols), keeping only rows
       where at least one departure load or arrival unload timestamp exists.
    6. scan_leg_with_seq: Derive SCAN_LEG_SEQ_NUM (ROW_NUMBER) and MAX_SCAN_LEG_SEQ_NUM
       (COUNT) per AWB piece, ordered by dep/arr timestamps and source seq num.
    7. pln_leg_filtered: Select valid planned legs from CMDTY_CRGO_PLN_LEG that exist
       in staged events, are within effective date range (max_evnt_flag=1), and are
       not deleted.
    8. combined_legs: FULL OUTER JOIN between planned (pln_leg_filtered) and scanned
       (scan_leg_with_seq) data on the full leg key, using MAX/COALESCE to merge.
    9. evnt_wt_bal: Find the latest Weight/Balance event (WA/WC) per AWB + airport.
    10. evnt_pce_wt: Find the latest Piece Count event (CR/CC) per AWB.
    11. Final SELECT DISTINCT: Apply all column-level transformations (flag defaults,
        derived onboard/error flags), join with weight/count events, dedup, and
        materialize into a temp table.
    12. Delete/Load: Delete staging, insert from temp table, drop temp table.

    """
    return f"""
        CREATE TEMP TABLE {tbl_nm}_crgo_leg_result AS
        WITH deleted_awbs AS (
            SELECT DISTINCT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM
            FROM {tgt_schema}.CMDTY_CRGO
            WHERE CMDTY_CRGO_PCE_STAT_CDE = 'D'
              AND EFF_TO_CENT_TZ = '2099-12-31 00:00:00.000000'::timestamptz
        ),
        event_stg AS (
            SELECT DISTINCT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM
            FROM {src_schema}.{tbl_nm}
            UNION
            SELECT DISTINCT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM
            FROM {src_schema}.CMDTY_CRGO_EVNT_SCAN_LEG
        ),
        latest_evnts AS (
            SELECT E.*
            FROM {tgt_schema}.CMDTY_CRGO_EVNT E
            INNER JOIN event_stg S
                ON  S.AIR_WB_PRFX_ID  = E.AIR_WB_PRFX_ID
                AND S.AIR_WB_NUM       = E.AIR_WB_NUM
                AND S.AIR_WB_CRE_DT    = E.AIR_WB_CRE_DT
                AND S.AIR_WB_CRE_H2SI  = E.AIR_WB_CRE_H2SI
                AND S.AIR_WB_PCE_NUM   = E.AIR_WB_PCE_NUM
        ),
        evnt_w AS (
            SELECT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
                EVNT_ASGD_ARPT_CDE AS LEG_ORIG_ARPT_CDE,
                CMDTY_CRGO_WT_BAL_CDE,
                EVNT_TZ AS SCAN_CRGO_WT_BAL_EVNT_TZ
            FROM latest_evnts
            WHERE CMDTY_CRGO_EVNT_TYPE_CDE IN ('WA', 'WC')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT,
                             AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM, EVNT_ASGD_ARPT_CDE
                ORDER BY EVNT_TZ DESC
            ) = 1
        ),
        evnt_pc AS (
            SELECT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
                CRGO_PCE_WT_LBS_QTY
            FROM latest_evnts
            WHERE CMDTY_CRGO_EVNT_TYPE_CDE IN ('CR', 'CC')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT,
                             AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM
                ORDER BY EVNT_TZ DESC
            ) = 1
        ),
        plan_leg AS (
            SELECT
                P.AIR_WB_PRFX_ID, P.AIR_WB_NUM, P.AIR_WB_CRE_DT, P.AIR_WB_CRE_H2SI, P.AIR_WB_PCE_NUM,
                P.OPNG_CARR_CDE, P.OPNG_FLT_NUM, P.OPNG_FLT_NUM_SUFX_TXT, P.FLT_DEP_DT,
                P.LEG_ORIG_ARPT_CDE, P.LEG_DEST_ARPT_CDE,
                P.CMDTY_FLT_LEG_TYPE_CDE,
                P.PLN_CRGO_LEG_SEQ_NUM,
                P.PLN_MAX_CRGO_LEG_SEQ_NUM,
                P.PLN_CRGO_DEP_LD_FLAG,
                P.PLN_CRGO_ARR_UNLD_FLAG,
                P.EFF_TO_CENT_TZ,
                P.ORIGINAL_JOB_ID AS PLN_ORIGINAL_JOB_ID,
                P.LATEST_JOB_ID   AS PLN_LATEST_JOB_ID
            FROM {tgt_schema}.CMDTY_CRGO_PLN_LEG P
            INNER JOIN latest_evnts E
                ON  P.AIR_WB_PRFX_ID  = E.AIR_WB_PRFX_ID
                AND P.AIR_WB_NUM       = E.AIR_WB_NUM
                AND P.AIR_WB_CRE_DT    = E.AIR_WB_CRE_DT
                AND P.AIR_WB_CRE_H2SI  = E.AIR_WB_CRE_H2SI
                AND P.AIR_WB_PCE_NUM   = E.AIR_WB_PCE_NUM
                AND E.MAX_EVNT_FLAG    = 1
                AND P.EFF_FM_CENT_TZ  <= E.EVNT_TZ
                AND P.EFF_TO_CENT_TZ   > E.EVNT_TZ
            WHERE NOT EXISTS (
                SELECT 1 FROM deleted_awbs D
                WHERE D.AIR_WB_PRFX_ID  = P.AIR_WB_PRFX_ID
                  AND D.AIR_WB_NUM       = P.AIR_WB_NUM
                  AND D.AIR_WB_CRE_DT    = P.AIR_WB_CRE_DT
                  AND D.AIR_WB_CRE_H2SI  = P.AIR_WB_CRE_H2SI
                  AND D.AIR_WB_PCE_NUM   = P.AIR_WB_PCE_NUM
            )
        ),
        plan_leg_with_max_eff AS (
            SELECT
                P.*,
                MAX(P.EFF_TO_CENT_TZ) OVER (
                    PARTITION BY P.AIR_WB_PRFX_ID, P.AIR_WB_NUM, P.AIR_WB_CRE_DT,
                                 P.AIR_WB_CRE_H2SI, P.AIR_WB_PCE_NUM,
                                 P.OPNG_CARR_CDE, P.OPNG_FLT_NUM, P.OPNG_FLT_NUM_SUFX_TXT,
                                 P.FLT_DEP_DT, P.LEG_ORIG_ARPT_CDE, P.LEG_DEST_ARPT_CDE
                ) AS PLN_MAX_EFF_TO_CENT_TZ
            FROM plan_leg P
        ),
        scan_leg_raw AS (
            SELECT
                S.AIR_WB_PRFX_ID, S.AIR_WB_NUM, S.AIR_WB_CRE_DT, S.AIR_WB_CRE_H2SI, S.AIR_WB_PCE_NUM,
                S.OPNG_CARR_CDE, S.OPNG_FLT_NUM, S.OPNG_FLT_NUM_SUFX_TXT, S.FLT_DEP_DT,
                S.LEG_ORIG_ARPT_CDE, S.LEG_DEST_ARPT_CDE,
                S.CMDTY_FLT_LEG_TYPE_CDE,
                S.EVNT_TZ,
                S.EVNT_SCAN_LEG_SEQ_NUM,
                S.SCAN_CRGO_DEP_LD_FLAG,
                S.SCAN_CRGO_DEP_THRU_FLAG,
                S.SCAN_CRGO_ARR_UNLD_FLAG,
                S.SCAN_REQR_CRGO_ARR_UNLD_FLAG,
                S.ORIGINAL_JOB_ID AS SCAN_ORIGINAL_JOB_ID,
                S.LATEST_JOB_ID   AS SCAN_LATEST_JOB_ID,
                CASE WHEN S.SCAN_CRGO_ARR_UNLD_FLAG = 1
                     THEN S.LEG_DEST_ARPT_CDE
                     ELSE S.LEG_ORIG_ARPT_CDE
                END AS ASGN_ARPT_CDE
            FROM {tgt_schema}.CMDTY_CRGO_EVNT_SCAN_LEG S
            WHERE EXISTS (
                SELECT 1 FROM latest_evnts E
                WHERE S.AIR_WB_PRFX_ID  = E.AIR_WB_PRFX_ID
                  AND S.AIR_WB_NUM       = E.AIR_WB_NUM
                  AND S.AIR_WB_CRE_DT    = E.AIR_WB_CRE_DT
                  AND S.AIR_WB_CRE_H2SI  = E.AIR_WB_CRE_H2SI
                  AND S.AIR_WB_PCE_NUM   = E.AIR_WB_PCE_NUM
            )
            AND NOT EXISTS (
                SELECT 1 FROM deleted_awbs D
                WHERE S.AIR_WB_PRFX_ID  = D.AIR_WB_PRFX_ID
                  AND S.AIR_WB_NUM       = D.AIR_WB_NUM
                  AND S.AIR_WB_CRE_DT    = D.AIR_WB_CRE_DT
                  AND S.AIR_WB_CRE_H2SI  = D.AIR_WB_CRE_H2SI
                  AND S.AIR_WB_PCE_NUM   = D.AIR_WB_PCE_NUM
            )
        ),
        scan_leg_at_max AS (
            SELECT
                R.*,
                MAX(R.EVNT_TZ) OVER (
                    PARTITION BY R.AIR_WB_PRFX_ID, R.AIR_WB_NUM, R.AIR_WB_CRE_DT,
                                 R.AIR_WB_CRE_H2SI, R.AIR_WB_PCE_NUM,
                                 R.OPNG_CARR_CDE, R.OPNG_FLT_NUM, R.OPNG_FLT_NUM_SUFX_TXT,
                                 R.FLT_DEP_DT, R.LEG_ORIG_ARPT_CDE, R.LEG_DEST_ARPT_CDE,
                                 R.ASGN_ARPT_CDE, R.CMDTY_FLT_LEG_TYPE_CDE
                ) AS MAX_ARPT_EVNT_TZ
            FROM scan_leg_raw R
        ),
        scan_leg_latest AS (
            SELECT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
                OPNG_CARR_CDE, OPNG_FLT_NUM, OPNG_FLT_NUM_SUFX_TXT, FLT_DEP_DT,
                LEG_ORIG_ARPT_CDE, LEG_DEST_ARPT_CDE,
                CMDTY_FLT_LEG_TYPE_CDE,
                EVNT_TZ,
                EVNT_SCAN_LEG_SEQ_NUM,
                SCAN_CRGO_DEP_LD_FLAG,
                SCAN_CRGO_DEP_THRU_FLAG,
                SCAN_CRGO_ARR_UNLD_FLAG,
                SCAN_REQR_CRGO_ARR_UNLD_FLAG,
                SCAN_ORIGINAL_JOB_ID,
                SCAN_LATEST_JOB_ID,
                CASE WHEN SCAN_CRGO_DEP_LD_FLAG = 1 OR SCAN_CRGO_DEP_THRU_FLAG = 1
                     THEN EVNT_TZ
                END AS SCAN_CRGO_DEP_LD_EVNT_TZ,
                CASE WHEN SCAN_CRGO_ARR_UNLD_FLAG = 1
                     THEN EVNT_TZ
                END AS SCAN_CRGO_ARR_UNLD_EVNT_TZ
            FROM scan_leg_at_max
            WHERE EVNT_TZ = MAX_ARPT_EVNT_TZ
        ),
        scan_leg_agg AS (
            SELECT
                AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
                OPNG_CARR_CDE, OPNG_FLT_NUM, OPNG_FLT_NUM_SUFX_TXT, FLT_DEP_DT,
                LEG_ORIG_ARPT_CDE, LEG_DEST_ARPT_CDE, CMDTY_FLT_LEG_TYPE_CDE,
                MAX(SCAN_CRGO_DEP_LD_FLAG)          AS SCAN_CRGO_DEP_LD_FLAG,
                MAX(SCAN_CRGO_DEP_THRU_FLAG)        AS SCAN_CRGO_DEP_THRU_FLAG,
                MAX(SCAN_CRGO_ARR_UNLD_FLAG)        AS SCAN_CRGO_ARR_UNLD_FLAG,
                MAX(SCAN_REQR_CRGO_ARR_UNLD_FLAG)   AS SCAN_REQR_CRGO_ARR_UNLD_FLAG,
                MAX(SCAN_CRGO_DEP_LD_EVNT_TZ)       AS SCAN_CRGO_DEP_LD_EVNT_TZ,
                MAX(SCAN_CRGO_ARR_UNLD_EVNT_TZ)     AS SCAN_CRGO_ARR_UNLD_EVNT_TZ,
                MAX(EVNT_SCAN_LEG_SEQ_NUM)           AS EVNT_SCAN_LEG_SEQ_NUM,
                MAX(SCAN_ORIGINAL_JOB_ID)            AS SCAN_ORIGINAL_JOB_ID,
                MAX(SCAN_LATEST_JOB_ID)              AS SCAN_LATEST_JOB_ID
            FROM scan_leg_latest
            WHERE SCAN_CRGO_DEP_LD_EVNT_TZ IS NOT NULL
               OR SCAN_CRGO_ARR_UNLD_EVNT_TZ IS NOT NULL
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
        ),
        scan_leg_with_seq AS (
            SELECT
                SLA.*,
                ROW_NUMBER() OVER (
                    PARTITION BY SLA.AIR_WB_PRFX_ID, SLA.AIR_WB_NUM, SLA.AIR_WB_CRE_DT,
                                 SLA.AIR_WB_CRE_H2SI, SLA.AIR_WB_PCE_NUM
                    ORDER BY COALESCE(SLA.SCAN_CRGO_DEP_LD_EVNT_TZ, SLA.SCAN_CRGO_ARR_UNLD_EVNT_TZ),
                             SL.EVNT_SCAN_LEG_SEQ_NUM
                ) AS SCAN_LEG_SEQ_NUM,
                COUNT(*) OVER (
                    PARTITION BY SLA.AIR_WB_PRFX_ID, SLA.AIR_WB_NUM, SLA.AIR_WB_CRE_DT,
                                 SLA.AIR_WB_CRE_H2SI, SLA.AIR_WB_PCE_NUM
                ) AS MAX_SCAN_LEG_SEQ_NUM
            FROM scan_leg_agg SLA
            JOIN {tgt_schema}.CMDTY_CRGO_EVNT_SCAN_LEG SL
                ON  SL.AIR_WB_PRFX_ID        = SLA.AIR_WB_PRFX_ID
                AND SL.AIR_WB_NUM             = SLA.AIR_WB_NUM
                AND SL.AIR_WB_CRE_DT          = SLA.AIR_WB_CRE_DT
                AND SL.AIR_WB_CRE_H2SI        = SLA.AIR_WB_CRE_H2SI
                AND SL.AIR_WB_PCE_NUM         = SLA.AIR_WB_PCE_NUM
                AND SL.OPNG_CARR_CDE          = SLA.OPNG_CARR_CDE
                AND SL.OPNG_FLT_NUM           = SLA.OPNG_FLT_NUM
                AND SL.OPNG_FLT_NUM_SUFX_TXT  = SLA.OPNG_FLT_NUM_SUFX_TXT
                AND SL.FLT_DEP_DT             = SLA.FLT_DEP_DT
                AND SL.LEG_ORIG_ARPT_CDE      = SLA.LEG_ORIG_ARPT_CDE
                AND SL.LEG_DEST_ARPT_CDE      = SLA.LEG_DEST_ARPT_CDE
                AND SL.EVNT_TZ                = COALESCE(SLA.SCAN_CRGO_DEP_LD_EVNT_TZ, SLA.SCAN_CRGO_ARR_UNLD_EVNT_TZ)
        ),
        combined AS (
            SELECT
                COALESCE(P.AIR_WB_PRFX_ID, SL.AIR_WB_PRFX_ID)                AS AIR_WB_PRFX_ID,
                COALESCE(P.AIR_WB_NUM, SL.AIR_WB_NUM)                          AS AIR_WB_NUM,
                COALESCE(P.AIR_WB_CRE_DT, SL.AIR_WB_CRE_DT)                  AS AIR_WB_CRE_DT,
                COALESCE(P.AIR_WB_CRE_H2SI, SL.AIR_WB_CRE_H2SI)              AS AIR_WB_CRE_H2SI,
                COALESCE(P.AIR_WB_PCE_NUM, SL.AIR_WB_PCE_NUM)                  AS AIR_WB_PCE_NUM,
                COALESCE(P.OPNG_CARR_CDE, SL.OPNG_CARR_CDE)                    AS OPNG_CARR_CDE,
                COALESCE(P.OPNG_FLT_NUM, SL.OPNG_FLT_NUM)                      AS OPNG_FLT_NUM,
                COALESCE(P.OPNG_FLT_NUM_SUFX_TXT, SL.OPNG_FLT_NUM_SUFX_TXT)  AS OPNG_FLT_NUM_SUFX_TXT,
                COALESCE(P.FLT_DEP_DT, SL.FLT_DEP_DT)                          AS FLT_DEP_DT,
                COALESCE(P.LEG_ORIG_ARPT_CDE, SL.LEG_ORIG_ARPT_CDE)           AS LEG_ORIG_ARPT_CDE,
                COALESCE(P.LEG_DEST_ARPT_CDE, SL.LEG_DEST_ARPT_CDE)           AS LEG_DEST_ARPT_CDE,
                MAX(COALESCE(P.CMDTY_FLT_LEG_TYPE_CDE, SL.CMDTY_FLT_LEG_TYPE_CDE)) AS CMDTY_FLT_LEG_TYPE_CDE,
                MAX(P.PLN_CRGO_LEG_SEQ_NUM)        AS PLN_CRGO_LEG_SEQ_NUM,
                MAX(P.PLN_MAX_CRGO_LEG_SEQ_NUM)    AS PLN_MAX_CRGO_LEG_SEQ_NUM,
                MAX(P.PLN_CRGO_DEP_LD_FLAG)        AS PLN_CRGO_DEP_LD_FLAG,
                MAX(P.PLN_CRGO_ARR_UNLD_FLAG)      AS PLN_CRGO_ARR_UNLD_FLAG,
                MAX(P.PLN_MAX_EFF_TO_CENT_TZ)      AS PLN_MAX_EFF_TO_CENT_TZ,
                MAX(SL.SCAN_CRGO_DEP_LD_FLAG)      AS SCAN_CRGO_DEP_LD_FLAG,
                MAX(SL.SCAN_CRGO_DEP_THRU_FLAG)    AS SCAN_CRGO_DEP_THRU_FLAG,
                MAX(SL.SCAN_CRGO_ARR_UNLD_FLAG)    AS SCAN_CRGO_ARR_UNLD_FLAG,
                MAX(SL.SCAN_REQR_CRGO_ARR_UNLD_FLAG) AS SCAN_REQR_CRGO_ARR_UNLD_FLAG,
                MAX(SL.SCAN_CRGO_DEP_LD_EVNT_TZ)  AS SCAN_CRGO_DEP_LD_EVNT_TZ,
                MAX(SL.SCAN_CRGO_ARR_UNLD_EVNT_TZ) AS SCAN_CRGO_ARR_UNLD_EVNT_TZ,
                MAX(SL.SCAN_LEG_SEQ_NUM)           AS SCAN_LEG_SEQ_NUM,
                MAX(SL.MAX_SCAN_LEG_SEQ_NUM)       AS MAX_SCAN_LEG_SEQ_NUM,
                MAX(P.PLN_ORIGINAL_JOB_ID)         AS PLN_ORIGINAL_JOB_ID,
                MAX(P.PLN_LATEST_JOB_ID)           AS PLN_LATEST_JOB_ID,
                MAX(SL.SCAN_ORIGINAL_JOB_ID)       AS SCAN_ORIGINAL_JOB_ID,
                MAX(SL.SCAN_LATEST_JOB_ID)         AS SCAN_LATEST_JOB_ID
            FROM plan_leg_with_max_eff P
            FULL OUTER JOIN scan_leg_with_seq SL
                ON  P.AIR_WB_PRFX_ID        = SL.AIR_WB_PRFX_ID
                AND P.AIR_WB_NUM             = SL.AIR_WB_NUM
                AND P.AIR_WB_CRE_DT          = SL.AIR_WB_CRE_DT
                AND P.AIR_WB_CRE_H2SI        = SL.AIR_WB_CRE_H2SI
                AND P.AIR_WB_PCE_NUM         = SL.AIR_WB_PCE_NUM
                AND P.OPNG_CARR_CDE          = SL.OPNG_CARR_CDE
                AND P.OPNG_FLT_NUM           = SL.OPNG_FLT_NUM
                AND P.OPNG_FLT_NUM_SUFX_TXT  = SL.OPNG_FLT_NUM_SUFX_TXT
                AND P.FLT_DEP_DT             = SL.FLT_DEP_DT
                AND P.LEG_ORIG_ARPT_CDE      = SL.LEG_ORIG_ARPT_CDE
                AND P.LEG_DEST_ARPT_CDE      = SL.LEG_DEST_ARPT_CDE
                AND P.CMDTY_FLT_LEG_TYPE_CDE = SL.CMDTY_FLT_LEG_TYPE_CDE
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        ),
        final_result AS (
            SELECT DISTINCT
                BL.AIR_WB_PRFX_ID,
                BL.AIR_WB_NUM,
                BL.AIR_WB_CRE_DT,
                BL.AIR_WB_CRE_H2SI,
                BL.AIR_WB_PCE_NUM,
                BL.OPNG_CARR_CDE,
                BL.OPNG_FLT_NUM,
                BL.OPNG_FLT_NUM_SUFX_TXT,
                BL.FLT_DEP_DT,
                BL.LEG_ORIG_ARPT_CDE,
                BL.LEG_DEST_ARPT_CDE,
                COALESCE(BL.PLN_CRGO_LEG_SEQ_NUM, 0)     AS CRGO_LEG_SEQ_NUM,
                COALESCE(BL.PLN_MAX_CRGO_LEG_SEQ_NUM, 0)  AS MAX_CRGO_LEG_SEQ_NUM,
                COALESCE(BL.PLN_CRGO_DEP_LD_FLAG, 0) AS REQR_CRGO_DEP_LD_FLAG,
                COALESCE(BL.SCAN_CRGO_DEP_LD_FLAG, 0)     AS SCAN_CRGO_DEP_LD_FLAG,
                BL.SCAN_CRGO_DEP_LD_EVNT_TZ,
                COALESCE(BL.SCAN_CRGO_DEP_THRU_FLAG, 0)   AS SCAN_CRGO_DEP_THRU_FLAG,
                COALESCE(BL.PLN_CRGO_ARR_UNLD_FLAG, 0)    AS REQR_CRGO_ARR_UNLD_FLAG,
                COALESCE(BL.SCAN_CRGO_ARR_UNLD_FLAG, 0)   AS SCAN_CRGO_ARR_UNLD_FLAG,
                BL.SCAN_CRGO_ARR_UNLD_EVNT_TZ,
                CASE
                    WHEN COALESCE(BL.SCAN_CRGO_DEP_LD_FLAG, 0) = 1
                      OR COALESCE(BL.SCAN_CRGO_DEP_THRU_FLAG, 0) = 1
                    THEN 1 ELSE 0
                END AS SCAN_CRGO_DEP_ONBD_FLAG,
                CASE
                    WHEN (COALESCE(BL.SCAN_CRGO_DEP_LD_FLAG, 0) = 1
                       OR COALESCE(BL.SCAN_CRGO_DEP_THRU_FLAG, 0) = 1)
                     AND (COALESCE(BL.SCAN_REQR_CRGO_ARR_UNLD_FLAG, 0) = 0
                       OR COALESCE(BL.PLN_CRGO_ARR_UNLD_FLAG, 0) = 0)
                     AND COALESCE(BL.SCAN_CRGO_ARR_UNLD_FLAG, 0) = 0
                    THEN 1 ELSE 0
                END AS SCAN_CRGO_REM_ONBD_FLAG,
                CASE
                    WHEN COALESCE(BL.SCAN_CRGO_DEP_LD_FLAG, 0) = 0
                     AND COALESCE(BL.SCAN_CRGO_DEP_THRU_FLAG, 0) = 0
                     AND COALESCE(BL.SCAN_CRGO_ARR_UNLD_FLAG, 0) = 1
                    THEN 1 ELSE 0
                END AS ERR_SCAN_CRGO_DEP_ONBD_FLAG,
                CASE
                    WHEN (COALESCE(BL.SCAN_REQR_CRGO_ARR_UNLD_FLAG, 0) = 1
                       OR COALESCE(BL.PLN_CRGO_ARR_UNLD_FLAG, 0) = 1)
                     AND COALESCE(BL.SCAN_CRGO_ARR_UNLD_FLAG, 0) = 0
                    THEN 1 ELSE 0
                END AS ERR_SCAN_CRGO_ARR_UNLD_FLAG,
                BL.PLN_MAX_EFF_TO_CENT_TZ,
                CASE
                    WHEN COALESCE(BL.SCAN_CRGO_DEP_LD_FLAG, 0) = 1
                      OR COALESCE(BL.SCAN_CRGO_DEP_THRU_FLAG, 0) = 1
                      OR COALESCE(BL.SCAN_CRGO_ARR_UNLD_FLAG, 0) = 1
                    THEN 1 ELSE 0
                END AS SCAN_CRGO_LEG_EVNT_FLAG,
                COALESCE(EW.CMDTY_CRGO_WT_BAL_CDE, '-')   AS CMDTY_CRGO_WT_BAL_CDE,
                COALESCE(PC.CRGO_PCE_WT_LBS_QTY, 0.00)    AS CRGO_PCE_WT_LBS_QTY,
                EW.SCAN_CRGO_WT_BAL_EVNT_TZ,
                BL.CMDTY_FLT_LEG_TYPE_CDE,
                COALESCE(BL.SCAN_LEG_SEQ_NUM, 0)           AS SCAN_LEG_SEQ_NUM,
                COALESCE(BL.MAX_SCAN_LEG_SEQ_NUM, 0)       AS MAX_SCAN_LEG_SEQ_NUM,
                COALESCE(BL.SCAN_ORIGINAL_JOB_ID, BL.PLN_ORIGINAL_JOB_ID) AS ORIGINAL_JOB_ID,
                COALESCE(BL.SCAN_LATEST_JOB_ID, BL.PLN_LATEST_JOB_ID)     AS LATEST_JOB_ID
            FROM combined BL
            LEFT JOIN evnt_w EW
                ON  EW.AIR_WB_PRFX_ID    = BL.AIR_WB_PRFX_ID
                AND EW.AIR_WB_NUM         = BL.AIR_WB_NUM
                AND EW.AIR_WB_CRE_DT      = BL.AIR_WB_CRE_DT
                AND EW.AIR_WB_CRE_H2SI    = BL.AIR_WB_CRE_H2SI
                AND EW.AIR_WB_PCE_NUM     = BL.AIR_WB_PCE_NUM
                AND EW.LEG_ORIG_ARPT_CDE  = BL.LEG_ORIG_ARPT_CDE
            LEFT JOIN evnt_pc PC
                ON  PC.AIR_WB_PRFX_ID  = BL.AIR_WB_PRFX_ID
                AND PC.AIR_WB_NUM       = BL.AIR_WB_NUM
                AND PC.AIR_WB_CRE_DT    = BL.AIR_WB_CRE_DT
                AND PC.AIR_WB_CRE_H2SI  = BL.AIR_WB_CRE_H2SI
                AND PC.AIR_WB_PCE_NUM   = BL.AIR_WB_PCE_NUM
        )
        SELECT
            AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
            OPNG_CARR_CDE, OPNG_FLT_NUM, OPNG_FLT_NUM_SUFX_TXT, FLT_DEP_DT,
            LEG_ORIG_ARPT_CDE, LEG_DEST_ARPT_CDE,
            CRGO_LEG_SEQ_NUM, MAX_CRGO_LEG_SEQ_NUM,
            REQR_CRGO_DEP_LD_FLAG, SCAN_CRGO_DEP_LD_FLAG, SCAN_CRGO_DEP_LD_EVNT_TZ,
            SCAN_CRGO_DEP_THRU_FLAG,
            REQR_CRGO_ARR_UNLD_FLAG, SCAN_CRGO_ARR_UNLD_FLAG, SCAN_CRGO_ARR_UNLD_EVNT_TZ,
            SCAN_CRGO_DEP_ONBD_FLAG, SCAN_CRGO_REM_ONBD_FLAG,
            ERR_SCAN_CRGO_DEP_ONBD_FLAG, ERR_SCAN_CRGO_ARR_UNLD_FLAG,
            PLN_MAX_EFF_TO_CENT_TZ, SCAN_CRGO_LEG_EVNT_FLAG,
            CMDTY_CRGO_WT_BAL_CDE, CRGO_PCE_WT_LBS_QTY, SCAN_CRGO_WT_BAL_EVNT_TZ,
            CMDTY_FLT_LEG_TYPE_CDE,
            SCAN_LEG_SEQ_NUM, MAX_SCAN_LEG_SEQ_NUM,
            ORIGINAL_JOB_ID, LATEST_JOB_ID
        FROM final_result;

        DELETE FROM {src_schema}.{tbl_nm};

        INSERT INTO {src_schema}.{tbl_nm}
        SELECT * FROM {tbl_nm}_crgo_leg_result;

        DROP TABLE {tbl_nm}_crgo_leg_result;
    """


def sql_combined_cargo_columns(_self, **kwargs):
    """Generate SQL to derive all 5 cargo columns in one operation using CTE."""
    src_schema = kwargs['src_schema']
    tgt_schema = kwargs['tgt_schema']
    return f'''
 WITH wb_events AS (
    SELECT
        air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num,
        cmdty_crgo_wt_bal_cde, evnt_tz,
        LEAD(evnt_tz) OVER (
            PARTITION BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num
            ORDER BY evnt_tz
        ) AS next_evnt_tz
    FROM {tgt_schema}.cmdty_crgo_evnt
    WHERE cmdty_crgo_evnt_type_cde IN ('WA', 'WC')
),
piece_events AS (
    SELECT
        air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num,
        crgo_pce_wt_lbs_qty, crgo_svc_lvl_cde, crgo_spcl_hndlg_ary_txt, evnt_tz,
        LEAD(evnt_tz) OVER (
            PARTITION BY air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num
            ORDER BY evnt_tz
        ) AS next_evnt_tz
    FROM {tgt_schema}.cmdty_crgo_evnt
    WHERE cmdty_crgo_evnt_type_cde IN ('CR', 'CC')
),
matched_data AS (
    SELECT DISTINCT
        main.air_wb_prfx_id, main.air_wb_num, main.air_wb_cre_dt, main.air_wb_cre_h2si, main.air_wb_pce_num,
        main.eff_fm_cent_tz, main.eff_to_cent_tz,

        COALESCE((
            SELECT wb.cmdty_crgo_wt_bal_cde
            FROM wb_events wb
            WHERE wb.air_wb_prfx_id = main.air_wb_prfx_id
                AND wb.air_wb_num = main.air_wb_num
                AND wb.air_wb_cre_dt = main.air_wb_cre_dt
                AND wb.air_wb_cre_h2si = main.air_wb_cre_h2si
                AND wb.air_wb_pce_num = main.air_wb_pce_num
                AND wb.evnt_tz <= main.eff_fm_cent_tz
                AND (wb.next_evnt_tz IS NULL OR wb.next_evnt_tz > main.eff_fm_cent_tz)
            ORDER BY wb.evnt_tz DESC
            LIMIT 1
        ), '-') AS cmdty_crgo_wt_bal_cde,

        COALESCE((
            SELECT pc.crgo_pce_wt_lbs_qty
            FROM piece_events pc
            WHERE pc.air_wb_prfx_id = main.air_wb_prfx_id
                AND pc.air_wb_num = main.air_wb_num
                AND pc.air_wb_cre_dt = main.air_wb_cre_dt
                AND pc.air_wb_cre_h2si = main.air_wb_cre_h2si
                AND pc.air_wb_pce_num = main.air_wb_pce_num
                AND pc.evnt_tz <= main.eff_fm_cent_tz
                AND (pc.next_evnt_tz IS NULL OR pc.next_evnt_tz > main.eff_fm_cent_tz)
            ORDER BY pc.evnt_tz DESC
            LIMIT 1
        ), 0.00) AS crgo_pce_wt_lbs_qty,

        COALESCE((
            SELECT pc.crgo_svc_lvl_cde
            FROM piece_events pc
            WHERE pc.air_wb_prfx_id = main.air_wb_prfx_id
                AND pc.air_wb_num = main.air_wb_num
                AND pc.air_wb_cre_dt = main.air_wb_cre_dt
                AND pc.air_wb_cre_h2si = main.air_wb_cre_h2si
                AND pc.air_wb_pce_num = main.air_wb_pce_num
                AND pc.evnt_tz <= main.eff_fm_cent_tz
                AND (pc.next_evnt_tz IS NULL OR pc.next_evnt_tz > main.eff_fm_cent_tz)
            ORDER BY pc.evnt_tz DESC
            LIMIT 1
        ), '-') AS crgo_svc_lvl_cde,

        COALESCE((
            SELECT pc.crgo_spcl_hndlg_ary_txt
            FROM piece_events pc
            WHERE pc.air_wb_prfx_id = main.air_wb_prfx_id
                AND pc.air_wb_num = main.air_wb_num
                AND pc.air_wb_cre_dt = main.air_wb_cre_dt
                AND pc.air_wb_cre_h2si = main.air_wb_cre_h2si
                AND pc.air_wb_pce_num = main.air_wb_pce_num
                AND pc.evnt_tz <= main.eff_fm_cent_tz
                AND (pc.next_evnt_tz IS NULL OR pc.next_evnt_tz > main.eff_fm_cent_tz)
            ORDER BY pc.evnt_tz DESC
            LIMIT 1
        ), '-') AS crgo_spcl_hndlg_ary_txt
        FROM {src_schema}.cmdty_crgo main
    )
    UPDATE {src_schema}.cmdty_crgo
    SET
        CMDTY_CRGO_WT_BAL_CDE = matched_data.cmdty_crgo_wt_bal_cde,
        CRGO_PCE_WT_LBS_QTY = matched_data.crgo_pce_wt_lbs_qty,
        CRGO_SVC_LVL_CDE = matched_data.crgo_svc_lvl_cde,
        CRGO_SPCL_HNDLG_ARY_TXT = matched_data.crgo_spcl_hndlg_ary_txt
    FROM matched_data
    WHERE cmdty_crgo.air_wb_prfx_id = matched_data.air_wb_prfx_id
    AND cmdty_crgo.air_wb_num = matched_data.air_wb_num
    AND cmdty_crgo.air_wb_cre_dt = matched_data.air_wb_cre_dt
    AND cmdty_crgo.air_wb_cre_h2si = matched_data.air_wb_cre_h2si
    AND cmdty_crgo.air_wb_pce_num = matched_data.air_wb_pce_num
    AND cmdty_crgo.eff_fm_cent_tz = matched_data.eff_fm_cent_tz
    AND cmdty_crgo.eff_to_cent_tz = matched_data.eff_to_cent_tz;
  '''

def sql_cust_set_eff_dates_and_flags(self, **kwargs):
    """Manages effective date ranges and versioning flags to enable Change Data Capture.
    Uses window functions to determine next effective date and rank within partitions.
    """
    src_schema = kwargs['src_schema']
    tbl_nm = kwargs['tbl_nm']

    pks = self.get_keys({KEYS_PRIMARY})
    partition_keys = [pk for pk in pks if pk != 'eff_fm_cent_tz']
    eff_keys = pks + ['eff_fm_cent_tz']

    return f'''
    WITH leg_versions AS (
        SELECT *,
            LEAD(eff_fm_cent_tz) OVER (PARTITION BY {', '.join(partition_keys)} ORDER BY eff_fm_cent_tz) as next_eff_fm_cent_tz,
            DENSE_RANK() OVER (PARTITION BY {', '.join(partition_keys)} ORDER BY eff_fm_cent_tz) as min_rank,
            DENSE_RANK() OVER (PARTITION BY {', '.join(partition_keys)} ORDER BY eff_fm_cent_tz DESC) as max_rank
        FROM {src_schema}.{tbl_nm}
    )
    UPDATE {src_schema}.{tbl_nm}
    SET eff_to_cent_tz = COALESCE(lv.next_eff_fm_cent_tz, '2099-12-31 00:00:00.000'::timestamp),
        min_eff_fm_flag = CASE WHEN lv.min_rank = 1 THEN 1 ELSE 0 END,
        max_eff_to_flag = CASE WHEN lv.max_rank = 1 THEN 1 ELSE 0 END
    FROM leg_versions lv
    WHERE {' AND '.join([f'{src_schema}.{tbl_nm}.{pk} = lv.{pk}' for pk in eff_keys])}
    '''





def sql_pln_leg_set_eff_dates_and_flags(_self, **kwargs):
    """Calculate effective dates with signature-based deduplication using CTEs.

    Uses LISTAGG signatures to detect actual itinerary changes across timestamps,
    then calculates effective date ranges and flags.
    """
    src_schema = kwargs['src_schema']
    tbl_nm = kwargs['tbl_nm']

    return f"""
        WITH
        deduped_stage AS (
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY
                               AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
                               OPNG_CARR_CDE, OPNG_FLT_NUM, OPNG_FLT_NUM_SUFX_TXT, FLT_DEP_DT,
                               LEG_ORIG_ARPT_CDE, LEG_DEST_ARPT_CDE, EFF_FM_CENT_TZ,
                               PLN_CRGO_LEG_SEQ_NUM, PLN_MAX_CRGO_LEG_SEQ_NUM,
                               PLN_CRGO_DEP_LD_FLAG, PLN_CRGO_ARR_UNLD_FLAG, CMDTY_FLT_LEG_TYPE_CDE
                           ORDER BY EFF_FM_CENT_TZ, LATEST_JOB_ID DESC
                       ) AS rn
                FROM {src_schema}.{tbl_nm}
            ) ranked
            WHERE rn = 1
        ),

        deduped_pln AS (
            SELECT
                AIR_WB_PRFX_ID,
                AIR_WB_NUM,
                AIR_WB_CRE_DT,
                AIR_WB_CRE_H2SI,
                AIR_WB_PCE_NUM,
                EFF_FM_CENT_TZ,
                LISTAGG(
                    '(' || CAST(PLN_CRGO_LEG_SEQ_NUM AS VARCHAR) || ')|'
                        || OPNG_CARR_CDE || '|'
                        || CAST(OPNG_FLT_NUM AS VARCHAR) || '|'
                        || OPNG_FLT_NUM_SUFX_TXT || '|'
                        || CAST(FLT_DEP_DT AS VARCHAR) || '|'
                        || LEG_ORIG_ARPT_CDE || '|'
                        || LEG_DEST_ARPT_CDE,
                    '  ,'
                ) WITHIN GROUP (ORDER BY PLN_CRGO_LEG_SEQ_NUM) AS SIG
            FROM deduped_stage
            GROUP BY 1, 2, 3, 4, 5, 6
        ),

        sig_data AS (
            SELECT
                AIR_WB_PRFX_ID,
                AIR_WB_NUM,
                AIR_WB_CRE_DT,
                AIR_WB_CRE_H2SI,
                AIR_WB_PCE_NUM,
                EFF_FM_CENT_TZ,
                SIG,
                PRV_SIG
            FROM (
                SELECT
                    AIR_WB_PRFX_ID,
                    AIR_WB_NUM,
                    AIR_WB_CRE_DT,
                    AIR_WB_CRE_H2SI,
                    AIR_WB_PCE_NUM,
                    EFF_FM_CENT_TZ,
                    SIG,
                    LAG(SIG) OVER (
                        PARTITION BY AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT,
                                     AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM
                        ORDER BY EFF_FM_CENT_TZ
                    ) AS PRV_SIG
                FROM deduped_pln
            ) subq
            WHERE SIG != PRV_SIG OR PRV_SIG IS NULL
        ),

        fnl_dup AS (
            SELECT DS.*
            FROM deduped_stage DS
            JOIN sig_data SD
                USING (AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI,
                       AIR_WB_PCE_NUM, EFF_FM_CENT_TZ)
        ),

        distinct_timestamps AS (
            SELECT DISTINCT
                AIR_WB_PRFX_ID,
                AIR_WB_NUM,
                AIR_WB_CRE_DT,
                AIR_WB_CRE_H2SI,
                AIR_WB_PCE_NUM,
                EFF_FM_CENT_TZ
            FROM fnl_dup
        ),

        timestamp_windows AS (
            SELECT
                AIR_WB_PRFX_ID,
                AIR_WB_NUM,
                AIR_WB_CRE_DT,
                AIR_WB_CRE_H2SI,
                AIR_WB_PCE_NUM,
                EFF_FM_CENT_TZ,
                LEAD(EFF_FM_CENT_TZ) OVER (
                    PARTITION BY AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT,
                                 AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM
                    ORDER BY EFF_FM_CENT_TZ
                ) AS NEXT_EFF_TZ
            FROM distinct_timestamps
        ),

        enriched_data AS (
            SELECT
                N.AIR_WB_PRFX_ID,
                N.AIR_WB_NUM,
                N.AIR_WB_CRE_DT,
                N.AIR_WB_CRE_H2SI,
                N.AIR_WB_PCE_NUM,
                N.OPNG_CARR_CDE,
                N.OPNG_FLT_NUM,
                N.OPNG_FLT_NUM_SUFX_TXT,
                N.FLT_DEP_DT,
                N.LEG_ORIG_ARPT_CDE,
                N.LEG_DEST_ARPT_CDE,
                N.EFF_FM_CENT_TZ,
                COALESCE(
                    TW.NEXT_EFF_TZ,
                    CAST('2099-12-31 00:00:00.000' AS TIMESTAMP)
                ) AS EFF_TO_CENT_TZ,
                CASE
                    WHEN RANK() OVER (
                        PARTITION BY N.AIR_WB_PRFX_ID, N.AIR_WB_NUM, N.AIR_WB_CRE_DT,
                                     N.AIR_WB_CRE_H2SI, N.AIR_WB_PCE_NUM
                        ORDER BY N.EFF_FM_CENT_TZ
                    ) = 1 THEN 1
                    ELSE 0
                END AS MIN_EFF_FM_FLAG,
                CASE
                    WHEN TW.NEXT_EFF_TZ IS NULL THEN 1
                    ELSE 0
                END AS MAX_EFF_TO_FLAG,
                N.PLN_CRGO_LEG_SEQ_NUM,
                N.PLN_MAX_CRGO_LEG_SEQ_NUM,
                N.PLN_CRGO_DEP_LD_FLAG,
                N.PLN_CRGO_ARR_UNLD_FLAG,
                N.CMDTY_FLT_LEG_TYPE_CDE,
                N.ORIGINAL_JOB_ID,
                N.LATEST_JOB_ID
            FROM fnl_dup N
            INNER JOIN timestamp_windows TW
                ON N.AIR_WB_PRFX_ID = TW.AIR_WB_PRFX_ID
                AND N.AIR_WB_NUM = TW.AIR_WB_NUM
                AND N.AIR_WB_CRE_DT = TW.AIR_WB_CRE_DT
                AND N.AIR_WB_CRE_H2SI = TW.AIR_WB_CRE_H2SI
                AND N.AIR_WB_PCE_NUM = TW.AIR_WB_PCE_NUM
                AND N.EFF_FM_CENT_TZ = TW.EFF_FM_CENT_TZ
        ),

        final_dedup AS (
            SELECT
                AIR_WB_PRFX_ID,
                AIR_WB_NUM,
                AIR_WB_CRE_DT,
                AIR_WB_CRE_H2SI,
                AIR_WB_PCE_NUM,
                OPNG_CARR_CDE,
                OPNG_FLT_NUM,
                OPNG_FLT_NUM_SUFX_TXT,
                FLT_DEP_DT,
                LEG_ORIG_ARPT_CDE,
                LEG_DEST_ARPT_CDE,
                EFF_FM_CENT_TZ,
                EFF_TO_CENT_TZ,
                MIN_EFF_FM_FLAG,
                MAX_EFF_TO_FLAG,
                PLN_CRGO_LEG_SEQ_NUM,
                PLN_MAX_CRGO_LEG_SEQ_NUM,
                PLN_CRGO_DEP_LD_FLAG,
                PLN_CRGO_ARR_UNLD_FLAG,
                CMDTY_FLT_LEG_TYPE_CDE,
                ORIGINAL_JOB_ID,
                LATEST_JOB_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        AIR_WB_PRFX_ID, AIR_WB_NUM, AIR_WB_CRE_DT, AIR_WB_CRE_H2SI, AIR_WB_PCE_NUM,
                        OPNG_CARR_CDE, OPNG_FLT_NUM, OPNG_FLT_NUM_SUFX_TXT, FLT_DEP_DT,
                        LEG_ORIG_ARPT_CDE, LEG_DEST_ARPT_CDE, EFF_TO_CENT_TZ
                    ORDER BY EFF_TO_CENT_TZ DESC, LATEST_JOB_ID DESC
                ) AS rn_final
            FROM enriched_data
        )

        SELECT
            AIR_WB_PRFX_ID,
            AIR_WB_NUM,
            AIR_WB_CRE_DT,
            AIR_WB_CRE_H2SI,
            AIR_WB_PCE_NUM,
            OPNG_CARR_CDE,
            OPNG_FLT_NUM,
            OPNG_FLT_NUM_SUFX_TXT,
            FLT_DEP_DT,
            LEG_ORIG_ARPT_CDE,
            LEG_DEST_ARPT_CDE,
            EFF_FM_CENT_TZ,
            EFF_TO_CENT_TZ,
            MIN_EFF_FM_FLAG,
            MAX_EFF_TO_FLAG,
            PLN_CRGO_LEG_SEQ_NUM,
            PLN_MAX_CRGO_LEG_SEQ_NUM,
            PLN_CRGO_DEP_LD_FLAG,
            PLN_CRGO_ARR_UNLD_FLAG,
            CMDTY_FLT_LEG_TYPE_CDE,
            ORIGINAL_JOB_ID,
            LATEST_JOB_ID
        INTO TEMP TABLE {tbl_nm}_final
        FROM final_dedup
        WHERE rn_final = 1;

        DELETE FROM {src_schema}.{tbl_nm};

        INSERT INTO {src_schema}.{tbl_nm}
        SELECT * FROM {tbl_nm}_final;

        DROP TABLE {tbl_nm}_final;
    """


def get_dep_ld_flag(_) -> Column:
    """Returns departure load flag based on connection breaks."""
    return when(
        col("leg_pos") == lit(0), lit(1)
    ).when(
        col("prev_leg").isNull(), lit(1)
    ).when(
        (col("prev_leg.flightleg.flightnumber") != col("exploded_leg.flightleg.flightnumber")) |
        (col("prev_leg.flightleg.operationalcarrier") != col("exploded_leg.flightleg.operationalcarrier")) |
        (col("prev_leg.flightleg.arrivalairportiatacode") != col("exploded_leg.flightleg.departureairportiatacode")),
        lit(1)
    ).otherwise(lit(0))


def get_arr_unld_flag(_) -> Column:
    """Returns arrival unload flag based on connection breaks."""
    return when(
        col("leg_pos") == expr("size(planneditinerary) - 1"), lit(1)
    ).when(
        col("next_leg").isNull(), lit(1)
    ).when(
        (col("next_leg.flightleg.flightnumber") != col("exploded_leg.flightleg.flightnumber")) |
        (col("next_leg.flightleg.operationalcarrier") != col("exploded_leg.flightleg.operationalcarrier")) |
        (col("next_leg.flightleg.departureairportiatacode") != col("exploded_leg.flightleg.arrivalairportiatacode")),
        lit(1)
    ).otherwise(lit(0))


def get_pln_leg_type_code(_) -> Column:
    """Returns flight leg type code based on flightlegtypeid, with connection break override."""
    return (
        when(lower(trim(col("exploded_leg.flightlegtypeid"))) == 'unknown', lit("-"))
        .when(lower(trim(col("exploded_leg.flightlegtypeid"))) == "originating", lit("0"))
        .when(lower(trim(col("exploded_leg.flightlegtypeid"))) == "transfer", lit("1"))
        .when(lower(trim(col("exploded_leg.flightlegtypeid"))) == "thru", lit("2"))
        .when(lower(trim(col("exploded_leg.flightlegtypeid"))) == "standbyearly", lit("3"))
        .otherwise(lit("-"))
    )

def get_cargo_bin_code() -> Column:
    """Returns cargo bin code with event validation - direct mapping without prefix removal.

    - Validates event_name is in the approved list
    - Extracts container from exploded_scan_code if conditions are met
    - Uses container value directly (no Bin- prefix removal)
    - Returns '-' if conditions not met or container is null/empty

    Returns:
        Column: Cargo bin code or '-' as default
    """
    valid_events = ['piece-loaded-on-flight', 'piece-offloaded-from-flight', 'piece-unloaded-from-flight',
                   'piece-loaded-by-backup-mode', 'piece-not-loaded-by-backup-mode', 'piece-tracking']

    return when(
        (col("exploded_scan_code").isNotNull()) &
        (col("event_name").isin(valid_events)) &
        (col("exploded_scan_code").container.isNotNull()) &
        (trim(col("exploded_scan_code").container) != ""),
        col("exploded_scan_code").container
    ).otherwise(lit("-"))


def schemas(_fk_job) -> dict:  # noqa: ANN001, PGH003 # type: ignore
    """Table schemas from the 'Commodity Cargo' data domain.

    :param _fk_job: a job instance that might be useful in creating a dynamic schema
    :type _fk_job: class: `AWSJob`

    :return: dictionary with table names as keys and `StructType`s as values
    :rtype: dict
    """
    return {
        "cmdty_crgo_curated": StructType([
            StructField('event_name', StringType(), True, {'path': lambda _: get_json_object(col('headers'), '$.event_name')}),
            StructField('event_created', StringType(), True, {'path': lambda _: get_json_object(col('headers'), '$.event_created')}),
            StructField('id', StringType(), True, {'path': lambda _: get_json_object(col('headers'), '$.id')}),
            StructField('event_fqcn', StringType(), True, {'path': lambda _: get_json_object(col('headers'), '$.event_fqcn')}),

            # CargoEvent -> commoditypk (struct field extraction)
            StructField('airwaybillprefix', StringType(), True, {'path': lambda _: col('cargoevent.commoditypk.airwaybillprefix')}),
            StructField('airwaybillnumber', StringType(), True, {'path': lambda _: col('cargoevent.commoditypk.airwaybillnumber')}),
            StructField('airwaybillcreationdate', StringType(), True, {'path': lambda _: col('cargoevent.commoditypk.airwaybillcreationdate')}),
            StructField('airwaybillpiecenumber', StringType(), True, {'path': lambda _: col('cargoevent.commoditypk.airwaybillpiecenumber')}),

            # cargoevent -> pieceinfo (struct field extraction)
            StructField('isunknown', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.isunknown')}),
            StructField('isdeleted', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.isdeleted')}),
            StructField('ishazmat', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.ishazmat')}),
            StructField('pieceweight', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.pieceweight')}),
            StructField('servicelevel', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.servicelevel')}),
            StructField('wabcategory', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.wabcategory')}),
            StructField('specialhandlingcodes', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.specialhandlingcodes')}),
            StructField('radioactivecategory', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.radioactivecategory')}),
            StructField('transportindex', StringType(), True, {'path': lambda _: col('cargoevent.pieceinfo.transportindex')}),

            # planneditinerary
            StructField('planneditinerary', ArrayType(
                StructType([
                    StructField('flightleg', StructType([
                        StructField('departureairportiatacode', StringType(), True),
                        StructField('arrivalairportiatacode', StringType(), True),
                        StructField('origindate', StringType(), True),
                        StructField('flightnumber', StringType(), True),
                        StructField('operationalcarrier', StringType(), True),
                        StructField('operationalsuffix', StringType(), True),
                        StructField('repeatnumber', StringType(), True)
                    ]), True),
                    StructField('flightlegtypeid', StringType(), True)
                ])
            ), True, {'path': lambda _: expr("""
                CASE WHEN cargoevent.planneditinerary IS NOT NULL
                THEN transform(cargoevent.planneditinerary, x ->
                    struct(
                        CASE WHEN x.flightleg IS NOT NULL THEN x.flightleg ELSE null END as flightleg,
                        x.flightlegtypeid as flightlegtypeid
                    )
                ) ELSE array() END
            """)}),

            # commodityscans
            StructField('commodityscans', ArrayType(
                StructType([
                    StructField('flightleg', StructType([
                        StructField('departureairportiatacode', StringType(), True),
                        StructField('arrivalairportiatacode', StringType(), True),
                        StructField('origindate', StringType(), True),
                        StructField('flightnumber', StringType(), True),
                        StructField('operationalcarrier', StringType(), True),
                        StructField('operationalsuffix', StringType(), True),
                        StructField('repeatnumber', StringType(), True)
                    ]), True),
                    StructField('trackinglocation', StructType([
                        StructField('area', StringType(), True),
                        StructField('location', StringType(), True),
                        StructField('action', StringType(), True)
                    ]), True),
                    StructField('scantype', StringType(), True),
                    StructField('scantime', StringType(), True),
                    StructField('scaniatastationcode', StringType(), True),
                    StructField('userid', StringType(), True),
                    StructField('container', StringType(), True),
                    StructField('devicename', StringType(), True)
                ])
            ), True, {'path': lambda _: expr("""
                CASE WHEN cargoevent.commodityscans IS NOT NULL
                THEN transform(cargoevent.commodityscans, x ->
                    struct(
                        CASE WHEN x.flightleg IS NOT NULL THEN x.flightleg ELSE null END as flightleg,
                        CASE WHEN x.trackinglocation IS NOT NULL THEN x.trackinglocation ELSE null END as trackinglocation,
                        x.scantype as scantype,
                        x.scantime as scantime,
                        x.scaniatastationcode as scaniatastationcode,
                        x.userid as userid,
                        x.container as container,
                        x.devicename as devicename
                    )
                ) ELSE array() END
            """)}),

            # s3 object
            StructField('s3_object', StringType(), True),

            # Partition fields (based on airWaybillCreationDate)
            StructField('year',  StringType(), False, {'path': lambda _: split(col('cargoevent.commoditypk.airWaybillCreationDate'), '-').getItem(0), 'purpose': PRPS_PARTITION, 'lpad': (4, '0')}), # type: ignore
            StructField('month', StringType(), False, {'path': lambda _: split(col('cargoevent.commoditypk.airWaybillCreationDate'), '-').getItem(1), 'purpose': PRPS_PARTITION, 'lpad': (2, '0')}), # type: ignore
            StructField('day',   StringType(), False, {'path': lambda _: split(col('cargoevent.commoditypk.airWaybillCreationDate'), '-').getItem(2), 'purpose': PRPS_PARTITION, 'lpad': (2, '0')}), # type: ignore
        ]),
        "cmdty_crgo_evnt": StructType([
            StructField("air_wb_prfx_id",                StringType(),      False, {"type": "char(3)",                  "path": "airwaybillprefix",                     "key": KEYS_PRIMARY}),
            StructField("air_wb_num",                    StringType(),      False, {"type": "char(8)",                  "path": "airwaybillnumber",                     "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_dt",                 DateType(),        False, {"type": "date",                     "path": lambda _: to_date(col("airwaybillcreationdate")), "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_h2si",               StringType(),      False, {"type": "VARCHAR(64)",              "path": lambda _: date_format(to_timestamp(col("airwaybillcreationdate")), "HH:mm:ss.SSS"),       "key": KEYS_PRIMARY}),
            StructField("air_wb_pce_num",                ShortType(),       False, {"type": "smallint",                 "path": lambda _: col("airwaybillpiecenumber").cast("smallint"),   "key": KEYS_PRIMARY}),
            StructField("evnt_tz",                       TimestampType(),   False, {"type": "timestamp",                "path": lambda _: to_timestamp(col("event_created"))}),
            StructField("max_evnt_flag",                 ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: lit(0)}),
            StructField("cmdty_crgo_evnt_type_cde",      StringType(),      False, {"type": "char(2)",                  "default": "-",                   "path": map_event_name_to_code,   "key": KEYS_PRIMARY}),
            StructField("cmdty_crgo_evnt_bkup_mode_flag",ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(col("event_name").isin(['piece-loaded-by-backup-mode', 'piece-not-loaded-by-backup-mode']), lit(1)).otherwise(lit(0))}),
            StructField("cmdty_crgo_pce_stat_cde",       StringType(),      False, {"type": "char(1)",                  "default": "A",                   "path": lambda _: when(lower(trim(col("event_name"))) == "piece-deleted", lit("D")).otherwise(lit("A"))}),
            StructField("evnt_asgd_arpt_cde",            StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": get_evnt_asgd_arpt_cde,    "key": KEYS_PRIMARY, "preproc": add_matched_scan_codes_and_explode}),
            StructField("cmdty_devc_arpt_cde",           StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _: get_exploded_scan_code_field("scaniatastationcode") }),
            StructField("cmdty_devc_id",                 StringType(),      False, {"type": "varchar(64)",              "default": "-",                   "path": lambda _: get_exploded_scan_code_field("devicename") }),
            StructField("cmdty_user_id",                 StringType(),      False, {"type": "varchar(15)",              "default": "-",                   "path": lambda _: get_exploded_scan_code_field("userid") }),
            StructField("cmdty_crgo_wt_bal_cde",         StringType(),      False, {"type": "char(2)",                  "default": "-",                   "path": lambda _: col("wabcategory") }),
            StructField("cmdty_sys_txn_tz",              StringType(),      False, {"type": "varchar(32)",              "default": "1099-12-31 12:00:00.000",                   "path": lambda _: translate(regexp_replace(col("event_created"), "Z$", "+00:00"), "T", " ")}),
            StructField("cmdty_sys_txn_sess_tz",         TimestampType(),   False, {"type": "timestamp",                "default": "2099-12-31 12:00:00.000", "path": lambda _: to_timestamp(col("event_created"))}),
            StructField("crgo_bin_cde",                  StringType(),      False, {"type": "varchar(25)",              "default": "-",                   "path": lambda _: get_cargo_bin_code()}),
            StructField("pln_leg_ct",                    ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(col("planneditinerary").isNotNull(), size(col("planneditinerary"))).otherwise(lit(0)) }),
            StructField("scan_leg_ct",                   ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(col("commodityscans").isNotNull(), size(filter( col("commodityscans"), lambda scan: ( ((col("event_name") == 'piece-tracking') & (scan.scantype.like("PieceTracking%")) ) | ( scan.scantype == col("event_name")) ) & (to_timestamp(scan.scantime) == to_timestamp(col("event_created"))) )) ).otherwise(lit(0)) }),
            StructField("crgo_pce_wt_lbs_qty",           DecimalType(9,2),  True,  {"type": "decimal(9,2)",             "default": 0.0,                   "path": lambda _: default_if_null_or_empty("pieceweight", 0.0).cast("decimal(9,2)")}),
            StructField("crgo_svc_lvl_cde",              StringType(),      False, {"type": "varchar(20)",              "default": "-",                   "path": lambda _: default_if_null_or_empty("servicelevel", "-")}),
            StructField("crgo_spcl_hndlg_ary_txt",       StringType(),      True,  {"type": "varchar(50)",              "default": "-",                   "path": lambda _: default_if_null_or_empty("specialhandlingcodes", "-")}),
            StructField("cmdty_unkn_flag",               ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(lower(trim(col("isunknown"))) == "true", lit(1)).otherwise(lit(0))}),
            StructField("cmdty_loc_nme",                 StringType(),      False, {"type": "varchar(50)",              "default": "-",                   "path": lambda _: get_tracking_location_info('area')}),
            StructField("cmdty_loc_area_nme",            StringType(),      False, {"type": "varchar(50)",              "default": "-",                   "path": lambda _: get_tracking_location_info("location")}),
            StructField("cmdty_evnt_actn_cde",           StringType(),      False, {"type": "varchar(50)",              "default": "-",                   "path": lambda _: get_tracking_location_info("action")}),
            StructField("year",                          StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
            StructField("month",                         StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("day",                           StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("isdeleted",                     StringType(),      False, {"type": "string",                                                  "path": "isdeleted", "purpose": PRPS_DATALAKE_ONLY}),
            StructField("cmdty_sys_txn_cent_tz",         TimestampType(),   False, {"type": "timestamp",                "path": lambda _: col("cmdty_sys_txn_sess_tz"), "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("eff_fm_evnt_tz",                TimestampType(),   False, {"type": "timestamp",                "path": lambda _: to_timestamp(col("event_created")), "purpose": PRPS_DATALAKE_ONLY}),
            StructField("latest_job_id",                 IntegerType(),     False, {"type": "INTEGER",                                                   "path": lambda _: lit(latest_job_id)}),
            StructField("original_job_id",               IntegerType(),     False, {"type": "INTEGER",                                                   "path": lambda _: col("latest_job_id"),         "purpose": PRPS_REDSHIFT_ONLY})
       ]),
        "cmdty_crgo_evnt_scan_leg": StructType([
            StructField("air_wb_prfx_id",                StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": "airwaybillprefix",                     "key": KEYS_PRIMARY}),
            StructField("air_wb_num",                    StringType(),      False, {"type": "char(8)",                  "default": "-",                   "path": "airwaybillnumber",                     "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_dt",                 DateType(),        False, {"type": "date",                     "default": "1900-01-01",          "path": lambda _: to_date(col("airwaybillcreationdate")), "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_h2si",               StringType(),      False, {"type": "VARCHAR(64)",              "default": "-",                   "path": lambda _: date_format(to_timestamp(col("airwaybillcreationdate")), "HH:mm:ss.SSS"),       "key": KEYS_PRIMARY}),
            StructField("air_wb_pce_num",                ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: col("airwaybillpiecenumber").cast("smallint"),                   "key": KEYS_PRIMARY}),
            StructField("evnt_tz",                       TimestampType(),   False, {"type": "timestamp",                "default": "2099-12-31T12:00:00.000", "path": lambda _: to_timestamp(col("event_created"))}),
            StructField("year",                          StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
            StructField("month",                         StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("day",                           StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("opng_carr_cde",                 StringType(),      False, {"type": "char(3)",                  "default": "-",                  "path":lambda _:  when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), col("matched_scan_codes")[0].flightleg.operationalcarrier)   , "key": KEYS_PRIMARY , "preproc": add_matched_scan_codes_column}),
            StructField("opng_flt_num",                  ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), col("matched_scan_codes")[0].flightleg.flightnumber.cast("smallint")),  "key": KEYS_PRIMARY}),
            StructField("opng_flt_num_sufx_txt",         StringType(),      False, {"type": "char(1)",                  "default": "-",                   "path": lambda _: when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), col("matched_scan_codes")[0].flightleg.operationalsuffix ).otherwise("-"),"key": KEYS_PRIMARY}),
            StructField("flt_dep_dt",                    DateType(),        False, {"type": "date",                     "default": "1900-01-01",          "path": lambda _: when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), to_date(col("matched_scan_codes")[0].flightleg.origindate)) , "key": KEYS_PRIMARY}),
            StructField("leg_orig_arpt_cde",             StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _: when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), col("matched_scan_codes")[0].flightleg.departureairportiatacode).otherwise("-") , "key": KEYS_PRIMARY}),
            StructField("leg_dest_arpt_cde",             StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _:  when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), col("matched_scan_codes")[0].flightleg.arrivalairportiatacode).otherwise("-"), "key": KEYS_PRIMARY}),
            StructField("cmdty_devc_arpt_cde",           StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _: when( (col("matched_scan_codes").isNotNull()) & (size(col("matched_scan_codes")) > 0), col("matched_scan_codes")[0].scaniatastationcode).otherwise("-"),"key": KEYS_PRIMARY}),
            StructField("scan_crgo_dep_evnt_flag",       ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when((lower(trim(col("event_name"))) == "piece-loaded-on-flight") | (lower(trim(col("event_name"))) == "piece-offloaded-from-flight") | (lower(trim(col("event_name"))) == "piece-loaded-by-backup-mode") | (lower(trim(col("event_name"))) == "piece-not-loaded-by-backup-mode"),lit(1)).otherwise(lit(0))}),
            StructField("scan_crgo_dep_ld_flag",         ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(((lower(trim(col("event_name"))) == "piece-loaded-on-flight") | (lower(trim(col("event_name"))) == "piece-loaded-by-backup-mode")) & (col("matched_scan_codes")[0].flightleg.departureairportiatacode == col("matched_scan_codes")[0].scaniatastationcode), lit(1)).otherwise(lit(0))}),
            StructField("scan_crgo_dep_thru_flag",       ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(((lower(trim(col("event_name"))) == "piece-loaded-on-flight") | (lower(trim(col("event_name"))) == "piece-loaded-by-backup-mode")) & (col("matched_scan_codes")[0].flightleg.departureairportiatacode != col("matched_scan_codes")[0].scaniatastationcode), lit(1)).otherwise(lit(0))}),
            StructField("scan_crgo_arr_unld_flag",       ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: when(lower(trim(col("event_name"))) == "piece-unloaded-from-flight", lit(1)).otherwise(lit(0))}),
            StructField("scan_reqr_crgo_arr_unld_flag",  ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path":  lambda _: lit(0)}),
            StructField("cmdty_flt_leg_type_cde",        StringType(),      False, {"type": "char(1)",                  "default": "-",                   "path":  get_flight_leg_type_code}),
            StructField("evnt_scan_leg_seq_num",         ShortType(),       False, {"type": "smallint",                 "default": 0,                    "path":   get_evnt_scan_leg_seq_num}),
            StructField("eff_fm_evnt_tz",                TimestampType(),   False, {"type": "timestamp",                "path": lambda _: to_timestamp(col("event_created")), "purpose": PRPS_DATALAKE_ONLY}),
            StructField("original_job_id",               IntegerType(),     False, {"type": "INTEGER",                                                   "path": lambda _: col("latest_job_id"),         "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("latest_job_id",                 IntegerType(),     False, {"type": "INTEGER",                                                   "path": lambda _: lit(latest_job_id)})
        ]),
        "cmdty_crgo_hndlg": StructType([
            StructField("air_wb_prfx_id",                StringType(),      False, {"type": "char(3)",                  "default": "-",                        "path": "air_wb_prfx_id",                                 "key": KEYS_PRIMARY}),
            StructField("air_wb_num",                    StringType(),      False, {"type": "char(8)",                  "default": "-",                        "path": "air_wb_num",                                     "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_dt",                 DateType(),        False, {"type": "date",                     "default": "1900-01-01",               "path": "air_wb_cre_dt",                                  "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_h2si",               StringType(),      False, {"type": "VARCHAR(64)",              "default": "-",                        "path": "air_wb_cre_h2si",                                "key": KEYS_PRIMARY}),
            StructField("air_wb_pce_num",                ShortType(),       False, {"type": "smallint",                 "default": 0,                          "path": "air_wb_pce_num",                                 "key": KEYS_PRIMARY}),
            StructField("crgo_hndlg_seq_num",            ShortType(),       False, {"type": "smallint",                 "default": 0,                          "path": lambda _: col("pos") + 1,                         "key": KEYS_PRIMARY}),
            StructField("eff_fm_cent_tz",                TimestampType(),   False, {"type": "timestamp",                "default": "1900-01-01 00:00:00.000",  "path": lambda _: to_timestamp(col("evnt_tz"))}),
            StructField("eff_to_cent_tz",                TimestampType(),   False, {"type": "timestamp",                                                       "path": lambda _: to_timestamp(lit("2099-12-31T00:00:00.000")),  "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("min_eff_fm_flag",               ShortType(),       False, {"type": "smallint",                                                        "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("max_eff_to_flag",               ShortType(),       False, {"type": "smallint",                                                        "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("crgo_spcl_hndlg_cde",           StringType(),      False, {"type": "CHAR(3)",                  "default": "-",                        "path": lambda _: trim(col("exploded_code")), "preproc": lambda _, df: add_max_seq_num(_, df.select("*", posexplode(split(col("crgo_spcl_hndlg_ary_txt"), ",")).alias("pos", "exploded_code")))}),
            StructField("year",                          StringType(),      False, {"type": "string",                                                          "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
            StructField("month",                         StringType(),      False, {"type": "string",                                                          "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("day",                           StringType(),      False, {"type": "string",                                                          "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("original_job_id",               IntegerType(),     False, {"type": "INTEGER",                                                         "path": lambda _: col("latest_job_id"),         "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("latest_job_id",                 IntegerType(),     False, {"type": "INTEGER",                                                         "path": lambda _: lit(latest_job_id)})
        ]),
        "cmdty_crgo_leg": StructType([
            StructField("air_wb_prfx_id",               StringType(),       False, {"type": "char(3)",      "default": "-",                       "path": 'air_wb_prfx_id',      "key": KEYS_PRIMARY}),
            StructField("air_wb_num",                   StringType(),       False, {"type": "char(8)",      "default": "-",                       "path": 'air_wb_num',          "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_dt",                DateType(),         False, {"type": "date",         "default": "1999-12-31",              "path": 'air_wb_cre_dt', "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_h2si",              StringType(),       False, {"type": "VARCHAR(64)",  "default": "00:00:00.000000",         "path": 'air_wb_cre_h2si', "key": KEYS_PRIMARY}),
            StructField("air_wb_pce_num",               ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": 'air_wb_pce_num', "key": KEYS_PRIMARY}),
            StructField("opng_carr_cde",                StringType(),       False, {"type": "char(3)",      "default": "-",                       "path": lambda _: col("opng_carr_cde")}),
            StructField("opng_flt_num",                 ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": "opng_flt_num"}),
            StructField("opng_flt_num_sufx_txt",        StringType(),       False, {"type": "char(1)",      "default": "-",                       "path": "opng_flt_num_sufx_txt"}),
            StructField("flt_dep_dt",                   DateType(),         False, {"type": "date",         "default": "1999-12-31",              "path": "flt_dep_dt"}),
            StructField("leg_orig_arpt_cde",            StringType(),       False, {"type": "char(3)",      "default": "-",                       "path": "leg_orig_arpt_cde"}),
            StructField("leg_dest_arpt_cde",            StringType(),       False, {"type": "char(3)",      "default": "-",                       "path": "leg_dest_arpt_cde"}),
            StructField("crgo_leg_seq_num",             ShortType(),        True,  {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("max_crgo_leg_seq_num",         ShortType(),        True,  {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("reqr_crgo_dep_ld_flag",        ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("scan_crgo_dep_ld_flag",        ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("scan_crgo_dep_ld_evnt_tz",     TimestampType(),    True,  {"type": "timestamp",    "default": "2099-12-31 12:00:00.000", "path": lambda _: lit("2099-12-31 12:00:00.000").cast("timestamp")}),
            StructField("scan_crgo_dep_thru_flag",      ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("reqr_crgo_arr_unld_flag",      ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("scan_crgo_arr_unld_flag",      ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("scan_crgo_arr_unld_evnt_tz",   TimestampType(),    True,  {"type": "timestamp",    "default": "2099-12-31 12:00:00.000", "path": lambda _: lit("2099-12-31 12:00:00.000").cast("timestamp")}),
            StructField("scan_crgo_dep_onbd_flag",      ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("scan_crgo_rem_onbd_flag",      ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("err_scan_crgo_dep_onbd_flag",  ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("err_scan_crgo_arr_unld_flag",  ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("pln_max_eff_to_cent_tz",       TimestampType(),    True,  {"type": "timestamp",    "default": "2099-12-31 12:00:00.000", "path": lambda _: lit("2099-12-31 12:00:00.000").cast("timestamp")}),
            StructField("scan_crgo_leg_evnt_flag",      ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("cmdty_crgo_wt_bal_cde",        StringType(),       False, {"type": "char(2)",      "default": "-",                       "path": lambda _: lit("-")}),
            StructField("crgo_pce_wt_lbs_qty",          DecimalType(9,2),   True,  {"type": "decimal(9,2)", "default": 0.00,                      "path": lambda _: lit(0.00).cast("decimal(9,2)")}),
            StructField("scan_crgo_wt_bal_evnt_tz",     TimestampType(),    True,  {"type": "timestamp",    "default": "2099-12-31 12:00:00.000", "path": lambda _: lit("2099-12-31 12:00:00.000").cast("timestamp")}),
            StructField("cmdty_flt_leg_type_cde",       StringType(),       False, {"type": "char(1)",      "default": "-",                       "path": lambda _: col("cmdty_flt_leg_type_cde")}),
            StructField("scan_leg_seq_num",             ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("max_scan_leg_seq_num",         ShortType(),        False, {"type": "smallint",     "default": 0,                         "path": lambda _: lit(0)}),
            StructField("year",                          StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
            StructField("month",                         StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("day",                           StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("eff_fm_evnt_tz",                TimestampType(),   False, {"type": "timestamp",                                          "path": lambda _: lit("2099-12-31 12:00:00.000").cast("timestamp"), "purpose": PRPS_DATALAKE_ONLY}),
            StructField("original_job_id",              IntegerType(),      False, {"type": "INTEGER",                                            "path": lambda _: col("latest_job_id"),  "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("latest_job_id",                IntegerType(),      False, {"type": "INTEGER",                                            "path": lambda _: lit(latest_job_id)})
        ]),
          "cmdty_crgo": StructType([
           StructField("air_wb_prfx_id",                StringType(),      False, {"type": "char(3)",                                                     "path": "air_wb_prfx_id",                    "key": KEYS_PRIMARY}),
           StructField("air_wb_num",                    StringType(),      False, {"type": "char(8)",                                                     "path": "air_wb_num",                        "key": KEYS_PRIMARY}),
           StructField("air_wb_cre_dt",                 DateType(),        False, {"type": "date",                                                        "path": lambda _: col("air_wb_cre_dt"),      "key": KEYS_PRIMARY}),
           StructField("air_wb_cre_h2si",               StringType(),      False, {"type": "VARCHAR(64)",                                                 "path": lambda _: col("air_wb_cre_h2si"),    "key": KEYS_PRIMARY}),
           StructField("air_wb_pce_num",                ShortType(),       False, {"type": "smallint",                                                    "path": lambda _: col("air_wb_pce_num"),     "key": KEYS_PRIMARY}),
           StructField("eff_fm_cent_tz",                TimestampType(),   False, {"type": "timestamp",                                                   "path": lambda _: to_timestamp(col("evnt_tz"))}),
           StructField("eff_to_cent_tz",                TimestampType(),   False, {"type": "timestamp",                                                   "path": lambda _: to_timestamp(lit('2099-12-31 00:00:00.000')), "purpose": PRPS_REDSHIFT_ONLY}),
           StructField("min_eff_fm_flag",               ShortType(),       False, {"type": "smallint",                                                    "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
           StructField("max_eff_to_flag",               ShortType(),       False, {"type": "smallint",                                                    "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
           StructField("cmdty_crgo_pce_stat_cde",       StringType(),      False, {"type": "CHAR(1)",            "default": "-",                          "path": lambda _: col("cmdty_crgo_pce_stat_cde")}),
           StructField("cmdty_crgo_wt_bal_cde",         StringType(),      False, {"type": "CHAR(2)",            "default": "-",                          "path": lambda _: lit("-")}),
           StructField("crgo_itin_canc_flag",           ShortType(),       False, {"type": "smallint",           "default": 0,                            "path": lambda _: when(lower(trim(col("isdeleted"))) == "true", lit(1)).otherwise(lit(0))}),
           StructField("crgo_pce_wt_lbs_qty",           DecimalType(9,2),  True,  {"type": "decimal(9,2)",       "default": 0.0,                          "path": lambda _: col("crgo_pce_wt_lbs_qty")}),
           StructField("crgo_svc_lvl_cde",              StringType(),      False, {"type": "VARCHAR(20)",        "default": "-",                          "path": lambda _: lit("-")}),
           StructField("crgo_spcl_hndlg_ary_txt",       StringType(),      True,  {"type": "VARCHAR(50)",        "default": "-",                          "path": lambda _: lit("-")}),
           StructField("cmdty_unkn_flag",               ShortType(),       False, {"type": "smallint",                                                    "path": lambda _: col("cmdty_unkn_flag")}),
           StructField("year",                          StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
           StructField("month",                         StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
           StructField("day",                           StringType(),      False, {"type": "string",                                                    "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
           StructField("original_job_id",               IntegerType(),     False, {"type": "INTEGER",                                                     "path": lambda _: col("latest_job_id"),         "purpose": PRPS_REDSHIFT_ONLY}),
           StructField("latest_job_id",                 IntegerType(),     False, {"type": "INTEGER",                                                     "path": lambda _: lit(latest_job_id)})
        ]),
        "cmdty_crgo_pln_leg": StructType([
            StructField("air_wb_prfx_id",                StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": "airwaybillprefix",                     "key": KEYS_PRIMARY}),
            StructField("air_wb_num",                    StringType(),      False, {"type": "char(8)",                  "default": "-",                   "path": "airwaybillnumber",                     "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_dt",                 DateType(),        False, {"type": "date",                     "default": "1900-01-01",          "path": lambda _: to_date(col("airwaybillcreationdate")), "key": KEYS_PRIMARY}),
            StructField("air_wb_cre_h2si",               StringType(),      False, {"type": "VARCHAR(64)",              "default": "-",                   "path": lambda _: date_format(to_timestamp(col("airwaybillcreationdate")), "HH:mm:ss.SSS"),        "key": KEYS_PRIMARY}),
            StructField("air_wb_pce_num",                ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: col("airwaybillpiecenumber").cast("smallint"),                   "key": KEYS_PRIMARY}),
            StructField("opng_carr_cde",                 StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _: col("exploded_leg.flightleg.operationalcarrier"), "key": KEYS_PRIMARY}),
            StructField("opng_flt_num",                  ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: col("exploded_leg.flightleg.flightnumber").cast("smallint"), "key": KEYS_PRIMARY}),
            StructField("opng_flt_num_sufx_txt",         StringType(),      False, {"type": "char(1)",                  "default": "-",                   "path": lambda _: coalesce(col("exploded_leg.flightleg.operationalsuffix"), lit("-")), "key": KEYS_PRIMARY}),
            StructField("flt_dep_dt",                    DateType(),        False, {"type": "date",                     "default": "1900-01-01",          "path": lambda _: to_date(col("exploded_leg.flightleg.origindate")), "key": KEYS_PRIMARY}),
            StructField("leg_orig_arpt_cde",             StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _: col("exploded_leg.flightleg.departureairportiatacode"), "key": KEYS_PRIMARY}),
            StructField("leg_dest_arpt_cde",             StringType(),      False, {"type": "char(3)",                  "default": "-",                   "path": lambda _: col("exploded_leg.flightleg.arrivalairportiatacode"), "key": KEYS_PRIMARY}),
            StructField("eff_fm_cent_tz",                TimestampType(),   False, {"type": "timestamp",                "default": "1900-01-01 00:00:00.000", "path": lambda _: to_timestamp(col("event_created"))}),
            StructField("eff_to_cent_tz",                TimestampType(),   False, {"type": "timestamp",                                                      "path": lambda _: to_timestamp(lit("2099-12-31T00:00:00.000")), "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("min_eff_fm_flag",               ShortType(),       False, {"type": "smallint",                                                       "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("max_eff_to_flag",               ShortType(),       False, {"type": "smallint",                                                       "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("pln_crgo_leg_seq_num",          IntegerType(),     False, {"type": "smallint",                 "default": 0,                     "path": lambda _: expr("array_position(planneditinerary, exploded_leg)")}),
            # StructField("pln_max_crgo_leg_seq_num",      ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": lambda _: spark_max(expr("size(planneditinerary)")).over(Window.partitionBy("airwaybillprefix", "airwaybillnumber", "airwaybillcreationdate", "airwaybillpiecenumber"))}),
            StructField("pln_max_crgo_leg_seq_num",      IntegerType(),     False, {"type": "smallint",                 "default": 0,                     "path": lambda _: coalesce(size(col("planneditinerary")), lit(0))}),
            StructField("pln_crgo_dep_ld_flag",          ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": get_dep_ld_flag}),
            StructField("pln_crgo_arr_unld_flag",        ShortType(),       False, {"type": "smallint",                 "default": 0,                     "path": get_arr_unld_flag}),
            StructField("cmdty_flt_leg_type_cde",        StringType(),      False, {"type": "char(1)",                  "default": "-",                   "path": get_pln_leg_type_code}),
            StructField("year",                          StringType(),      False, {"type": "string",                                                     "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
            StructField("month",                         StringType(),      False, {"type": "string",                                                     "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("day",                           StringType(),      False, {"type": "string",                                                     "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
            StructField("original_job_id",               IntegerType(),     False, {"type": "integer",                                                   "path": lambda _: col("latest_job_id"),         "purpose": PRPS_REDSHIFT_ONLY}),
            StructField("latest_job_id",                 IntegerType(),     False, {"type": "integer",                                                   "path": lambda _: lit(latest_job_id)})
        ]),
        "cmdty_crgo_stats": StructType([
            StructField("orc_run_id",   StringType(),  False, {"key": KEYS_PRIMARY, "purpose": PRPS_PARTITION}),
            StructField("target_table", StringType(),  False, {"key": KEYS_PRIMARY}),
            StructField("job_run_id",   StringType(),  False, {"key": KEYS_PRIMARY}),
            StructField("job_name",     StringType(),  False),
            StructField("job_runtime",  IntegerType(), False),
            StructField("input_cnt",    IntegerType(), False),
            StructField("output_cnt",   IntegerType(), False),
            StructField("reject_cnt",   IntegerType(), True),
            StructField("dupe_cnt",     IntegerType(), True),
            StructField("warning_str",  StringType(),  True)
        ]),

        "metadata" : {
            "cmdty_crgo_curated": {
                'CDC': False,
                'curated': True,
                "parent": "refined_zone_cargo_v2_dev1_v1",
                "replay_calc_flds": ["s3_object","year", "month", "day"]
            },
            'cmdty_crgo_evnt': {
                'parent': 'cmdty_crgo_curated',
                'remove_dupes': True,
                'orderby_keys': ['evnt_tz'],
                'extract_filter': lambda _job, df: df.filter(col('event_name').isin(event_filter_list)),
                'custom_sqls': {
                    'sql_update_max_evnt_flag': sql_update_max_evnt_flag,
                    'sql_distinct_stage': sql_cust_sql_distinct_stage,
                    'sql_delete_unchanged_from_staging': sql_cust_delete_unchanged_from_staging,
                    'sql_delete_older_records_from_target': sql_delete_older_records_from_target,
                    'sql_cdc_stage_target_union': sql_cdc_stage_target_union,
                    'sql_delete_outdated_staging_records': sql_delete_outdated_staging_records
                },
                'preactions': ['sql_delete_preaction'],
                'postactions': [
                    'sql_delete_outdated_staging_records',
                    'sql_cdc_stage_target_union',
                    'sql_distinct_stage',
                    'sql_update_staging',
                    'sql_update_max_evnt_flag',
                    'sql_delete_unchanged_from_staging',
                    'sql_delete_older_records_from_target',
                    'sql_insert_to_target'
                ]
            },


            'cmdty_crgo_hndlg': {
                'parent': 'cmdty_crgo_evnt',
                'CDC': False,
                'remove_dupes': True,
                'orderby_keys': ['eff_fm_cent_tz'],
                'extract_filter': lambda _job, df: df.filter((col('cmdty_crgo_evnt_type_cde').isin(['CC', 'CR'])) & (length(col('crgo_spcl_hndlg_ary_txt')) > 0)),
                'custom_sqls': {
                    'sql_cust_sql_distinct_stage': sql_cust_sql_distinct_stage,
                    'sql_cust_set_eff_dates_and_flags': sql_cust_set_eff_dates_and_flags,
                    'sql_cdc_stage_target_union': sql_cdc_stage_target_union,
                    'sql_delete_all_matching_from_target': sql_delete_older_records_from_target,
                },
                'preactions': ['sql_delete_preaction'],
                'postactions': [
                    'sql_cdc_stage_target_union',
                    'sql_cust_sql_distinct_stage',
                    'sql_update_staging',
                    'sql_cust_set_eff_dates_and_flags',
                    'sql_delete_outdated_from_staging',
                    'sql_delete_all_matching_from_target',
                    'sql_insert_to_target'
                ]
            },
            'cmdty_crgo_evnt_scan_leg': {
                'parent': 'cmdty_crgo_curated',
                'orderby_keys': ['evnt_tz'],
                'preactions': ['sql_delete_preaction'],
                'remove_dupes': True,
                'extract_filter': lambda _job, df: df.filter(col('event_name').isin(scan_leg_filter_list)),
                'custom_sqls': {
                    'sql_update_scan_reqr_crgo_arr_unld_flag': sql_update_scan_reqr_crgo_arr_unld_flag,
                    'sql_distinct_stage': sql_cust_sql_distinct_stage,
                    'sql_delete_unchanged_from_staging': sql_cust_delete_unchanged_from_staging,
                    'sql_delete_older_records_from_target' : sql_delete_older_records_from_target
                },
                'postactions': [
                    'sql_distinct_stage',
                    'sql_delete_unchanged_from_staging',
                    'sql_update_staging',
                    'sql_update_scan_reqr_crgo_arr_unld_flag',
                    'sql_delete_older_records_from_target',
                    'sql_insert_to_target'
                ]
            },
            'cmdty_crgo_leg': {
                'parent': 'cmdty_crgo_pln_leg',
                'remove_dupes': True,
                'orderby_keys': ['opng_carr_cde', 'opng_flt_num', 'opng_flt_num_sufx_txt', 'flt_dep_dt', 'leg_orig_arpt_cde', 'leg_dest_arpt_cde', 'crgo_leg_seq_num'],
                'custom_sqls': {
                    'sql_cust_sql_cmdty_crgo_leg'    : sql_cust_sql_cmdty_crgo_leg,
                    'sql_distinct_stage': sql_cust_sql_distinct_stage,
                    'sql_delete_older_records_from_target': sql_delete_older_records_from_target
                },
                'preactions': ['sql_delete_preaction'],
                'postactions': [
                    'sql_cust_sql_cmdty_crgo_leg',
                    'sql_distinct_stage',
                    'sql_update_staging',
                    'sql_delete_older_records_from_target',
                    'sql_insert_to_target'
                ]
            },
            'cmdty_crgo': {
                'parent': 'cmdty_crgo_evnt',
                'CDC': False,
                'remove_dupes': True,
                'orderby_keys': ['eff_fm_cent_tz'],
                'custom_sqls' : {
                    'sql_combined_update_cargo_columns': sql_combined_cargo_columns,
                    'sql_distinct_stage': sql_cust_sql_distinct_stage,
                    'sql_cdc_stage_target_union': sql_cdc_stage_target_union,
                    'sql_delete_all_matching_from_target': sql_delete_older_records_from_target,
                    'sql_cust_set_eff_dates_and_flags': sql_cust_set_eff_dates_and_flags
                    },
                'preactions': ['sql_delete_preaction'],
                'postactions': [
                    'sql_cdc_stage_target_union',
                    'sql_combined_update_cargo_columns',
                    'sql_distinct_stage',
                    'sql_cust_set_eff_dates_and_flags',
                    'sql_update_staging',
                    'sql_delete_outdated_from_staging',
                    'sql_delete_all_matching_from_target',
                    'sql_insert_to_target'
                ]
            },
              'cmdty_crgo_pln_leg': {
                'parent': 'cmdty_crgo_curated',
                'CDC': False,
                'remove_dupes': True,
                'orderby_keys': ['eff_fm_cent_tz'],
                'extract_filter': lambda _job, df: df.filter(col('event_name').isin(event_filter_list)).select( "*", expr("posexplode(planneditinerary) as (leg_pos, exploded_leg)"),
                                                                                                                    expr("planneditinerary[leg_pos - 1] as prev_leg"),
                                                                                                                    expr("planneditinerary[leg_pos + 1] as next_leg") ),
                'custom_sqls': {
                    'sql_cust_set_eff_dates_and_flags': sql_pln_leg_set_eff_dates_and_flags,
                    'sql_cdc_stage_target_union': sql_cdc_stage_target_union,
                    'sql_delete_older_records_from_target': sql_delete_older_records_from_target,
                },
                'preactions': ['sql_delete_preaction'],
                'postactions': [
                    'sql_cdc_stage_target_union',
                    'sql_cust_set_eff_dates_and_flags',
                    'sql_delete_outdated_from_staging',
                    'sql_update_staging',
                    'sql_delete_older_records_from_target',
                    'sql_insert_to_target',
                ]
            },
            "cmdty_crgo_stats": {
                "stats": True,
                "parent": "*various: programatically generated data*"
            }
        }
    } # type: ignore
