"""cmdty_crgo_pln_leg validation - PySpark version for Athena Spark / Glue.

Runs the same source -> transform -> compare flow as
cmdty_crgo_pln_leg_validation.sql, but in PySpark, reusing the EXACT dev
expressions from cmdty_crgo_schemas.py (posexplode + array_position + the
same when() chains + the same na.fill defaults). Because this executes the
identical Spark functions the dev Glue job uses, it removes every
SQL-translation caveat from the parity audit (timezone handling, cast edge
cases, struct equality semantics are all natively identical).

Where to run:
  * Athena Spark workgroup notebook: paste into a cell; the notebook provides
    `spark` - then call run_validation(spark).
  * Glue job / Glue interactive session: same, after the usual session setup.

Output: a one-row summary DataFrame (counts + one mismatch counter per
non-identity field), plus an optional row-level mismatch DataFrame.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SOURCE_TBL = "datalake_prod1_entp_ctds.cmdty_crgo_curated"
TARGET_TBL = "datalake_prod1_entp_ctds.cmdty_crgo_pln_leg"
BEG_DT = "2026-05-20"
END_DT = "2026-06-07"

# Same list as cmdty_crgo_schemas.event_filter_list
EVENT_FILTER_LIST = [
    'piece-created', 'piece-itinerary-changed', 'piece-wab-added',
    'piece-wab-changed', 'piece-loaded-on-flight', 'piece-unloaded-from-flight',
    'piece-offloaded-from-flight', 'piece-undeleted', 'piece-deleted',
    'piece-updated', 'piece-tracking', 'piece-loaded-by-backup-mode',
    'piece-not-loaded-by-backup-mode'
]

# Identity = which AWB piece + which event version + which leg slot.
IDENTITY_COLS = [
    'air_wb_prfx_id', 'air_wb_num', 'air_wb_cre_dt', 'air_wb_cre_h2si',
    'air_wb_pce_num', 'eff_fm_cent_tz', 'pln_crgo_leg_seq_num'
]

# Every other field gets its own mismatch counter.
COMPARED_COLS = [
    'opng_carr_cde', 'opng_flt_num', 'opng_flt_num_sufx_txt', 'flt_dep_dt',
    'leg_orig_arpt_cde', 'leg_dest_arpt_cde', 'pln_max_crgo_leg_seq_num',
    'pln_crgo_dep_ld_flag', 'pln_crgo_arr_unld_flag', 'cmdty_flt_leg_type_cde'
]

BUSINESS_COLS = IDENTITY_COLS + COMPARED_COLS


def get_dep_ld_flag():
    """Verbatim port of cmdty_crgo_schemas.get_dep_ld_flag."""
    return F.when(
        F.col("leg_pos") == F.lit(0), F.lit(1)
    ).when(
        F.col("prev_leg").isNull(), F.lit(1)
    ).when(
        (F.col("prev_leg.flightleg.flightnumber") != F.col("exploded_leg.flightleg.flightnumber")) |
        (F.col("prev_leg.flightleg.operationalcarrier") != F.col("exploded_leg.flightleg.operationalcarrier")) |
        (F.col("prev_leg.flightleg.arrivalairportiatacode") != F.col("exploded_leg.flightleg.departureairportiatacode")),
        F.lit(1)
    ).otherwise(F.lit(0))


def get_arr_unld_flag():
    """Verbatim port of cmdty_crgo_schemas.get_arr_unld_flag."""
    return F.when(
        F.col("leg_pos") == F.expr("size(planneditinerary) - 1"), F.lit(1)
    ).when(
        F.col("next_leg").isNull(), F.lit(1)
    ).when(
        (F.col("next_leg.flightleg.flightnumber") != F.col("exploded_leg.flightleg.flightnumber")) |
        (F.col("next_leg.flightleg.operationalcarrier") != F.col("exploded_leg.flightleg.operationalcarrier")) |
        (F.col("next_leg.flightleg.departureairportiatacode") != F.col("exploded_leg.flightleg.arrivalairportiatacode")),
        F.lit(1)
    ).otherwise(F.lit(0))


def get_pln_leg_type_code():
    """Verbatim port of cmdty_crgo_schemas.get_pln_leg_type_code."""
    return (
        F.when(F.lower(F.trim(F.col("exploded_leg.flightlegtypeid"))) == 'unknown', F.lit("-"))
        .when(F.lower(F.trim(F.col("exploded_leg.flightlegtypeid"))) == "originating", F.lit("0"))
        .when(F.lower(F.trim(F.col("exploded_leg.flightlegtypeid"))) == "transfer", F.lit("1"))
        .when(F.lower(F.trim(F.col("exploded_leg.flightlegtypeid"))) == "thru", F.lit("2"))
        .when(F.lower(F.trim(F.col("exploded_leg.flightlegtypeid"))) == "standbyearly", F.lit("3"))
        .otherwise(F.lit("-"))
    )


def build_expected(spark: SparkSession) -> DataFrame:
    """Source -> transform, mirroring the dev pipeline expression-for-expression."""

    # Extract: same filter + posexplode + prev/next legs as the dev
    # metadata['cmdty_crgo_pln_leg'].extract_filter.
    df = (
        spark.table(SOURCE_TBL)
        .filter(F.col('event_name').isin(EVENT_FILTER_LIST))
        .filter(F.col('planneditinerary').isNotNull() & (F.size('planneditinerary') > 0))
        .filter(F.to_date(F.col('airwaybillcreationdate')).between(BEG_DT, END_DT))
        .select("*", F.expr("posexplode(planneditinerary) as (leg_pos, exploded_leg)"))
        .withColumn("prev_leg", F.expr("planneditinerary[leg_pos - 1]"))
        .withColumn("next_leg", F.expr("planneditinerary[leg_pos + 1]"))
    )

    # Transform: the schema 'path' lambdas, verbatim.
    df = df.select(
        F.col("airwaybillprefix").alias("air_wb_prfx_id"),
        F.col("airwaybillnumber").alias("air_wb_num"),
        F.to_date(F.col("airwaybillcreationdate")).alias("air_wb_cre_dt"),
        F.date_format(F.to_timestamp(F.col("airwaybillcreationdate")), "HH:mm:ss.SSS").alias("air_wb_cre_h2si"),
        F.col("airwaybillpiecenumber").cast("smallint").alias("air_wb_pce_num"),
        F.col("exploded_leg.flightleg.operationalcarrier").alias("opng_carr_cde"),
        F.col("exploded_leg.flightleg.flightnumber").cast("smallint").alias("opng_flt_num"),
        F.coalesce(F.col("exploded_leg.flightleg.operationalsuffix"), F.lit("-")).alias("opng_flt_num_sufx_txt"),
        F.to_date(F.col("exploded_leg.flightleg.origindate")).alias("flt_dep_dt"),
        F.col("exploded_leg.flightleg.departureairportiatacode").alias("leg_orig_arpt_cde"),
        F.col("exploded_leg.flightleg.arrivalairportiatacode").alias("leg_dest_arpt_cde"),
        F.to_timestamp(F.col("event_created")).alias("eff_fm_cent_tz"),
        F.expr("array_position(planneditinerary, exploded_leg)").cast("integer").alias("pln_crgo_leg_seq_num"),
        F.coalesce(F.size(F.col("planneditinerary")), F.lit(0)).cast("integer").alias("pln_max_crgo_leg_seq_num"),
        get_dep_ld_flag().cast("smallint").alias("pln_crgo_dep_ld_flag"),
        get_arr_unld_flag().cast("smallint").alias("pln_crgo_arr_unld_flag"),
        get_pln_leg_type_code().alias("cmdty_flt_leg_type_cde"),
    )

    # Defaults: same values, same na.fill semantics as the dev schema
    # (string/numeric fills apply; date/timestamp fills are no-ops, exactly
    # like the dev job).
    df = df.na.fill({
        "air_wb_prfx_id": "-",
        "air_wb_num": "-",
        "air_wb_pce_num": 0,
        "opng_carr_cde": "-",
        "opng_flt_num": 0,
        "opng_flt_num_sufx_txt": "-",
        "leg_orig_arpt_cde": "-",
        "leg_dest_arpt_cde": "-",
        "pln_crgo_leg_seq_num": 0,
        "pln_max_crgo_leg_seq_num": 0,
        "pln_crgo_dep_ld_flag": 0,
        "pln_crgo_arr_unld_flag": 0,
        "cmdty_flt_leg_type_cde": "-",
    })

    # remove_dupes=True on the business columns.
    return df.dropDuplicates(BUSINESS_COLS).select(*BUSINESS_COLS)


def build_actual(spark: SparkSession) -> DataFrame:
    """Target/dev table, logical rows (latest_job_id excluded)."""
    return (
        spark.table(TARGET_TBL)
        .filter(F.col("air_wb_cre_dt").between(BEG_DT, END_DT))
        .select(*BUSINESS_COLS)
        .dropDuplicates(BUSINESS_COLS)
    )


def run_validation(spark: SparkSession, show_detail: bool = False):
    """Full outer join on row identity; every other field gets a mismatch
    counter (+1 per differing key-matched row, +0 on match)."""

    expected = build_expected(spark).alias("s")
    actual = build_actual(spark).alias("t")

    join_cond = [
        F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}")) for c in IDENTITY_COLS
    ]
    joined = expected.join(actual, on=join_cond, how="full_outer")

    s_present = F.col("s.air_wb_prfx_id").isNotNull()
    t_present = F.col("t.air_wb_prfx_id").isNotNull()

    def mismatch(c):
        return F.sum(
            F.when(
                s_present & t_present &
                ~F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}")),
                1
            ).otherwise(0)
        ).alias(f"mismatch_{c}")

    summary = joined.agg(
        F.lit(BEG_DT).alias("beg_dt"),
        F.lit(END_DT).alias("end_dt"),
        F.sum(F.when(s_present, 1).otherwise(0)).alias("source_logical_count"),
        F.sum(F.when(t_present, 1).otherwise(0)).alias("target_logical_count"),
        F.sum(F.when(s_present & t_present, 1).otherwise(0)).alias("matched_on_keys"),
        F.sum(F.when(s_present & ~t_present, 1).otherwise(0)).alias("missing_in_target"),
        F.sum(F.when(~s_present & t_present, 1).otherwise(0)).alias("extra_in_target"),
        *[mismatch(c) for c in COMPARED_COLS],
    )

    detail = None
    if show_detail:
        any_mismatch = F.lit(False)
        for c in COMPARED_COLS:
            any_mismatch = any_mismatch | ~F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
        detail = (
            joined
            .filter(s_present & t_present & any_mismatch)
            .select(
                *[F.col(f"s.{c}").alias(c) for c in IDENTITY_COLS],
                *[col
                  for c in COMPARED_COLS
                  for col in (F.col(f"s.{c}").alias(f"s_{c}"),
                              F.col(f"t.{c}").alias(f"t_{c}"))],
            )
        )

    return summary, detail


if __name__ == "__main__":
    spark = SparkSession.builder.appName("cmdty_crgo_pln_leg_validation").getOrCreate()
    summary_df, detail_df = run_validation(spark, show_detail=True)
    summary_df.show(truncate=False, vertical=True)
    if detail_df is not None:
        detail_df.show(50, truncate=False)
