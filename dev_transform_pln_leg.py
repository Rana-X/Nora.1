"""
COMPLETE DEV TRANSFORMATION LOGIC for cmdty_crgo_pln_leg
========================================================

Self-contained extract of the production AWS Glue / PySpark transform that
builds the `cmdty_crgo_pln_leg` table. Pulled verbatim from:
  * cmdty_crgo_schemas.py  (helpers, the StructType schema, the metadata entry)
  * glue_clss.py           (transform_df: the engine that APPLIES the schema)

How the dev pipeline actually runs for cmdty_crgo_pln_leg:
  EXTRACT  -> read source events from cmdty_crgo_curated
  FILTER   -> metadata['extract_filter'] (event_filter_list + posexplode
              plannedItinerary into one row per leg, with prev_leg/next_leg)
  TRANSFORM-> transform_df():
                1. apply each field's metadata['preproc'] (none for pln_leg)
                2. build source->target columns from each field's
                   metadata['path'] (a literal source column name, or a lambda
                   returning a Spark Column); left-pad if metadata['lpad']
                3. df.na.fill(default_vals) for fields with metadata['default']
                   (NOTE: na.fill only fills columns whose type matches the
                    fill value -> string/numeric defaults apply, Date/Timestamp
                    columns are NOT filled and stay NULL on parse failure)
                4. remove_dupes: groupBy(all non-partition/non-redshift cols)
                   keeping min(partition struct) == SELECT DISTINCT on the
                   business columns
  VALIDATE -> AWS Glue Data Quality rules (rejects rows that fail) [not here]
  LOAD     -> write to datalake; Redshift CDC handled separately

Primary keys (metadata 'key': KEYS_PRIMARY) — the 11 business keys:
  air_wb_prfx_id, air_wb_num, air_wb_cre_dt, air_wb_cre_h2si, air_wb_pce_num,
  opng_carr_cde, opng_flt_num, opng_flt_num_sufx_txt, flt_dep_dt,
  leg_orig_arpt_cde, leg_dest_arpt_cde
"""

from time import time

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    coalesce, col, lit, split, when, to_date, to_timestamp, date_format,
    get_json_object, trim, lower, expr, regexp_replace, size, transform,
    filter, translate, array, element_at, posexplode, posexplode_outer,
    length, max as spark_max,
)
from pyspark.sql.types import (
    ArrayType, IntegerType, StringType, StructField, StructType, ShortType,
    DecimalType, DateType, TimestampType,
)

# --- purpose / key constants (from glue_clss.py + cmdty_crgo_schemas.py) ------
KEYS_PRIMARY       = 'p'   # field is part of the primary/business key
PRPS_PARTITION     = 'p'   # partition column (year/month/day)
PRPS_REDSHIFT_ONLY = 'r'   # exists only in Redshift, not the datalake table
PRPS_DATALAKE_ONLY = 'd'   # exists only in the datalake, dropped before Redshift

# latest_job_id is generated ONCE per Glue run as the wall-clock epoch second.
latest_job_id = int(time())

# Events the pln_leg job keeps (metadata['extract_filter']).
event_filter_list = [
    'piece-created', 'piece-itinerary-changed', 'piece-wab-added',
    'piece-wab-changed', 'piece-loaded-on-flight', 'piece-unloaded-from-flight',
    'piece-offloaded-from-flight', 'piece-undeleted', 'piece-deleted',
    'piece-updated', 'piece-tracking', 'piece-loaded-by-backup-mode',
    'piece-not-loaded-by-backup-mode',
]


# =============================================================================
# Per-field derivation helpers (verbatim from cmdty_crgo_schemas.py)
# =============================================================================

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


# =============================================================================
# The cmdty_crgo_pln_leg schema (verbatim from cmdty_crgo_schemas.py)
#
# Each StructField metadata dict carries:
#   'path'    : source column name (str) OR a lambda(self)->Column producing the value
#   'default' : na.fill default (applied only if column type matches the value)
#   'type'    : target DB datatype (used by DQ validation)
#   'key'     : KEYS_PRIMARY -> part of the business/primary key
#   'purpose' : PRPS_PARTITION / PRPS_REDSHIFT_ONLY / PRPS_DATALAKE_ONLY
#   'lpad'    : (width, char) left-pad spec
# =============================================================================

CMDTY_CRGO_PLN_LEG_SCHEMA = StructType([
    StructField("air_wb_prfx_id",                StringType(),      False, {"type": "char(3)",      "default": "-",                       "path": "airwaybillprefix",                     "key": KEYS_PRIMARY}),
    StructField("air_wb_num",                    StringType(),      False, {"type": "char(8)",      "default": "-",                       "path": "airwaybillnumber",                     "key": KEYS_PRIMARY}),
    StructField("air_wb_cre_dt",                 DateType(),        False, {"type": "date",         "default": "1900-01-01",              "path": lambda _: to_date(col("airwaybillcreationdate")), "key": KEYS_PRIMARY}),
    StructField("air_wb_cre_h2si",               StringType(),      False, {"type": "VARCHAR(64)",  "default": "-",                       "path": lambda _: date_format(to_timestamp(col("airwaybillcreationdate")), "HH:mm:ss.SSS"),        "key": KEYS_PRIMARY}),
    StructField("air_wb_pce_num",                ShortType(),       False, {"type": "smallint",     "default": 0,                         "path": lambda _: col("airwaybillpiecenumber").cast("smallint"),                   "key": KEYS_PRIMARY}),
    StructField("opng_carr_cde",                 StringType(),      False, {"type": "char(3)",      "default": "-",                       "path": lambda _: col("exploded_leg.flightleg.operationalcarrier"), "key": KEYS_PRIMARY}),
    StructField("opng_flt_num",                  ShortType(),       False, {"type": "smallint",     "default": 0,                         "path": lambda _: col("exploded_leg.flightleg.flightnumber").cast("smallint"), "key": KEYS_PRIMARY}),
    StructField("opng_flt_num_sufx_txt",         StringType(),      False, {"type": "char(1)",      "default": "-",                       "path": lambda _: coalesce(col("exploded_leg.flightleg.operationalsuffix"), lit("-")), "key": KEYS_PRIMARY}),
    StructField("flt_dep_dt",                    DateType(),        False, {"type": "date",         "default": "1900-01-01",              "path": lambda _: to_date(col("exploded_leg.flightleg.origindate")), "key": KEYS_PRIMARY}),
    StructField("leg_orig_arpt_cde",             StringType(),      False, {"type": "char(3)",      "default": "-",                       "path": lambda _: col("exploded_leg.flightleg.departureairportiatacode"), "key": KEYS_PRIMARY}),
    StructField("leg_dest_arpt_cde",             StringType(),      False, {"type": "char(3)",      "default": "-",                       "path": lambda _: col("exploded_leg.flightleg.arrivalairportiatacode"), "key": KEYS_PRIMARY}),
    StructField("eff_fm_cent_tz",                TimestampType(),   False, {"type": "timestamp",    "default": "1900-01-01 00:00:00.000", "path": lambda _: to_timestamp(col("event_created"))}),
    StructField("eff_to_cent_tz",                TimestampType(),   False, {"type": "timestamp",                                          "path": lambda _: to_timestamp(lit("2099-12-31T00:00:00.000")), "purpose": PRPS_REDSHIFT_ONLY}),
    StructField("min_eff_fm_flag",               ShortType(),       False, {"type": "smallint",                                           "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
    StructField("max_eff_to_flag",               ShortType(),       False, {"type": "smallint",                                           "path": lambda _: lit(0), "purpose": PRPS_REDSHIFT_ONLY}),
    StructField("pln_crgo_leg_seq_num",          IntegerType(),     False, {"type": "smallint",     "default": 0,                         "path": lambda _: expr("array_position(planneditinerary, exploded_leg)")}),
    # NOTE: the window-based version below is the OLD logic, kept commented in dev:
    # StructField("pln_max_crgo_leg_seq_num",    ShortType(),       False, {"type": "smallint",     "default": 0,                         "path": lambda _: spark_max(expr("size(planneditinerary)")).over(Window.partitionBy("airwaybillprefix", "airwaybillnumber", "airwaybillcreationdate", "airwaybillpiecenumber"))}),
    StructField("pln_max_crgo_leg_seq_num",      IntegerType(),     False, {"type": "smallint",     "default": 0,                         "path": lambda _: coalesce(size(col("planneditinerary")), lit(0))}),
    StructField("pln_crgo_dep_ld_flag",          ShortType(),       False, {"type": "smallint",     "default": 0,                         "path": get_dep_ld_flag}),
    StructField("pln_crgo_arr_unld_flag",        ShortType(),       False, {"type": "smallint",     "default": 0,                         "path": get_arr_unld_flag}),
    StructField("cmdty_flt_leg_type_cde",        StringType(),      False, {"type": "char(1)",      "default": "-",                       "path": get_pln_leg_type_code}),
    StructField("year",                          StringType(),      False, {"type": "string",                                             "purpose": PRPS_PARTITION,       "lpad": (4, "0")}),
    StructField("month",                         StringType(),      False, {"type": "string",                                             "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
    StructField("day",                           StringType(),      False, {"type": "string",                                             "purpose": PRPS_PARTITION,       "lpad": (2, "0")}),
    StructField("original_job_id",               IntegerType(),     False, {"type": "integer",                                            "path": lambda _: col("latest_job_id"),         "purpose": PRPS_REDSHIFT_ONLY}),
    StructField("latest_job_id",                 IntegerType(),     False, {"type": "integer",                                            "path": lambda _: lit(latest_job_id)}),
])


# =============================================================================
# The metadata entry for cmdty_crgo_pln_leg (verbatim from cmdty_crgo_schemas.py)
#
# extract_filter is THE source-shaping step: keep event_filter_list events,
# posexplode plannedItinerary into one row per leg, and attach the previous /
# next leg structs so the dep/arr connection-break flags can be computed.
# =============================================================================

CMDTY_CRGO_PLN_LEG_METADATA = {
    'parent': 'cmdty_crgo_curated',
    'CDC': False,
    'remove_dupes': True,
    'orderby_keys': ['eff_fm_cent_tz'],
    'extract_filter': lambda _job, df: df.filter(col('event_name').isin(event_filter_list)).select(
        "*",
        expr("posexplode(planneditinerary) as (leg_pos, exploded_leg)"),
        expr("planneditinerary[leg_pos - 1] as prev_leg"),
        expr("planneditinerary[leg_pos + 1] as next_leg"),
    ),
    # custom_sqls / preactions / postactions below run in the REDSHIFT load,
    # not the datalake transform — included for completeness.
    'custom_sqls': {
        'sql_cust_set_eff_dates_and_flags': 'sql_pln_leg_set_eff_dates_and_flags',
        'sql_cdc_stage_target_union': 'sql_cdc_stage_target_union',
        'sql_delete_older_records_from_target': 'sql_delete_older_records_from_target',
    },
    'preactions': ['sql_delete_preaction'],
    'postactions': [
        'sql_cdc_stage_target_union',
        'sql_cust_set_eff_dates_and_flags',
        'sql_delete_outdated_from_staging',
        'sql_update_staging',
        'sql_delete_older_records_from_target',
        'sql_insert_to_target',
    ],
}


# =============================================================================
# The transform engine (verbatim core of glue_clss.AWSGLueJob.transform_df)
#
# This is what turns the schema above into the output dataframe. `self` is the
# job; self.get_schema(tbl) returns CMDTY_CRGO_PLN_LEG_SCHEMA; self.tbl_metadata
# is CMDTY_CRGO_PLN_LEG_METADATA; self.get_partition_keys() -> ['year','month','day'].
# =============================================================================

def transform_df(self, df: DataFrame) -> DataFrame:
    schema = CMDTY_CRGO_PLN_LEG_SCHEMA

    # 1) extract_filter (event filter + posexplode + prev/next leg)
    if extract_filter := self.tbl_metadata.get('extract_filter'):
        df = extract_filter(self, df)

    # 2) preproc hooks (none defined for pln_leg)
    for fld in schema.fields:
        if preproc := fld.metadata.get('preproc'):
            df = preproc(self, df)

    # 3) source -> target: evaluate each field's 'path' (lambda or column name),
    #    left-pad when 'lpad' is present. Redshift-only fields are skipped.
    s2t_exprs = [
        (lpad_col(tgt_val.cast(StringType()), pad_spec) if pad_spec else tgt_val).alias(fld.name)
        for fld in schema.fields
        if fld.metadata.get('purpose') != PRPS_REDSHIFT_ONLY
        if (tgt_val := (fld.metadata['path'](self) if callable(fld.metadata['path'])
                        else col(fld.metadata['path']))) is not None
        if (pad_spec := fld.metadata.get('lpad')) or True
    ]
    df = df.select(*s2t_exprs)

    # 4) fill defaults (na.fill only affects columns whose type matches the
    #    value, so Date/Timestamp columns are NOT defaulted -> stay NULL)
    default_vals = {
        fld.name: fld.metadata['default']
        for fld in schema.fields
        if 'default' in fld.metadata
    }
    if default_vals:
        df = df.na.fill(default_vals)

    # 5) remove_dupes: dedupe on every non-partition, non-redshift column
    if self.tbl_metadata.get('remove_dupes'):
        from pyspark.sql.functions import struct, min as cmin
        part_keys = self.get_partition_keys()  # ['year','month','day']
        unique_df = df.groupBy(
            *[fld.name for fld in schema.fields
              if fld.metadata.get('purpose') not in [PRPS_PARTITION, PRPS_REDSHIFT_ONLY]]
        ).agg(
            cmin(struct(*[col(f).alias(f) for f in part_keys])).alias('min_struct')
        )
        for f in part_keys:
            unique_df = unique_df.withColumn(f, col('min_struct')[f])
        df = unique_df.drop('min_struct')

    return df


def lpad_col(c: Column, pad_spec) -> Column:
    """Helper: schema 'lpad' is (width, char) -> Spark lpad(c, width, char)."""
    from pyspark.sql.functions import lpad
    width, char = pad_spec
    return lpad(c, width, char)
