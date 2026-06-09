'''The config module creates a dictionary named ``settings`` that defines the application configuration. The
most important key for glue jobs is ``spark``; it sets the configuration parameters for optimal performance of
your application. Additional settings are application independent; for example the ``predicate`` key defines
boundaries for the default push down predicate.
'''

from pyspark.conf import SparkConf

settings = {
    'predicate': {
        'future':   2,
        'history': 30
    },

    'spark': (SparkConf()
        .set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .set('spark.sql.hive.convertMetastoreParquet', 'false')
        .set('spark.eventLog.enabled', 'true')
        .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .set("spark.sql.parquet.writeLegacyFormat", "true")
    )
}
