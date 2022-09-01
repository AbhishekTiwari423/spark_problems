"""
This snippet can be used to validate the format of timestamp column in spark dataframe
"""

import pyspark.sql.functions as f
from pyspark.sql.functions import *

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("timestamp_format_validation").getOrCreate()

timestamp_df = spark.createDataFrame(
    [
        ("1", "20200615115512567823"),
        ("7", "06152020115512"),
        ("2", "2020-06-14 11:45:12.345786"),
        ("3", "06-11-2020 11:55:12"),
        ("4", "2020-12-06 11:55:12.124300"),
    ],
    ['id', 'timestampCol']
)
display(timestamp_df)

# check if the timestamp format is valid based on input format supplied
validated_timestamp_df = timestamp_df.withColumn("badRecords",
                                                 f.when(
                                                     to_timestamp(f.col("timestampCol"),
                                                                  "yyyy-MM-dd HH:mm:ss.SSSXXX").cast(
                                                         "Timestamp").isNull() &
                                                     f.col("timestampCol").isNotNull(), f.lit("Not a valid Timestamp")
                                                 ).otherwise(f.lit('valid timestamp'))
                                                 )
display(validated_timestamp_df)
