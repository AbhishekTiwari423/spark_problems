"""
This snippet can be used to validate the integer value column in spark dataframe
"""

import pyspark.sql.functions as f

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("integer_value_validation").getOrCreate()

df = spark.read.csv('/FileStore/tables/Data_quality-1.txt', header=True, sep='|', inferSchema=True)
df.show()

# Single column
df.withColumn("dataTypeValidationErrors",
              f.when(
                  f.col("Age").cast("int").isNull() & f.col("Age").isNotNull(),
                  f.lit("Not a valid age value")
              ).otherwise(f.lit("None"))
              ).show()
