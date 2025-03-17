from pyspark.sql import (
    SparkSession,
    functions as F,
)
import os

spark = (
    SparkSession.builder
    .appName("p2_app")
    .getOrCreate()
)

from p2 import DFIO, calc_components, get_links, parquet

dfio = DFIO(spark)

bidi_links = get_links(
    spark,
    parquet(spark, "page"),
    parquet(spark, "linktarget"),
    parquet(spark, "redirect"),
    parquet(spark, "pagelinks"),
)
dfio.write(bidi_links, "mutual_links")
bidi_links = dfio.read("mutual_links")

edges = bidi_links.selectExpr("src", "dst")
connected_components = calc_components(spark, edges, dfio)
dfio.write(connected_components, "wikipedia_components")