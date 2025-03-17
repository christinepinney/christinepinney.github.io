import unittest
from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T,
)

from p2 import get_links, calc_components

class DFIOSynth:
    def __init__(self, spark):
        self.spark = spark

    def read(self, path):
        struct = T.StructType([
            T.StructField("src", T.IntegerType()),
            T.StructField("dst", T.IntegerType()),
        ])
        path = f"./testing-workspace/{path}/"
        return self.spark.read.schema(struct).json(path)
    
    def write(self, df, path):
        path = f"./testing-workspace/{path}/"
        df.coalesce(1).write.mode("OVERWRITE").json(path)

def read_json(spark, path):
    return spark.read.json(f"./synthetic/{path}.jsonl")

expected_links = frozenset({
    (2, 1), 
    (7, 2),
    (8, 2),
    (13, 9),
    (13, 10),
    (15, 14),
    (16, 1),
    (18, 17),
    (20, 18),
    (21, 20),
    (22, 21),
    (23, 22),
    (24, 13),
    (24, 23)
})

expected_components = frozenset({
    frozenset({1, 2, 7, 8, 16}),
    frozenset({14, 15}),
    frozenset({9, 10, 13, 17, 18, 20, 21, 22, 23, 24}),
})

class TestLinksAndComponents(unittest.TestCase):
    def test_create_links_and_components(self):
        spark = (
            SparkSession
            .builder
            .master("local")
            .appName("p2_app")
            .getOrCreate() 
        )

        # bidi links
        dfio = DFIOSynth(spark)
        bidi_links = get_links(
            spark,
            read_json(spark, "page"),
            read_json(spark, "linktarget"),
            read_json(spark, "redirect"),
            read_json(spark, "pagelinks"),
        )
        dfio.write(bidi_links, "bidi_links")
        exp_pairs = frozenset(tuple(r) for r in bidi_links.collect())
        self.assertEqual(exp_pairs, expected_links)

        # connected components
        edges = bidi_links.selectExpr("src", "dst")
        connected_components = calc_components(spark, edges, dfio)
        components = frozenset(frozenset(c.members) for c in 
            connected_components
            .groupby("component")
            .agg(F.expr("collect_list(vertex) as members"))
            .collect()
        )
        self.assertEqual(components, expected_components)
        dfio.write(connected_components, "wikipedia_components")

if __name__ == '__main__':
    unittest.main()