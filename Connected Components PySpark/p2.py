from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T,
)
from pyspark import StorageLevel
import os

class DFIO:
    def __init__(self, spark):
        self.spark = spark

    def read(self, path):
        path = f"{os.environ['CS535_S3_WORKSPACE']}{path}"
        return self.spark.read.parquet(path)
    
    def write(self, df, path):
        workspace = os.environ['CS535_S3_WORKSPACE']
        df.write.mode("OVERWRITE").parquet(f"{workspace}{path}")

def parquet(spark, table):
    spark.read.parquet(f"s3://bsu-c535-fall2024-commons/arjun-workspace/{table}/").createOrReplaceTempView(f"{table}")
    return spark.table(f"{table}")


spark = (
    SparkSession.builder
    .appName("p2_app")
    .getOrCreate()
)

def get_links(
        spark, 
        page, 
        linktarget, 
        redirect, 
        pagelinks
):
    print("----------------------------------------------------------------------\n")
    print("fetchin' those bidi links\n")
    print("----------------------------------------------------------------------")

    # connect pages and linktarget ids
    # (page_id | page_title | linktarget_id | redirect?)
    (
        page.filter("page_namespace = 0")
        .join(
            linktarget.filter("lt_namespace = 0"),
            F.expr("lt_title = page_title"),
            "inner"
        )
        .selectExpr("page_id", "page_title", "lt_id", "page_is_redirect as redirect")
        .createOrReplaceTempView("page_with_link_ids")
    )

    # connect redirect source pages with corresponding destination pages 
    # (rd_src_page_id | rd_dst_dst_page_id)
    (
        spark.table("page_with_link_ids")
        .filter("redirect = false") # don't want redirects that link to redirects
        .join(
            redirect.filter("rd_namespace = 0"),
            F.expr("rd_title = page_title"),
            "inner"
        )
        .selectExpr("rd_from", "page_id as rd_dst")
        .createOrReplaceTempView("redirect_pages")
    )

    # connect the destination page of a redirect to the linktarget id of the redirect source,
    # this means pages that link to redirect linktarget ids in the pagelinks table will be linked
    # with the page id of the redirect destination rather than the source
    # (rd_dst_page_id | rd_src_linktarget_id)
    (
        spark.table("page_with_link_ids")
        .filter("redirect = true") # get all redirects
        .join(
            spark.table("redirect_pages"),
            F.expr("page_id = rd_from"),
            "inner"
        )
        .selectExpr("rd_dst as page_id", "lt_id")
        .createOrReplaceTempView("redirect_with_link_ids")
    )

    # union together non-redirects and redirects
    # (page_id | linktarget_id)
    # (rd_dst_page_id | rd_src_linktarget_id)
    (
        spark.table("page_with_link_ids")
        .filter("redirect = false") # non-redirects
        .select("page_id", "lt_id")
        .union(spark.table("redirect_with_link_ids")) # redirects
        .select("page_id", "lt_id")
        .createOrReplaceTempView("all_pages_with_links")
    )

    # get pagelinks from source to destination and create pairs
    (
        spark.table("all_pages_with_links")
        .join(
            pagelinks,
            F.expr("pl_target_id = lt_id"),
            "inner"
        )
        .selectExpr("pl_from", "page_id")
        .selectExpr(
            "greatest(pl_from, page_id) as page_a",
            "least(pl_from, page_id) as page_b",
            "pl_from > page_id as direction"
        )
        .filter("page_a != page_b")
        .groupby("page_a", "page_b")
        # get directional links
        .agg( 
            F.expr("bool_or(direction) as a_to_b"), 
            F.expr("bool_or(not direction) as b_to_a")
        )
        # filter links that aren't bidirectional to get mutual pairs
        .filter("a_to_b and b_to_a")
        .drop("direction")
        .drop("a_to_b")
        .drop("b_to_a")
        .selectExpr("page_a as src", "page_b as dst")
        .createOrReplaceTempView("edges")
    )

    return spark.table("edges")

def calc_components(spark, edges, dfio):

    print("----------------------------------------------------------------------\n")
    print("calculatin' connected components\n")
    print("----------------------------------------------------------------------")

    def get_bidi_edges(a, b):
        '''
        UDF to create edges list (neighbors)

        Args:
            a: src
            b: dst
            
        Returns:
            bidi_edges: list of edges containing [[src, dst], [dst, src], [src, src], [dst, dst]]
        '''
        bidi_edges = []
        bidi_edges.append([a, b])
        bidi_edges.append([b, a])
        bidi_edges.append([b, b])
        bidi_edges.append([a, a])
        return bidi_edges
    
    def get_vertices(a, b):
        '''
        UDF to create list of all vertices and initial labels 

        Args:
            a: src
            b: dst
            
        Returns:
            init_vertices: list of vertices and labels containing [[src, src], [dst, dst]]
        '''
        init_vertices = []
        init_vertices.append([b, b])
        init_vertices.append([a, a])
        return init_vertices
    
    # register UDFs
    spark.udf.register("get_bidi_edges", get_bidi_edges, T.ArrayType(T.ArrayType(T.IntegerType())))
    spark.udf.register("get_vertices", get_vertices, T.ArrayType(T.ArrayType(T.IntegerType())))

    # get edge list to keep track of neighbors
    edge_list = (
        edges
        .withColumn("bidi_edges", F.expr("get_bidi_edges(src, dst)"))
        .select(F.explode("bidi_edges").alias("edges"))
        .withColumn("src", F.col("edges").getItem(0))
        .withColumn("dst", F.col("edges").getItem(1))
        .drop("edges")
        .distinct()
    )

    # checkpoint to avoid re-computation
    dfio.write(edge_list, "init_edge_list")
    edge_list = dfio.read("init_edge_list")
    count = edge_list.count()
    print(f"Number of rows in bidi edge list: {count}")

    # get vertices and initial labels
    vertices = (
        edges
        .withColumn("all_vertices", F.expr("get_vertices(src, dst)"))
        .select(F.explode("all_vertices").alias("vertices"))
        .withColumn("src", F.col("vertices").getItem(0))
        .withColumn("dst", F.col("vertices").getItem(1))
        .drop("vertices")
        .distinct()
    )

    # checkpoint to avoid re-computation
    dfio.write(vertices, "init_vertices")
    vertices = dfio.read("init_vertices")
    count = vertices.count()
    print(f"Number of rows in vertex list: {count}\n")

    # iteratively propogate component labels
    i = 0
    while True:
        print(f"\nIteration: {i+1}")
        # update vertices with new label
        update_vertices = (
            vertices
            .selectExpr(
                "src as a", 
                "dst as b",
            )
            .join(
                edge_list
                .groupby("src") # group neighbors together
                .agg(F.min("dst").alias("dst")), # get min of neighbors
                F.expr("b = src"),
                "inner"
            )
            .selectExpr(
                "a as src", 
                "dst",
            )
        ).persist(StorageLevel.MEMORY_AND_DISK)
        
        # update edge list with new labels
        temp = (
            edge_list
            .selectExpr(
                "src as a", 
                "dst as b"
            )
            .join(
                update_vertices,
                F.expr("b = src"),
                "inner"
            )
            .selectExpr(
                "a as src", 
                "dst",
            )
            # add edges to keep track of neighbors
            .withColumn("bidi_edges", F.expr("get_bidi_edges(src, dst)"))
            .select(F.explode("bidi_edges").alias("edges"))
            .withColumn("src", F.col("edges").getItem(0))
            .withColumn("dst", F.col("edges").getItem(1))
            .drop("edges")
            .distinct()
        )

        # check for convergence
        changes = vertices.subtract(update_vertices)
        count = changes.count()
        if changes.isEmpty():
            print(f"No more vertices to change!")
            break
        else:
            print(f"Vertices changed: {count}\n")

        # checkpoint
        i += 1
        if (i % 3 == 0):
            print("\ntruncating DAG...\n")
            dfio.write(update_vertices, "update_vertices")
            update_vertices.unpersist()
            vertices = dfio.read("update_vertices")
            dfio.write(temp, "update_edges")
            edge_list = dfio.read("update_edges")

        else:
            vertices = update_vertices
            update_vertices.unpersist()
            edge_list = temp

    components = vertices.selectExpr("src as vertex", "dst as component")

    print("\n----------------------------------------------------------------------\n")
    print("gettin' size stats\n")
    print("----------------------------------------------------------------------")

    # get sizes of components
    cc_counts = (
        components
        .groupby("component")
        .agg(
            F.expr("count(*) as size")
        )
    )

    # output the largest components with sizes
    cc_counts.orderBy(F.desc("size")).show(truncate=False)
    total_components = cc_counts.count()
    print(f"Total Components: {total_components}\n")

    print("----------------------------------------------------------------------\n")
    print("All done :) \n")
    print("----------------------------------------------------------------------")

    return components