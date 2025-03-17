from pyspark.sql import (
    SparkSession,
    functions as F,
    types as T,
)
from pyspark import SparkContext as sc
import os
from collections import defaultdict

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
        .select("page_a", "page_b")
        .createOrReplaceTempView("edges")
    )

    return spark.table("edges")

def find_connected_components(edges):

    graph = defaultdict(list)
    for u, v in edges:
        graph[u].append(v)
        graph[v].append(u)

    visited = set()
    components = []
    def dfs(node):
        visited.add(node)
        component.append(node)
        for neighbor in graph[node]:
            if neighbor not in visited:
                dfs(neighbor)

    i = 0
    for node in graph:
        if node not in visited:
            i += 1
            print(f"Iteration: {i}")
            component = []
            dfs(node)
            components.append(component)

    return components

def calc_components(spark, edges, dfio):

    print("----------------------------------------------------------------------\n")
    print("calculatin' connected components\n")
    print("----------------------------------------------------------------------\n")

    src = edges.select('page_a').rdd.map(lambda x: x[0]).collect()
    dst = edges.select('page_b').rdd.map(lambda x: x[0]).collect()
    edge_list = [list(pair) for pair in zip(src, dst)]
    connected_components = find_connected_components(edge_list)
    cc = {} 
    i = 0 # component labels
    for group in connected_components:
        for vertex in group:
            cc[vertex] = i
        i += 1

    schema = T.StructType([
        T.StructField("vertex", T.IntegerType()),
        T.StructField("component", T.IntegerType()),
    ])

    components = spark.createDataFrame(cc.items(), schema)
    dfio.write(components, "wiki_components")

    # # initialize each node with a component ID (value of page_b)
    # components = (
    #     edges
    #     .withColumn("component", F.col("page_b"))
    #     .selectExpr(
    #         "page_a as src",
    #         "page_b as dst",
    #         "component"
    #     )
    # )

    # # iteratively propagate component labels until convergence
    # i = 0 # keep track of iterations
    # vertices = 0 # keep track of the number of vertices changed
    # while True:
    #     print(f"Iteration: {i + 1}")
    #     iter_changes = (
    #         components
    #         .join(
    #             edges
    #             .select("page_a", "page_b"), 
    #             F.expr("component = page_a"), 
    #             "inner"
    #         )
    #         .selectExpr(
    #             "src as src_iter",
    #             "dst as dst_iter",
    #             # update new component labels if applicable, otherwise keep old component labels
    #             "coalesce(least(component, page_b), component) as component_iter",
    #         )
    #     )
    #     changes = iter_changes.count() # keep track of vertices changed and check for convergence

    #     components = (
    #         components
    #         .join(
    #             iter_changes,
    #             F.expr("src = src_iter"),
    #             "left"
    #         )
    #         .selectExpr(
    #             "src",
    #             "dst",
    #             "least(component, component_iter) as component"
    #         )
    #     )

    #     # check for convergence
    #     convergence = iter_changes.isEmpty()
    #     if (convergence == True):
    #         print(f"\nNo more vertices to change!\n")
    #         break
    #     else:
    #         vertices += changes # update vertices changed
    #         print(f"Vertices changed this iteration: {changes}")
    #         print(f"Total vertices changed so far: {vertices}\n")

    #     i += 1
    #     # checkpoint
    #     if (i % 3 == 0):
    #         print("truncating...\n")
    #         dfio.write(components, "wiki_components")
    #         components = dfio.read("wiki_components")

    # components = (
    #     components
    #     .selectExpr(
    #         "src as vertex",
    #         "component as component"
    #     )
    # )




    # def check_neighbors(src, dst):
    #     new_component = [src, dst]
    #     edge_list.filter(F.col("a").equalTo(F.lit(src))).groupby("a").agg(F.min(''))
    #     return new_component
    
    # check = F.udf(check_neighbors, T.ArrayType(T.ArrayType(T.IntegerType())))
    
    # i = 0
    # while i < 3:
    #     new_edges = (
    #         edges
    #         .select("src", "dst")
    #         .withColumn("component", check("src", "dst"))
    #     )
    #     new_edges.show()
    #     edges = new_edges
    #     i += 1

    # return edges

    # schema = T.StructType([
    #     T.StructField("vertex", T.IntegerType()),
    #     T.StructField("component", T.IntegerType()),
    # ])

    # # initialize each node with component
    # edges = edges.rdd
    # components = edges.flatMap(lambda x: [(x[0], x[1]), (x[1], x[1])]).distinct()
    # new_edges = edges.flatMap(lambda x: [(x[1], x[0])])

    # cnt = components.count()
    # print(f"\n Components: {components.take(cnt)}\n")
    # cnt = new_edges.count()
    # print(f"\nNew Edges: {new_edges.take(cnt)}\n")
    # a = components.join(new_edges)
    # cnt = a.count()
    # print(f"\nJoin: {a.take(cnt)}\n")
    # c = a.map(lambda x: (x[1][1], x[1][0]))
    # cnt = c.count()
    # print(f"\nMap: {c.take(cnt)}\n")
    
    # # iteratively propogate component IDs
    # i = 1
    # while True:
    #     print(f"\nIteration: {i}\n")
    #     # propagate smallest component ID along edges
    #     new_components = (
    #         components
    #         .join(new_edges)
    #         .distinct()
    #         .map(lambda x: (x[1][1], x[1][0]))
    #         .union(components)
    #         .reduceByKey(min)
    #     )
        
    #     # check for convergence
    #     changes = components.subtract(new_components)
    #     if changes.isEmpty():
    #         break

    #     # checkpoint
    #     if (i % 3 == 0):
    #         components = spark.createDataFrame(components, schema)
    #         print("\ntruncating...\n")
    #         dfio.write(components, "wiki_components")
    #         components = dfio.read("wiki_components").rdd

    #     i += 1
    #     components = new_components

    # components = spark.createDataFrame(components, schema)


    # src = edges.select('page_a').rdd.map(lambda x: x[0]).collect()
    # dst = edges.select('page_b').rdd.map(lambda x: x[0]).collect()
    # edge_list = [list(pair) for pair in zip(src, dst)]
    # connected_components = find_connected_components(edge_list, dfio)
    # cc = {} 
    # i = 0 # component labels
    # for group in connected_components:
    #     for vertex in group:
    #         cc[vertex] = i
    #     i += 1

    # schema = T.StructType([
    #     T.StructField("vertex", T.IntegerType()),
    #     T.StructField("component", T.IntegerType()),
    # ])

    # components = spark.createDataFrame(cc.items(), schema)
    # dfio.write(components, "wiki_components")






        # src_list = edges.select('src')
    # src_array = [int(row.src) for row in src_list.collect()]
    # dst_list = edges.select('dst')
    # dst_array = [int(row.dst) for row in dst_list.collect()]
    # new = list(zip(src_array, dst_array))
    # rdd = edges.rdd
    # rdd1, rdd2 = rdd.randomSplit([0.5, 0.5], seed=42)
    # broadcast1 = spark.sparkContext.broadcast(rdd1.collectAsMap())
    # broadcast2 = spark.sparkContext.broadcast(rdd2.collectAsMap())

    # def update_row(row):
    #     neighbors = []

    #     items = broadcast1.value.items()
    #     relevant = {k:v for k,v in items.iteritems() if row in k}
    #     neighbors = relevant.values()

        # for key,value in broadcast1.value.items():
        #     if key == row:
        #         neighbors.append(value)

        # items = broadcast2.value.items()
        # relevant = {k:v for k,v in items.iteritems() if row in k}
        # neighbors = relevant.values()

        # for key,value in broadcast2.value.items():
        #     if key == row:
        #         neighbors.append(value)

        # if neighbors:
        #     update = min(neighbors)
        # else:
        #     update = row
        # return update
    
    # update = F.udf(update_row, T.IntegerType())
    # spark.udf.register("update", update_row, T.IntegerType())

    # i = 0
    # while True:
    #     print(f"\nIteration: {i+1}\n")
    #     old_edges = edge_list
    #     edge_list = (
    #         edge_list
    #         .withColumn("new_dst", F.expr("update(dst)"))
    #         .selectExpr(
    #             "src", 
    #             "new_dst as dst")
    #         .groupby("src")
    #         .agg(F.min("dst").alias("dst"))
    #     )

    #     changes = old_edges.subtract(edge_list)
    #     if changes.isEmpty():
    #         break

    #     i += 1
    #     if (i % 3 == 0):
    #         print("\ntruncating...\n")
    #         dfio.write(edge_list, "wiki_components")
    #         edge_list = dfio.read("wiki_components")





    # i = 0
    # while True:
    #     print(f"\nIteration: {i+1}\n")
    #     join = (
    #         edge_list
    #         .selectExpr(
    #             "src as a", 
    #             "dst as b"
    #         )
    #         .join(
    #             edges, 
    #             F.expr("b = src"),
    #             "left"
    #         )
    #         # .show()
    #         # .withColumn("dst", F.expr("src"))
    #         .withColumn("dst", F.coalesce("dst", "b"))
    #         .selectExpr(
    #             "a as src", 
    #             "dst"
    #         )
    #         # .show()
    #         .distinct()
    #     )
    #     # join.show()
    #     # print(join.count())
    #     # new_join = (
    #     #     edge_list
    #     #     .selectExpr(
    #     #         "src as a", 
    #     #         "dst as b"
    #     #     )
    #     #     .join(
    #     #         join, 
    #     #         F.expr("a = src"), 
    #     #         "left"
    #     #     )
    #     #     .selectExpr(
    #     #         "a as src", 
    #     #         "coalesce(dst, b) as dst"
    #     #     )
    #     #     .distinct()
    #     # )
    #     # print(new_join.count())
    #     # edge_list = new_join

    #     i += 1
    #     # edge_list.show()
    #     changes = edge_list.subtract(join)
    #     if changes.isEmpty():
    #         break

    #     edge_list = join
    #     if (i % 3 == 0):
    #         print("\ntruncating...\n")
    #         dfio.write(edge_list, "wiki_components")
    #         edge_list = dfio.read("wiki_components")



    print("----------------------------------------------------------------------\n")
    print("gettin' size stats\n")
    print("----------------------------------------------------------------------")

    cc_counts = (
        components
        .groupby("component")
        .agg(
            F.expr("count(*) as size")
        )
    )

    cc_counts.orderBy(F.desc("size")).show(truncate=False)
    total_components = cc_counts.count()
    print(f"Total Components: {total_components}\n")

    print("----------------------------------------------------------------------\n")
    print("All done :) \n")
    print("----------------------------------------------------------------------")

    return components

    # adj = edge_list.groupBy("src").agg(F.collect_set("dst").alias("neighbors"))
    # adj.show(truncate=False)

    # rows = [list(row) for row in adj.collect()] 

    # dict = {}
    # for node in rows:
    #     dict[node[0]] = node[1]

    # # def bfs(dict, node, visited):
    # #     """Perform BFS starting from a node, marking visited nodes."""
    # #     queue = deque([node])
    # #     component = []

    # #     while queue:
    # #         current_node = queue.popleft()
    # #         if current_node not in visited:
    # #             visited.add(current_node)
    # #             component.append(current_node)
    # #             queue.extend(dict.get(current_node))

    # #     return component

    # schema = T.StructType([
    #     T.StructField("vertex", T.IntegerType()),
    #     T.StructField("component", T.IntegerType()),
    # ])
    
    # visited = set()
    # components = {}
    # i = 1

    # for node in dict.items():
    #     if node[0] not in visited:
    #         print(f"\nIteration: {i}")
    #         i += 1

            
                

    #         queue = deque([node[0]])
    #         component = []

    #         while queue:
    #             current_node = queue.popleft()
    #             if current_node not in visited:
    #                 visited.add(current_node)
    #                 component.append(current_node)
    #                 queue.extend(dict.get(current_node))
                    
    #         components.setdefault(i, [])
    #         for c in component:
    #             components[i].append(c)

    # rows = []
    # for key, value in components.items():
    #     for v in value:
    #         rows.append([v, key])

    # components = spark.createDataFrame(rows, schema)