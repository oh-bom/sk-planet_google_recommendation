# Databricks notebook source
# MAGIC %md
# MAGIC - test 데이터 전체(co-count)와 train 데이터 전체(1라벨 확률)의 순위 비교

# COMMAND ----------

test_df = spark.table("hive_metastore.item2item.test")

# COMMAND ----------

# 연도-분기 구분 없이 gmap_id 1,2 기준으로 co_count 합산하기
grouped_df = test_df.groupBy("gmap_id1", "gmap_id2").sum("co_count")

# COMMAND ----------

# 컬럼명 co_count로 변경
grouped_df = grouped_df.withColumnRenamed("sum(co_count)", "co_count")

# COMMAND ----------

# gmap_id2 - gmap_id1인 데이터가 없으므로 쌍 데이터 대칭 만들기

from pyspark.sql.functions import col

# gmap_id1과 gmap_id2를 서로 바꾼 대칭 쌍 생성
df_symmetric = grouped_df.select(col("gmap_id2").alias("gmap_id1"),
                         col("gmap_id1").alias("gmap_id2"),
                         col("co_count"))

# 원래 데이터와 대칭 쌍 데이터 결합
ground_truth = grouped_df.union(df_symmetric)


# COMMAND ----------

# gmap_id1을 기준으로 gmap_id2의 랭킹 추가

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# 데이터프레임을 gmap_id1으로 파티셔닝하고, Score로 내림차순 정렬할 윈도우 정의
window_spec = Window.partitionBy("gmap_id1").orderBy(col("co_count").desc())

# rank 컬럼 추가: 각 그룹 내에서 Score를 기준으로 순위 매기기
ground_truth_rank = ground_truth.withColumn("rank", row_number().over(window_spec))

# COMMAND ----------

ground_truth_rank.write.mode("overwrite").saveAsTable("hive_metastore.item2item.test_ground_truth_rank")
