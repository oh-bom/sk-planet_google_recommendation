# Databricks notebook source
# MAGIC %md
# MAGIC #### Streamlit에 업로드 할 샘플 데이터
# MAGIC - 3가지 candidate 모두에 존재하는 gmap_id1 1000개를 기준으로 20개씩 gmap_id2 후보 선정하여 각 모델별로 20,000개의 추천 후보가 담긴 데이터셋 생성

# COMMAND ----------

gbdt_df = spark.table("hive_metastore.item2item.gbdt_prediction_rank")
hybrid_df = spark.table("hive_metastore.item2item.hybrid_prediction_rank")
text_df = spark.table("hive_metastore.item2item.cosine_candidate_rank")

# COMMAND ----------

from pyspark.sql.functions import col, count, rand
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# gbdt_df 테이블에서 gmap_id2가 20개 이상이고 prob이 0.7 이상인 gmap_id1 필터링
gbdt_valid_ids = gbdt_df.filter(col("prob") >= 0.7).groupBy("gmap_id1").agg(count("gmap_id2").alias("cnt")).filter(col("cnt") >= 20).select("gmap_id1")

# hybrid_df 테이블에서 gmap_id2가 20개 이상이고 prob이 0.7 이상인 gmap_id1 필터링
hybrid_valid_ids = hybrid_df.filter(col("prob") >= 0.7).groupBy("gmap_id1").agg(count("gmap_id2").alias("cnt")).filter(col("cnt") >= 20).select("gmap_id1")

# text_df 테이블에서 gmap_id2가 20개 이상이고 cosine_top4가 0.7 이상인 gmap_id1 필터링
text_valid_ids = text_df.filter(col("cosine_top4") >= 0.7).groupBy("gmap_id1").agg(count("gmap_id2").alias("cnt")).filter(col("cnt") >= 20).select("gmap_id1")


# COMMAND ----------

# Step 2: 공통된 gmap_id1 추출
common_valid_gmap_ids = gbdt_valid_ids.intersect(hybrid_valid_ids).intersect(text_valid_ids)
common_valid_gmap_ids.count()

# COMMAND ----------

# Step 3: 무작위로 1,000개의 공통 gmap_id1 샘플링
sampled_gmap_ids = common_valid_gmap_ids.orderBy(rand(seed=1234)).limit(1000)
sampled_gmap_ids.count()

# COMMAND ----------

# Step 4: 최종 테이블 생성

# gbdt_df 테이블 처리
gbdt_filtered = gbdt_df.join(sampled_gmap_ids, on="gmap_id1") \
    .withColumn("rank_order", row_number().over(Window.partitionBy("gmap_id1").orderBy(col("prob").desc()))) \
    .filter(col("rank_order") <= 20) \
    .drop("rank_order")

# hybrid_df 테이블 처리
hybrid_filtered = hybrid_df.join(sampled_gmap_ids, on="gmap_id1") \
    .withColumn("rank_order", row_number().over(Window.partitionBy("gmap_id1").orderBy(col("prob").desc()))) \
    .filter(col("rank_order") <= 20) \
    .drop("rank_order")

# text_df 테이블 처리
text_filtered = text_df.join(sampled_gmap_ids, on="gmap_id1") \
    .withColumn("rank_order", row_number().over(Window.partitionBy("gmap_id1").orderBy(col("cosine_top4").desc()))) \
    .filter(col("rank_order") <= 20) \
    .drop("rank_order")

# COMMAND ----------

# 테이블로 저장시 데이터가 사라지는 에러가 발생하여
# CSV로 저장 (컬럼명을 포함하여 저장)
gbdt_filtered.write.format("csv").option("header", "true").save("/tmp/streamlit_sample/gbdt_sample")
hybrid_filtered.write.format("csv").option("header", "true").save("/tmp/streamlit_sample/hybrid_sample")
text_filtered.write.format("csv").option("header", "true").save("/tmp/streamlit_sample/text_sample")
