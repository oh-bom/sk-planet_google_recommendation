# Databricks notebook source
#유사도 기준으로 랭킹을 매기는 코드
#10000X10000된 유사도 테이블을 가져옴
rank_df = spark.read.table("asacdataanalysis.embedding_data.cos_candidate_all_")

# COMMAND ----------

#상위 4개로 평균된 코사인유사도를 기준으로 지정
rank_df_select = rank_df.select('gmap_id1','gmap_id2','cosine_top4')

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# 데이터프레임을 gmap_id1으로 파티셔닝하고, Score로 내림차순 정렬할 윈도우 정의
window_spec = Window.partitionBy("gmap_id1").orderBy(col("cosine_top4").desc())

# rank 컬럼 추가: 각 그룹 내에서 Score를 기준으로 순위 매기기
predicion_rank = rank_df_select.withColumn("rank", row_number().over(window_spec))

# COMMAND ----------

display(predicion_rank)

# COMMAND ----------

predicion_rank.write.mode('overwrite').saveAsTable('hive_metastore.item2item.cosine_candidate_rank')
