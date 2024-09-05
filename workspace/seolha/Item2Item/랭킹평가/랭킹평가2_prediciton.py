# Databricks notebook source
# MAGIC %md
# MAGIC - test 데이터 전체(co_count)와 train 데이터 전체(prediction) 비교를 위해 
# MAGIC - 훈련된 모델로 train 데이터 전체에 대해 prob값 가져오기

# COMMAND ----------

# MAGIC %md
# MAGIC #### 랭킹평가

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

# 모델 불러오기
import mlflow
logged_model = 'runs://model'
loaded_model = mlflow.spark.load_model(logged_model)

# COMMAND ----------

# 훈련된 모델과 동일한 feature를 가지게 전처리 된 train 데이터 불러오기
train_df = spark.table("hive_metastore.item2item.train_processed")

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, when

# 라벨링 기준 설정
train_df = train_df.withColumn("label", when(col("co_count") >= 4, 1).otherwise(0))

# gmap_id1, gmap_id2를 제외한 피처 벡터화
columns_to_exclude = ["label", "co_count", "gmap_id1", "gmap_id2"]
feature_columns = [c for c in train_df.columns if c not in columns_to_exclude]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

train_df = assembler.transform(train_df)

# COMMAND ----------

# 훈련된 모델을 통해 train의 prediction 값 받기
prediction = loaded_model.transform(train_df)

# COMMAND ----------

prediction_df = prediction.select('gmap_id1', 'gmap_id2', 'probability','year','quarter')

# COMMAND ----------

# vector안에 있는 probability값만 추출하여 컬럼으로 추가

from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col

# 'probability' 컬럼을 배열로 변환
prediction_df = prediction_df.withColumn("probability_array", vector_to_array(col("probability")))

# 배열에서 두 번째 값(인덱스 1)만 추출하여 'prob' 컬럼으로 추가
prediction_df = prediction_df.withColumn("prob", col("probability_array")[1])

# COMMAND ----------

# 필요없는 컬럼 삭제
prediction_df = prediction_df.drop("probability","probability_array")

# COMMAND ----------

# 연도-분기별로 나뉘어있는 gmap_id 1,2 쌍의 대표값 찾기 위해 gmap_id1, gmap_id2 기준으로 prob의 평균을 구하기
from pyspark.sql.functions import col, avg

result_df = prediction_df.groupBy("gmap_id1", "gmap_id2").agg(avg("prob").alias("prob"))

# COMMAND ----------

# (gmap_id2,gmap_id1)인 경우의 값이 없으므로 쌍 데이터 대칭 만들기

from pyspark.sql.functions import col

# gmap_id1과 gmap_id2를 서로 바꾼 대칭 쌍 생성
df_symmetric = result_df.select(col("gmap_id2").alias("gmap_id1"),
                         col("gmap_id1").alias("gmap_id2"),
                         col("prob"))

# 원래 데이터와 대칭 쌍 데이터 결합
predict_df_combined = result_df.union(df_symmetric)

# COMMAND ----------

# gmap_id1을 기준으로, prob값이 높은 gmap_id2 순으로 랭킹 부여

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# 데이터프레임을 gmap_id1으로 파티셔닝하고, Score로 내림차순 정렬할 윈도우 정의
window_spec = Window.partitionBy("gmap_id1").orderBy(col("prob").desc())

# rank 컬럼 추가: 각 그룹 내에서 Score를 기준으로 순위 매기기
predicion_rank = predict_df_combined.withColumn("rank", row_number().over(window_spec))

# COMMAND ----------

predicion_rank.write.mode("overwrite").saveAsTable("hive_metastore.item2item.gbdt_prediction_rank")
