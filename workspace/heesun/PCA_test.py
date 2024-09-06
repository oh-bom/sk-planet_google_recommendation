# Databricks notebook source
# MAGIC %md
# MAGIC #테이터 로드

# COMMAND ----------

review_data = spark.read.table('hive_metastore.test.sentence_bert_emb_v1')

# COMMAND ----------

review_data_1000 = review_data.limit(2)
# print(review_data.count())
# print(review_data_1000.count())

# COMMAND ----------

display(review_data_1000)

# COMMAND ----------

# MAGIC %md
# MAGIC #PCA_Test

# COMMAND ----------

from pyspark.sql.functions import posexplode, col, udf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import PCA

# asin 열을 포함하여 explode하고, array를 vector로 변환하는 과정을 진행
df_exploded_with_gmap = review_data.select("gmap_id", posexplode("emb").alias("pos", "array"))

# UDF를 사용하여 array를 dense vector로 변환
to_vector = udf(lambda x: Vectors.dense(x), VectorUDT())
df_vector_with_gmap = df_exploded_with_gmap.withColumn("features", to_vector(col("array")))

# COMMAND ----------

# Step 3: 새로운 스키마에 데이터 저장
df_vector_with_gmap.write.mode('overwrite').saveAsTable('hive_metastore.dimension_reduce.dimension_reduce_pre_data')

# COMMAND ----------

display(df_vector_with_gmap.limit(5))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import posexplode, col, udf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import PCA

# asin 열을 포함하여 explode하고, array를 vector로 변환하는 과정을 진행
df_exploded_with_asin = review_data_1000.select("gmap_id", posexplode("emb").alias("pos", "array"))

# UDF를 사용하여 array를 dense vector로 변환
to_vector = udf(lambda x: Vectors.dense(x), VectorUDT())
df_vector_with_asin = df_exploded_with_asin.withColumn("features", to_vector(col("array")))

# PCA 모델 생성 및 적용
pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df_vector_with_asin)
result_pca_with_asin = model.transform(df_vector_with_asin).select("gmap_id", "pos", "pcaFeatures")

# 결과 확인
result_pca_with_asin.show()

# COMMAND ----------

display(result_pca_with_asin)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

# DenseVector에서 값을 추출
extract_vector_values = udf(lambda vector: vector.toArray().tolist(), ArrayType(DoubleType()))

# pcaFeatures 열에서 값들만 추출하여 새로운 열에 저장
result_pca_with_values = result_pca_with_asin.withColumn("pcaValues", extract_vector_values("pcaFeatures"))

display(result_pca_with_values)

# COMMAND ----------

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import pandas as pd
import hashlib


pandas_df = result_pca_with_values.toPandas()

# asin 값을 기반으로 고유한 색상 코드 생성
def generate_color_code(gmap_id):
    hash_object = hashlib.md5(gmap_id.encode())
    return '#' + hash_object.hexdigest()[:6]

pandas_df['color'] = pandas_df['gmap_id'].apply(generate_color_code)

# 3D 플롯 생성
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')

# 각 점을 3D 공간에 플롯
for index, row in pandas_df.iterrows():
    ax.scatter(row['pcaValues'][0], row['pcaValues'][1], row['pcaValues'][2], color=row['color'])

ax.set_xlabel('PCA 1')
ax.set_ylabel('PCA 2')
ax.set_zlabel('PCA 3')

plt.show()

# COMMAND ----------

import plotly.graph_objects as go
import pandas as pd

# 3D 스캐터 플롯 생성
fig = go.Figure()

for index, row in pandas_df.iterrows():
    fig.add_trace(go.Scatter3d(x=[row['pcaValues'][0]],
                               y=[row['pcaValues'][1]],
                               z=[row['pcaValues'][2]],
                               mode='markers',
                               marker=dict(size=5, color=row['color']),
                               text=row['gmap_id'],  
                               name=row['gmap_id']))

fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #샘플링 및 차원 선정

# COMMAND ----------

# PCA 차원수 정해서 실행
from pyspark.sql.functions import posexplode, col, udf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import PCA

# asin 열을 포함하여 explode하고, array를 vector로 변환하는 과정을 진행
df_exploded_with_asin = review_data.select("gmap_id", posexplode("emb").alias("pos", "array"))

# UDF를 사용하여 array를 dense vector로 변환
to_vector = udf(lambda x: Vectors.dense(x), VectorUDT())
df_vector_with_asin = df_exploded_with_asin.withColumn("features", to_vector(col("array")))

# PCA 모델 생성 및 적용
pca = PCA(k=128, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(df_vector_with_asin)
result_pca_with_asin = model.transform(df_vector_with_asin).select("gmap_id", "pos", "pcaFeatures")

model.getK()

model.setOutputCol("output")

#model.transform(df).collect()[0].output

aa = model.explainedVariance
print(sum(aa))



# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

# DenseVector에서 값을 추출
extract_vector_values = udf(lambda vector: vector.toArray().tolist(), ArrayType(DoubleType()))

# pcaFeatures 열에서 값들만 추출하여 새로운 열에 저장
result_pca_with_values = result_pca_with_asin.withColumn("pcaValues", extract_vector_values("pcaFeatures"))

display(result_pca_with_values.limit(1))

# COMMAND ----------


display(result_pca_with_values.limit(1))

# COMMAND ----------



# COMMAND ----------

result_pca_with_values1 = result_pca_with_values.select(['gmap_id','pcaValues'])

# COMMAND ----------

# Step 1: 유니티 카탈로그에 새로운 스키마 생성
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.dimension_reduce")

# COMMAND ----------

# Step 1: 유니티 카탈로그에 새로운 스키마 생성
spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.dimension_reduce")

# Step 2: 기존 테이블 데이터 읽기
#review_data = spark.read.table('hive_metastore.test.dimension_reduce_pca128')

# Step 3: 새로운 스키마에 데이터 저장
result_pca_with_values1.write.mode('overwrite').saveAsTable('hive_metastore.dimension_reduce.dimension_reduce_pca128')

# COMMAND ----------

print(sum(aa))

# COMMAND ----------

display(result_pca_with_asin)

# COMMAND ----------



# COMMAND ----------

# asin 열을 포함하여 explode하고, array를 vector로 변환하는 과정을 진행
df_exploded_with_asin = review_data.select("gmap_id", posexplode("emb").alias("pos", "array"))

# UDF를 사용하여 array를 dense vector로 변환
to_vector = udf(lambda x: Vectors.dense(x), VectorUDT())
df_vector_with_asin = df_exploded_with_asin.withColumn("features", to_vector(col("array")))

# COMMAND ----------

pca_data = spark.read.table('hive_metastore.dimension_reduce.dimension_reduce_pca128') 

