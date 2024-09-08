# Databricks notebook source
#128차원의 벡터값 불러오기
review_data = spark.read.table('asacdataanalysis.default.sentence_bert_emb_default_128') 

# COMMAND ----------

review_data_1000 = review_data.limit(2)

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

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import random



# COMMAND ----------

# 데이터 샘플링 (필요한 경우 주석 해제)
df_sampled = df_vector_with_gmap.sample(fraction=0.01, seed=42)  # 데이터의 10% 샘플링
#df_sampled.count()

# COMMAND ----------

from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col, udf
import pandas as pd
import matplotlib.pyplot as plt
# 테스트할 PCA 차원의 리스트
results = []

#20~60차원의 분산설명력 출력
for k in range(20,61):
    pca = PCA(k=k, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df_sampled)

    explained_variance = model.explainedVariance
    total_explained_variance = float(sum(explained_variance))

    results.append((k, total_explained_variance))



# COMMAND ----------

results

# COMMAND ----------

# Spark DataFrame을 pandas DataFrame으로 변환
results_df = pd.DataFrame(results, columns=['Dimensions', 'Explained Variance'])

# 결과 시각화
plt.figure(figsize=(10, 6))
plt.plot(results_df['Dimensions'], results_df['Explained Variance'], marker='o', linestyle='-', color='b')
plt.xlabel('Number of Dimensions')
plt.ylabel('Total Explained Variance')
plt.title('PCA Explained Variance by Number of Dimensions')
plt.grid(True)

# 90% 및 95% 설명된 분산의 스레시홀드 추가
threshold_90 = 0.90
threshold_85 = 0.85
plt.axhline(y=threshold_90, color='r', linestyle='--', label='90% Explained Variance')
plt.axhline(y=threshold_85, color='g', linestyle='--', label='85% Explained Variance')

plt.legend()
plt.show()
