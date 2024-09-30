# Databricks notebook source
#128벡터데이터 불러오기
review_data = spark.read.table('asacdataanalysis.default.sentence_bert_emb_default_128') 

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

# PCA  모델 생성 및 적용 51차원으로 지정
pca = PCA(k=51, inputCol="features", outputCol="pcaFeatures")
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


# COMMAND ----------

result_pca_with_values1 = result_pca_with_values.select(['gmap_id','pcaValues'])

# COMMAND ----------

#스키마에 데이터 저장
result_pca_with_values1.write.mode('overwrite').saveAsTable('hive_metastore.dimension_reduce.dimension_reduce_pca51')

# COMMAND ----------

display(result_pca_with_values1.limit(1))
