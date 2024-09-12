# Databricks notebook source
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler

#51차원 벡터 가져오기
cell_128 = spark.read.table("hive_metastore.dimension_reduce.dimension_reduce_pca51")





# COMMAND ----------

#gmap_id가 크로스조인된 테이블 가져오기
item_df = spark.read.table("hive_metastore.test.california_cocount_final6")
#크로스조인된 값 추출
gmap_id_df = item_df.select(['gmap_id1', 'gmap_id2'])

# COMMAND ----------

#데이터의 자료형을 바꿔줌
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

schema = StructType([
    StructField("gmap_id", StringType(), True),
    StructField("pcaValues", ArrayType(DoubleType()), True)
])

gmap_df = cell_128
gmap_df = gmap_df.selectExpr(
    "cast(gmap_id as string) as gmap_id",
    "cast(pcaValues as array<double>) as pcaValues"
)



# COMMAND ----------

#하나의 gmap_id의 벡터값을 대표하기 위해서 통계값 별로 대표값을 뽑음
import numpy as np
from scipy.stats import skew, kurtosis
from pyspark.sql.functions import udf, collect_list
from pyspark.sql.types import ArrayType, DoubleType
gmap_id_name = 'gmap_id'
# 각 차원별로 통계를 계산하는 사용자 정의 함수
def calculate_dimensional_stats(vectors, stat_function):
    arr = np.array(vectors)
    return stat_function(arr, axis=0).tolist()

# 개별 통계 함수 정의
#맥스값 함수
def max_values(arr, axis=0):
    return np.max(arr, axis=axis)
#최솟값 함수
def min_values(arr, axis=0):
    return np.min(arr, axis=axis)
#평균값 함수
def mean_values(arr, axis=0):
    return np.mean(arr, axis=axis)
#표준편차 함수
def std_dev_values(arr, axis=0):
    return np.std(arr, axis=axis)
#왜도 함수
def skewness_values(arr, axis=0):
    return skew(arr, axis=axis)
#첨도 함수
def kurtosis_values(arr, axis=0):
    return kurtosis(arr, axis=axis)
#q1값 함수
def q1_values(arr, axis=0):
    return np.percentile(arr, 25, axis=axis)
#q2값 함수
def q2_values(arr, axis=0):
    return np.percentile(arr, 50, axis=axis)
#q3값 함수
def q3_values(arr, axis=0):
    return np.percentile(arr, 75, axis=axis)
#q4값 함수
def q4_values(arr, axis=0):
    return np.percentile(arr, 100, axis=axis)

# 각 통계값에 대한 UDF 등록
udf_max = udf(lambda v: calculate_dimensional_stats(v, max_values), ArrayType(DoubleType()))
udf_min = udf(lambda v: calculate_dimensional_stats(v, min_values), ArrayType(DoubleType()))
udf_mean = udf(lambda v: calculate_dimensional_stats(v, mean_values), ArrayType(DoubleType()))
udf_std_dev = udf(lambda v: calculate_dimensional_stats(v, std_dev_values), ArrayType(DoubleType()))
udf_skewness = udf(lambda v: calculate_dimensional_stats(v, skewness_values), ArrayType(DoubleType()))
udf_kurtosis = udf(lambda v: calculate_dimensional_stats(v, kurtosis_values), ArrayType(DoubleType()))
udf_q1 = udf(lambda v: calculate_dimensional_stats(v, q1_values), ArrayType(DoubleType()))
udf_q2 = udf(lambda v: calculate_dimensional_stats(v, q2_values), ArrayType(DoubleType()))
udf_q3 = udf(lambda v: calculate_dimensional_stats(v, q3_values), ArrayType(DoubleType()))
udf_q4 = udf(lambda v: calculate_dimensional_stats(v, q4_values), ArrayType(DoubleType()))

# DataFrame에 UDF 적용하여 각 통계값을 별도의 컬럼으로 저장
stats_df = gmap_df.groupBy("gmap_id").agg(collect_list("pcaValues").alias("vectors"))
stats_df = stats_df.withColumnRenamed("gmap_id", "gmap_id")
stats_df = stats_df.withColumn("gmap_id_max", udf_max("vectors"))
stats_df = stats_df.withColumn("gmap_id_min", udf_min("vectors"))
stats_df = stats_df.withColumn("gmap_id_mean", udf_mean("vectors"))
stats_df = stats_df.withColumn("gmap_id_std_dev", udf_std_dev("vectors"))
stats_df = stats_df.withColumn("gmap_id_skewness", udf_skewness("vectors"))
stats_df = stats_df.withColumn("gmap_id_kurtosis", udf_kurtosis("vectors"))
stats_df = stats_df.withColumn("gmap_id_Q1", udf_q1("vectors"))
stats_df = stats_df.withColumn("gmap_id_Q2", udf_q2("vectors"))
stats_df = stats_df.withColumn("gmap_id_Q3", udf_q3("vectors"))
stats_df = stats_df.withColumn("gmap_id_Q4", udf_q4("vectors"))

# 결과 출력
stats_df.show(truncate=False)



# COMMAND ----------

stats_df_re = stats_df.drop('vectors')
# gmap_id2라는 데이터프레임을 만들고 컬럼명을 gmap_id2를 포함해서 복제
stats_df_re2 = stats_df_re
for col_name in stats_df_re.columns:
    if 'gmap_id1' in col_name:
        new_col_name = col_name.replace('gmap_id1', 'gmap_id2')
        stats_df_re2 = stats_df_re2.withColumnRenamed(col_name, new_col_name)

# COMMAND ----------

# 데이터 병합gmap_id1,gmap_id2기준으로
joined_df = gmap_id_df.join(stats_df_re, on="gmap_id1", how="left")
joined_df = joined_df.join(stats_df_re2, on="gmap_id2", how="left")

# COMMAND ----------

#빈값과 결측치를 확인
joined_df_fil = joined_df.filter(joined_df["gmap_id1_max"].isNotNull() & joined_df["gmap_id2_max"].isNotNull()).limit(10)

# COMMAND ----------

#데이터 저장
joined_df.write.mode('overwrite').saveAsTable('hive_metastore.dimension_reduce.vector_exm')
