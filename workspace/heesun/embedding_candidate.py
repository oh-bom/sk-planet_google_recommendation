# Databricks notebook source
#text review의 유사도값을 이용한 추천 테이블을 생성
representative_value_dis = spark.read.table("asacdataanalysis.embedding_data.cosine_similarity_distinct_51_v2")

# COMMAND ----------

#조인할 테이블
model_train = spark.read.table("hive_metastore.item2item.train_under_v3")

# COMMAND ----------

display(model_train.limit(5))

# COMMAND ----------

# 조인을 수행
left_outer_joined_df = model_train.join(
    representative_value_dis,
    (model_train.gmap_id1 == representative_value_dis.gmap_id1) & 
    (model_train.gmap_id2 == representative_value_dis.gmap_id2),
    "left_outer"
)

# 중복 컬럼 제거
left_outer_joined_df = left_outer_joined_df.drop(representative_value_dis.gmap_id1, representative_value_dis.gmap_id2)

# 결과 확인
left_outer_joined_df.show()

# COMMAND ----------

#실버데이터 가져오기
silver = spark.read.table("hive_metastore.silver.california_meta")

# COMMAND ----------

#gmap_id별 벡터 통계값 테이블
representative_value = spark.read.table("hive_metastore.dimension_reduce.vector_gmap_features")

# COMMAND ----------

#gmap_id와 리뷰수 추출
silver_select = silver.select(['gmap_id','num_of_reviews'])
silver_select = silver_select.fillna(0)

# COMMAND ----------

# 조인을 수행
pre_df = representative_value.join(
   silver_select,
    (representative_value.gmap_id == silver_select.gmap_id),
    "left_outer"
)

# 중복 컬럼 제거
pre_df = pre_df.drop(silver_select.gmap_id)

# 결과 확인
pre_df.columns

# COMMAND ----------

pre_df = pre_df.drop("vectors")


# COMMAND ----------

#실버데이터에서 리뷰가 많은순으로 10000개추출
from pyspark.sql.functions import desc

silver_10000 = pre_df.orderBy(desc("num_of_reviews")).limit(10000)

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import desc

#각 gmap_id1,gmap_id2에 해당하는 데이터를 컬럼을 1,2 로 변경후 조인 수행
df_10000_gmap_id1 = silver_10000.withColumnRenamed("gmap_id", "gmap_id1")
df_10000_gmap_id2 = silver_10000.withColumnRenamed("gmap_id", "gmap_id2")
for column_name in df_10000_gmap_id1.columns:
    df_10000_gmap_id1 = df_10000_gmap_id1.withColumnRenamed(column_name, column_name + "_1")

# df_10000_asin2의 모든 컬럼 이름에 _2를 추가
for column_name in df_10000_gmap_id2.columns:
    df_10000_gmap_id2 = df_10000_gmap_id2.withColumnRenamed(column_name, column_name + "_2")

# 이제 컬럼 이름이 변경된 두 데이터프레임을 crossJoin 수행
df_cross_joined = df_10000_gmap_id1.crossJoin(df_10000_gmap_id2)
df_cross_joined = df_cross_joined.withColumnRenamed("gmap_id1_1", "gmap_id1")
df_cross_joined = df_cross_joined.withColumnRenamed("gmap_id2_2", "gmap_id2")

df_combinations = df_cross_joined.filter(col("gmap_id1") != col("gmap_id2"))

# COMMAND ----------

from pyspark.sql.functions import expr, col
from pyspark.sql.functions import col, sqrt, sum as _sum, when

#각 크로스조인된 데이터를 통해 통계값 별로 코사인 유사도 계산
columns = [("gmap_id_max_1","gmap_id_max_2"),
               ("gmap_id_min_1","gmap_id_min_2"),
               ("gmap_id_mean_1","gmap_id_mean_2"),
               ("gmap_id_std_dev_1","gmap_id_std_dev_2"),
               ("gmap_id_skewness_1","gmap_id_skewness_2"),
               ("gmap_id_kurtosis_1","gmap_id_kurtosis_2"),
               ("gmap_id_Q1_1","gmap_id_Q1_2"),
               ("gmap_id_Q2_1","gmap_id_Q2_2"),
               ("gmap_id_Q3_1","gmap_id_Q3_2"),]

# 각 컬럼 쌍에 대해 반복
for col1, col2 in columns:
    # Dot product
    dot_product_expr = " + ".join([f"({col1}[{i}]) * ({col2}[{i}])" for i in range(51)])
    
    # Norms
    norm_v1_expr = "SQRT(" + " + ".join([f"({col1}[{i}]) * ({col1}[{i}])" for i in range(51)]) + ")"
    norm_v2_expr = "SQRT(" + " + ".join([f"({col2}[{i}]) * ({col2}[{i}])" for i in range(51)]) + ")"
    
    # Cosine Similarity
    cosine_similarity_expr = f"({dot_product_expr}) / ({norm_v1_expr} * {norm_v2_expr})"
    
    # DataFrame에 코사인 유사도 컬럼 추가
    df_combinations = df_combinations.withColumn(f"{col1[:-2]}_cosine_similarity", expr(cosine_similarity_expr))
    df_combinations = df_combinations.fillna(0, subset=[f"{col1[:-2]}_cosine_similarity"])



# COMMAND ----------

# 코사인 유사도 있는거 전부다 평균하여 유사도 대표값을 출력
df_feat_final = df_combinations.withColumn("cosine_fin", (col("gmap_id_max_cosine_similarity") + col("gmap_id_min_cosine_similarity") + col("gmap_id_mean_cosine_similarity") +col("gmap_id_std_dev_cosine_similarity")+col("gmap_id_skewness_cosine_similarity")+col("gmap_id_kurtosis_cosine_similarity")+col("gmap_id_Q1_cosine_similarity")+col("gmap_id_Q2_cosine_similarity")+col("gmap_id_Q3_cosine_similarity")
                                            ) / 9)

# COMMAND ----------

#cnadidate data 컬럼 설정
df_final =df_feat_final.select(['gmap_id1','gmap_id2','gmap_id_max_cosine_similarity',
 'gmap_id_min_cosine_similarity',
 'gmap_id_mean_cosine_similarity',
 'gmap_id_std_dev_cosine_similarity',
 'gmap_id_skewness_cosine_similarity',
 'gmap_id_kurtosis_cosine_similarity',
 'gmap_id_Q1_cosine_similarity',
 'gmap_id_Q2_cosine_similarity',
 'gmap_id_Q3_cosine_similarity',
 'cosine_fin'])

# COMMAND ----------

#코사인 유사도 상위 5개
df_final = df_final.withColumn("cosine_top5", (col("gmap_id_skewness_cosine_similarity")+col("gmap_id_min_cosine_similarity")+col("gmap_id_max_cosine_similarity")+col("gmap_id_Q3_cosine_similarity")+col("gmap_id_kurtosis_cosine_similarity")) / 5)

# COMMAND ----------

#코사인 유사도 상위4개
df_final = df_final.withColumn("cosine_top4", (col("gmap_id_skewness_cosine_similarity")                                 +col("gmap_id_min_cosine_similarity")
+col("gmap_id_max_cosine_similarity")+col("gmap_id_Q3_cosine_similarity")) / 4)

# COMMAND ----------

result = spark.read.table("asacdataanalysis.embedding_data.cos_candidate_all_")
