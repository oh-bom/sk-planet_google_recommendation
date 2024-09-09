# Databricks notebook source
#gmap_id당 대표된 벡터값이 담긴 테이블을 가져옴
representative_value = spark.read.table("hive_metastore.dimension_reduce.vector_exm")

# COMMAND ----------

#결측치가 있는지 확인 후 제거
representative_value1 = representative_value.filter(
    representative_value["gmap_id1_max"].isNotNull() & representative_value["gmap_id2_max"].isNotNull()
)

# COMMAND ----------


from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import numpy as np
from pyspark.sql.functions import col, size, array, expr

#각 대표값 별로 계산될 대표값 컬럼 설정
columns = [("gmap_id1_max","gmap_id2_max"),
               ("gmap_id1_min","gmap_id2_min"),
               ("gmap_id1_mean","gmap_id2_mean"),
               ("gmap_id1_std_dev","gmap_id2_std_dev"),
               ("gmap_id1_skewness","gmap_id2_skewness"),
               ("gmap_id1_kurtosis","gmap_id2_kurtosis"),
               ("gmap_id1_Q1","gmap_id2_Q1"),
               ("gmap_id1_Q2","gmap_id2_Q2"),
               ("gmap_id1_Q3","gmap_id2_Q3"),
               ("gmap_id1_Q4","gmap_id2_Q4")]
@pandas_udf("double", PandasUDFType.SCALAR)
#gmap_id1,gmap_id2에 대표값에 관하여 코사인 유사도를 구하기위한 함수
def cosine_similarity_udf(v1: pd.Series, v2: pd.Series) -> pd.Series:
    # 각 Series의 요소가 벡터인 경우를 처리하기 위한 수정
    dot_product = np.array([np.dot(a, b) for a, b in zip(v1, v2)])
    norm_v1 = np.sqrt(np.array([np.dot(a, a) for a in v1]))
    norm_v2 = np.sqrt(np.array([np.dot(b, b) for b in v2]))
    cosine_similarity = dot_product / (norm_v1 * norm_v2)
    return pd.Series(cosine_similarity)

df_final_66 = representative_value1
# DataFrame에 코사인 유사도 컬럼 추가
for col1,col2 in columns:
    df_final_66 = df_final_66.withColumn(f"{col1[9:]}_cosine_similarity",  cosine_similarity_udf(col(col1), col(col2)))
    df_final_66 = df_final_66.fillna(0, subset=[f"{col1[9:]}_cosine_similarity"])





# COMMAND ----------

#저장하기 위한 변수 선택
df_final_66_select = df_final_66.select(['gmap_id1','gmap_id2','max_cosine_similarity',
'min_cosine_similarity',
'mean_cosine_similarity',
'std_dev_cosine_similarity',
 'skewness_cosine_similarity',
 'kurtosis_cosine_similarity',
 'Q1_cosine_similarity',
 'Q2_cosine_similarity',
 'Q3_cosine_similarity']) 

# COMMAND ----------

#중복값 제거
df_final_66_select1 = df_final_66_select.distinct()

# COMMAND ----------

df_final_66.write.mode('overwrite').saveAsTable('hive_metastore.test.cosine_similarity_51_v2')
