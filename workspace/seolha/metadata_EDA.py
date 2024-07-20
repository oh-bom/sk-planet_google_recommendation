# Databricks notebook source
import pyspark.pandas as ps
from pyspark.sql.functions import col, count, when, isnan
import numpy as np

# COMMAND ----------

df = spark.read.table('hive_metastore.default.meta_data')

# COMMAND ----------

type(df)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
display(null_counts)

# COMMAND ----------

df = df.to_pandas_on_spark()

# COMMAND ----------

df.shape

# COMMAND ----------

df.info()

# COMMAND ----------

data = []
for i in df.columns:
    count=[]
    count.append(i)
    count.append(df[i].isnull().sum())
    data.append(count)
va_df = ps.DataFrame(data)

# COMMAND ----------

va_df['2'] = 0
va_df

# COMMAND ----------

total = len(df)
total

# COMMAND ----------

for i in range(len(va_df)):
    va_df.loc[i,'2'] = round(int(va_df.loc[i,1])/total,3)

# COMMAND ----------

va_df

# COMMAND ----------

len(df['gmap_id'])

# COMMAND ----------

df['gmap_id'].nunique()

# COMMAND ----------

len(df['gmap_id'])-df['gmap_id'].nunique()

# COMMAND ----------

# Enable 'compute.ops_on_diff_frames' option
ps.options.compute.ops_on_diff_frames = True

# 1. 전체 행의 수
total_rows = df.shape[0]

# 2. 고유한 gmap_id의 개수
unique_gmap_ids = df['gmap_id'].nunique()

# 3. 중복된 gmap_id 찾기
duplicated_gmap_ids = df[df.duplicated('gmap_id', keep=False)]

# 4. 중복된 gmap_id의 개수
num_duplicated_gmap_ids = duplicated_gmap_ids['gmap_id'].nunique()

# 5. 중복된 행의 수
num_duplicated_rows = duplicated_gmap_ids.shape[0]

# 결과 출력
print(f"전체 행의 수: {total_rows}")
print(f"고유한 gmap_id의 개수: {unique_gmap_ids}")
print(f"고유한 중복된 gmap_id의 개수: {num_duplicated_gmap_ids}")
print(f"중복된 행의 수: {num_duplicated_rows}")

# COMMAND ----------

duplicated_rows = df[df.duplicated(keep=False)]
len(duplicated_rows)

# COMMAND ----------

display(df)

# COMMAND ----------

# num_of_reviews에 로그(base 10) 적용하여 log_num 컬럼 추가
df['log_num'] = np.log10(df['num_of_reviews'])

# COMMAND ----------

display(df.to_spark().limit(5))

# COMMAND ----------

display

# COMMAND ----------

df['range_rating'] = df['avg_rating'].apply(lambda x: int(x) if x < 5 else 5)

# COMMAND ----------

df['num_of_reviews'].describe()

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(num_of_reviews)
# MAGIC from hive_metastore.default.meta_data;

# COMMAND ----------

df['relative_results'].fillna('',inplace=True) #NaN 데이터를 빈칸으로 채움

# COMMAND ----------

df['num_of_reviews']

# COMMAND ----------

display(df[df['num_of_reviews']>=5].limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM hive_metastore.default.meta_data
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     AVG(avg_rating) AS `평균 평점`
# MAGIC FROM 
# MAGIC     hive_metastore.default.meta_data
# MAGIC GROUP BY 
# MAGIC     region
# MAGIC ORDER BY 
# MAGIC     AVG(avg_rating) DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     SUM(num_of_reviews) AS `리뷰_수_합계`
# MAGIC FROM 
# MAGIC     hive_metastore.default.meta_data
# MAGIC GROUP BY 
# MAGIC     region
# MAGIC ORDER BY 
# MAGIC     SUM(num_of_reviews)DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     AVG(num_of_reviews) AS `평균_리뷰_수`
# MAGIC FROM 
# MAGIC     hive_metastore.default.meta_data
# MAGIC GROUP BY 
# MAGIC     region
# MAGIC ORDER BY 
# MAGIC     `평균_리뷰_수` DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,category,url
# MAGIC from hive_metastore.default.meta_data
# MAGIC where name = 'Washington’s Monument';

# COMMAND ----------

# MAGIC %sql
# MAGIC select category,name, url
# MAGIC from hive_metastore.default.meta_data
# MAGIC where LATERAL VIEW explode(category) AS c
# MAGIC   WHERE lower(c) LIKE '%landmark%'
