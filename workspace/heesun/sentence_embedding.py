# Databricks notebook source
# meta_hawaii(Table)로 불러와서 review,meta_data(DataFrame)으로 저장
review_data = spark.read.table('hive_metastore.silver.review_data')
meta_data = spark.read.table('hive_metastore.silver.meta_data')

# COMMAND ----------

from pyspark.sql import functions as F
#캘리포니아 데이터로 필터링
meta_datacalifornia = meta_data.filter(F.col('region') == 'California')

#meta data에서 state에서 폐업이라고 표시된 데이터 저장
df_filtered = meta_datacalifornia.filter(meta_datacalifornia['state'] == 'Permanently closed')
delete_list = df_filtered.select('gmap_id').collect()

# COMMAND ----------

#제거된 데이터 갯수 확인
display(meta_data.count())
display(meta_data.filter(meta_data['state'] == 'Permanently closed').count())
display(meta_data.filter(meta_data['state'].isNull()).count())
display(meta_data.filter(meta_data['state']=='N/A').count())
df_filtered = meta_data.filter(meta_data['state'] != 'Permanently closed')
display(df_filtered.count())

# COMMAND ----------

display(meta_datacalifornia.count())
display(df_filtered.count())

# COMMAND ----------

from pyspark.sql import functions as F
#리뷰데이터
# 캘리포니아 데이터로 필터링
review_data_california = review_data.filter(F.col('region') == 'California')

# 폐점된 가게의 리뷰데이터 제거
delete_list_df = spark.createDataFrame(delete_list, ['gmap_id'])

# 캘리포니아 데이터 조인
filtered_review_data = review_data_california.join(delete_list_df, on='gmap_id', how='left_anti')

# Display the filtered DataFrame
display(filtered_review_data)
display(review_data_california.select('gmap_id').distinct().count())
display(filtered_review_data.select('gmap_id').distinct().count())

# COMMAND ----------

from pyspark.sql import functions as F

# 필요한 열을 선택하고 1000개의 행으로 제한합니다.
review_texts_1000 = filtered_review_data.select('gmap_id', 'user_id', 'text')

# 'text' 열의 null 값을 빈 문자열로 채웁니다.
review_texts_1000 = review_texts_1000.withColumn('text', 
    F.when(F.col('text').isNull(), '').otherwise(
        F.when(F.col('text') == 'N/A', '').otherwise(F.col('text'))
    )
)

# 'text'에서 "(Translated by Google)" 부분을 제거합니다.
review_texts_1000 = review_texts_1000.withColumn('text', 
    F.regexp_replace(F.col('text'), r'\(Translated by Google\)', '')
)

# 이모티콘과 기타 비알파벳 문자를 제거합니다.
review_texts_1000 = review_texts_1000.withColumn('text', 
    F.regexp_replace(F.col('text'), r'[^\w\s,]', '')
)

# 전체 DataFrame을 Spark DataFrame으로 변환합니다.
review_texts = review_data_california.select('gmap_id', 'user_id', 'text')

# DataFrame을 표시합니다.
display(review_texts_1000)

# COMMAND ----------

# 필요한 라이브러리를 불러옵니다.
from pyspark.sql.functions import lower, regexp_replace, concat_ws, collect_list, col

# 리뷰 텍스트를 소문자로 변환합니다.
review_texts_1000 = review_texts_1000.withColumn("text", lower(col("text")))

# 리뷰 텍스트에서 특수 문자를 제거합니다. 알파벳, 숫자, 공백만 남깁니다.
review_texts_1000 = review_texts_1000.withColumn("text", regexp_replace(col("text"), "[^a-zA-Z0-9\s]", ""))
review_texts_1000 = review_texts_1000.withColumn(
    "text", 
    regexp_replace(col("text"), r"(\n|<.*?>)", "")
)
review_texts_1000.write.mode('overwrite').saveAsTable('hive_metastore.test.review_preprocessing_California')

