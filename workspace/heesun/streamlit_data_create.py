# Databricks notebook source
#모델별 샘플 데이터 가져오기(gbdt,hybrid,review_text)
rank_df = spark.read.table("hive_metastore.streamlit.text_sample")
rank_df2 = spark.read.table("hive_metastore.streamlit.gbdt_sample")
rank_df3 = spark.read.table("hive_metastore.streamlit.hybrid_sample")

# COMMAND ----------

#각 gmap_id2를 추출
rank_df_g2 = rank_df.select(['gmap_id2'])
rank_df2_g2 = rank_df2.select(['gmap_id2'])
rank_df3_g2 = rank_df3.select(['gmap_id2'])

# COMMAND ----------

# gmap_id2의 정보 테이블을 만들기위한 gmap_id2통합
union_df = rank_df_g2.union(rank_df2_g2).union(rank_df3_g2)

# Display the result
display(union_df)

# COMMAND ----------

#중복제거
union_df_distinct = union_df.distinct()

# COMMAND ----------

#실버데이터 가져오기(데이터 정보 뽑기)
silver = spark.read.table("hive_metastore.silver.california_meta ")

# COMMAND ----------

rank_df_gmap1 = rank_df.select(['gmap_id1'])

# COMMAND ----------

#실버데이터에서 가져올 데이터 선택
silver_select = silver.select(['address','gmap_id','avg_rating','latitude','longitude','name','main_category','description','state','url','num_of_reviews','price','first_main_category','region','city','hash_tag'])

# COMMAND ----------

from pyspark.sql.functions import col
#데이터를 조인하고 병합히기 위해 컬러명 변경후 저장
# DataFrame 'silver'의 컬럼 선택 및 이름 변경
silver_select_2 = silver.select(
    col('address').alias('address2'),
    col('gmap_id').alias('gmap_id2'),
    col('avg_rating').alias('avg_rating2'),
    col('latitude').alias('latitude2'),
    col('longitude').alias('longitude2'),
    col('name').alias('name2'),
    col('main_category').alias('main_category2'),
    col('description').alias('description2'),
    col('state').alias('state2'),
    col('url').alias('url2'),
    col('num_of_reviews').alias('num_of_reviews2'),
    col('price').alias('price2'),
    col('first_main_category').alias('first_main_category2'),
    col('region').alias('region2'),
    col('city').alias('city2'),
    col('hash_tag').alias('hash_tag2')
)
# DataFrame 'silver'의 컬럼 선택 및 이름 변경
silver_select_1 = silver.select(
    col('address').alias('address1'),
    col('gmap_id').alias('gmap_id1'),
    col('avg_rating').alias('avg_rating1'),
    col('latitude').alias('latitude1'),
    col('longitude').alias('longitude1'),
    col('name').alias('name1'),
    col('main_category').alias('main_category1'),
    col('description').alias('description1'),
    col('state').alias('state1'),
    col('url').alias('url1'),
    col('num_of_reviews').alias('num_of_reviews1'),
    col('price').alias('price1'),
    col('first_main_category').alias('first_main_category1'),
    col('region').alias('region1'),
    col('city').alias('city1'),
    col('hash_tag').alias('hash_tag1')
)



# COMMAND ----------

#gmap_id1데이터 조인 진행
gmap1_df = rank_df_gmap1.join(
    silver_select_1,
    (rank_df_gmap1.gmap_id1 == silver_select_1.gmap_id1),
    "left_outer"
)
gmap1_df = gmap1_df.drop(silver_select_1.gmap_id1).distinct()

# COMMAND ----------

#gmap_id2 조인 진행
rank_df_gmap2 = union_df_distinct
gmap2_df = rank_df_gmap2.join(
    silver_select_2,
    (rank_df_gmap2.gmap_id2 == silver_select_2.gmap_id2),
    "left_outer"
)
gmap2_df = gmap2_df.drop(silver_select_2.gmap_id2).distinct()

# COMMAND ----------

# Save the DataFrame as a table
gmap1_df.write.mode('overwrite').saveAsTable('hive_metastore.streamlit.gmap_id1_info')

# COMMAND ----------

gmap2_df.write.mode('overwrite').saveAsTable('hive_metastore.streamlit.gmap_id2_info')

# COMMAND ----------

# 조인을 수행
left_df = rank_df_gmap1.join(
    silver_select_1,
    (rank_df_gmap1.gmap_id1 == silver_select_1.gmap_id1),
    "left_outer"
)
left_df = left_df.join(
    silver_select_2,
    (left_df.gmap_id2 == silver_select_2.gmap_id2),
    "left_outer"
)
# 중복 컬럼 제거
left_df = left_df.drop(silver_select_1.gmap_id1, silver_select_2.gmap_id2)

# COMMAND ----------

display(left_df)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

# 데이터프레임을 gmap_id1으로 파티셔닝하고, Score로 내림차순 정렬할 윈도우 정의
window_spec = Window.partitionBy("gmap_id1").orderBy(col("cosine_top4").desc())

# rank 컬럼 추가: 각 그룹 내에서 Score를 기준으로 순위 매기기
predicion_rank = left_df.withColumn("rank", row_number().over(window_spec))

# COMMAND ----------

display(predicion_rank)

# COMMAND ----------

predicion_rank.count()
