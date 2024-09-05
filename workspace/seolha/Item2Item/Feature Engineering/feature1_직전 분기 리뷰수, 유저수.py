# Databricks notebook source
# MAGIC %md
# MAGIC ### Review 데이터에 연도, 분기 추가

# COMMAND ----------

cali_review = spark.table("hive_metastore.silver.california_review")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, when, concat, lit

# Year,month 추출
pattern = r"(\d{4})-(\d{2})"

time_extracted_df = cali_review.withColumn(
    "year", 
    regexp_extract(col("time"), pattern, 1)
).withColumn(
    "month", 
    regexp_extract(col("time"), pattern, 2)
)

time_extracted_df.createOrReplaceTempView("time_extracted_df")

# 분기 추가
quarter_added_df=time_extracted_df.withColumn("quarter",
    when(col("month").between("01","03"),1)
    .when(col("month").between("04","06"),2)
    .when(col("month").between("07","09"),3)
    .when(col("month").between("10","12"),4))

quarter_added_df.createOrReplaceTempView("quarter_added_df")

# 연-분기 컬럼 추가
year_quarter_added_df=quarter_added_df.withColumn("year_quarter",concat(col("year"),lit("-"),col("quarter")))

year_quarter_added_df.createOrReplaceTempView("year_quarter_added_df")



# COMMAND ----------

year_quarter_added_df.write.mode("overwrite").saveAsTable("hive_metastore.item2item.cali_review_quarter")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 직전분기 리뷰수 넣기

# COMMAND ----------

cali_review_quarter = spark.table("hive_metastore.item2item.cali_review_quarter")

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_v3_syleeie")

# COMMAND ----------

columns_to_drop = ["before_review1", "before_review2", "before_user1","before_user2"]
train_df = train_df.drop(*columns_to_drop)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# 직전 연도와 직전 분기 계산하여 컬럼 추가
train_df_with_prev_quarter = train_df.withColumn("prev_year", F.when(col("quarter") == 1, col("year") - 1).otherwise(col("year"))) \
                                     .withColumn("prev_quarter", F.when(col("quarter") == 1, F.lit(4)).otherwise(col("quarter") - 1))

# cali_review_quarter에서 gmap_id, year, quarter별 리뷰 개수 세기
count_reviews = cali_review_quarter.groupBy("gmap_id", "year", "quarter").agg(F.count("*").alias("review_count"))

# train_df_with_prev_quarter와 count_reviews를 조인하여 before_review1 컬럼 추가
train_df_with_counts = train_df_with_prev_quarter.join(
    count_reviews,
    (train_df_with_prev_quarter.gmap_id1 == count_reviews.gmap_id) & 
    (train_df_with_prev_quarter.prev_year == count_reviews.year) & 
    (train_df_with_prev_quarter.prev_quarter == count_reviews.quarter),
    "left"
).select(
    train_df_with_prev_quarter["*"], count_reviews["review_count"].alias("before_review1")
)

# before_review1 컬럼이 없는 경우 0으로 채우기
train_df_with_counts = train_df_with_counts.fillna({"before_review1": 0})

# train_df_with_counts와 count_reviews를 조인하여 before_review2 컬럼 추가
train_df_with_counts = train_df_with_counts.join(
    count_reviews,
    (train_df_with_counts.gmap_id2 == count_reviews.gmap_id) & 
    (train_df_with_counts.prev_year == count_reviews.year) & 
    (train_df_with_counts.prev_quarter == count_reviews.quarter),
    "left"
).select(
    train_df_with_counts["*"], count_reviews["review_count"].alias("before_review2")
)

# before_review2 컬럼이 없는 경우 0으로 채우기
train_df_final = train_df_with_counts.fillna({"before_review2": 0})

# COMMAND ----------

columns_to_drop = ["prev_year", "prev_quarter"]
train_df_final = train_df_final.drop(*columns_to_drop)

# COMMAND ----------

train_df_final.write.mode("overwrite").saveAsTable("hive_metastore.item2item.train_before_review")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 직전분기 유저수 넣어보기

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_before_review")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# 직전 연도와 직전 분기 계산하여 컬럼 추가
train_df_with_prev_quarter = train_df.withColumn(
    "prev_year", F.when(col("quarter") == 1, col("year") - 1).otherwise(col("year"))
).withColumn(
    "prev_quarter", F.when(col("quarter") == 1, F.lit(4)).otherwise(col("quarter") - 1)
)

# cali_review_quarter에서 gmap_id, year, quarter별 유니크한 user_id 개수 세기
count_users = cali_review_quarter.groupBy("gmap_id", "year", "quarter").agg(
    F.countDistinct("user_id").alias("user_count")
)

# train_df_with_prev_quarter와 count_users를 조인하여 before_user1 컬럼 추가
train_df_with_counts = train_df_with_prev_quarter.join(
    count_users,
    (train_df_with_prev_quarter.gmap_id1 == count_users.gmap_id) & 
    (train_df_with_prev_quarter.prev_year == count_users.year) & 
    (train_df_with_prev_quarter.prev_quarter == count_users.quarter),
    "left"
).select(
    train_df_with_prev_quarter["*"], count_users["user_count"].alias("before_user1")
)

# before_user1 컬럼이 없는 경우 0으로 채우기
train_df_with_counts = train_df_with_counts.fillna({"before_user1": 0})

# train_df_with_counts와 count_users를 조인하여 before_user2 컬럼 추가
train_df_with_counts = train_df_with_counts.join(
    count_users,
    (train_df_with_counts.gmap_id2 == count_users.gmap_id) & 
    (train_df_with_counts.prev_year == count_users.year) & 
    (train_df_with_counts.prev_quarter == count_users.quarter),
    "left"
).select(
    train_df_with_counts["*"], count_users["user_count"].alias("before_user2")
)

# before_user2 컬럼이 없는 경우 0으로 채우기
train_df_final = train_df_with_counts.fillna({"before_user2": 0})


# COMMAND ----------

columns_to_drop = ["prev_year", "prev_quarter"]
train_df_final = train_df_final.drop(*columns_to_drop)

# COMMAND ----------

train_df_final.write.mode("overwrite").saveAsTable("hive_metastore.item2item.trian_before_user")
