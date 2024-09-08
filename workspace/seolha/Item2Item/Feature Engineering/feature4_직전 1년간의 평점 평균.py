# Databricks notebook source
# MAGIC %md
# MAGIC ### 직전 1년간의 평점 평균

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_word_cnt")

# COMMAND ----------

review_df = spark.table("hive_metastore.item2item.cali_review_quarter")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 직전 연도와 직전 분기 계산하여 컬럼 추가
train_df = train_df.withColumn(
    "prev_year", F.when(F.col("quarter") == 1, F.col("year") - 1).otherwise(F.col("year"))
).withColumn(
    "prev_quarter", F.when(F.col("quarter") == 1, F.lit(4)).otherwise(F.col("quarter") - 1)
)

# review_df에서 필요한 컬럼 선택
review_df = review_df.select("gmap_id", "year", "quarter", "rating")

# 기준 연도와 분기 추가
train_df = train_df.withColumn("start_year", F.col("year") - 1)
train_df = train_df.withColumn("start_quarter", F.col("quarter"))

# gmap_id1에 대한 직전 1년의 평균 평점 계산
review_df_broadcast = F.broadcast(review_df.alias("review1"))
prev_year_avg_rating1 = train_df.alias("train1").join(
    review_df_broadcast,
    (F.col("train1.gmap_id1") == F.col("review1.gmap_id")) &
    (
        ((F.col("review1.year") == F.col("train1.start_year")) & (F.col("review1.quarter") >= F.col("train1.start_quarter"))) |
        ((F.col("review1.year") == F.col("train1.year")) & (F.col("review1.quarter") <= F.col("train1.prev_quarter")))
    ),
    "left"
).groupBy(
    "train1.gmap_id1", "train1.gmap_id2", "train1.year", "train1.quarter", "train1.start_year", "train1.start_quarter", "train1.prev_year", "train1.prev_quarter"
).agg(
    F.avg("review1.rating").alias("prev_year_avg_rating1")
)

# 필요한 컬럼 유지
prev_year_avg_rating1 = prev_year_avg_rating1.select(
    "gmap_id1", "gmap_id2", "year", "quarter", "prev_year", "prev_quarter", "start_year", "start_quarter", "prev_year_avg_rating1"
)

# gmap_id2에 대한 직전 1년의 평균 평점 계산
review_df_broadcast2 = F.broadcast(review_df.alias("review2"))
prev_year_avg_rating2 = prev_year_avg_rating1.alias("train2").join(
    review_df_broadcast2,
    (F.col("train2.gmap_id2") == F.col("review2.gmap_id")) &
    (
        ((F.col("review2.year") == F.col("train2.start_year")) & (F.col("review2.quarter") >= F.col("train2.start_quarter"))) |
        ((F.col("review2.year") == F.col("train2.year")) & (F.col("review2.quarter") <= F.col("train2.prev_quarter")))
    ),
    "left"
).groupBy(
    "train2.gmap_id1", "train2.gmap_id2", "train2.year", "train2.quarter", "train2.prev_year", "train2.prev_quarter", "train2.start_year", "train2.start_quarter", "train2.prev_year_avg_rating1"
).agg(
    F.avg("review2.rating").alias("prev_year_avg_rating2")
)

# 최종 결과를 train_df에 추가
result_df = train_df.alias("train").join(
    prev_year_avg_rating2.alias("avg"),
    (F.col("train.gmap_id1") == F.col("avg.gmap_id1")) &
    (F.col("train.gmap_id2") == F.col("avg.gmap_id2")) &
    (F.col("train.year") == F.col("avg.year")) &
    (F.col("train.quarter") == F.col("avg.quarter")),
    "left"
).select(
    F.col("train.*"),
    F.col("avg.prev_year_avg_rating1"),
    F.col("avg.prev_year_avg_rating2")
)


# COMMAND ----------

result_df = result_df.drop("prev_year", "prev_quarter", "start_year", "start_quarter")

# COMMAND ----------

train_df_final = result_df.fillna({"prev_year_avg_rating1": -9999, "prev_year_avg_rating2": -9999})

# COMMAND ----------

# Null 값 체크

from pyspark.sql.functions import col, sum

columns_to_check = train_df_final.columns
null_counts = train_df_final.select([sum(col(c).isNull().cast("int")).alias(c + "_null_count") for c in columns_to_check])

# COMMAND ----------

display(null_counts)

# COMMAND ----------

train_df_final.write.mode("overwrite").saveAsTable("hive_metastore.item2item.train_under_v3")
