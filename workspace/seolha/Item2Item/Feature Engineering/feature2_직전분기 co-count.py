# Databricks notebook source
train_df = spark.table("hive_metastore.item2item.trian_before_user")

# COMMAND ----------

from pyspark.sql.functions import expr, col

# 이전 분기 계산
train_df = train_df.withColumn("prev_year", expr("case when quarter = 1 then year - 1 else year end"))
train_df = train_df.withColumn("prev_quarter", expr("case when quarter = 1 then 4 else quarter - 1 end"))

# 현재 데이터프레임을 복제하여 clone_df 생성
clone_df = train_df.selectExpr(
    "gmap_id1 as clone_gmap_id1",
    "gmap_id2 as clone_gmap_id2",
    "year as clone_year",
    "quarter as clone_quarter",
    "co_count as clone_co_count"
)

# gmap_id1과 gmap_id2가 동시에 만족할 때 직전 분기의 co_count 가져오기
train_df = train_df.join(
    clone_df,
    (train_df.gmap_id1 == clone_df.clone_gmap_id1) &
    (train_df.gmap_id2 == clone_df.clone_gmap_id2) &
    (train_df.prev_year == clone_df.clone_year) &
    (train_df.prev_quarter == clone_df.clone_quarter),
    "left"
).select(
    train_df["*"], clone_df["clone_co_count"].alias("before_co_count")
)

# before_co_count 값이 null인 경우 0으로 설정
train_df_final = train_df.fillna({"before_co_count": 0})

# COMMAND ----------

columns_to_drop = ["prev_year","prev_quarter"]
train_df_final = train_df_final.drop(*columns_to_drop)

# COMMAND ----------

train_df_final.write.mode("overwrite").saveAsTable("hive_metastore.item2item.before_co_count")
