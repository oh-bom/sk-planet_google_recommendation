# Databricks notebook source
# MAGIC %md
# MAGIC ### 가격

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.before_co_count")

# COMMAND ----------

meta_df = spark.table("hive_metastore.silver.california_meta")

# COMMAND ----------

from pyspark.sql.functions import col

# meta_df에서 필요한 컬럼 선택 및 컬럼 이름 변경
meta_df = meta_df.select(col("gmap_id"), col("price").alias("price_meta"))

# gmap_id1과 gmap_id를 기준으로 조인하여 price1 추가
train_df = train_df.join(meta_df, train_df.gmap_id1 == meta_df.gmap_id, how='left') \
                   .withColumnRenamed("price_meta", "price1")

# gmap_id2과 gmap_id를 기준으로 조인하여 price2 추가
train_df = train_df.join(meta_df, train_df.gmap_id2 == meta_df.gmap_id, how='left') \
                   .withColumnRenamed("price_meta", "price2")

# COMMAND ----------

columns_to_drop = ["gmap_id"]
train_df = train_df.drop(*columns_to_drop)

# COMMAND ----------

train_df_final = train_df.fillna({"price1": -9999 , "price2":-9999})

# COMMAND ----------

train_df_final.write.mode("overwrite").saveAsTable("hive_metastore.item2item.train_price")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 설명 유무

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_price")

# COMMAND ----------

meta_df = spark.table("hive_metastore.silver.california_meta")

# COMMAND ----------

from pyspark.sql.functions import col, when

# 필요한 컬럼 선택 및 조인을 위한 데이터 준비
meta_df_description = meta_df.select(col("gmap_id"), col("description"))

# gmap_id1과 gmap_id를 기준으로 조인하여 description1 추가
train_df = train_df.join(meta_df_description, train_df.gmap_id1 == meta_df_description.gmap_id, how='left') \
                   .withColumn("description1", when(col("description").isNull(), 0).otherwise(1)) \
                   .drop(meta_df_description.gmap_id, meta_df_description.description)

# gmap_id2와 gmap_id를 기준으로 조인하여 description2 추가
train_df = train_df.join(meta_df_description, train_df.gmap_id2 == meta_df_description.gmap_id, how='left') \
                   .withColumn("description2", when(col("description").isNull(), 0).otherwise(1)) \
                   .drop(meta_df_description.gmap_id, meta_df_description.description)

# COMMAND ----------

train_df.write.mode("overwrite").saveAsTable("hive_metastore.item2item.train_description")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 설명 단어수

# COMMAND ----------

train_df = spark.table("hive_metastore.item2item.train_description")

# COMMAND ----------

from pyspark.sql.functions import col, size, split

# 필요한 컬럼 선택 및 조인을 위한 데이터 준비
meta_df_description = meta_df.select(col("gmap_id"), col("description"))

# gmap_id1과 gmap_id를 기준으로 조인하여 desc_word_cnt1 추가
train_df = train_df.join(meta_df_description, train_df.gmap_id1 == meta_df_description.gmap_id, how='left') \
                   .withColumn("desc_word_cnt1", size(split(col("description"), " "))) \
                   .drop(meta_df_description.gmap_id, meta_df_description.description)

# gmap_id2와 gmap_id를 기준으로 조인하여 desc_word_cnt2 추가
train_df = train_df.join(meta_df_description, train_df.gmap_id2 == meta_df_description.gmap_id, how='left') \
                   .withColumn("desc_word_cnt2", size(split(col("description"), " "))) \
                   .drop(meta_df_description.gmap_id, meta_df_description.description)

# COMMAND ----------

train_df_final = train_df.fillna({"desc_word_cnt1": 0, "desc_word_cnt2": 0})

# COMMAND ----------

# Null 값 체크

from pyspark.sql.functions import col, sum

columns_to_check = train_df_final.columns
null_counts = train_df_final.select([sum(col(c).isNull().cast("int")).alias(c + "_null_count") for c in columns_to_check])

# COMMAND ----------

display(null_counts)

# COMMAND ----------

train_df_final.write.mode("overwrite").saveAsTable("hive_metastore.item2item.train_word_cnt")
