# Databricks notebook source
# MAGIC %md
# MAGIC ### _vector db_

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01. data load, preprocess

# COMMAND ----------

# MAGIC %pip install emoji

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

data=spark.sql("SELECT * FROM hive_metastore.silver.california_review")

# COMMAND ----------

data.count()

# COMMAND ----------

# review text가 존재하는 애들로 필터리
data=data.filter(col("text")!="N/A")

# COMMAND ----------

data.count()

# COMMAND ----------

from pyspark.sql import functions as F

data = data.orderBy(F.desc("gmap_id"))

# COMMAND ----------

all_columns=["gmap_id","name","rating","resp","text","time"]
data=data.select(all_columns)

# COMMAND ----------

display(data.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC resp 전처리
# MAGIC : text 추출, N/A 제거, 이모지 묘사

# COMMAND ----------

import re
import emoji
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

@udf(returnType=StringType())
def filter_resp(resp):
    resp_text = resp["text"]

    if resp_text=="N/A":clean_text=""
    else: clean_text = re.sub(r'\\n|\\u', '', resp_text)
    
    final_text=emoji.demojize(clean_text)
    
    return final_text

data=data.withColumn("resp",filter_resp(col("resp")))

# COMMAND ----------

display(data.select("resp").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC review text 전처리

# COMMAND ----------

import re
import emoji
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

@udf(returnType=StringType())
def filter_resp(text):

    if text=="N/A":clean_text=""
    else: clean_text = re.sub(r'\\n|\\u', '', text)
    
    final_text=emoji.demojize(clean_text)
    
    return final_text

data=data.withColumn("text",filter_resp(col("text")))

# COMMAND ----------

display(data.select("text").limit(10))

# COMMAND ----------

data=data.drop(col("name"))

# COMMAND ----------

data.write.mode("overwrite").saveAsTable("hive_metastore.vector_db.reviews_filtered2")

# COMMAND ----------

document=data

# COMMAND ----------

# 모든 컬럼 string으로 변환
@udf(StringType())
def convert_to_str(x):
    return json.dumps(x)

for col in document.columns:
    if not isinstance(document.schema[col].dataType,StringType):
        document=document.withColumn(col,convert_to_str(document[col]))

# COMMAND ----------

@udf(returnType=StringType())
def combine_columns(*cols):
     return ". ".join([f"{name}:{str(col)}" for name, col in zip(all_columns, cols) if col is not None and str(col).lower() != 'null'])

document=document.withColumn("combined_doc",combine_columns(*document.columns))

# COMMAND ----------

display(document.select("combined_doc").limit(2))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.vector_db")

# COMMAND ----------

document.write.mode("overwrite").saveAsTable("hive_metastore.vector_db.reviews_filtered2")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### +) meta 가게 이름 추가

# COMMAND ----------

review=spark.sql("SELECT *FROM hive_metastore.vector_db.reviews_filtered2")

# COMMAND ----------

review.count()

# COMMAND ----------

meta=spark.sql("SELECT name,gmap_id FROM hive_metastore.vector_db.raw_california_filtered3")

# COMMAND ----------

joined=review.join(meta,on="gmap_id")

# COMMAND ----------

joined.count()

# COMMAND ----------

display(joined.limit(5))

# COMMAND ----------

joined.write.mode("overwrite").saveAsTable("hive_metastore.vector_db.reviews_filtered3")

# COMMAND ----------

# MAGIC %md
# MAGIC #### gmap_id 기준으로 묶기 (사용 x, 참고용)

# COMMAND ----------

from pyspark.sql import functions as F

df_grouped = data.groupBy("gmap_id").agg(
    F.first("name").alias("name"),
    F.concat_ws(",", F.collect_list("text")).alias("text_list"),
    F.concat_ws(",", F.collect_list("resp")).alias("resp_list")
)


# COMMAND ----------

df_grouped.count()

# COMMAND ----------

display(df_grouped.limit(10))

# COMMAND ----------

import re

@udf(returnType=StringType())
def filter_text_list(text):
    return re.sub(r'\n', '', text)

df_grouped=df_grouped.withColumn("text_cleaned",filter_text_list(col("text_list")))

# COMMAND ----------

display(df_grouped.limit(3))

# COMMAND ----------

import re

@udf(returnType=StringType())
def filter_resp_list(resp):
    return re.sub(r',', '', resp)

df_grouped=df_grouped.withColumn("resp_cleaned",filter_resp_list(col("resp_list")))

# COMMAND ----------

display(df_grouped.limit(3))

# COMMAND ----------

df_grouped.write.mode("overwrite").saveAsTable("hive_metastore.vector_db.reviews_groupby")
