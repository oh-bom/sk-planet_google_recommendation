# Databricks notebook source
# MAGIC %md
# MAGIC ### _vector db_

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01. data load, preprocess

# COMMAND ----------

# raw data
data=spark.read.json("dbfs:/data/meta-California.json.gz")

# COMMAND ----------

data.filter(col("state")!="Permanently closed").count()

# COMMAND ----------

null_data=data.filter(col("state").isNull())

# COMMAND ----------

closed_data=data.filter(col("state")!="Permanently closed")

# COMMAND ----------

final_data=null_data.union(closed_data)

# COMMAND ----------

final_data.count()

# COMMAND ----------

data=final_data.dropDuplicates(["gmap_id"])
data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### misc 전처리

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

document=spark.sql("SELECT *FROM hive_metastore.silver.california_meta")

# COMMAND ----------

# 모든 컬럼 string으로 변환
@udf(StringType())
def convert_to_str(x):
    return json.dumps(x)

for col in document.columns:
    if not isinstance(document.schema[col].dataType,StringType):
        document=document.withColumn(col,convert_to_str(document[col]))

# COMMAND ----------

document.count()

# COMMAND ----------

import re
from pyspark.sql.functions import *
from pyspark.sql.types import *

# MISC 정리
@udf(returnType=StringType())
def filter_null_and_empty_list(col_str):
    if col_str is None or not isinstance(col_str, str):
        return ""
    # 정규식 패턴: null 또는 빈 리스트([])
    pattern = re.compile(r'\[|\]|null|"')

    # 패턴과 매칭되는 부분을 제거
    filtered_str = pattern.sub('', col_str)

    # 불필요한 쉼표와 공백 제거
    filtered_str = re.sub(r'\s*,\s*', ',', filtered_str)
    filtered_str = re.sub(r',+', ',', filtered_str)
    
    # 문자열의 시작과 끝에 있는 쉼표 제거
    filtered_str = filtered_str.strip(', ')
    return filtered_str

document = document.withColumn("MISC", filter_null_and_empty_list(col("MISC")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### category 전처리

# COMMAND ----------

import re
from pyspark.sql.functions import *
from pyspark.sql.types import *

document = document.withColumn("category", filter_null_and_empty_list(col("category")))
display(document.select("category").limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### hours 전처리

# COMMAND ----------

import re
from pyspark.sql.functions import *
from pyspark.sql.types import *

@udf(returnType=StringType())
def filter_hours(col_str):
    # 정규식 패턴: null 또는 빈 리스트([])
    pattern = re.compile(r'\[|null|"')
    to_pattern=re.compile(r'\\u2013')
    split_pattern=re.compile(r'\],')

    # 패턴과 매칭되는 부분을 제거
    filtered_str = pattern.sub('', col_str)
    filtered_str= to_pattern.sub(" to ",filtered_str)
    filtered_str= split_pattern.sub("/",filtered_str)
    
    # 불필요한 쉼표와 공백 제거
    filtered_str = re.sub(r'\s*,\s*', ',', filtered_str)
    filtered_str = re.sub(r',+', ',', filtered_str)

    # 문자열의 시작과 끝에 있는 쉼표 제거
    filtered_str = filtered_str.strip(', ')
    filtered_str = filtered_str.rstrip(']]')
    
    return filtered_str

document=document.withColumn("hours",filter_hours(col("hours")))

# COMMAND ----------

document.columns

# COMMAND ----------

exclude_cols=["region","relative_results"]

all_columns=[col for col in document.columns if col not in exclude_cols]
print(all_columns)

# 모든 컬럼의 데이터를 하나의 문서로 병합
@udf(returnType=StringType())
def combine_columns(*cols):
     return ". ".join([f"{name} is {str(col)}" for name, col in zip(all_columns, cols) if col is not None and str(col).lower() != 'null'])

document=document.withColumn("combined_doc",combine_columns(*all_columns))

# COMMAND ----------

display(document.limit(1))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS hive_metastore.vector_db")

# COMMAND ----------

document.write.mode("overwrite").saveAsTable("asacdataanalysis.vector_db.meta")
