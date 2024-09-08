# Databricks notebook source
del df

# COMMAND ----------

# MAGIC %md
# MAGIC # meta data union

# COMMAND ----------

# meta-data union

from pyspark.sql.functions import col, from_json, lit, to_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
import re

# JSON 스키마 정의 (공통된 구조로 정의)
json_schema = StructType([
    StructField("Accessibility", ArrayType(StringType()), True),
    StructField("Activities", ArrayType(StringType()), True),
    StructField("Amenities", ArrayType(StringType()), True),
    StructField("Atmosphere", ArrayType(StringType()), True),
    StructField("Crowd", ArrayType(StringType()), True),
    StructField("Dining_options", ArrayType(StringType()), True),
    StructField("From_the_business", ArrayType(StringType()), True),
    StructField("Getting_here", ArrayType(StringType()), True),
    StructField("Health_and_safety", ArrayType(StringType()), True),
    StructField("Highlights", ArrayType(StringType()), True),
    StructField("Lodging_options", ArrayType(StringType()), True),
    StructField("Offerings", ArrayType(StringType()), True),
    StructField("Payments", ArrayType(StringType()), True),
    StructField("Planning", ArrayType(StringType()), True),
    StructField("Popular_for", ArrayType(StringType()), True),
    StructField("Recycling", ArrayType(StringType()), True),
    StructField("Service_options", ArrayType(StringType()), True)
])

# DBFS에서 파일 목록 가져오기
file_path = "dbfs:/data/raw/metadata/"
files = dbutils.fs.ls(file_path)
file_names = [file.name for file in files if file.path.endswith(".json.gz")]

for i, file_name in enumerate(file_names):
    # 파일명에서 지역 이름 추출
    region = file_name.split("-")[1].split(".")[0]
    
    # 파일 읽기
    path = f"{file_path}{file_name}"
    file_df = spark.read.json(path)
    
    # "region" 컬럼 추가
    file_df = file_df.withColumn("region", lit(region))

    # MISC 컬럼을 문자열로 변환
    file_df = file_df.withColumn("MISC", to_json(col("MISC")))
    
    # MISC 컬럼을 공통된 JSON 구조로 변환
    file_df = file_df.withColumn("MISC", from_json(col("MISC"), json_schema))
    
    # 컬럼 이름 변경
    for column in file_df.columns:
        new_column = re.sub(r'[ ,;{}()\n\t=]', '_', column)
        file_df = file_df.withColumnRenamed(column, new_column)

    # 데이터프레임에 추가
    if i == 0:
        df = file_df
    else:
        # 컬럼 수 확인
        if len(file_df.columns) != len(df.columns):
            print(f"{i}, {file_df.columns}")
            print(f"{i}, {df.columns}")
            print(f"File {file_name} has a different number of columns.")
        else:
            print(f"pass_{i}")
        df = df.union(file_df)

# COMMAND ----------

display(unique_regions)

# COMMAND ----------

df.count()

# COMMAND ----------

# hive_metastore에 저장
df.write.mode('overwrite').format("delta").saveAsTable('hive_metastore.default.meta_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), count(distinct gmap_id)
# MAGIC from hive_metastore.default.meta_data

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # review data union

# COMMAND ----------

# review-data union

from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType

# DBFS에서 파일 목록 가져오기
file_path = "dbfs:/data/raw/review/"
files = dbutils.fs.ls(file_path)
file_names = [file.name for file in files if file.path.endswith(".json.gz")]

for i, file_name in enumerate(file_names):
    # 파일명에서 지역 이름 추출
    region = file_name.split("-")[1].split(".")[0]
    
    # 파일 읽기
    path = f"{file_path}{file_name}"
    file_df = spark.read.json(path)
    
    # "region" 컬럼 추가
    file_df = file_df.withColumn("region", lit(region))
    
    # 데이터프레임에 추가
    if i == 0:
        review_df = file_df
    else:
        # 컬럼 수 확인
        if len(file_df.columns) != len(review_df.columns):
            print(f"{i}, {file_df.columns}")
            print(f"{i}, {review_df.columns}")
            print(f"File {file_name} has a different number of columns.")
        else:
            print(f"pass_{i}")
            
        review_df = review_df.union(file_df)       

# COMMAND ----------

# hive_metastore에 저장
review_df.write.mode('overwrite').format("delta").saveAsTable('hive_metastore.default.review_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hive_metastore.default.review_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.default.review_data
