# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

#전체 데이터 로드
data=spark.sql("SELECT support,lift,lift_log,lift_log10,confidence,confidence2,jaccard,co_count,gmap_count1,gmap_count2 FROM hive_metastore.test.california_cocount_final5")
data.createOrReplaceTempView("data")

# COMMAND ----------

data_over2=data.filter(col("co_count")>2)

# COMMAND ----------

data_over3=data.filter(col("co_count")>3)

# COMMAND ----------

total2=data_over2.count()

# COMMAND ----------

total=data_over3.count()

# COMMAND ----------

def get_stats_quantile(data, target,target_quantile):
    # 상위 target_quantile 지점 계산
    quantiles = data.approxQuantile(target, [1-target_quantile], 0.01)
    result = quantiles[0]
    
    return result

# COMMAND ----------

def check_percentile(data,data_size,target_col,threshold):
    return data.filter(col(target_col)>threshold).count()/data_size

# COMMAND ----------

def calc_check(data,data_size,target_col,target_quantile):
    threshold=get_stats_quantile(data,target_col,target_quantile)
    print("threshold:",threshold)
    print("ratio check:",check_percentile(data,data_size,target_col,threshold))

# COMMAND ----------

object_quantile_list=[0.05,0.1,0.15,0.2]

# COMMAND ----------

# MAGIC %md
# MAGIC ### co-count >=3

# COMMAND ----------

# confidence
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over2,total2,"confidence",target_quantile)
    print("")

# COMMAND ----------

# lift_log
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over2,total2,"lift_log",target_quantile)
    print("")

# COMMAND ----------

# lift_log10
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over2,total2,"lift_log10",target_quantile)
    print("")

# COMMAND ----------

# jaccard
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over2,total2,"jaccard",target_quantile)
    print("")

# COMMAND ----------

# MAGIC %md
# MAGIC ### co-count >=4

# COMMAND ----------

# confidence
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over3,total,"confidence",target_quantile)
    print("")

# COMMAND ----------

# confidence2
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over3,total,"confidence2",target_quantile)
    print("")

# COMMAND ----------

# lift
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over3,total,"lift",target_quantile)
    print("")

# COMMAND ----------

# lift_log
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over3,total,"lift_log",target_quantile)
    print("")

# COMMAND ----------

# lift_log10
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over3,total,"lift_log10",target_quantile)
    print("")

# COMMAND ----------

# jaccard
for target_quantile in object_quantile_list:
    print(f"result of {int(target_quantile*100)}%:")
    calc_check(data_over3,total,"jaccard",target_quantile)
    print("")

# COMMAND ----------


