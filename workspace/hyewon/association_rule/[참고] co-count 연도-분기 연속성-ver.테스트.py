# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pyspark.sql.functions import udf

# COMMAND ----------

#전체 데이터 로드
data=spark.sql("SELECT gmap_id1,gmap_id2,co_count,year_quarter,year,quarter FROM hive_metastore.test.california_cocount_final4")
data.createOrReplaceTempView("data")

# COMMAND ----------

sample=data.limit(10000)

# COMMAND ----------

year_quarters=(sample.groupBy("gmap_id1","gmap_id2").agg(collect_list("year_quarter").alias("year_quarters")))
display(year_quarters)

# COMMAND ----------

# year_quarters의 길이가 1인 경우 삭제
temp=year_quarters.filter(size(col("year_quarters"))>1)
print(temp.count())
display(temp)

# COMMAND ----------

# year_quarters 오름 차순으로 정렬
def sort_list(year_quarters):
    return sorted(year_quarters)

sort_list_udf = udf(sort_list, ArrayType(StringType()))

sorted_data = temp.withColumn("year_quarters", sort_list_udf(col("year_quarters")))
display(sorted_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### a. 분기 연속성 카운트

# COMMAND ----------

#  분기 연속성 카운트
def count_consecutive(year_quarters):
    cnt=0
    prev_year,prev_quarter=map(int,year_quarters[0].split("-"))

    for i in range(1,len(year_quarters)):
        current_year,current_quarter=map(int,year_quarters[i].split("-"))
        
        if current_year==prev_year and current_quarter==prev_quarter+1:
            cnt+=1
        elif current_year==prev_year+1 and current_quarter==1 and prev_quarter==4:
            cnt+=1

        prev_year,prev_quarter=current_year,current_quarter  
         

    return cnt 

# COMMAND ----------

# 분기 연속성 카운트 추가
consecutive_udf=udf(count_consecutive,IntegerType())
consecutive_df=sorted_data.withColumn("consecutive_quarters_cnt",consecutive_udf(col("year_quarters")))
display(consecutive_df)

# COMMAND ----------

# filter 1: 연도-분기의 연속성이 3회 이사인 경우
co_filter1=consecutive_df.filter(col("consecutive_quarters_cnt")>=3)
display(co_filter1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. 연도 연속성 카운트

# COMMAND ----------

# 연도 dict 생성
def count_consecutive_ver2(year_quarters):
    year_dict={}
    prev_year,prev_quarter=map(int,year_quarters[0].split("-"))

    for i in range(1,len(year_quarters)):
        current_year,current_quarter=map(int,year_quarters[i].split("-"))
        
        if (current_year == prev_year and current_quarter == prev_quarter + 1) or \
           (current_year == prev_year + 1 and current_quarter == 1 and prev_quarter == 4):
            if current_year in year_dict:
                year_dict[current_year] += 1
            else:
                year_dict[current_year] = 1

        prev_year,prev_quarter=current_year,current_quarter  
         

    return year_dict 

# COMMAND ----------

count_consecutive_ver2(["2016-1","2016-3","2017-2","2018-1","2018-2","2018-3","2018-4","2019-1","2019-2","2019-3","2020-1","2020-3"])

# COMMAND ----------

# 연도 dict 추가
consecutive2_udf=udf(count_consecutive_ver2,MapType(StringType(),IntegerType()))
consecutive_df2=sorted_data.withColumn("year_dict",consecutive2_udf(col("year_quarters")))
display(consecutive_df2)

# COMMAND ----------

# 연도 연속성 카운트
def count_consecutive_year_ver2(year_dict):
    if not year_dict: return 0
    
    cnt=0
    sorted_years=sorted(map(int,year_dict.keys()))
    
    # 단순히 1개라도 존재하면 카운트
    for i in range(len(years)-1):
        if int(years[i+1])==int(years[i])+1 and year_dict:cnt+=1

    # 2개 이상이면 카운트
    # for i in range(len(sorted_years)-1):
    #     if int(sorted_years[i+1])==int(sorted_years[i])+1 and year_dict[sorted_years[i]>=2]:cnt+=1

    return cnt

# COMMAND ----------

# 연도 연속성 카운트 추가
consecutive2_year_udf=udf(count_consecutive_year_ver2,IntegerType())
consecutive_year_df2=consecutive_df2.withColumn("consecutive_year_cnt",consecutive2_year_udf(col("year_dict")))
display(consecutive_year_df2)

# COMMAND ----------

# filter 2: 연도연속성이 3년 이상인 경우
co_filter2=consecutive_year_df2.filter(col("consecutive_year_cnt")>=3)
display(co_filter2)
