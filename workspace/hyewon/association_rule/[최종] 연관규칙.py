# Databricks notebook source
from pyspark.sql.functions import *

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

target_region="California" # 원하는 지역으로 변경 , California

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. 전체 데이터 전처리 및 co-count 테이블 생성

# COMMAND ----------

# MAGIC %md
# MAGIC #### 0-1) ver1(전체 데이터), ver2(동일 연/월), ver3(동일 분기), ver4(동일 연-분기)

# COMMAND ----------

#전체 데이터 로드
data=spark.sql(f"SELECT gmap_id, user_id,rating, region,time,state FROM hive_metastore.test.joined_review_data WHERE region='{target_region}'")
data.createOrReplaceTempView("data")
display(data)

# COMMAND ----------

data=data.fillna({"state":"N/A"})

# COMMAND ----------

# 폐점 데이터 날리기 전
data.count()

# COMMAND ----------

data.filter(col("state")!="Permanently closed").count()

# COMMAND ----------

data=data.filter(col("state")!="Permanently closed")

# COMMAND ----------

data.count()

# COMMAND ----------

data.count()

# COMMAND ----------

# Year,month 추출
pattern = r"(\d{4})-(\d{2})"

time_extracted_df = data.withColumn(
    "year", 
    regexp_extract(col("time"), pattern, 1)
).withColumn(
    "month", 
    regexp_extract(col("time"), pattern, 2)
)

time_extracted_df.createOrReplaceTempView("time_extracted_df")

# COMMAND ----------

# 분기 추가
quarter_added_df=time_extracted_df.withColumn("quarter",
    when(col("month").between("01","03"),1)
    .when(col("month").between("04","06"),2)
    .when(col("month").between("07","09"),3)
    .when(col("month").between("10","12"),4))

quarter_added_df.createOrReplaceTempView("quarter_added_df")

display(quarter_added_df)

# COMMAND ----------

# 연-분기 컬럼 추가
year_quarter_added_df=quarter_added_df.withColumn("year_quarter",concat(col("year"),lit("-"),col("quarter")))

year_quarter_added_df.createOrReplaceTempView("year_quarter_added_df")

display(year_quarter_added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 0-2)계산시 필요한 temp table view 생성

# COMMAND ----------

# 원하는 데이터로 변경
data= year_quarter_added_df #data(얘는 셀 실행 불필요),time_extracted_df, quarter_added_df, year_quarter_added_df

# COMMAND ----------

# n : 전체 transaction 수 
df_unique=data.dropDuplicates(["gmap_id","user_id"])
n=df_unique.count()
print(n)

# df_unique를 뷰로 등록
df_unique.createOrReplaceTempView("df_unique")


# COMMAND ----------

# MAGIC %md
# MAGIC co_df 생성: gmap_id 는 다르고, user_id는 동일한 데이터 join<br>
# MAGIC - ver1: 아무런 필터링 없는 전체
# MAGIC - ver2: 동일 연/월 로 필터링
# MAGIC - ver3: 동일 분기로 필터링
# MAGIC - ver4: 동인 연/분기로 필터링

# COMMAND ----------

# ver1: 아무런 제약 없는 전체 데이터 실행용
def create_co_df(df):
    co_df = df.alias("a").join(
        df.alias("b"),
        (col("a.user_id") == col("b.user_id")) & 
        (col("a.gmap_id") != col("b.gmap_id"))
    ).select(
        col("a.gmap_id").alias("gmap_id1"),
        col("b.gmap_id").alias("gmap_id2"),
        col("a.user_id")
    )

    # 임시 뷰 생성
    co_df.createOrReplaceTempView("co_df")
    display(co_df)

    return co_df

# COMMAND ----------

# ver2: 동일 연/월로 필터링 된 데이터
def create_co_df_year_month(df):
    co_df = df.alias("a").join(
        df.alias("b"),
        (col("a.user_id") == col("b.user_id")) & 
        (col("a.gmap_id") != col("b.gmap_id")) &
        (col("a.year") == col("b.year")) &
        (col("a.month") == col("b.month"))
    ).select(
        col("a.gmap_id").alias("gmap_id1"),
        col("b.gmap_id").alias("gmap_id2"),
        col("a.user_id")
    )

    # 임시 뷰 생성
    co_df.createOrReplaceTempView("co_df")
    display(co_df)

    return co_df

# COMMAND ----------

# ver3: 동일 분기로 필터링 된 데이터
def create_co_df_quarter(df):
    co_df = df.alias("a").join(
        df.alias("b"),
        (col("a.user_id") == col("b.user_id")) & 
        (col("a.gmap_id") != col("b.gmap_id")) &
        (col("a.quarter") == col("b.quarter"))
    ).select(
        col("a.gmap_id").alias("gmap_id1"),
        col("b.gmap_id").alias("gmap_id2"),
        col("a.user_id")
    )

    # 임시 뷰 생성
    co_df.createOrReplaceTempView("co_df")
    display(co_df)

    return co_df

# COMMAND ----------

# ver4: 동일 연-분기로 필터링 된 데이터
def create_co_df_year_quarter(df):
    co_df = df.alias("a").join(
        df.alias("b"),
        (col("a.user_id") == col("b.user_id")) & 
        (col("a.gmap_id") != col("b.gmap_id")) &
        (col("a.year_quarter") == col("b.year_quarter"))
    ).select(
        # 정렬 조건 추가해서 중복 제거
        least(col("a.gmap_id"), col("b.gmap_id")).alias("gmap_id1"),
        greatest(col("a.gmap_id"), col("b.gmap_id")).alias("gmap_id2"),
        col("a.user_id"),
        col("a.rating").alias("rating1"),
        col("b.rating").alias("rating2"),
        col("a.year"),
        col("a.quarter"),
        col("a.year_quarter")
    ).distinct()

    # 임시 뷰 생성
    co_df.createOrReplaceTempView("co_df")
    display(co_df)

    return co_df

# COMMAND ----------

# 사용데이터에 맞는 함수 호출

# co_df=create_co_df(data)
# co_df=create_co_df_year_month(time_extracted_df)
# co_df=create_co_df_quarter(quarter_added_df)
co_df=create_co_df_year_quarter(year_quarter_added_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 0-3) co_count 계산

# COMMAND ----------

# n(XUY) : year-quarter로 세분화 (distinct user_id)
co_count_distinct=co_df.groupBy("gmap_id1","gmap_id2","year_quarter").agg(countDistinct("user_id").alias("co_count"))

print(co_count_distinct.count())
display(co_count_distinct)
# co_count.write.mode("overwrite").saveAsTable(f"hive_metastore.test.{target_region}_cocount{mode}")

# COMMAND ----------

# n(XUY) : year-quarter로 세분화 (distinct user_id) + year, quarter 컬럼 추가
co_count_distinct=co_df.groupBy("gmap_id1","gmap_id2","year_quarter").agg(countDistinct("user_id").alias("co_count"))
co_count_distinct=co_count_distinct.join(co_df.select("gmap_id1", "gmap_id2","year_quarter", "year", "quarter").distinct(),
    on=["gmap_id1", "gmap_id2", "year_quarter"],
    how="left")
print(co_count_distinct.count())
display(co_count_distinct)

# COMMAND ----------

# rating1과 rating2가 동일할 때의 집계
rating_equals_counts = co_df.filter(col("rating1") == col("rating2")).groupBy("gmap_id1", "gmap_id2", "year_quarter").agg(
    countDistinct(when(col("rating1") == 1, col("user_id"))).alias("equal_rating_count_1"),
    countDistinct(when(col("rating1") == 2, col("user_id"))).alias("equal_rating_count_2"),
    countDistinct(when(col("rating1") == 3, col("user_id"))).alias("equal_rating_count_3"),
    countDistinct(when(col("rating1") == 4, col("user_id"))).alias("equal_rating_count_4"),
    countDistinct(when(col("rating1") == 5, col("user_id"))).alias("equal_rating_count_5")
)

# print(rating_equals_counts.count())
# display(rating_equals_counts)

# 두 집계 결과 병합
final_co = co_count_distinct.join(
    rating_equals_counts,
    on=["gmap_id1", "gmap_id2", "year_quarter"],
    how="left"
)
display(final_co)

# COMMAND ----------

# co-rating 제외시 실행
final_co=co_count_distinct

# COMMAND ----------

# MAGIC %md
# MAGIC #### 0-4) co-count 분포 확인

# COMMAND ----------

def get_co_distribution(co_count,threshold):
    temp_co=co_count.filter(col("co_count")>threshold)
    print(temp_co.count())
    aggregated_co_df=temp_co.groupby("co_count").agg(count("*").alias("frequency")).orderBy("co_count")
    display(aggregated_co_df)

# COMMAND ----------

# co-count > 1로 필터링
get_co_distribution(co_count_distinct,threshold=1)

# COMMAND ----------

# co-count > 5로 필터링
get_co_distribution(co_count_distinct,threshold=5)

# COMMAND ----------

# co-count > 10로 필터링
get_co_distribution(co_count_distinct,threshold=10)

# COMMAND ----------

final_cocount.write.mode("overwrite").saveAsTable(f"hive_metastore.test.{target_region}_cocount")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. 지표값 계산

# COMMAND ----------

# MAGIC %md
# MAGIC #####1)support
# MAGIC :(n(XUY)/N)
# MAGIC
# MAGIC #####2)confidence
# MAGIC :  n(XUY) / n(X)
# MAGIC
# MAGIC ##### 3) lift
# MAGIC : ver1. n(X∩Y)/ n(X) * n(Y) or confidence(x->y)/ support(y) <br>
# MAGIC ver2. P(X∩Y)/ P(X) * P(Y) 
# MAGIC
# MAGIC ##### 4) jaccard
# MAGIC : n(X∩Y)/ (n(X) + n(Y)-n(X∩Y))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 지표 계산 통합 버전 1)~4)

# COMMAND ----------

# 전체 테이블과 합치는 코드
def association_rule(co_count):
    # support
    df_support=co_count.withColumn("support",col("co_count")/n)

    # confidence
    
    # n(X): gmap_id 1개당 unique user_id 수
    gmap_count=df_unique.groupBy("gmap_id").agg(countDistinct("user_id").alias("gmap_count"))

    # gamp_count1: n(X), gmap_count2:n(Y)
    df_confidence1=df_support.join(gmap_count.alias("gcnt"),col("gmap_id1")==col("gcnt.gmap_id")).withColumn("confidence",col("co_count")/col("gcnt.gmap_count"))\
    .withColumnRenamed("gmap_count", "gmap_count1")  # gmap_count 컬럼을 gmap_count1으로 변경

    df_confidence2 = df_confidence1.join(gmap_count.alias("gcnt2"), col("gmap_id2") == col("gcnt2.gmap_id")).withColumn("confidence2",col("co_count")/col("gcnt2.gmap_count"))\
    .withColumnRenamed("gmap_count", "gmap_count2")  # gmap_count 컬럼을 gmap_count2으로 변경

    # df_confidence=df_confidence2.select("gmap_id1","gmap_id2","gmap_count1","gmap_count2","co_count","support","confidence")

    # lift
    df_lift=df_confidence2.withColumn("lift",n*col("co_count")/(col("gmap_count1")*col("gmap_count2")))

    df_lift = df_lift.withColumn("lift_log", log("lift"))
    df_lift = df_lift.withColumn("lift_log10", log10("lift"))


    # jaccard
    df_jaccard = df_lift.withColumn("jaccard", when((col("gmap_count1") + col("gmap_count2") - col("co_count")) != 0, col("co_count") / (col("gmap_count1") + col("gmap_count2") - col("co_count"))).otherwise(None))

    df_final=df_jaccard.select("gmap_id1","gmap_id2","co_count","gmap_count1","gmap_count2","year_quarter","year","quarter","support","confidence","confidence2","lift","lift_log","lift_log10","jaccard")

    # df_final=df_jaccard.select("gmap_id1","gmap_id2","co_count","gmap_count1","gmap_count2","year_quarter","year","quarter","equal_rating_count_1","equal_rating_count_2","equal_rating_count_3","equal_rating_count_4","equal_rating_count_5","support","confidence","confidence2","lift","lift_log","lift_log10","jaccard")


    # Null 값을 0으로 채우기
    # columns_to_fill = [
    #     "equal_rating_count_1", 
    #     "equal_rating_count_2", 
    #     "equal_rating_count_3", 
    #     "equal_rating_count_4", 
    #     "equal_rating_count_5"
    # ]

    # 선택한 컬럼에 대해 Null 값을 0으로 채우기
    # df_final = df_final.fillna(0, subset=columns_to_fill)

    display(df_final)

    return df_final


# COMMAND ----------

# 딱 통계량만 뽑는 코드
# def association_rule(df,co_count):
#     # support
#     df_support=co_count.withColumn("support",col("co_count")/n)

#     # confidence
    
#     # n(X): gmap_id 1개당 unique user_id 수
#     gmap_count=df_unique.groupBy("gmap_id").agg(countDistinct("user_id").alias("gmap_count"))

#     # gamp_count1: n(X), gmap_count2:n(Y)
#     df_confidence1=df_support.join(gmap_count.alias("gcnt"),col("gmap_id1")==col("gcnt.gmap_id")).withColumn("confidence1",col("co_count")/col("gcnt.gmap_count"))\
#     .withColumnRenamed("gmap_count", "gmap_count1")  # gmap_count 컬럼을 gmap_count1으로 변경

#     df_confidence2 = df_confidence1.join(gmap_count.alias("gcnt2"), col("gmap_id2") == col("gcnt2.gmap_id")).withColumn("confidence2", col("co_count") / col("gcnt2.gmap_count"))\
#     .withColumnRenamed("gmap_count", "gmap_count2")  # gmap_count 컬럼을 gmap_count2으로 변경

#     df_confidence=df_confidence2.select("gmap_id1","gmap_id2","gmap_count1","gmap_count2","co_count","support","confidence1","confidence2")

#     # lift
#     df_lift=df_confidence.withColumn("lift",col("co_count")/(col("gmap_count1")*col("gmap_count2")))

#     # jaccard
#     df_jaccard = df_lift.withColumn("jaccard", when((col("gmap_count1") + col("gmap_count2") - col("co_count")) != 0, col("co_count") / (col("gmap_count1") + col("gmap_count2") - col("co_count"))).otherwise(None))

#     display(df_jaccard)

#     return df_jaccard


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. 지표 통계값

# COMMAND ----------

# 기본 통계 정보 계산
def get_stats(df_result): 
    # NaN 값을 0으로 처리
    df_result_clean = df_result.fillna(0, subset=["co_count", "support", "confidence", "lift", "jaccard"])
       
    stats = df_result.describe([
        "co_count", "support", "confidence", "lift","lift_log","lift_log10", "jaccard"
    ]).toPandas()

    stats2 = df_result.describe([
       "support", "confidence", "lift","lift_log","lift_log10","jaccard"
    ]).toPandas()

    # 'count' 행을 제외한 나머지 통계량만 선택
    stats2_filtered = stats2[stats2["summary"] != "count"]

    display(stats)
    # display(stats2_filtered)

    return stats2_filtered

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. 최종 결과

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-1) 연관 규칙 계산 결과

# COMMAND ----------

# ver1.lift
final=association_rule(final_co)

# COMMAND ----------

# co_count 개수 유효 확인-> 결과값 0일시 유효
display(final.filter(col("co_count")<col("equal_rating_count_1")+col("equal_rating_count_2")+col("equal_rating_count_3")+col("equal_rating_count_4")+col("equal_rating_count_5")))

# COMMAND ----------

# ver2.lift
final=association_rule(final_co)

# COMMAND ----------

# ver3. gmap_count,confidence2 추가
final3=association_rule(final_co)

# COMMAND ----------

# ver5. 폐점 데이터 삭제, co-rating 삭제
final5=association_rule(final_co)

# COMMAND ----------

final5.count()

# COMMAND ----------

# ver1 최종 데이터 통계량 확인
get_stats(x)

# COMMAND ----------

#ver2 최종 데이터 통계량 확인
get_stats(final)

# COMMAND ----------

# 최종 테이블 저장
final5.write.mode("overwrite").saveAsTable(f"hive_metastore.test.{target_region}_cocount_final6")

# COMMAND ----------

# >1 테이블 저장
final.filter(col("co_count")>1).write.mode("overwrite").saveAsTable(f"hive_metastore.test.{target_region}_cocount_over1")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-2. 연관 규칙 지표값 분포

# COMMAND ----------

#전체 데이터 로드
codata=spark.sql("SELECT support,lift,lift_log,lift_log10,confidence,jaccard,co_count FROM hive_metastore.test.california_cocount_final2")
codata.createOrReplaceTempView("codata")

# COMMAND ----------

stats=get_stats(codata)

# COMMAND ----------

def get_stats_distribution(stats,target):
    aggregated_df=stats.groupby(f"{target}").agg(count("*").alias("frequency")).orderBy(f"{target}")
    display(aggregated_df)

# COMMAND ----------

get_stats_distribution(codata,"support")

# COMMAND ----------

get_stats_distribution(codata,"confidence")

# COMMAND ----------

get_stats_distribution2(codata,"lift")

# COMMAND ----------

get_stats_distribution(codata,"jaccard")

# COMMAND ----------

get_stats_distribution2(codata,"jaccard")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.(더미)다른 지역 실험 결과 모음

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-1) 하와이 전체 데이터

# COMMAND ----------

df_result=association_rule(data) #data, time_extracted_df, quarter_added_df 중 선택
get_stats(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-2) 하와이 동일 연월 필터링

# COMMAND ----------

df_result_month=association_rule(time_extracted_df) #data, time_extracted_df, quarter_added_df 중 선택
get_stats(df_result_month)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-3) 하와이 동일 분기 필터링

# COMMAND ----------

df_result_quarter=association_rule(quarter_added_df) #data, time_extracted_df, quarter_added_df 중 선택
get_stats(df_result_quarter)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) 캘리포니아

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-1) 캘리포니아 전체 데이터

# COMMAND ----------

df_result=association_rule(data) #data, time_extracted_df, quarter_added_df 중 선택
stats=get_stats(df_result)
visualize_stats_log_scale(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-2) 캘리포니아 동일 연월

# COMMAND ----------

df2_result_month=association_rule(time_extracted_df) #data, time_extracted_df, quarter_added_df 중 선택
stats=get_stats(df2_result_month)
visualize_stats_log_scale(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-3) 캘리포니아 동일 분기 필터링

# COMMAND ----------

df2_result_quarter=association_rule(quarter_added_df) #data, time_extracted_df, quarter_added_df 중 선택
stats=get_stats(df2_result_quarter)
visualize_stats_log_scale(stats)
