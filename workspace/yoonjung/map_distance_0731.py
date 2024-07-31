# Databricks notebook source
# MAGIC %md
# MAGIC ## 테이블 불러오기 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from hive_metastore.test.california_cocount_final6;

# COMMAND ----------

# MAGIC %md
# MAGIC ##co-count 테이블에 위도, 경도 정보 join

# COMMAND ----------

test1=spark.sql(f"SELECT gmap_id, latitude, longitude FROM hive_metastore.test.california_meta_lanlon_changed")

test2=spark.sql(f"SELECT * FROM hive_metastore.test.california_cocount_final6")

test3 = test2.join(test1.withColumnRenamed("gmap_id", "gmap_id1")
                             .withColumnRenamed("Latitude", "lat1")
                             .withColumnRenamed("Longitude", "lon1"),
                       on="gmap_id1", how="inner") \
                 .join(test1.withColumnRenamed("gmap_id", "gmap_id2")
                             .withColumnRenamed("Latitude", "lat2")
                             .withColumnRenamed("Longitude", "lon2"),
                       on="gmap_id2", how="inner")

display(test3)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from hive_metastore.test.california_meta_lanlon_changed
# MAGIC where gmap_id = "0x80c29ba0e8d07a7f:0x790bc0a814ab5195";

# COMMAND ----------

# MAGIC %md
# MAGIC ### gmap_id1, gmap_id2 열 순서 재배치 

# COMMAND ----------

from pyspark.sql.functions import col

# test3 DataFrame의 열 이름 목록을 구하고, gmap_id1과 gmap_id2의 순서를 변경합니다.
# 여기서는 예시로 몇 가지 열 이름만 나열했지만, 실제 열 이름을 사용해야 합니다.
column_order = ["gmap_id1", "gmap_id2", "co_count", "gmap_count1", "gmap_count2", "year_quarter", "year", "quarter", "support", "confidence", "confidence2", "lift", "lift_log", "lift_log10", "jaccard", "lat1", "lon1", "lat2", "lon2"]  # 필요한 열들을 적절히 추가/재배열
# 열 순서를 변경합니다.
test3_reordered = test3.select([col(c) for c in column_order])

# 변경된 순서로 데이터를 출력합니다.
display(test3_reordered)

# COMMAND ----------

# 조건에 맞는 데이터를 필터링하여 확인
filtered_df = test3_reordered.filter(col("gmap_id1") == "0x80d953dbffb2491d:0xf4bbd0e491709ae0")
display(filtered_df)

# COMMAND ----------

test3_reordered.write.saveAsTable('hive_metastore.test.cali_cocount_distance_all_final')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select count(*) from hive_metastore.test.cali_cocount_distance_all_final;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select count(*) from hive_metastore.test.california_cocount_final6;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 거리계산

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf
import pandas as pd
import math

class LatLongCalc():
    def cal_lat_log_dist(self, df, lat1, lon1, lat2, lon2):
        @pandas_udf(DoubleType())
        def vincenty_distance(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
            def calc_distance(lat1, lon1, lat2, lon2):
                a = 6378137.0
                f = 1 / 298.257223563
                b = (1 - f) * a

                lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
                L = lon2 - lon1
                U1 = math.atan((1 - f) * math.tan(lat1))
                U2 = math.atan((1 - f) * math.tan(lat2))
                sinU1 = math.sin(U1)
                cosU1 = math.cos(U1)
                sinU2 = math.sin(U2)
                cosU2 = math.cos(U2)

                lambda_ = L
                for _ in range(1000):
                    sinLambda = math.sin(lambda_)
                    cosLambda = math.cos(lambda_)
                    sinSigma = math.sqrt((cosU2 * sinLambda) ** 2 +
                                         (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) ** 2)
                    if sinSigma == 0:  # Points are coincident
                        return 0.0
                    cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda
                    sigma = math.atan2(sinSigma, cosSigma)
                    sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma
                    cos2Alpha = 1 - sinAlpha ** 2
                    if cos2Alpha == 0:  # Points are on the equatorial line
                        cos2SigmaM = 0
                    else:
                        cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cos2Alpha
                    C = f / 16 * cos2Alpha * (4 + f * (4 - 3 * cos2Alpha))
                    lambdaPrev = lambda_
                    lambda_ = L + (1 - C) * f * sinAlpha * (
                            sigma + C * sinSigma * (
                            cos2SigmaM + C * cosSigma * (
                            -1 + 2 * cos2SigmaM ** 2)))

                    if abs(lambda_ - lambdaPrev) < 1e-12:
                        break
                else:
                    return None

                u2 = cos2Alpha * (a ** 2 - b ** 2) / (b ** 2)
                A = 1 + u2 / 16384 * (4096 + u2 * (-768 + u2 * (320 - 175 * u2)))
                B = u2 / 1024 * (256 + u2 * (-128 + u2 * (74 - 47 * u2)))
                deltaSigma = B * sinSigma * (
                        cos2SigmaM + B / 4 * (
                        cosSigma * (-1 + 2 * cos2SigmaM ** 2) -
                        B / 6 * cos2SigmaM * (-3 + 4 * sinSigma ** 2) *
                        (-3 + 4 * cos2SigmaM ** 2)))

                s = b * A * (sigma - deltaSigma)
                return round(s / 1000, 4)

            distances = [calc_distance(lat1[i], lon1[i], lat2[i], lon2[i]) for i in range(len(lat1))]
            return pd.Series(distances)

        df = df.withColumn('distance_in_kms', 
                           vincenty_distance(F.col(lat1), F.col(lon1), F.col(lat2), F.col(lon2)))
        return df
    

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Distance Calculation").getOrCreate()

    # Read the existing table into a DataFrame
    cali_cocount = spark.read.table('hive_metastore.test.cali_cocount_distance_all_final')

    # Create an instance of the LatLongCalc class
    calc = LatLongCalc()
    
    # Calculate the distance between the coordinates
    df = calc.cal_lat_log_dist(cali_cocount, "lat1", "lon1", "lat2", "lon2")

    # Show the results
    df.show()

# COMMAND ----------

# 서버리스 사용하여 저장
df.write.mode("overwrite").saveAsTable("hive_metastore.test.cali_cocount_distance_all_result_final_4")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 거리가 1000km 이상인 것들 데이터 확인 
# MAGIC select * from hive_metastore.test.cali_cocount_distance_all_result_final_3
# MAGIC where distance_in_kms >= 1000; 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 이상치 자르기
# MAGIC - 캘리포니아 전체 거리 1391km -> 1391km 이상 인 것은 이상치로 판단 
# MAGIC - 이상치 기준 잡기
# MAGIC - 이상치 값은 null 값으로 변경 

# COMMAND ----------

# 캘리포니아 거리 계산 

# 북서쪽 : 위 42.0  경 -124.4
# 남동쪽 : 위 32.5 경 -114.1333
1391.1363

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf
import pandas as pd
import math

class LatLongCalc():
    def cal_lat_log_dist(self, df, lat1, long1, lat2, long2):
        # Define a Pandas UDF to calculate geodesic distance using Vincenty formula
        @pandas_udf(DoubleType())
        def vincenty_distance(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
            def calc_distance(lat1, lon1, lat2, lon2):
                # Constants for WGS-84
                a = 6378137.0  # major axis
                f = 1 / 298.257223563  # flattening
                b = (1 - f) * a  # minor axis

                # Convert degrees to radians
                lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

                L = lon2 - lon1
                U1 = math.atan((1 - f) * math.tan(lat1))
                U2 = math.atan((1 - f) * math.tan(lat2))
                sinU1 = math.sin(U1)
                cosU1 = math.cos(U1)
                sinU2 = math.sin(U2)
                cosU2 = math.cos(U2)

                lambda_ = L
                for _ in range(1000):  # iteration limit to avoid infinite loop
                    sinLambda = math.sin(lambda_)
                    cosLambda = math.cos(lambda_)
                    sinSigma = math.sqrt((cosU2 * sinLambda) ** 2 +
                                         (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) ** 2)
                    cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda
                    sigma = math.atan2(sinSigma, cosSigma)
                    sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma
                    cos2Alpha = 1 - sinAlpha ** 2
                    cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cos2Alpha
                    C = f / 16 * cos2Alpha * (4 + f * (4 - 3 * cos2Alpha))
                    lambdaPrev = lambda_
                    lambda_ = L + (1 - C) * f * sinAlpha * (
                            sigma + C * sinSigma * (
                            cos2SigmaM + C * cosSigma * (
                            -1 + 2 * cos2SigmaM ** 2)))

                    # Break if change in lambda is insignificant
                    if abs(lambda_ - lambdaPrev) < 1e-12:
                        break
                else:
                    return None  # formula failed to converge

                u2 = cos2Alpha * (a ** 2 - b ** 2) / (b ** 2)
                A = 1 + u2 / 16384 * (4096 + u2 * (-768 + u2 * (320 - 175 * u2)))
                B = u2 / 1024 * (256 + u2 * (-128 + u2 * (74 - 47 * u2)))
                deltaSigma = B * sinSigma * (
                        cos2SigmaM + B / 4 * (
                        cosSigma * (-1 + 2 * cos2SigmaM ** 2) -
                        B / 6 * cos2SigmaM * (-3 + 4 * sinSigma ** 2) *
                        (-3 + 4 * cos2SigmaM ** 2)))

                s = b * A * (sigma - deltaSigma)
                return round(s / 1000, 4)  # distance in kilometers

            distances = [calc_distance(lat1[i], lon1[i], lat2[i], lon2[i]) for i in range(len(lat1))]
            return pd.Series(distances)

        # Apply the Pandas UDF to the dataframe
        df = df.withColumn('distance_in_kms', 
                           vincenty_distance(F.col(lat1), F.col(long1), F.col(lat2), F.col(long2)))
        return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Distance Calculation").getOrCreate()

    # Create a DataFrame with the coordinates
    data = [("A", 42.0, -124.4, "B", 32.5, -114.1333)]

    df = spark.createDataFrame(data, ["coord1", "lat1", "long1", "coord2", "lat2", "long2"])

    # Calculate the distance between the coordinates
    calc = LatLongCalc()
    df = calc.cal_lat_log_dist(df, "lat1", "long1", "lat2", "long2")

    # Show the results
    df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 거리계산 분포 확인_시도 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 거리계산별 count
# MAGIC
# MAGIC select distance_in_kms, count(*) as cnt from hive_metastore.test.cali_cocount_distance_all_result_final
# MAGIC group by distance_in_kms
# MAGIC order by cnt desc; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- distance_in_kms를 범위로 나눠서 각 범위 내의 개수를 카운트하는 쿼리
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE
# MAGIC         WHEN distance_in_kms >= 0 AND distance_in_kms < 5 THEN '0-5'
# MAGIC         WHEN distance_in_kms >= 5 AND distance_in_kms < 10 THEN '5-10'
# MAGIC         WHEN distance_in_kms >= 10 AND distance_in_kms < 50 THEN '10-50'
# MAGIC         WHEN distance_in_kms >= 50 AND distance_in_kms < 100 THEN '50-100'
# MAGIC         WHEN distance_in_kms >= 100 AND distance_in_kms < 500 THEN '100-500'
# MAGIC         WHEN distance_in_kms >= 500 AND distance_in_kms < 1000 THEN '500-1000'
# MAGIC         WHEN distance_in_kms >= 1000 AND distance_in_kms < 5000 THEN '1000-5000'
# MAGIC         ELSE '5000+'
# MAGIC     END AS distance_range,
# MAGIC     COUNT(*) AS count
# MAGIC FROM 
# MAGIC     hive_metastore.test.cali_cocount_distance_all_result_final
# MAGIC GROUP BY 
# MAGIC     CASE
# MAGIC         WHEN distance_in_kms >= 0 AND distance_in_kms < 5 THEN '0-5'
# MAGIC         WHEN distance_in_kms >= 5 AND distance_in_kms < 10 THEN '5-10'
# MAGIC         WHEN distance_in_kms >= 10 AND distance_in_kms < 50 THEN '10-50'
# MAGIC         WHEN distance_in_kms >= 50 AND distance_in_kms < 100 THEN '50-100'
# MAGIC         WHEN distance_in_kms >= 100 AND distance_in_kms < 500 THEN '100-500'
# MAGIC         WHEN distance_in_kms >= 500 AND distance_in_kms < 1000 THEN '500-1000'
# MAGIC         WHEN distance_in_kms >= 1000 AND distance_in_kms < 5000 THEN '1000-5000'
# MAGIC         ELSE '5000+'
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(distance_in_kms) AS min_distance,
# MAGIC     MAX(distance_in_kms) AS max_distance,
# MAGIC     AVG(distance_in_kms) AS avg_distance,
# MAGIC     PERCENTILE(distance_in_kms, 0.5) AS median_distance,
# MAGIC     STDDEV(distance_in_kms) AS stddev_distance
# MAGIC FROM
# MAGIC     hive_metastore.test.cali_cocount_distance_all_result_final;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count

# SparkSession 생성 및 Hive 연결
spark = SparkSession.builder \
    .appName("HiveDataLoad") \
    .enableHiveSupport() \
    .getOrCreate()

# Hive 테이블 로드
df = spark.sql("SELECT distance_in_kms FROM hive_metastore.test.cali_cocount_distance_all_result_final")

# 각 범위에 해당하는 조건을 사용해 그룹화 컬럼 추가
df_grouped = df.withColumn(
    "distance_group",
    when(col("distance_in_kms") < 5, "0-5")
    .when((col("distance_in_kms") >= 5) & (col("distance_in_kms") < 10), "5-10")
    .when((col("distance_in_kms") >= 10) & (col("distance_in_kms") < 50), "10-50")
    .when((col("distance_in_kms") >= 50) & (col("distance_in_kms") < 100), "50-100")
    .when((col("distance_in_kms") >= 100) & (col("distance_in_kms") < 500), "100-500")
    .when((col("distance_in_kms") >= 500) & (col("distance_in_kms") < 1000), "500-1000")
    .when((col("distance_in_kms") >= 1000) & (col("distance_in_kms") < 5000), "1000-5000")
    .otherwise("5000+")
)

# 그룹별 데이터 개수 집계
grouped_counts = df_grouped.groupBy("distance_group").agg(count("*").alias("count"))

# 결과를 Pandas DataFrame으로 변환
pandas_grouped_counts = grouped_counts.toPandas()


# COMMAND ----------

# Pandas DataFrame 데이터 확인
print(pandas_grouped_counts)

# COMMAND ----------

import matplotlib.pyplot as plt

# Pandas DataFrame에서 distance_in_kms 값을 추출하여 Boxplot 생성
plt.figure(figsize=(10, 6))
plt.boxplot(pandas_grouped_counts['count'])
plt.title('Boxplot of Distance Group Counts')
plt.ylabel('Count')
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Bar Plot 생성
plt.figure(figsize=(10, 6))
plt.bar(pandas_grouped_counts['distance_group'], pandas_grouped_counts['count'])
plt.title('Distribution of Distance Groups')
plt.xlabel('Distance Range (KMs)')
plt.ylabel('Count')
plt.xticks(rotation=45)  # x축 라벨을 회전하여 가독성 향상
plt.grid(axis='y')  # y축 기준으로 그리드 추가
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# 지정된 순서대로 x축을 구성하기 위한 순서 리스트
order = ["0-5", "5-10", "10-50", "50-100", "100-500", "500-1000", "1000-5000"]

# distance_group 컬럼을 Categorical 타입으로 변환하고, 순서를 지정
pandas_grouped_counts['distance_group'] = pd.Categorical(
    pandas_grouped_counts['distance_group'], 
    categories=order, 
    ordered=True
)

# distance_group을 기준으로 DataFrame 정렬
pandas_grouped_counts = pandas_grouped_counts.sort_values('distance_group')

# Bar Plot 생성
plt.figure(figsize=(10, 6))
plt.bar(pandas_grouped_counts['distance_group'], pandas_grouped_counts['count'])
plt.title('Distribution of Distance Groups')
plt.xlabel('Distance Range (KMs)')
plt.ylabel('Count')
plt.xticks(rotation=45)  # x축 라벨을 회전하여 가독성 향상
plt.grid(axis='y')  # y축 기준으로 그리드 추가
plt.show()


# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 캘리포니아 거리보다 큰 거리 건 count
# MAGIC
# MAGIC select * from hive_metastore.test.cali_cocount_distance_all_result_final
# MAGIC where distance_in_kms >= 1391.1363 ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 거리 3000km 이상 건 거리 및 장소 확인
# MAGIC
# MAGIC select * from hive_metastore.test.cali_cocount_distance_all_result_final
# MAGIC where distance_in_kms >= 3000 ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 거리계산 분포 확인_시도2
# MAGIC - 이상치 부분들의 장소 위도/경도 확인 및 위도/경도 재추출
# MAGIC - 위도/경도 재추출 (Geocode활용) : https://docs.google.com/spreadsheets/d/1o3yzlcWof4eU5-V-JBOR0cAB9lPT7HgUdD0Xq5ZyFpQ/edit?usp=sharing
# MAGIC - 캘리포니아 meta_data에서 위/경도 수정하여 메타데이터 테이블 생성 (hive_metastore.test.california_meta_lanlon_changed) 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 거리계산별 count
# MAGIC
# MAGIC select distance_in_kms, count(*) as cnt from hive_metastore.test.cali_cocount_distance_all_result_final_2
# MAGIC group by distance_in_kms
# MAGIC order by cnt desc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- distance_in_kms를 범위로 나눠서 각 범위 내의 개수를 카운트하는 쿼리
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE
# MAGIC         WHEN distance_in_kms >= 0 AND distance_in_kms < 5 THEN '0-5'
# MAGIC         WHEN distance_in_kms >= 5 AND distance_in_kms < 10 THEN '5-10'
# MAGIC         WHEN distance_in_kms >= 10 AND distance_in_kms < 50 THEN '10-50'
# MAGIC         WHEN distance_in_kms >= 50 AND distance_in_kms < 100 THEN '50-100'
# MAGIC         WHEN distance_in_kms >= 100 AND distance_in_kms < 500 THEN '100-500'
# MAGIC         WHEN distance_in_kms >= 500 AND distance_in_kms < 1000 THEN '500-1000'
# MAGIC         WHEN distance_in_kms >= 1000 AND distance_in_kms < 5000 THEN '1000-5000'
# MAGIC         ELSE '5000+'
# MAGIC     END AS distance_range,
# MAGIC     COUNT(*) AS count
# MAGIC FROM 
# MAGIC     hive_metastore.test.cali_cocount_distance_all_result_final_2
# MAGIC GROUP BY 
# MAGIC     CASE
# MAGIC         WHEN distance_in_kms >= 0 AND distance_in_kms < 5 THEN '0-5'
# MAGIC         WHEN distance_in_kms >= 5 AND distance_in_kms < 10 THEN '5-10'
# MAGIC         WHEN distance_in_kms >= 10 AND distance_in_kms < 50 THEN '10-50'
# MAGIC         WHEN distance_in_kms >= 50 AND distance_in_kms < 100 THEN '50-100'
# MAGIC         WHEN distance_in_kms >= 100 AND distance_in_kms < 500 THEN '100-500'
# MAGIC         WHEN distance_in_kms >= 500 AND distance_in_kms < 1000 THEN '500-1000'
# MAGIC         WHEN distance_in_kms >= 1000 AND distance_in_kms < 5000 THEN '1000-5000'
# MAGIC         ELSE '5000+'
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(distance_in_kms) AS min_distance,
# MAGIC     MAX(distance_in_kms) AS max_distance,
# MAGIC     AVG(distance_in_kms) AS avg_distance,
# MAGIC     PERCENTILE(distance_in_kms, 0.5) AS median_distance,
# MAGIC     STDDEV(distance_in_kms) AS stddev_distance
# MAGIC FROM
# MAGIC     hive_metastore.test.cali_cocount_distance_all_result_final_2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 캘리포니아 거리보다 큰 거리 건 count
# MAGIC
# MAGIC select * from hive_metastore.test.cali_cocount_distance_all_result_final_2
# MAGIC where distance_in_kms >= 1391.1363 ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 캘리포니아 거리보다 큰 거리 건 count
# MAGIC
# MAGIC select count(*) from hive_metastore.test.cali_cocount_distance_all_result_final_2
# MAGIC where distance_in_kms >= 1391.1363 ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 거리계산 분포 확인_시도_마지막 

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC -- 거리계산별 count
# MAGIC
# MAGIC select distance_in_kms, count(*) as cnt from hive_metastore.test.cali_cocount_distance_all_result_final_4
# MAGIC group by distance_in_kms
# MAGIC order by cnt desc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- distance_in_kms를 범위로 나눠서 각 범위 내의 개수를 카운트하는 쿼리
# MAGIC
# MAGIC SELECT 
# MAGIC     CASE
# MAGIC         WHEN distance_in_kms >= 0 AND distance_in_kms < 5 THEN '0-5'
# MAGIC         WHEN distance_in_kms >= 5 AND distance_in_kms < 10 THEN '5-10'
# MAGIC         WHEN distance_in_kms >= 10 AND distance_in_kms < 50 THEN '10-50'
# MAGIC         WHEN distance_in_kms >= 50 AND distance_in_kms < 100 THEN '50-100'
# MAGIC         WHEN distance_in_kms >= 100 AND distance_in_kms < 500 THEN '100-500'
# MAGIC         WHEN distance_in_kms >= 500 AND distance_in_kms < 1000 THEN '500-1000'
# MAGIC         WHEN distance_in_kms >= 1000 AND distance_in_kms < 3000 THEN '1000-3000'
# MAGIC         WHEN distance_in_kms >= 3000 AND distance_in_kms < 5000 THEN '3000-5000'        
# MAGIC         ELSE '5000+'
# MAGIC     END AS distance_range,
# MAGIC     COUNT(*) AS count
# MAGIC FROM 
# MAGIC     hive_metastore.test.cali_cocount_distance_all_result_final_4
# MAGIC GROUP BY 
# MAGIC     CASE
# MAGIC         WHEN distance_in_kms >= 0 AND distance_in_kms < 5 THEN '0-5'
# MAGIC         WHEN distance_in_kms >= 5 AND distance_in_kms < 10 THEN '5-10'
# MAGIC         WHEN distance_in_kms >= 10 AND distance_in_kms < 50 THEN '10-50'
# MAGIC         WHEN distance_in_kms >= 50 AND distance_in_kms < 100 THEN '50-100'
# MAGIC         WHEN distance_in_kms >= 100 AND distance_in_kms < 500 THEN '100-500'
# MAGIC         WHEN distance_in_kms >= 500 AND distance_in_kms < 1000 THEN '500-1000'
# MAGIC         WHEN distance_in_kms >= 1000 AND distance_in_kms < 3000 THEN '1000-3000'
# MAGIC         WHEN distance_in_kms >= 3000 AND distance_in_kms < 5000 THEN '3000-5000'
# MAGIC         ELSE '5000+'
# MAGIC     END;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count

# SparkSession 생성 및 Hive 연결
spark = SparkSession.builder \
    .appName("HiveDataLoad") \
    .enableHiveSupport() \
    .getOrCreate()

# Hive 테이블 로드
df = spark.sql("SELECT distance_in_kms FROM hive_metastore.test.cali_cocount_distance_all_result_final_4")

# 각 범위에 해당하는 조건을 사용해 그룹화 컬럼 추가
df_grouped = df.withColumn(
    "distance_group",
    when(col("distance_in_kms") < 5, "0-5")
    .when((col("distance_in_kms") >= 5) & (col("distance_in_kms") < 10), "5-10")
    .when((col("distance_in_kms") >= 10) & (col("distance_in_kms") < 50), "10-50")
    .when((col("distance_in_kms") >= 50) & (col("distance_in_kms") < 100), "50-100")
    .when((col("distance_in_kms") >= 100) & (col("distance_in_kms") < 500), "100-500")
    .when((col("distance_in_kms") >= 500) & (col("distance_in_kms") < 1000), "500-1000")
    .when((col("distance_in_kms") >= 1000) & (col("distance_in_kms") < 5000), "1000-5000")
    .otherwise("5000+")
)

# 그룹별 데이터 개수 집계
grouped_counts = df_grouped.groupBy("distance_group").agg(count("*").alias("count"))

# 결과를 Pandas DataFrame으로 변환
pandas_grouped_counts = grouped_counts.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt

# Pandas DataFrame에서 distance_in_kms 값을 추출하여 Boxplot 생성
plt.figure(figsize=(10, 6))
plt.boxplot(pandas_grouped_counts['count'])
plt.title('Boxplot of Distance Group Counts')
plt.ylabel('Count')
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# 지정된 순서대로 x축을 구성하기 위한 순서 리스트
order = ["0-5", "5-10", "10-50", "50-100", "100-500", "500-1000", "1000-5000"]

# distance_group 컬럼을 Categorical 타입으로 변환하고, 순서를 지정
pandas_grouped_counts['distance_group'] = pd.Categorical(
    pandas_grouped_counts['distance_group'], 
    categories=order, 
    ordered=True
)

# distance_group을 기준으로 DataFrame 정렬
pandas_grouped_counts = pandas_grouped_counts.sort_values('distance_group')

# Bar Plot 생성
plt.figure(figsize=(10, 6))
plt.bar(pandas_grouped_counts['distance_group'], pandas_grouped_counts['count'])
plt.title('Distribution of Distance Groups')
plt.xlabel('Distance Range (KMs)')
plt.ylabel('Count')
plt.xticks(rotation=45)  # x축 라벨을 회전하여 가독성 향상
plt.grid(axis='y')  # y축 기준으로 그리드 추가
plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(distance_in_kms) AS min_distance,
# MAGIC     MAX(distance_in_kms) AS max_distance,
# MAGIC     AVG(distance_in_kms) AS avg_distance,
# MAGIC     PERCENTILE(distance_in_kms, 0.5) AS median_distance,
# MAGIC     STDDEV(distance_in_kms) AS stddev_distance
# MAGIC FROM
# MAGIC     hive_metastore.test.cali_cocount_distance_all_result_final_4;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 캘리포니아 거리보다 큰 거리 건 
# MAGIC
# MAGIC select * from hive_metastore.test.cali_cocount_distance_all_result_final_4
# MAGIC where distance_in_kms >= 1391 ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- 캘리포니아 거리보다 큰 거리 건 count
# MAGIC
# MAGIC select count(*) from hive_metastore.test.cali_cocount_distance_all_result_final_4
# MAGIC where distance_in_kms >= 1391.1363 ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 이상치 2건은 null로 변경

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Nullify Distances") \
    .enableHiveSupport() \
    .getOrCreate()

# Hive 테이블 로드
df = spark.table("hive_metastore.test.cali_cocount_distance_all_result_final_4")

# distance_in_kms 컬럼에서 특정 값들을 null로 변경
updated_df = df.withColumn("distance_in_kms",
                           when(col("distance_in_kms").isin(1668.3167, 1785.3869), None)
                           .otherwise(col("distance_in_kms")))

# 변경된 데이터 확인 (선택적)

updated_df.filter(col("distance_in_kms").isNull()).show()


# COMMAND ----------

a = updated_df.filter(col("distance_in_kms").isNull())
display(a)

# COMMAND ----------

# 변경된 DataFrame을 Hive 테이블에 저장
updated_df.write.mode("overwrite").saveAsTable("hive_metastore.test.cali_cocount_distance_all_result_final_5")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select count(*) from hive_metastore.test.cali_cocount_distance_all_result_final_5; 

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
