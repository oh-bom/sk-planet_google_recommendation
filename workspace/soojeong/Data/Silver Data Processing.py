# Databricks notebook source
# MAGIC %md
# MAGIC ### hive_metastore에 silver 스키마 생성

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %md
# MAGIC # review_data 전처리

# COMMAND ----------

# MAGIC %md
# MAGIC ### 결측치

# COMMAND ----------

review_df = spark.table('hive_metastore.silver.review_data')

# COMMAND ----------

from pyspark.sql.functions import col, sum

# 각 컬럼의 결측치 개수를 계산
missing_values = review_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in review_df.columns])

# 결과 출력
display(missing_values)

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, struct

# 'pics' 컬럼의 결측치를 빈 배열로 채우기
review_df = review_df.withColumn(
    'pics', 
    when(col('pics').isNull(), []).otherwise(col('pics'))
)

# 'pics' 컬럼의 결측치를 채울 값 정의
default_resp = struct(lit("N/A").alias("text"), lit(0).alias("time"))

# 'pics' 컬럼의 결측치를 채우기
review_df = review_df.withColumn(
    'resp', 
    when(col('resp').isNull(), default_resp).otherwise(col('resp'))
)

# 나머지 컬럼들의 결측치를 특정 값으로 채우기
review_df = review_df.fillna({
    'rating': 0,
    'text': 'N/A',
    'user_id': 'N/A'
})

# COMMAND ----------

from pyspark.sql.functions import col, sum

# 각 컬럼의 결측치 개수를 계산
missing_values = review_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in review_df.columns])

# 결과 출력
display(missing_values)

# COMMAND ----------

review_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.silver.review_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### time

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW temp_review_data AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   from_unixtime(time / 1000, 'yyyy-MM-dd HH:mm:ss') as converted_time
# MAGIC FROM
# MAGIC   hive_metastore.default.review_data;

# COMMAND ----------

display(temp_review_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.sliver.review_data

# COMMAND ----------

# 임시 뷰를 DataFrame으로 로드
temp_review_df = spark.table("temp_review_data")

# 기존 time 컬럼을 제거하고, new_time 컬럼을 time으로 이름 변경
final_df = temp_review_df.drop("time").withColumnRenamed("converted_time", "time")

# sliver 스키마에 새로운 테이블 저장
final_df.write.mode('overwrite').format("delta").saveAsTable('hive_metastore.sliver.review_data')
# 기존 테이블 덮어쓰기
#temp_review_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("hive_metastore.sliver.review_data")

review_data = spark.read.table('hive_metastore.sliver.review_data')
display(review_data)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # meta_data 전처리

# COMMAND ----------

meta_df = spark.table("hive_metastore.silver.california_meta")
display(meta_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 결측치

# COMMAND ----------

from pyspark.sql.functions import col, sum

# 각 컬럼의 결측치 개수를 계산
missing_values = meta_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in meta_df.columns])

# 결과 출력
display(missing_values)

# COMMAND ----------

from pyspark.sql.functions import col, when, array, lit

# # 'MISC' 구조체 내의 각 배열 필드의 결측치를 빈 배열로 채우기
# default_misc_fields = [
#     "Accessibility", "Activities", "Amenities", "Atmosphere", "Crowd", "Dining_options",
#     "From_the_business", "Getting_here", "Health_and_safety", "Highlights", "Lodging_options",
#     "Offerings", "Payments", "Planning", "Popular_for", "Recycling", "Service_options"
# ]

# for field in default_misc_fields:
#     meta_df = meta_df.withColumn(
#         f"MISC.{field}",
#         when(
#             col(f"MISC.{field}").isNull(),
#             array(lit("")).cast("array<string>")
#         ).otherwise(
#             col(f"MISC.{field}")
#         )
#     )

# 'category' 컬럼의 결측치를 빈 배열로 채우기
meta_df = meta_df.withColumn(
    'category', 
    when(col('category').isNull(), []).otherwise(col('category'))
)
# 'relative_results' 컬럼의 결측치를 빈 배열로 채우기
meta_df = meta_df.withColumn(
    'relative_results', 
    when(col('relative_results').isNull(), []).otherwise(col('relative_results'))
)

# 각 컬럼의 결측치를 채울 기본값 정의
default_values = {
    'address': 'N/A',
    'description': 'N/A',
    'name': 'N/A',
    'price': 'N/A',
    'state': 'N/A'
}

# 다른 컬럼들의 결측치를 기본값으로 채우기
meta_df = meta_df.fillna(default_values)

# COMMAND ----------

from pyspark.sql.functions import col, sum

# 각 컬럼의 결측치 개수를 계산
missing_values = meta_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in meta_df.columns])

# 결과 출력
display(missing_values)

# COMMAND ----------

meta_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("hive_metastore.silver.meta_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### gmap_id 기존 중복 데이터 제거

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.default.meta_data

# COMMAND ----------

# 테이블 로드
df = spark.table("hive_metastore.default.meta_data")

# 중복된 'gmap_id' 컬럼의 값을 가진 행 제거
df_deduped = df.dropDuplicates(["gmap_id"])

# 결과 확인
df_deduped.show(truncate=False)

# 중복 제거된 데이터 저장
df_deduped.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("hive_metastore.sliver.meta_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.silver.meta_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### hours

# COMMAND ----------

# 기존 hours 데이터
%sql
select *
from hive_metastore.default.meta_data

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType

# 테이블 로드
df = spark.table("hive_metastore.silver.meta_data")

# 올바른 순서 정의
correct_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# UDF 정의: 리스트를 올바른 순서로 재배치
def reorder_hours(hours):
    if not hours:
        return []
    
    # 딕셔너리로 변환
    hours_dict = {day_time[0]: day_time[1] for day_time in hours}
    
    # 올바른 순서로 리스트 재구성
    reordered = [[day, hours_dict.get(day, "Closed")] for day in correct_order]
    
    return reordered

# UDF 등록
reorder_hours_udf = udf(reorder_hours, ArrayType(ArrayType(StringType())))

# 새로운 컬럼으로 변환
df = df.withColumn("reordered_hours", reorder_hours_udf(col("hours")))

# 원래 컬럼을 대체
df = df.drop("hours").withColumnRenamed("reordered_hours", "hours")

# 결과 확인 (선택적)
display(df)

# 새 테이블
#df.write.mode('overwrite').format("delta").saveAsTable("hive_metastore.sliver.meta_data")
# 기존 테이블 덮어쓰기
#df.write.mode("overwrite").saveAsTable("hive_metastore.default.meta_data")


# COMMAND ----------

# 덮어쓰기
df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.silver.meta_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### category

# COMMAND ----------

# 기존 카테고리 중 식음료 확인
%sql
select category
from hive_metastore.default.meta_data
where LATERAL VIEW explode(category) AS c
  WHERE lower(c) LIKE '%restaurant%'
  OR lower(c) LIKE '%cafe%'

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 테이블 로드
df = spark.table("hive_metastore.default.meta_data")

# UDF 정의
def classify_category(category):
    try:
        keywords = ["restaurant", "cafe", "food", "coffee", "bakery", "bar", "cafeteria"]
        if category is not None:
            if any(keyword in cat.lower() for cat in category for keyword in keywords):
                return "Food & Beverage"
        return None
    except Exception as e:
        return str(e)

# UDF 등록
classify_category_udf = udf(classify_category, StringType())

# 새로운 컬럼 추가
df = df.withColumn("classification", classify_category_udf(col("category")))

# 결과 확인
display(df)


# COMMAND ----------

from pyspark.sql.functions import col, count

# 테이블 로드
df = spark.table("hive_metastore.silver.meta_data")

# classification 컬럼의 각 값에 대한 데이터 수 집계
classification_counts = df.groupBy("classification").agg(count("*").alias("count"))

# 결과 확인
classification_counts.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, count

# category 컬럼의 각 값에 대한 데이터 수 집계 및 정렬
category_counts = df.groupBy("category").agg(count("*").alias("count")).orderBy(col("count").desc())

# 결과 확인
display(category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 단일 대분류

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 테이블 로드
# df = spark.table("hive_metastore.silver.meta_data")
df = meta_df

# 대분류를 정의하는 함수
def classify_category(categories):
    if categories is None:
        return "Other"
    
    categories_lower = [cat.lower() for cat in categories]
    
    food_and_beverage_keywords = ["restaurant", "cafe", "food", "coffee", "bakery", "bar", "cafeteria"]
    retail_keywords = ["store", "mall", "market", "retail", "shopping", "clothing", "cloth"]
    service_keywords = ["salon", "repair", "service", "gas station", "car wash", "auto", "barber", "beauty", "laundromat", "groomer", "agency", "company", "gym", "bus", "station", "tire"]
    healthcare_keywords = ["doctor", "dentist", "pharmacy", "health", "veterinarian"]
    entertainment_and_recreation_keywords = ["campground", "entertainment"]
    nature_and_outdoor_keywords = ["park", "lake", "river", "beach"]
    religious_keywords = ["church", "religious", "baptist"]
    corporate_and_office_keywords = ["office", "organization", "corporate"]
    residential_keywords = ["apartment", "real estate", "building", "complex"]
    finance_and_legal_keywords = ["bank", "atm", "finance", "attorney", "insurance"]

    if any(keyword in cat for cat in categories_lower for keyword in food_and_beverage_keywords):
        return "Food & Beverage"
    if any(keyword in cat for cat in categories_lower for keyword in retail_keywords):
        return "Retail"
    if any(keyword in cat for cat in categories_lower for keyword in service_keywords):
        return "Service"
    if any(keyword in cat for cat in categories_lower for keyword in healthcare_keywords):
        return "Healthcare"
    if any(keyword in cat for cat in categories_lower for keyword in entertainment_and_recreation_keywords):
        return "Entertainment & Recreation"
    if any(keyword in cat for cat in categories_lower for keyword in nature_and_outdoor_keywords):
        return "Nature & Outdoor"
    if any(keyword in cat for cat in categories_lower for keyword in religious_keywords):
        return "Religious"
    if any(keyword in cat for cat in categories_lower for keyword in corporate_and_office_keywords):
        return "Corporate & Office"
    if any(keyword in cat for cat in categories_lower for keyword in residential_keywords):
        return "Residential"
    if any(keyword in cat for cat in categories_lower for keyword in finance_and_legal_keywords):
        return "Finance & Legal"
    
    return "Other"

# UDF 등록
classify_category_udf = udf(classify_category, StringType())

# 대분류 컬럼 추가
df = df.withColumn("first_main_category", classify_category_udf(col("category")))

# 여러 열 제거
df = df.drop('category')

# 결과 확인
# display(df)


# COMMAND ----------

display(df)

# COMMAND ----------

# 덮어쓰기
df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable('hive_metastore.silver.california_meta')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 대분류가 여러개 해당

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# 테이블 로드
df = spark.table("hive_metastore.sliver.meta_data")

def classify_category(categories):
    if categories is None:
        return "Other"
    
    categories_lower = [cat.lower() for cat in categories]
    
    food_and_beverage_keywords = ["restaurant", "cafe", "food", "coffee", "bakery", "bar", "cafeteria"]
    retail_keywords = ["store", "mall", "market", "retail", "shopping", "clothing", "cloth"]
    service_keywords = ["salon", "repair", "service", "gas station", "car wash", "auto", "barber", "beauty", "laundromat", "groomer", "agency", "company", "gym", "bus", "station", "tire"]
    healthcare_keywords = ["doctor", "dentist", "pharmacy", "health", "veterinarian"]
    entertainment_and_recreation_keywords = ["campground", "entertainment"]
    nature_and_outdoor_keywords = ["park", "lake", "river", "beach"]
    religious_keywords = ["church", "religious", "baptist"]
    corporate_and_office_keywords = ["office", "organization", "corporate"]
    residential_keywords = ["apartment", "real estate", "building", "complex"]
    finance_and_legal_keywords = ["bank", "atm", "finance", "attorney", "insurance"]
    
    main_categories = set()
    
    if any(keyword in cat for cat in categories_lower for keyword in food_and_beverage_keywords):
        main_categories.add("Food & Beverage")
    if any(keyword in cat for cat in categories_lower for keyword in retail_keywords):
        main_categories.add("Retail")
    if any(keyword in cat for cat in categories_lower for keyword in service_keywords):
        main_categories.add("Service")
    if any(keyword in cat for cat in categories_lower for keyword in healthcare_keywords):
        main_categories.add("Healthcare")
    if any(keyword in cat for cat in categories_lower for keyword in entertainment_and_recreation_keywords):
        main_categories.add("Entertainment & Recreation")
    if any(keyword in cat for cat in categories_lower for keyword in nature_and_outdoor_keywords):
        main_categories.add("Nature & Outdoor")
    if any(keyword in cat for cat in categories_lower for keyword in religious_keywords):
        main_categories.add("Religious")
    if any(keyword in cat for cat in categories_lower for keyword in corporate_and_office_keywords):
        main_categories.add("Corporate & Office")
    if any(keyword in cat for cat in categories_lower for keyword in residential_keywords):
        main_categories.add("Residential")
    if any(keyword in cat for cat in categories_lower for keyword in finance_and_legal_keywords):
        main_categories.add("Finance & Legal")
    
    return ", ".join(main_categories) if main_categories else "Other"

# UDF 등록
classify_category_udf = udf(classify_category, StringType())

# 대분류 컬럼 추가
df = df.withColumn("main_category", classify_category_udf(col("category")))

# 결과 확인
display(df)

# COMMAND ----------

from pyspark.sql.functions import split, size

# 'main_category' 컬럼을 쉼표로 구분하여 분리하고, 길이가 2개 이상인 행을 필터링
df_filtered = df.filter(size(split(col("main_category"), ", ")) > 1)

# 'main_category'와 'category' 컬럼을 선택
df_selected = df_filtered.select("main_category", "category")

# 결과 확인
display(df_selected)

# COMMAND ----------

df_selected.count()

# COMMAND ----------

df_selected.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hash Tag

# COMMAND ----------

df = spark.table('hive_metastore.silver.meta_data')
df.columns

# COMMAND ----------

meta_df.count()

# COMMAND ----------

from pyspark.sql.functions import col

# 2. 조인을 통해 gmap_id가 일치하는 데이터 찾기
meta_df = meta_df.join(df.select("gmap_id", "category"), on="gmap_id", how="left")

# 3. 결과 출력 (상위 10개 레코드)
display(meta_df.limit(10))


# COMMAND ----------

from pyspark.sql.functions import slice, col

# 2. category 배열의 상위 3개 요소를 추출하여 hash_tag 컬럼에 추가
meta_df = meta_df.withColumn("hash_tag", slice(col("category"), 1, 3))

display(meta_df.limit(10))

# COMMAND ----------

# 덮어쓰기
meta_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("hive_metastore.silver.california_meta")

# COMMAND ----------

# MAGIC %md
# MAGIC ### price

# COMMAND ----------

from pyspark.sql.functions import col, length, when

# # 데이터프레임 생성
df = spark.table("hive_metastore.silver.california_meta")

# # 'price' 컬럼을 문자열 길이로 대체 ('N/A' 제외)
# df = df.withColumn(
#     "price",
#     when(col("price") != "N/A", length(col("price"))).otherwise(col("price"))
# )

# 'price' 컬럼의 유니크 값 조회
unique_prices = df.select("price").distinct()

# 결과 출력
unique_prices.show()

# COMMAND ----------

# 덮어쓰기
df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("hive_metastore.silver.california_meta")

# COMMAND ----------

from pyspark.sql.functions import col

# string 타입의 price 컬럼을 int 타입으로 변환
df = df.withColumn("price", col("price").cast("int"))

# 'price' 컬럼의 유니크 값 조회
unique_prices = df.select("price").distinct()

# 결과 출력
unique_prices.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### address

# COMMAND ----------

# MAGIC %md
# MAGIC 캘리포니아 도시 246개 리스트

# COMMAND ----------

city_list = ["Los Angeles", "San Francisco", "San Diego", "San Jose", "Santa Clara", "Fresno", "Santa Monica", "Anaheim", "Berkeley", "Long Beach", 
        "Santa Barbara", "Oakland", "Riverside", "Stockton", "San Bernardino", "Modesto", "Costa Mesa", "Fullerton", "Downey", "Beverly Hills", 
        "Bakersfield", "Irvine", "Hayward", "Richmond", "Chula Vista", "Clovis", "San Mateo", "Mission Viejo", "Temecula", "Simi Valley", 
        "Santa Rosa", "Visalia", "Concord", "El Monte", "Thousand Oaks", "Antioch", "Camarillo", "Pleasanton", "Daly City", "Torrance", 
        "Inglewood", "Vacaville", "Westminster", "Santa Cruz", "Redding", "Lakewood", "Mountain View", "Perris", "Napa", "Baldwin Park", 
        "Chico", "Cypress", "Bellflower", "San Marcos", "Huntington Beach", "La Mesa", "Murrieta", "Folsom", "Lynwood", "Santa Ana", 
        "Laguna Niguel", "Culver City", "Dublin", "Lodi", "Palo Alto", "Redwood City", "Petaluma", "Sparks", "Yuba City", "Monterey Park", 
        "Montclair", "Brea", "Tustin", "Arcadia", "Walnut Creek", "Benicia", "Saratoga", "Calexico", "Cerritos", "Pico Rivera", 
        "Santa Fe Springs", "Kingsburg", "Lemoore", "Sonoma", "Tulare", "La Habra", "Yountville", "Woodland", "Livermore", "Fremont", 
        "Palm Springs", "Bishop", "Hesperia", "Porterville", "San Gabriel", "Highland", "Blythe", "Sierra Madre", "Culver City", 
        "Fountain Valley", "Manteca", "Burbank", "Santa Maria", "Napa", "Simi Valley", "El Cajon", "Chino Hills", "Cypress", "Tustin", 
        "San Rafael", "Huntington Park", "South Gate", "Ceres", "Lake Elsinore", "Lodi", "Yuba City", "Bell Gardens", "Lynwood", 
        "Oakley", "Ventura", "Lathrop", "Hollister", "Carmichael", "Dixon", "Turlock", "Sunnyvale", "Brea", "Laguna Hills", 
        "Redondo Beach", "Menifee", "La Mirada", "Tulare", "San Jacinto", "Alameda", "Bakersfield", "Galt", "Carson", "Ceres", 
        "Chino", "Dublin", "Pleasant Hill", "Santa Monica", "Burlingame", "Milpitas", "Rialto", "Yuba City", "Ceres", "Roseville", 
        "Santa Clara", "Los Altos", "Campbell", "Chico", "Monrovia", "Davis", "Burlingame", "Lemon Grove", "Folsom", "West Sacramento", 
        "Hesperia", "Murrieta", "Napa", "Baldwin Park", "Solana Beach", "Huntington Beach", "Tustin", "Moorpark", "Lynwood", "Santa Paula",
        "Alhambra", "Baldwin Park", "Bell", "Bellflower", "Calabasas", "Camarillo", "Carson", "Ceres", "Clovis", "Coachella", 
        "Colton", "Compton", "Covina", "Delano", "Duarte", "El Centro", "El Monte", "Escondido", "Fairfield", "Fremont", 
        "Gardena", "Glendale", "Glendora", "Hanford", "Hawthorne", "Huntington Beach", "Imperial Beach", "Indio", "La Habra Heights", 
        "Lakeport", "Lindsay", "Lompoc", "Los Banos", "Madera", "Marina", "Monterey", "Montrose", "Napa", 
        "Needles", "Newport Beach", "Nipomo", "Norwalk", "Oceanside", "Ontario", "Orange", "Perris", "Pico Rivera", 
        "Redlands", "Ridgecrest", "Rio Linda", "Rosemead", "Sacramento", "Salinas", "San Dimas", "San Fernando", "San Gabriel", 
        "San Jacinto", "San Leandro", "San Pedro", "Santa Clara", "Santa Paula", "Santa Rosa", "Saratoga", "Simi Valley", 
        "South San Francisco", "South Pasadena", "Stockton", "Sunnyvale", "Turlock", "Upland", "Vernon", "Victorville", "Walnut", 
        "Watsonville", "West Covina", "Whittier", "Woodland", "Yuba City"]

len(city_list)

# COMMAND ----------

from pyspark.sql.functions import col, when

meta_df = spark.table("hive_metastore.silver.california_meta")

# 3. city_list에 있는 각 도시를 확인하며 when 절을 연결
city_expr = None
for city in city_list:
    if city_expr is None:
        city_expr = when(col("address").contains(city), city)
    else:
        city_expr = city_expr.when(col("address").contains(city), city)

# 4. 도시 이름이 포함되지 않은 경우 "other"로 처리
city_expr = city_expr.otherwise("other")

# 5. city 컬럼 추가
meta_df = meta_df.withColumn("city", city_expr)

# 6. 상위 10개 레코드 표시
meta_df.show(10)

# COMMAND ----------

# meta_df 상위 10개를 display로 보기
display(meta_df.limit(10))

# COMMAND ----------

# city 컬럼의 유니크 값과 개수를 groupBy와 count로 확인
city_counts_df = meta_df.groupBy("city").count()

# 결과를 보기 좋게 정렬 (옵션)
city_counts_df = city_counts_df.orderBy("count", ascending=False)

# 결과를 출력
city_counts_df.show()


# COMMAND ----------

# 덮어쓰기
meta_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("hive_metastore.silver.california_meta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.silver.california_meta

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # California 데이터셋

# COMMAND ----------

review_df = spark.table('hive_metastore.silver.review_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 전체 review 데이터 수 확인
# MAGIC select count(*)
# MAGIC from hive_metastore.silver.review_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### region 컬럼에서 California 데이터만 필터링

# COMMAND ----------

cali_review_df = review_df.filter(review_df.region == 'California')
cali_review_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### state가 폐점인 리뷰 데이터 제거

# COMMAND ----------

california_meta_df = spark.table('hive_metastore.silver.california_meta')

# 전처리가 완료된 california_meta에서 'gmap_id' 컬럼 데이터 추출
gmap_id_list = california_meta_df.select('gmap_id').rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# california_meta unique값 개수 확인
print(len(set(gmap_id_list)))
gmap_id_list = set(gmap_id_list)

# COMMAND ----------

# gmap_id_list를 Spark 데이터프레임으로 변환
gmap_id_df = spark.createDataFrame([(gmap_id,) for gmap_id in gmap_id_list], ['gmap_id'])

# gmap_id_df를 임시 테이블로 등록
gmap_id_df.createOrReplaceTempView("gmap_id_list")
cali_review_df.createOrReplaceTempView("cali_review_data")

# COMMAND ----------

# SQL 쿼리를 사용하여 필터링
filtered_df = spark.sql("""
    SELECT crd.*
    FROM cali_review_data crd
    JOIN gmap_id_list gil
    ON crd.gmap_id = gil.gmap_id
""")

# 결과 확인
filtered_df.show()

# COMMAND ----------

# 처리 후 개수 및 컬럼 확인
print(filtered_df.count(), filtered_df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 중복 제거

# COMMAND ----------

# filtered_df 데이터프레임에서 'gmap_id', 'user_id', 'time'이 동일한 행 제거
filtered_unique_df = filtered_df.dropDuplicates(['gmap_id', 'user_id', 'time'])

# COMMAND ----------

# 덮어쓰기
filtered_unique_df.write.mode("overwrite").format("delta").saveAsTable("hive_metastore.silver.california_review")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from hive_metastore.silver.california_review

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 리뷰 데이터에 메타 정보 조인

# COMMAND ----------

california_review_df = spark.table('hive_metastore.silver.california_review')
california_meta_df = spark.table('hive_metastore.silver.california_meta')
print(len(california_review_df.columns),len(california_meta_df.columns))

# COMMAND ----------

from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder.appName("JoinTables").enableHiveSupport().getOrCreate()

# 테이블 로드
california_review_df = spark.table('hive_metastore.silver.california_review')
california_meta_df = spark.table('hive_metastore.silver.california_meta')

# 테이블을 임시 뷰로 등록
california_review_df.createOrReplaceTempView("california_review")
california_meta_df.createOrReplaceTempView("california_meta")

# SQL 쿼리를 사용하여 테이블 조인
joined_df = spark.sql("""
    SELECT cr.*, cm.MISC, cm.address, cm.avg_rating, cm.category, cm.description, 
           cm.hours, cm.latitude, cm.longitude, cm.name meta_name, cm.num_of_reviews, cm.price, 
           cm.relative_results, cm.state, cm.url, cm.main_category
    FROM california_review cr
    INNER JOIN california_meta cm
    ON cr.gmap_id = cm.gmap_id
""")

# 결과 확인
display(joined_df)
