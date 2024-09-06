# Databricks notebook source
# meta_hawaii(Table)로 불러와서 review,meta_data(DataFrame)으로 저장
review_data = spark.read.table('hive_metastore.test.review_preprocessing_california')

# COMMAND ----------

review_data_100000 = review_data.limit(100000)

# COMMAND ----------

#bert sentence embedding model "sent_small_bert_L2_128"임베딩
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import SentenceDetector, BertSentenceEmbeddings
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")
sentence = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")
embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L2_128") \
    .setInputCols(["sentence"]) \
    .setOutputCol("sentence_bert_embeddings")\
    .setCaseSensitive(True) \
    .setMaxSentenceLength(512)
pipeline = Pipeline(stages=[documentAssembler,
                            sentence,
                            embeddings])

# COMMAND ----------

#128차원의 벡터로 임베딩
embeddings = BertSentenceEmbeddings.pretrained("sent_small_bert_L2_128")
print(embeddings.getDimension())  # 이 코드로 실제 모델의 차원 수를 확인


# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace, concat_ws, collect_list, col, trim

# 데이터 로딩
# 예제로, 데이터가 이미 'review_data_100000' 변수에 DataFrame 형태로 로드되어 있다고 가정합니다.

# 텍스트 필드가 공백인지 확인하고, 공백이 아닌 경우만 필터링
# 'trim' 함수를 사용하여 양쪽 공백을 제거한 후, 길이가 0보다 큰 행만 선택
filtered_data = review_data.filter(trim(col("text")) != "")

# 필터링된 데이터에서 gmap_id를 기준으로 그룹화하고, 각 그룹의 텍스트를 ". "을 사용하여 연결
df_grouped = filtered_data.groupBy("gmap_id").agg(concat_ws(". ", collect_list("text")).alias("text"))

# 결과 출력
df_grouped.show(truncate=False)


# COMMAND ----------

#모델선택과 파이프라인을 통한 결과 출력
model = pipeline.fit(df_grouped)
result_bert = model.transform(df_grouped)

# COMMAND ----------

from pyspark.sql.functions import col, expr

# embeddings 값 추출
def extract_embeddings(embeddings):
    return [entry['embeddings'] for entry in embeddings]

# emb컬럼으로 생성함
sbert1000 = result_bert.withColumn("emb", expr("transform(sentence_bert_embeddings, x -> x['embeddings'])"))


# COMMAND ----------

from pyspark.sql.functions import col, expr

# embeddings 값 추출
def extract_embeddings(embeddings):
    return [entry['embeddings'] for entry in embeddings]

# emb컬럼으로 생성함
sbert1000 = result_bert.withColumn("emb", expr("transform(embeddings, x -> x['embeddings'])"))

# Display only 1 row
display(sbert1000.limit(1))

# COMMAND ----------

#저장할 데이터 컬럼 gmap_id,다큐먼트,임베딩값 선택
sbert1000_select = sbert1000[['gmap_id','document','emb']]
# Display the 0th element of the 0th row in the 'emb' column
display(sbert1000_select.selectExpr("emb[1] as emb_0").limit(1))
#데이터 저장
sbert1000_select.write.mode('overwrite').saveAsTable('asacdataanalysis.default.sentence_bert_emb_default_128')
