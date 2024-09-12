# Databricks notebook source
# meta_hawaii(Table)로 불러와서 meta_data(DataFrame)으로 저장
review_data = spark.read.table('hive_metastore.default.review_hawaii')

# COMMAND ----------

import pyspark.pandas as ps

#스파크 판다스로 변환
review_data = review_data.to_pandas_on_spark()

# COMMAND ----------

#사용할 데이터 만 추출 및 결측치 제거
review_texts_1000 = review_data[['user_id','text','rating']][:1000]
review_texts = review_data[['user_id','text','rating']]
review_texts['text'] = review_texts['text'].fillna('')
review_texts['text_len'] = review_texts['text'].apply(len)
review_texts_spark = review_texts.to_spark()


# COMMAND ----------

import pyspark.pandas as ps
# 자연어 처리 관련 모듈 임포트
from sparknlp.annotator import Lemmatizer, Stemmer, Tokenizer, Normalizer, LemmatizerModel
from sparknlp.annotator import Chunker
from sparknlp.base import Finisher, EmbeddingsFinisher
from nltk.corpus import stopwords
from sparknlp.annotator import StopWordsCleaner
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import PerceptronModel     
from sparknlp.annotator import Chunker
from pyspark.ml import Pipeline
import nltk
nltk.download('stopwords')

# COMMAND ----------

documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document") # 원시 데이터를 문서 형태로 변환
tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('tokenized') # 토큰화;쪼개기

normalizer = Normalizer() \
     .setInputCols(['tokenized']) \
     .setOutputCol('normalized') \
     .setLowercase(True)

lemmatizer = LemmatizerModel.pretrained()\
     .setInputCols(['normalized'])\
     .setOutputCol('lemmatized') # 단어의 기본형

eng_stopwords = stopwords.words('english')

stopwords_cleaner = StopWordsCleaner()\
     .setInputCols(['lemmatized'])\
     .setOutputCol('no_stop_lemmatized')\
     .setStopWords(eng_stopwords) # 불용어 제거

pos_tagger = PerceptronModel.pretrained('pos_anc') \
     .setInputCols(['document', 'lemmatized']) \
     .setOutputCol('pos') # 품사 태깅

allowed_tags = ['<JJ>+<NN>', '<NN>+<NN>']
chunker = Chunker() \
     .setInputCols(['document', 'pos']) \
     .setOutputCol('ngrams') \
     .setRegexParsers(allowed_tags)


allowed_3gram_tags = ['<RB>+<JJ>+<NN>', '<NN>+<NN>+<RB>', '<JJ>+<NN>+<NN>'] # RB : adverb, JJ : adjective, NN : noun
chunker_3 = Chunker() \
     .setInputCols(['document', 'pos']) \
     .setOutputCol('ngrams') \
     .setRegexParsers(allowed_3gram_tags) # 조건과 일치하는 품사 조합

allowed_4gram_tags = ['<RB>+<RB>+<JJ>+<NN>'] #'<RB>+<JJ>+<NN>+<NN>', 
chunker_4 = Chunker() \
     .setInputCols(['document', 'pos']) \
     .setOutputCol('ngrams') \
     .setRegexParsers(allowed_4gram_tags) # 조건과 일치하는 품사 조합


finisher = Finisher() \
     .setInputCols(['ngrams']) # 결과를 string으로 출력 

# COMMAND ----------

#단어 토토큰화 및 품사태깅 파이프라인
pipeline = Pipeline().setStages([documentAssembler,
                 tokenizer,
                 normalizer,
                 lemmatizer,
                 stopwords_cleaner,
                 pos_tagger,
                 ])

# COMMAND ----------

#워드클라우드 다운
%pip install wordcloud

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import explode, col, posexplode, monotonically_increasing_id
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import pandas as pd
from nltk.corpus import stopwords
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, Normalizer, LemmatizerModel, StopWordsCleaner, PerceptronModel

# 데이터 제한
review_texts_spark_test = review_texts_spark
review_texts_spark_test = review_texts_spark_test.withColumn("id", monotonically_increasing_id())
review_texts_spark_test.createOrReplaceTempView("text")

# 파이프라인 모델 생성 및 데이터 처리
processed_review = pipeline.fit(review_texts_spark_test).transform(review_texts_spark_test)

# lemmatized 결과를 먼저 확장
lemmatized_df = processed_review.select("id", posexplode(col("lemmatized.result")).alias("pos_idx", "word"))

# 품사 결과를 확장하고 위치를 추가
pos_df = processed_review.select("id", posexplode(col("pos.result")).alias("pos_idx", "pos"))

# lemmatized 데이터프레임과 pos 데이터프레임을 인덱스와 ID로 조인
expanded_df = lemmatized_df.join(pos_df, on=["id", "pos_idx"]).select("word", "pos")

# 특정 품사 (예: 명사 'NN')로 필터링
filtered_pos_df = expanded_df.filter(col("pos").isin("NN", "NNS", "NNP", "NNPS"))

# 단어 빈도수 계산
word_freq = filtered_pos_df.groupBy("word").count().orderBy("count", ascending=False)

# Pandas로 변환
word_freq_pd = word_freq.toPandas()

# 단어 빈도수 분포 Bar Chart
plt.figure(figsize=(10, 6))
plt.bar(word_freq_pd['word'][:10], word_freq_pd['count'][:10])
plt.xlabel('Words')
plt.ylabel('Frequency')
plt.title('Top 10 Words by Frequency')
plt.xticks(rotation=45)
plt.show()

# WordCloud 생성
wordcloud = WordCloud(width=800, height=400, max_words=100).generate_from_frequencies(dict(zip(word_freq_pd['word'], word_freq_pd['count'])))

# WordCloud 시각화
plt.figure(figsize=(10, 6))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('Word Cloud of Words by Frequency')
plt.show()

# COMMAND ----------

#상위 30개까지 보기
plt.figure(figsize=(10, 6))
plt.bar(word_freq_pd['word'][1:30], word_freq_pd['count'][1:30])
plt.xlabel('Words')
plt.ylabel('Frequency')
plt.title('Top 10 Words by Frequency')
plt.xticks(rotation=45)
plt.show()


# COMMAND ----------

# WordCloud 생성
wordcloud = WordCloud(width=800, height=400, max_words=100).generate_from_frequencies(dict(zip(word_freq_pd['word'][1:], word_freq_pd[1:]['count'])))

# WordCloud 시각화
plt.figure(figsize=(10, 6))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('Word Cloud of Words by Frequency')
plt.show()
