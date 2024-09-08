# Databricks notebook source
# meta_hawaii(Table)로 불러와서 meta_data(DataFrame)으로 저장
review_data = spark.read.table('hive_metastore.default.review_hawaii')

# COMMAND ----------

import pyspark.pandas as ps
#판다스데이터프레임으로 전환
review_data = review_data.to_pandas_on_spark()

#'user_id','text','rating'로 필터링 후 null값 대체 및 텍스트 길이 계산
review_texts_1000 = review_data[['user_id','text','rating']][:1000]
review_texts = review_data[['user_id','text','rating']]
review_texts['text'] = review_texts['text'].fillna('')
review_texts['text_len'] = review_texts['text'].apply(len)
review_texts_spark = review_texts.to_spark()


# COMMAND ----------

from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt

#데이터 확인및 텍스트 빈값 확인
display(review_texts.to_spark().limit(5))
len(review_data.loc[review_data['text'].isnull()==False])
review_texts_spark = review_texts.to_spark()

# COMMAND ----------

#텍스트 길이 계산 및 시각화
import pyspark.pandas as ps
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 데이터 생성
df = review_texts.loc[(review_texts['text_len'] != 0) & (review_texts['text_len'] < 251)]

# Convert to pandas DataFrame
pdf = df.to_pandas()

# text_len 컬럼 추가 (각 텍스트의 길이)
pdf['text_len'] = pdf['text'].apply(len)

# 시각화 스타일 설정
sns.set(style="whitegrid")

# text_len의 분포 시각화
plt.figure(figsize=(12, 8))
ax = sns.histplot(pdf['text_len'], bins=10, kde=False, color='skyblue')

# 막대 위에 실제 갯수 표시
for p in ax.patches:
    height = p.get_height()
    ax.annotate(f'{int(height)}', 
                xy=(p.get_x() + p.get_width() / 2, height), 
                xytext=(0, 5), 
                textcoords='offset points', 
                ha='center', va='bottom')

# 제목과 레이블 설정
plt.title('Distribution of Text Length', fontsize=16)
plt.xlabel('Text Length', fontsize=14)
plt.ylabel('Frequency', fontsize=14)

# 그래프 출력
plt.show()

#길이 및 비율 출력
print(len(df))
print(len(df)/3111531*100)

# 평균, 중앙값, 최대값, 최소값 계산
summary_stats = df['text_len'].agg(['mean', 'median', 'max', 'min']).to_frame().reset_index()
summary_stats.columns = ['Statistic', 'Value']

# 요약 통계 출력
print(summary_stats)


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
tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('tokenized') # 토큰화,쪼개기


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


allowed_3gram_tags = ['<RB>+<JJ>+<NN>', '<NN>+<NN>+<RB>', '<JJ>+<NN>+<NN>'] 
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

#전처리 및 토큰화,품사태깅
pipeline = Pipeline().setStages([documentAssembler,
                 tokenizer,
                 normalizer,
                 lemmatizer,
                 stopwords_cleaner,
                 pos_tagger,
                 ])

# COMMAND ----------

# Limit the DataFrame to the first 1000 rows
review_texts_spark_test = review_texts_spark.limit(1000)
review_texts_spark_test.createOrReplaceTempView("text")

# Apply the pipeline
processed_review = pipeline.fit(review_texts_spark_test).transform(review_texts_spark_test)

# Display the result
display(processed_review)

# COMMAND ----------

# 필요한 컬럼만 선택하고 explode 사용하여 단어를 펼침
from pyspark.sql.functions import explode, col
import matplotlib.pyplot as plt

words_df = processed_review.select(explode(col("lemmatized.result")).alias("word"))

# 단어 빈도수 계산
word_freq = words_df.groupBy("word").count().orderBy("count", ascending=False)

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

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import explode, col, posexplode
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

pos_df = processed_review.withColumn("pos_idx", explode(col("pos.result")))

display(pos_df)

# COMMAND ----------

display(lemmatized_df)

# COMMAND ----------

# MAGIC %pip install wordcloud

# COMMAND ----------

from wordcloud import WordCloud
import matplotlib.pyplot as plt

# WordCloud 생성
wordcloud = WordCloud(width=800, height=400, max_words=100).generate_from_frequencies(dict(zip(word_freq_pd['word'], word_freq_pd['count'])))

# WordCloud 시각화
plt.figure(figsize=(10, 6))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('Word Cloud of Words by Frequency')
plt.show()

# COMMAND ----------

display(processed_review)

# COMMAND ----------

review_t = processed_review.to_pandas_on_spark()

# COMMAND ----------

display(review_t)

# COMMAND ----------

display(processed_review)

# COMMAND ----------

pipeline_3gram = Pipeline() \
     .setStages([documentAssembler,
                 tokenizer,
                 normalizer,
                 lemmatizer,
                 stopwords_cleaner,
                 pos_tagger,
                 chunker_3,
                 finisher])

# COMMAND ----------

processed_3gram_review = pipeline_3gram.fit(review_texts_spark).transform(review_texts_spark)

# COMMAND ----------

rv_3grams = processed_3gram_review.toPandas()
rv_3grams

# COMMAND ----------

rv3grams = rv_3grams['finished_ngrams'].to_frame()
rv3grams

# COMMAND ----------

rv3grams = rv3grams.explode('finished_ngrams')
rv3grams['finished_ngrams'] = rv3grams['finished_ngrams'].str.lower()
rv3grams

# COMMAND ----------

rv3grams = rv3grams['finished_ngrams'].value_counts().to_frame().reset_index()
rv3grams

# COMMAND ----------

rv3grams = rv3grams.rename(columns={'index':'finished_ngrams','finished_ngrams':'count'})
rv3grams

# COMMAND ----------

review_data.printSchema()

# COMMAND ----------

review_data.describe()

# COMMAND ----------

pipeline_4gram = Pipeline() \
     .setStages([documentAssembler,
                 tokenizer,
                 normalizer,
                 lemmatizer,
                 stopwords_cleaner,
                 pos_tagger,
                 chunker_4,
                 finisher])

# COMMAND ----------

processed_4gram_review = pipeline_4gram.fit(review_texts_spark).transform(review_texts_spark)

# COMMAND ----------

rv_4grams = processed_4gram_review.toPandas()
rv_4grams

# COMMAND ----------

rv4grams = rv_4grams['finished_ngrams'].to_frame()
rv4grams

# COMMAND ----------

rv4grams = rv4grams.explode('finished_ngrams')
rv4grams['finished_ngrams'] = rv4grams['finished_ngrams'].str.lower()
rv4grams

# COMMAND ----------

rv4grams= rv4grams['finished_ngrams'].value_counts().to_frame().reset_index()
rv4grams

# COMMAND ----------

rv4grams = rv4grams.rename(columns={'index':'finished_ngrams','finished_ngrams':'count'})
rv4grams

# COMMAND ----------

# MAGIC %pip install --upgrade pip

# COMMAND ----------



# COMMAND ----------

from wordcloud import WordCloud
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

frequency_3 = rv3grams.set_index('finished_ngrams').to_dict()['count']
frequency_3

# COMMAND ----------

frequency_4 = rv4grams.set_index('finished_ngrams').to_dict()['count']
frequency_4

# COMMAND ----------

wc3 = WordCloud(background_color='white', max_words=70, min_font_size=5, colormap='Dark2')
wc4 = WordCloud(background_color='white', max_words=30, min_font_size=4, colormap='turbo')
fig = plt.figure(figsize=(20, 20))
wc_3 = wc3.generate_from_frequencies(frequency_3)
wc_4 = wc4.generate_from_frequencies(frequency_4)

ax1 = fig.add_subplot(2, 1, 1)
ax1.set_title('< 3-gram >')
plt.imshow(wc_3)
plt.axis('off')

ax2 = fig.add_subplot(2, 1, 2)
ax2.set_title('< 4-gram >')
plt.imshow(wc_4)
plt.axis('off')
plt.show()

# COMMAND ----------



# COMMAND ----------

review_data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

null_counts = review_data.select([sum(col(c).isNull().cast("int")).alias(c) for c in review_data.columns])
display(null_counts)

# COMMAND ----------

review_data.info()
