# Databricks notebook source
# meta_hawaii(Table)로 불러와서 meta_data(DataFrame)으로 저장
review_data = spark.read.table('hive_metastore.default.review_hawaii')

# COMMAND ----------

import pyspark.pandas as ps

#스파크 데이터프레임으로 변환
review_data = review_data.to_pandas_on_spark()

# COMMAND ----------

#데이터 전처리 및 결측치 제거
review_texts_1000 = review_data[['user_id','text','rating']][:1000]
review_texts = review_data[['user_id','text','rating']].loc[review_data['rating']>3]
review_texts['text'] = review_texts['text'].fillna('')
review_texts['text_len'] = review_texts['text'].apply(len)
review_texts_spark = review_texts.to_spark()


# COMMAND ----------

from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
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

#2gram 
allowed_tags = ['<JJ>+<NN>', '<NN>+<NN>']
chunker = Chunker() \
     .setInputCols(['document', 'pos']) \
     .setOutputCol('ngrams') \
     .setRegexParsers(allowed_tags)

#3gram
allowed_3gram_tags = ['<RB>+<JJ>+<NN>', '<NN>+<NN>+<RB>', '<JJ>+<NN>+<NN>'] # RB : adverb, JJ : adjective, NN : noun
chunker_3 = Chunker() \
     .setInputCols(['document', 'pos']) \
     .setOutputCol('ngrams') \
     .setRegexParsers(allowed_3gram_tags) # 조건과 일치하는 품사 조합
#4gram
allowed_4gram_tags = ['<RB>+<RB>+<JJ>+<NN>'] #'<RB>+<JJ>+<NN>+<NN>', 
chunker_4 = Chunker() \
     .setInputCols(['document', 'pos']) \
     .setOutputCol('ngrams') \
     .setRegexParsers(allowed_4gram_tags) # 조건과 일치하는 품사 조합


finisher = Finisher() \
     .setInputCols(['ngrams']) # 결과를 string으로 출력 

# COMMAND ----------

#3gram 파이프라인
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

#3gram 생성
processed_3gram_review = pipeline_3gram.fit(review_texts_spark).transform(review_texts_spark)
rv_3grams = processed_3gram_review.toPandas()
rv3grams = rv_3grams['finished_ngrams'].to_frame()
rv3grams = rv3grams.explode('finished_ngrams')
rv3grams['finished_ngrams'] = rv3grams['finished_ngrams'].str.lower()#소문자화
rv3grams = rv3grams['finished_ngrams'].value_counts().to_frame().reset_index()#빈도수 세기
rv3grams

# COMMAND ----------

#3gram 결과 나열
rv3grams = rv3grams.rename(columns={'index':'finished_ngrams','finished_ngrams':'count'})
rv3grams

# COMMAND ----------

#4gram 파이프라인
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

#4gram 데이터 생성
processed_4gram_review = pipeline_4gram.fit(review_texts_spark).transform(review_texts_spark)#파이프라인
rv_4grams = processed_4gram_review.toPandas()
rv4grams = rv_4grams['finished_ngrams'].to_frame()
rv4grams = rv4grams.explode('finished_ngrams')
rv4grams['finished_ngrams'] = rv4grams['finished_ngrams'].str.lower()#소문자화
rv4grams= rv4grams['finished_ngrams'].value_counts().to_frame().reset_index()#빈도수세기
rv4grams

# COMMAND ----------

#4gram 결과 빈도수로 정렬
rv4grams = rv4grams.rename(columns={'index':'finished_ngrams','finished_ngrams':'count'})
rv4grams

# COMMAND ----------

# MAGIC %pip install wordcloud

# COMMAND ----------

from wordcloud import WordCloud
import matplotlib.pyplot as plt
import numpy as np

# COMMAND ----------

#3,4 gram의 빈도수를 dict화 및 이상값 제거
frequency_3 = rv3grams.set_index('finished_ngrams').to_dict()['count']
frequency_4 = rv4grams.set_index('finished_ngrams').to_dict()['count']
key_to_delete = ['very pretty\n\n(original)\nmuy', 'very well\n\n(original)\nmuy']
for i in key_to_delete:
    if i in frequency_4:
        del frequency_4[i]


# COMMAND ----------

#3,4 gram의 빈도수를 워드클라우드 시각화
wc3 = WordCloud(
    background_color='white', 
    max_words=70, 
    min_font_size=5, 
    colormap='Dark2'
)
wc4 = WordCloud(
    background_color='white', 
    max_words=30, 
    min_font_size=4, 
    colormap='turbo'
)
fig = plt.figure(figsize=(20, 20))
wc_3 = wc3.generate_from_frequencies(frequency_3)
wc_4 = wc4.generate_from_frequencies(frequency_4)

ax1 = fig.add_subplot(2, 1, 1)
ax1.set_title('< 3-gram >')
ax1.imshow(wc_3, interpolation='bilinear')
ax1.axis('off')

ax2 = fig.add_subplot(2, 1, 2)
ax2.set_title('< 4-gram >')
ax2.imshow(wc_4, interpolation='bilinear')
ax2.axis('off')

plt.show()

# COMMAND ----------

#3,4 gram의 빈도수를 barchart로 시각화
# 데이터 정렬 및 상위 n개 선택
top_n = 20  # 상위 n개를 선택
top_3_grams = dict(sorted(frequency_3.items(), key=lambda item: item[1], reverse=True)[:top_n])
top_4_grams = dict(sorted(frequency_4.items(), key=lambda item: item[1], reverse=True)[:top_n])

# 막대 차트 생성
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

# 3-gram 막대 차트
ax1.bar(top_3_grams.keys(), top_3_grams.values(), color='skyblue')
ax1.set_title('Top 3-grams by Frequency')
ax1.set_xlabel('3-grams')
ax1.set_ylabel('Frequency')
ax1.set_xticklabels(top_3_grams.keys(), rotation=45, ha='right')

# 4-gram 막대 차트
ax2.bar(top_4_grams.keys(), top_4_grams.values(), color='lightgreen')
ax2.set_title('Top 4-grams by Frequency')
ax2.set_xlabel('4-grams')
ax2.set_ylabel('Frequency')
ax2.set_xticklabels(top_4_grams.keys(), rotation=45, ha='right')

plt.tight_layout()
plt.show()
