# Databricks notebook source
# MAGIC %md
# MAGIC #### 00. environ setting

# COMMAND ----------

# MAGIC %pip install --upgrade langchain faiss-cpu mlflow

# COMMAND ----------

# MAGIC %pip install -U langchain-openai langchain-community

# COMMAND ----------

# MAGIC %pip install mlflow transformers

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
import faiss

from langchain.document_loaders import PySparkDataFrameLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from transformers import AutoModelForCausalLM, AutoTokenizer
from langchain import PromptTemplate


# COMMAND ----------

import config
import os
os.environ["OPENAI_API_KEY"]=config.OPENAI_API_KEY

# COMMAND ----------

embed_model=OpenAIEmbeddings()

# COMMAND ----------

# MAGIC %md
# MAGIC ### _vector db_

# COMMAND ----------

# MAGIC %md
# MAGIC #### 01. data load, preprocess

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 데이터 로드 및 컬럼명 수정
# MAGIC : llm model 의 이해를 돕기위해 더 자세한 컬럼명으로 수정

# COMMAND ----------

data=spark.sql("SELECT * FROM hive_metastore.vector_db.reviews_filtered3")

# COMMAND ----------

data.count()

# COMMAND ----------

all_columns=data.columns

@udf(returnType=StringType())
def combine_columns(*cols):
     return ". ".join([f"{name} is {str(col)}" for name, col in zip(all_columns, cols) if col is not None and str(col).lower() != 'null'])

data=data.withColumn("combined_doc",combine_columns(*data.columns))

# COMMAND ----------

data=data.withColumnRenamed("resp","response of store owner").withColumnRenamed("text","review").withColumnRenamed("name","place name")

# COMMAND ----------

display(data.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC 메타 샘플링에 있는 것만 필터링

# COMMAND ----------

meta=spark.sql("SELECT gmap_id FROM asacdataanalysis.vector_db.meta_sample2")

# COMMAND ----------

document=data.join(meta,on="gmap_id")
document.count()

# COMMAND ----------

doc_sample=document.sample(withReplacement=False, fraction=0.0125)
doc_sample.count()

# COMMAND ----------

display(doc_sample.limit(1))

# COMMAND ----------

# df 저장을 위한 컬럼명에 "_" 추가
doc_sample_save=doc_sample.withColumnRenamed("response of store owner","response_of_store_owner").withColumnRenamed("place name","place_name")

# COMMAND ----------

doc_sample_save.write.mode("overwrite").saveAsTable('asacdataanalysis.vector_db.review_sample2')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03. index 생성

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. indexIVFPQ
# MAGIC :HP(cluster, sub vector) 설정 후 인덱스 생성

# COMMAND ----------

data_pandas=doc_sample.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 임베딩 및 테이블로 저장

# COMMAND ----------

from langchain_core.documents import Document

# combined_doc에 모든 컬럼정보 저장
docs=[Document(id=str(index),page_content=row["combined_doc"]) for index,row in data_pandas.iterrows()]

# COMMAND ----------

texts=[doc.page_content for doc in docs]

# COMMAND ----------

embeddings_list=embed_model.embed_documents(texts)
embeddings=np.array(embeddings_list,dtype="float32")

# COMMAND ----------

import pandas as pd

embedding_df = pd.DataFrame(embeddings)

spark_df = spark.createDataFrame(embedding_df)

spark_df.write.mode("overwrite").saveAsTable('hive_metastore.vector_db.review_embedding3')

# COMMAND ----------

# 샘플링 비율 설정 -> index.train이 너무 오래걸릴시 샘플링 사용 가능
sample_size = int(len(reduced_embeddings) * 0.1)  # 0.1로 샘플링
sample_indices = np.random.choice(len(reduced_embeddings), sample_size, replace=False)
sample_embeddings = reduced_embeddings[sample_indices]
len(sample_embeddings)

# COMMAND ----------

index_path="review_sample3.index"

# COMMAND ----------

# 인덱스 생성
import faiss
from langchain.embeddings import OpenAIEmbeddings

dim=embeddings.shape[1] # 임베딩 벡터 차원

ncentroids=int(np.sqrt(len(embeddings))) # 클러스터 수 - 데이터 수의 제곱근
m=32 # sub vector 분할
quantizer=faiss.IndexFlatL2(dim)

index=faiss.IndexIVFPQ(quantizer,dim,ncentroids,m,8) # 서브벡터를 8 bits로 양자화

faiss.normalize_L2(embeddings)

index.train(embeddings) # train:샘플 데이터 가능 !

index.add(embeddings) # add: 전체 데이터

index.make_direct_map()

faiss.write_index(index,index_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03. 벡터 스토어 업로드

# COMMAND ----------

# MAGIC %md
# MAGIC 벡터 스토어 초기화

# COMMAND ----------

from uuid import uuid4

uuids=[ str(uuid4()) for _ in range(len(embeddings))]

# COMMAND ----------

from langchain_community.docstore.in_memory import InMemoryDocstore

# FAISS 인덱스에서 ID와 UUID 간의 매핑 생성
index_to_docstore_id = {i: uuids[i] for i in range(len(uuids))}

# 벡터 스토어 초기화 시 매핑 제공
vector_store = FAISS(embedding_function=embed_model, index=index, docstore=InMemoryDocstore(), index_to_docstore_id=index_to_docstore_id)

vector_store.add_documents(documents=docs, ids=uuids)

# COMMAND ----------

vector_store_path="review_sample3"
vector_store.save_local(vector_store_path)

# COMMAND ----------

# 필요한 경우 DBFS 디렉토리 생성
dbutils.fs.mkdirs("/vector_db/review2")

# COMMAND ----------

# 로컬 작업 디렉토리에서 파일을 읽고 DBFS에 쓰기
with open("/Workspace/Users/spdlqj888888@gmail.com/langchainRetrieval/review_sample3/index.faiss", "rb") as f:
    data = f.read()
with open("/dbfs/vector_db/review2/index.faiss", "wb") as f:
    f.write(data)

with open("/Workspace/Users/spdlqj888888@gmail.com/langchainRetrieval/review_sample3/index.pkl", "rb") as f:
    data = f.read()
with open("/dbfs/vector_db/review2/index.pkl", "wb") as f:
    f.write(data)
