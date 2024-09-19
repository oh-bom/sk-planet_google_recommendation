# Databricks notebook source
# MAGIC %md
# MAGIC #### 00. environ setting

# COMMAND ----------

# MAGIC %pip install --upgrade langchain faiss-cpu mlflow

# COMMAND ----------

# MAGIC %pip install -U langchain-openai langchain-community

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
# MAGIC ##### 데이터 로드 combined_doc load

# COMMAND ----------

document=spark.sql("SELECT gmap_id,combined_doc from asacdataanalysis.vector_db.meta")

# COMMAND ----------

doc_sample=document.sample(withReplacement=False, fraction=0.22)
doc_sample.count()

# COMMAND ----------

doc_sample.write.mode("overwrite").saveAsTable('asacdataanalysis.vector_db.meta_sample2')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 비용 계산

# COMMAND ----------

# 전체 데이터 추출
target_col="combined_doc"

data_list=doc_sample.select(target_col).rdd.flatMap(lambda x:x).collect()

# COMMAND ----------

doc_str="".join(data_list)
print(doc_str)

# COMMAND ----------

len(doc_str)

# COMMAND ----------

doc_str

# COMMAND ----------

import tiktoken

encoding = tiktoken.get_encoding("cl100k_base")
# 주어진 모델 이름에 대해 올바른 인코딩을 자동으로 로드
encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

result=encoding.encode(doc_str)

# COMMAND ----------

len(result)

# COMMAND ----------

len(encoding.encode(data_list[122]))

# COMMAND ----------

len(encoding.encode(doc_str))

# COMMAND ----------

cost_per_1M_tokens = 0.15
total_tokens=24047905

# 전체 비용 계산
total_cost = (total_tokens / 1000000) * cost_per_1M_tokens
print("cost:",total_cost)

# COMMAND ----------

total_cost

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02. index 생성

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. indexIVFPQ
# MAGIC :HP(cluster, sub vector) 설정 후 인덱스 생성

# COMMAND ----------

# MAGIC %md
# MAGIC document 생성

# COMMAND ----------

data_pandas=doc_sample.toPandas()

# COMMAND ----------

# ver 1: combined_doc에 모든 컬럼정보 저장
from langchain_core.documents import Document

docs=[Document(page_content=row["combined_doc"]) for index,row in data_pandas.iterrows()]

# COMMAND ----------

texts=[doc.page_content for doc in docs]

# COMMAND ----------

# MAGIC %md
# MAGIC 임베딩 및 테이블로 저장

# COMMAND ----------

embeddings_list=embed_model.embed_documents(texts)
embeddings=np.array(embeddings_list,dtype="float32")

# COMMAND ----------

import pandas as pd

embedding_df = pd.DataFrame(embeddings)

spark_df = spark.createDataFrame(embedding_df)

spark_df.write.mode("overwrite").saveAsTable('asacdataanalysis.vector_db.meta_embedding2')

# COMMAND ----------

# 샘플링 비율 설정 -> index.train이 너무 오래걸릴시 샘플링 사용 가능
sample_size = int(len(embeddings) * 0.1)  # 0.1로 샘플링
sample_indices = np.random.choice(len(embeddings), sample_size, replace=False)
sample_embeddings = embeddings[sample_indices]
len(sample_embeddings)

# COMMAND ----------

# MAGIC %md
# MAGIC 인덱스 생성

# COMMAND ----------

index_path="meta_sample2.index"

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

# MAGIC %md
# MAGIC 벡터 스토어 로컬 저장 + dbfs 저장

# COMMAND ----------

vector_store_path="meta_sample2"
vector_store.save_local(vector_store_path)

# COMMAND ----------

# 필요한 경우 DBFS 디렉토리 생성
dbutils.fs.mkdirs("/vector_db/meta2")

# COMMAND ----------

# 로컬 작업 디렉토리에서 파일을 읽고 DBFS에 쓰기
with open("/Workspace/Users/spdlqj888888@gmail.com/langchainRetrieval/meta_sample2/index.faiss", "rb") as f:
    data = f.read()
with open("/dbfs/vector_db/meta2/index.faiss", "wb") as f:
    f.write(data)

with open("/Workspace/Users/spdlqj888888@gmail.com/langchainRetrieval/meta_sample2/index.pkl", "rb") as f:
    data = f.read()
with open("/dbfs/vector_db/meta2/index.pkl", "wb") as f:
    f.write(data)
