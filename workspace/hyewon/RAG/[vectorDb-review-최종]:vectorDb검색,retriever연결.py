# Databricks notebook source
# MAGIC %md
# MAGIC #### 0.environ setting

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
# MAGIC #### 검색

# COMMAND ----------

# MAGIC %md
# MAGIC local vector store load

# COMMAND ----------

vector_store_path="review_sample3"

# COMMAND ----------

db=FAISS.load_local(vector_store_path,OpenAIEmbeddings(), allow_dangerous_deserialization=True)

retriever = db.as_retriever(search_type="mmr", search_kwargs={"k": 2})
retriever.invoke("recommend place that have positive reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC llm model prompt template

# COMMAND ----------

# Prompt template
from langchain.prompts import ChatPromptTemplate


template="""
        Use the following pieces of context to answer the question at the end. If you don't know the answer, just say that you don't know, don't try to make up an answer.

        \n{context}

        \nQuestion: {question}
        \nrecommended place: ** name **
        \n
        \n- rating:
        \n- review:
        \n- [gmap_id]
        """

prompt = ChatPromptTemplate.from_template(template)

# COMMAND ----------

def format_docs(docs):
    return '\n\n'.join([d.page_content for d in docs])

# COMMAND ----------

# MAGIC %md
# MAGIC 검색

# COMMAND ----------

test_query="recommend place that have positive reviews"

# COMMAND ----------

# 검색
from langchain.chains import RetrievalQA
from langchain.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

def search_query(vector_store_path,query):
    db=FAISS.load_local(vector_store_path,embed_model, allow_dangerous_deserialization=True)
    retriever = db.as_retriever(search_type="mmr", search_kwargs={"k": 2})
    llm_model=ChatOpenAI(model_name="gpt-4o-mini")
    
    docs=retriever.get_relevant_documents(query)
    chain= prompt | llm_model | StrOutputParser()

    response = chain.invoke({'context': (format_docs(docs)), 'question':query})
    print(response)

search_query(vector_store_path,test_query)

# COMMAND ----------

test_query1="find place that has clean, comfortable bed or has friendly staff." 
search_query(vector_store_path,test_query1)

# COMMAND ----------

test_query2="recommend place that has visitors who visit that place twice "
search_query(vector_store_path,test_query2)

# COMMAND ----------

test_query3="find place that has more than 100 reviews"
search_query(vector_store_path,test_query3)

# COMMAND ----------

test_query4="recommend restaurant where most of reviews are positive"
search_query(vector_store_path,test_query4)

# COMMAND ----------

test_query5="search restaurant where most of reviews are negative"
search_query(vector_store_path,test_query5)

# COMMAND ----------

# MAGIC %md
# MAGIC 결과 확인용

# COMMAND ----------

document=spark.sql("SELECT * FROM hive_metastore.vector_db.meta_review")

# COMMAND ----------

test_query4="recommend restaurant where most of reviews are positive"

display(document.filter(col("gmap_id")=="0x80857d0a0a76bb03:0xbaef9e24962b9a76"))

# COMMAND ----------

test_query4="remind restaurant where most of reviews are negative"

display(document.filter(col("gmap_id")=="0x80dd4b7baa68a49b:0x211bc0aa067c364d"))

# COMMAND ----------

# 더미: 기본 faiss index 유사도 검색

import faiss
test_query="find place where opens 24 hours" #우선 한문장으로 예시
query_emb=model.embed_query(test_query)

# 쿼리 임베딩을 numpy 배열로 변환하고 2차원 배열로 만듦
query_emb = np.array(query_emb, dtype='float32').reshape(1, -1)

k=5 # hyper-param 상위 k개의 가장 가까운 벡터수
D,I=index.search(query_emb,k)
# D : 쿼리와의 거리
# I : 검색된 상위 k개 가까운 벡터들의 인덱스 배열
# L2_score=[d for d in D][0]
# ids=[i for i in I][0]
print(D,I) 
