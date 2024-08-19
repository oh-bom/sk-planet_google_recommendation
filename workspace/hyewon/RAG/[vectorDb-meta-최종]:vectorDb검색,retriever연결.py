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

embed_openai=OpenAIEmbeddings()

# COMMAND ----------

# MAGIC %md
# MAGIC ### _vector db_ 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 검색
# MAGIC :vector store, index load

# COMMAND ----------

# MAGIC %md
# MAGIC local vector store load

# COMMAND ----------

vector_store_path="meta_sample2"

db=FAISS.load_local(vector_store_path,embed_openai, allow_dangerous_deserialization=True)
# 내부 인덱스에 접근하여 Direct Map 초기화
db.index.make_direct_map()

retriever = db.as_retriever(search_type="mmr", search_kwargs={"k": 1})
retriever.invoke("find some place where wheelcahir accessible place and avg_rating is over 3.0")

# COMMAND ----------

# MAGIC %md
# MAGIC llm model prompt template

# COMMAND ----------

# Prompt template
from langchain.prompts import ChatPromptTemplate

helpful_answer='The place that is open 24 hours, has over 10 reviews, and is located in Texas is:**JFIT STUDIOS**\n- Address: 9642 Jones Rd, Houston, TX 77065\n- Rating: 4.1\n- Number of Reviews: 18\n- Open 24 hours'

template="""
        Use the following pieces of context to answer the question at the end. If you don't know the answer, just say that you don't know, don't try to make up an answer.

        \n{context}

        \nQuestion: {question}

        Helpful Answer: 
        \nrecommended place: ** name **
        \n
        \n- Address: 
        \n- Rating:
        \n- Number of Reviews:
        \n- hours:
        \n- state:
        \n- category:
        \n- [Google Maps Link]
        \n- [gmap_id]
        """

prompt = ChatPromptTemplate.from_template(template)

# COMMAND ----------

def format_docs(docs):
    return '\n\n'.join([d.page_content for d in docs])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 검색

# COMMAND ----------

# MAGIC %md
# MAGIC 1. page_content field + openAI 임베딩

# COMMAND ----------

# 검색 page_content filed, embed:open_ai

from langchain.chains import RetrievalQA
from langchain.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

test_query="find wheelchair accessible avg_rating is over 3.0" #우선 한문장으로 예시
    
def search_query(vector_store_path,query):
    db=FAISS.load_local(vector_store_path,embed_openai, allow_dangerous_deserialization=True)
    # db.index.make_direct_map()

    retriever = db.as_retriever(search_type="mmr", search_kwargs={"k": 2})
    llm_model=ChatOpenAI(model_name="gpt-4o-mini")

    # ver1.retrievalQA chain 생성
    retrieval_qa=RetrievalQA.from_chain_type(llm=llm_model,chain_type="stuff",retriever=retriever)
    result=retrieval_qa.run(query)
    print(result)

    print("-------------------------------------------------------------------")
    # ver2.chain
    docs=retriever.get_relevant_documents(query)
    chain= prompt | llm_model | StrOutputParser()

    response = chain.invoke({'context': (format_docs(docs)), 'question':query})
    print(response)

search_query(vector_store_path,test_query)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. metadata field + HF 임베딩 (사용 x, 참고용)

# COMMAND ----------

# 검색 meta_data에 올린 정보
from langchain.chains import RetrievalQA
from langchain.vectorstores import FAISS
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI

vector_store_path="meta_1000_v2"
test_query="find some restaurant where wheelchair accessible avg_rating is over 3.0" #우선 한문장으로 예시
    
def search_query_meta_field(vector_store_path,query):
    db=FAISS.load_local(vector_store_path,embed_model, allow_dangerous_deserialization=True)
    db.index.make_direct_map()

    retriever = db.as_retriever(search_type="mmr", search_kwargs={"k": 2})
    llm_model=ChatOpenAI(model_name="gpt-4o-mini")

    # ver1.retrievalQA chain 생성
    retrieval_qa=RetrievalQA.from_chain_type(llm=llm_model,chain_type="stuff",retriever=retriever)
    result=retrieval_qa.run(query)
    print(result)

    print("-------------------------------------------------------------------")
    # ver2.chain
    docs=retriever.get_relevant_documents(query)
    chain= prompt | llm_model | StrOutputParser()

    response = chain.invoke({'context': (format_docs_meta_field(docs)), 'question':query})
    print(response)

search_query_meta_field(vector_store_path,test_query)

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
