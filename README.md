## RAG와 CF를 이용한 구글맵 장소 추천시스템
 실제 방문 유저의 리뷰를 바탕으로 대화형 인터페이스를 통해 직관적이고 편리하게 장소를 추천해주는 시스템
### 📌 주제 
- RAG를 이용하여 추천시스템에 활용가능한 챗봇 구현
- 구글맵 데이터를 기반으로 멀티모달(좌표,텍스트,평점)을 이용한 추천시스템 구현

### 📌 구현 화면
<img width="723" alt="image" src="https://github.com/user-attachments/assets/92aef65d-91e7-447e-a64d-eb363dc5250e">
<img width="546" alt="image" src="https://github.com/user-attachments/assets/5df55ceb-b522-4ce4-877b-2988483d71bc">
<img width="481" alt="image" src="https://github.com/user-attachments/assets/79a951ec-b957-4cc6-8e88-9ae90f6d95b0">


### 📌 프로젝트 아키텍처
<img width="725" alt="image" src="https://github.com/user-attachments/assets/98314e88-6c1a-4e3c-85a3-02b5b70fd734">

### 📌 RAG 프로세스
<img width="665" alt="image" src="https://github.com/user-attachments/assets/7c3bd8ce-b4bd-44b0-ac68-6833ab086ce7">

<img width="717" alt="image" src="https://github.com/user-attachments/assets/314c2ca1-264e-4cf4-9a03-71e8f257a2a0">

<img width="746" alt="image" src="https://github.com/user-attachments/assets/e9cf64f8-1ffc-4dc4-8d5c-1f4631886a91">



### 📌 CF
<img width="650" alt="image" src="https://github.com/user-attachments/assets/bf5572c2-92b9-4d67-a115-72f3a679fb73">

- item to item
 <img width="692" alt="image" src="https://github.com/user-attachments/assets/511ceb35-f28b-49f6-b6be-efc5df16b6bc">

 <img width="718" alt="image" src="https://github.com/user-attachments/assets/4e8e19b7-0220-4e30-8b97-792fe98a3fcd">

- User to item
  <img width="718" alt="image" src="https://github.com/user-attachments/assets/842e5ae1-8908-4759-b8ab-d8e7f99ad2c9">



### 📌 팀원
| 이름   | Github Username | 역할 |
|--------|-----|-----|
| 이설하 |Seolhada|팀장, 메타 데이터 EDA<br>item 기반 추천 모델 개발, streamlit 구현|
| 양희선 |heesunTukorea|review text 분석 및 임베딩,<br>리뷰 유사도 추천 모델 개발, streamlit 구현|
| 임혜원 |oh-bom| VectorDB 리서치 및 데이터 업로드, Retriever 및 Langchain 리서치,<br> Langchain 연동한 chat-bot 구현, item to item 후보 데이터 셋 구성|
| 장윤정 |YoonjungJang|리뷰 데이터 EDA, 좌표 데이터 분석, Langchain 구현|
| 임수정 |soojeonglim|데이터 전처리 및 실버 데이터 구축, <br>User 기반 추천 모델 개발, LLM 텍스트 리뷰|

- - -

### 🛠️ Languages and Libraries

<div>
 <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" />
 <img src="https://img.shields.io/badge/OpenAI-412991?style=flat-square&logo=openai&logoColor=white" />
 <img src="https://img.shields.io/badge/Jupyter-F37626?style=flat-square&logo=jupyter&logoColor=white" />
 <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" />
 <img src="https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white" />
 <img src="https://img.shields.io/badge/LangChain-3E92CC?style=flat-square&logo=langchain&logoColor=white" />
 <img src="https://img.shields.io/badge/FAISS-009688?style=flat-square&logo=faiss&logoColor=white" />
</div>



### 📌 Ground Rules

1. **[ISSUE-ID][커밋 태그] 내용** / 커밋 태그 : Add, Fix, Change, Improve, Migrate 중 하나로 업로드 합니다. 
2. branch 는 트렁크 기반 개발 전략을 따릅니다. (https://tech.mfort.co.kr/blog/2022-08-05-trunk-based-development/)
3. 디펜던시 관리가 필요한 모듈/라이브러리 환경을 기록한다.
4. 학습 데이터, 모델 객체는 업로드하지 않는다.

Git 정책 가이드
코드리뷰 가이드


