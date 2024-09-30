
import sys
# sys.path.append('/usr/lib/python3.9/site-packages')


from langchain_core.runnables import RunnableParallel, RunnablePassthrough
from langchain.prompts import ChatPromptTemplate
from langchain_community.vectorstores import FAISS

from langchain_core.output_parsers import StrOutputParser
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain_community.vectorstores import FAISS

from langchain_core.runnables import RunnableLambda
from langchain.retrievers.multi_query import MultiQueryRetriever


import os
os.environ["OPENAI_API_KEY"]="sk-svcacct-zmV4n7IGd8VMdc7_hX9WcwOedVIzkbI_hmuRzmiaJ59Pph7xf5SCn3YAtFk3Ly4v3cItlnCr3T3BlbkFJ9yJ39HTd1u__WN02JN9c9sGN-ONU_htslf8ExXKBxqRfsLwq3hLqdtXl-qkaJUGNZtgHcDlxQA"


## meta chain, reveiw chain 

class Retriever:
    def __init__(self, retriever_type):
        self.retriever_type = retriever_type

        # Initialize common attributes first
        self.embed_model = OpenAIEmbeddings()  # Embedding model initialization
        self.llm_model = ChatOpenAI(model_name="gpt-4o-mini")  # LLM model initialization
        
        
        # Set vector store path based on the retriever type
        if retriever_type == "review":
            self.vector_store_path=os.path.abspath("vectorDB/review2")
            # Initialize the database (FAISS) before using it in retrievers
            self.db = FAISS.load_local(self.vector_store_path, self.embed_model, allow_dangerous_deserialization=True)

            self.retriever = MultiQueryRetriever.from_llm(
                retriever=self.db.as_retriever(search_type="mmr"),
                llm=self.llm_model
           )
            self.prompt_template=""""
            You are an expert in 'review information' \
            You will recommend places to the user based on their questions. When recommending places, it will do so based on Google Maps review data."
            This review data contains information about various places.
            Explaining columns in the data, 

            gmap_id: Unique map ID for your store.
            rating: the rating of a place on a scale of 1 to 5.
            review : reviews for the place. The review content includes information related to 'service(great customer service, great service, good service, friendly staff)', 'food taste(great food, good food, delicious food)', 'experience(great experience, great place, nice place, good place, here again next time, back again next time, back here next time)', 'tips for the place', 'time(Date the review was uploaded)', 'waiting time', 'great place to go with someone(friend, family)'

            response_of_store_owner : place owner's response to user reviews.
            time : Date the review was uploaded (review date, year).
    
            \

            Always answer questions starting with "Accroding to review information..."
            For 'review summary', please summarize the review contents mainly in relation to the questions\
            Respond to the following question:{context}

            Question: {question}
            Answer:
            \nrecommended place: ** name **
            \n
            \n- review summary:
            \n- [gmap_id]
            
            """


        elif retriever_type == "meta":
            self.vector_store_path=os.path.abspath("vectorDB/meta2")
            # Initialize the database (FAISS) before using it in retrievers
            self.db = FAISS.load_local(self.vector_store_path, self.embed_model, allow_dangerous_deserialization=True)
            self.retriever = MultiQueryRetriever.from_llm(
                retriever=self.db.as_retriever(),
                llm=self.llm_model
            )
             # self.retriever = self.db.as_retriever(search_type="mmr", search_kwargs={"k": 2})
            self.prompt_template=""""
            You are an expert in 'meta information'. \
            You will recommend places to the user based on their questions. When recommending places, it will do so based on Google Maps metadata."
            This meta data contains information about various places.
            Explaining columns in the data, 

            name : place name 
            category_filtered: The category of the place such as restaurants, retail shops, park, beach, etc. 
            MISC_filtered: Various other information about the store such as Accessibility, Activities, Amenities, Atmosphere, Crowd, Dining_options, From_the_business, Getting_here,Health_and_safety,Highlights,Lodging_options,Offerings,Payments,Planning,Popular_for ,Recycling ,Service_options
            address: Address of the place 
            avg_rating: the average rating of a place on a scale of 1 to 5.
            description: A brief description of the place
            gmap_id: Unique map ID for your store
            hours_filtered: place operating hours, business hours (The operating days and times) 
            latitude: Latitude of place location
            longitude: longitude of place location
            num_of_reviews: Number of google map reviews for the place
            price: Prices for items sold in a store. 
            state: Current operating status
            url: url link to the place
            relative_results: gmap_ids of places similar to the given place
    
            \

            Always answer questions starting with "Accroding to meta information...".\
            Respond to the following question:{context}

            Question: {question}
            Answer:
            \nrecommended place: ** name **
            \n
            \n- Address: 
            \n- avg_rating:
            \n- Number of Reviews:
            \n- hours:
            \n- category:
            \n- [Google Maps Link]
            \n- [gmap_id]

            \n\nPlease provide only one recommendation by returning the 1 most relevant result.
            """

        else:
            raise ValueError("Invalid retriever type. Choose 'review' or 'meta'.")


        # Set up the prompt template
        self.prompt_template = ChatPromptTemplate.from_template(self.prompt_template)

    def format_docs(self, docs):
        return '\n\n'.join([d.page_content for d in docs])

    # 번역을 위한 LLM 호출 함수
    def translate_with_llm(self, text, target_language):
        # 번역을 위해 OpenAI 모델을 사용하여 LLM을 호출하는 프롬프트 설정
        translation_prompt = f"Translate the following text to {target_language}:\n\n{text}"
        translated_text = self.llm_model.invoke(translation_prompt)
        return translated_text

    # 한국어 -> 영어 번역
    def translate_to_english(self, text):
        return self.translate_with_llm(text, "English")

    # 영어 -> 한국어 번역
    def translate_to_korean(self, text):
        return self.translate_with_llm(text, "Korean")

    def get_chain(self):
        chain = (
            {'context': lambda x: self.format_docs(self.retriever.invoke(self.translate_to_english(x["question"]))),
             'question': lambda x: x["question"]}
            | self.prompt_template
            | self.llm_model
        )
        return chain
    

    def search(self, query):
        chain = self.get_chain()
        res = chain.invoke({"question": query})
        return res.content



# Meta 정보와 리뷰 정보를 처리하는 체인을 리턴하는 클래스
class KoreanChainClassifier:
    def __init__(self):
        # 모델 이름을 전달하여 초기화
        self.llm_model = ChatOpenAI(model_name="gpt-4o-mini")
        self.meta_retriever = Retriever("meta")
        self.review_retriever = Retriever("review")

    # 분류 체인 정의
    def get_classify_chain(self):
        # 템플릿 정의
        prompt = ChatPromptTemplate.from_template(
            """Classify the given user question as either `meta information`, `review information`. Please do not respond with more than one word.
            For reference, 
            'meta information' includes 'place name', 'place category', 'place rating', 'place address', 'place operating hours', 'price', 'place status', 'place MISC information(Accessibility, Activities, Amenities,Payments)', etc.  

            ‘review information’ includes 'service(great customer service, great service, good service, friendly staff)', 'food taste(great food, good food, delicious food)', 'experience(great experience, great place, nice place, good place, here again next time, back again next time, back here next time)', 'tips for the place', 'time(Date the review was uploaded)', 'cleanliness', 'waiting time', 'great place to go with someone(friend, family)' etc. 
            <question>
            {question}
            </question>

            Classification:"""
        )

        # 체인 생성
        cl_chain = (
            prompt
            | self.llm_model
            | StrOutputParser()
        )

        return cl_chain

    # 정보 분류에 따라 체인 라우팅
    def route(self, info):
        # 메타 정보가 포함된 경우 메타 체인 반환
        if "meta information" in info["topic"].lower():
            return self.meta_retriever.get_chain()
        # 리뷰 정보가 포함된 경우 리뷰 체인 반환
        elif "review information" in info["topic"].lower():
            return self.review_retriever.get_chain()
        # 그 외의 경우 메타 체인 반환
        else:
            return self.meta_retriever.get_chain()

    # 번역을 위한 LLM 호출 함수
    def translate_with_llm(self, text, target_language):
        # 번역을 위해 OpenAI 모델을 사용하여 LLM을 호출하는 프롬프트 설정
        translation_prompt = f"Translate the following text to {target_language}:\n\n{text}"
        translated_text = self.llm_model.invoke(translation_prompt)
        return translated_text

    # 한국어 -> 영어 번역
    def translate_to_english(self, text):
        return self.translate_with_llm(text, "English")

    # 영어 -> 한국어 번역
    def translate_to_korean(self, text):
        return self.translate_with_llm(text, "Korean")


    # 전체 체인 정의
    def get_full_chain(self):
        full_chain = (
            {"topic": self.get_classify_chain(), "question": lambda x: x["question"]}
            | RunnableLambda( 
                # 라우팅 함수를 전달
                self.route
            )
            | StrOutputParser()
        )

        return full_chain

    # 전체 체인을 실행하고 결과 반환
    def search(self, query):
        full_chain = self.get_full_chain()
        result = full_chain.invoke({"question": query})

        # 최종 결과를 한국어로 번역
        translated_result = self.translate_to_korean(result).content

        return translated_result
