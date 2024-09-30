import streamlit as st
from secrets_1 import *
import sys
import re
from function import *
sys.path.append("../workspace/workspace/vectorDB") 

from retriever_module_final_KO_EN import KoreanChainClassifier
st.set_page_config(page_title="Chat Bot", page_icon="👾",layout="wide")


ko_chain=KoreanChainClassifier()

# 0x로 시작하는 gmap_id 추출
def clean_gmap_id(text):
    gmap_id_match = re.search(r'0x[0-9a-fA-F]+(?::0x[0-9a-fA-F]+)?', text)
    gmap_id = gmap_id_match.group(0) if gmap_id_match else None

    return gmap_id

# 장소 추천과 관련된 질문인지 파악
def is_place_related_question(user_input):
    keywords = ["가게", "레스토랑", "식당", "추천", "음식점", "장소", "여기", "위치","곳","알려줘","찾아줘","말해줘"]
    return any(keyword in user_input for keyword in keywords)

# chain 검색결과
def generate_answer(user_input):
    res=ko_chain.search(user_input)
    
    return res

st.title(f"Chat Bot👾")

# 대화 목록 초기화
if "messages" not in st.session_state:
    st.session_state.messages = []
if "gmap_id" not in st.session_state:
    st.session_state.gmap_id=None

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("안녕하세요 👋 무엇을 도와드릴까요?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    gmap_id=""
    opening_talk=["안녕","안녕하세요","하이","안녕?","안녕하세요?","하이?","안녕하세요!"]
    
    if prompt in opening_talk: res="안녕하세요 😊 궁금한 것을 질문해주세요!"
    elif is_place_related_question(prompt):
        res=generate_answer(prompt)
        gmap_id=clean_gmap_id(res)
    else:
        res="궁금한 장소에 대한 질문을 해주세요🤔"
       
    if gmap_id: st.session_state.gmap_id=gmap_id # gmap_id가 함께 반환 된경우 세션에 저장
        
    st.session_state.messages.append({"role": "assistant", "content": res})
    
    with st.chat_message("assistant"): st.write_stream(stream_data(res))
    
    if gmap_id: # gmap_id가 있을때 아이템 기반 장소 추천으로 페이지 이동
        st.markdown(f"[위의 장소와 관련된 장소를 더 추천해드릴게요😊](./아이템_기반_장소_추천?gmap_id={gmap_id})")