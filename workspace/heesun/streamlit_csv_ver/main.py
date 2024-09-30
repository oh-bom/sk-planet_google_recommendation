import streamlit as st
from PIL import Image

#팝업창 텍스트 및 이미지 설정
st.set_page_config(page_title="알감자의 추천시스템", page_icon="🥔",layout="wide")

#프로젝트 제목
st.markdown("""
# LLM과 협업 필터링을 이용한 구글맵 추천<br> 시스템 개발🥔
""", unsafe_allow_html=True)
st.write(' ')


#프로젝트 소개
x2 = st.expander('🍟프로젝트 소개')
x2.markdown('''
구글맵의 텍스트 리뷰, 평점, 메타 정보를 활용하여 개인화된 추천을 제공하는 챗봇 개발 프로젝트. <br> Item to Item, User to Item 두 종류의 협업 필터링과 LLM을 보완한 RAG기법을 통합해 사용자에게 맞춤형 서비스를 제공.
''',unsafe_allow_html=True)

#사이드바
with st.sidebar:
    st.write("목록")
st.write(' ')
option = st.selectbox(
    "🥚알감자 정보",
    ("페이지 정보", "팀원 소개", "기타"),
)

#각 페이지 정보 입력
if option == "페이지 정보":
    tab1, tab2, tab3, tab4= st.tabs(["1.📌 아이템 기반 추천", "2.📝 리뷰요약", "3.🕵️ 유저 기반 추천","4.👾 Chatbot"])

    with tab1:
        st.subheader("📌 아이템 기반 추천")
        
        st.markdown(f'''1️⃣ **구글맵에서 선택한 장소와 관련해서 장소를 추천해주는 서비스** <br>
                    2️⃣ **Item-to-item기반의 추천 시스템을 통해 해당 장소와 가장 연관성 있는 장소를 순위별 추천과 정보 제공**<br>
                    3️⃣ **분류 기반 GBDT모델, Review Text 데이터를 사용한 유사도 추천, 두 데이터를 종합한 Hybrid 모델로 3가지 추천시스템 결과 제공**''', unsafe_allow_html=True)
        with st.container(height=600):
            st.image('images_data/item_reco.png')
    with tab2:
        st.subheader("📝 리뷰요약")

        st.markdown(f'''1️⃣ **Review Text기반의 아이템 추천으로 생생된 추천 결과에 대해서 리뷰를 요약해 제공하는 서비스**<br>
                    2️⃣ **선택한 장소와 가장 유사한 리뷰 정보를 가진 데이터의 3줄 리뷰 요약과 10개의 키워드 제공**<br>
                    3️⃣ **해당 서비스는 Claude Api를 활용하여 리뷰 요약과 키워드 추출**''', unsafe_allow_html=True)
        with st.container(height=600):
            st.image('images_data/review_summay.png')
    with tab3:
        st.subheader("🕵️ 유저 기반 추천")
        
        st.markdown(f'''1️⃣ **유저의 정보를 사용하여 유저의 맞춤형 장소 추천을 제공하는 서비스**<br>
                    2️⃣ **사용자 맞춤 추천 장소 5개와 해당 사용자가 가장 최근에 작성한 리뷰 데이터 5개 제공**<br>
                    3️⃣ **Factorization Machine(FM) 모델을 기반으로 사용자와 아이템간의 상호작용 모델링 하여 사용자 맞춤형 추천 제공**''', unsafe_allow_html=True)
        with st.container(height=650):
            st.image('images_data/user_recommend.png')
    with tab4:
        st.subheader("👾 Chatbot")
        
        st.markdown(f'''1️⃣ **구글 맵에 대한 다양한 메타/리뷰 정보를 검색해주는 챗봇 서비스**<br>
                    2️⃣ **LLM과 RAG를 결합하여 세분화 되고 정확한 검색 결과를 제공**<br>
                    3️⃣ **또한, 검색 결과와 관련된 장소 추천 페이지를 연결하여 보다 나은 사용자 경험을 추구**''', unsafe_allow_html=True)
        with st.container(height=600):
            st.image('images_data/chat_bot_img.png')
# if option == "팀원 소개":
if option =='팀원 소개':
    with st.container():
        st.image('images_data/members.png')
if option == "기타":
    with st.container(height=100):
        github_url = 'https://github.com/da-analysis/asac_5_dataanalysis'
        st.markdown(f"**Github** :🔗[Visit my GitHub repository]({github_url})")
        
        data_url='https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/googlelocal/'
        st.markdown(f"**데이터 출처** : 🔗[Google Local Data]({data_url})")