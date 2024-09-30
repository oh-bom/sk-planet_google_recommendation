import os
import streamlit as st
import random
import pydeck as pdk

import pandas as pd
from function import *
import pandas as pd
st.set_page_config(page_title="User-to-Item", page_icon="👫",layout="wide")
df_history = pd.read_csv('gmap_csv_data/user2item_history.csv')
df_users = pd.read_csv('gmap_csv_data/user2item_recommendations2.csv')
# 데이터베이스 연결 설정

# connection = sql.connect(
#     server_hostname = HOST,
#     http_path = HTTP_PATH,
#     access_token = PERSONAL_ACCESS_TOKEN
# )
# Databricks 연결

if "selected_user_id" not in st.session_state:
    st.session_state.selected_user_id = ""
# 유저 데이터 함수

# 유저 데이터를 가져오는 함수
def get_user_data():
    # 실제 user_id, name 데이터를 담고 있는 CSV 파일 읽기
    df_users = pd.read_csv('gmap_csv_data/user2item_recommendations2.csv')
    user_data = {row['user_id']: row['name'] for _, row in df_users[['user_id', 'name']].drop_duplicates().iterrows()}
    return user_data

# 랜덤으로 유저 선택 함수
def random_select_user(user_data):
    selected_user_id = random.choice(list(user_data.keys()))
    nickname = user_data[selected_user_id]
    return selected_user_id, nickname

# 추천 아이템 가져오기 함수
def get_recommendations(user_id):
    # 추천 데이터가 있는 CSV 파일 읽기
    df_recommendations = pd.read_csv('gmap_csv_data/user2item_recommendations2.csv')

    # user_id에 맞는 추천 아이템 필터링
    df_recommendations_filtered = df_recommendations[df_recommendations['user_id'] == user_id].sort_values(by='rank')

    # 필요한 컬럼만 선택
    recommendations = df_recommendations_filtered[['meta_name', 'first_main_category', 'avg_rating', 'hash_tag', 'latitude', 'longitude']].to_dict(orient='records')
    return recommendations

# 유저의 히스토리 가져오기 함수
def get_user_history(user_id):
    # 유저 히스토리가 담긴 CSV 파일 읽기
    df_history = pd.read_csv('gmap_csv_data/user2item_history.csv')

    # user_id에 맞는 히스토리 필터링 및 날짜 기준 내림차순 정렬
    df_history_filtered = df_history[df_history['user_id'] == user_id].sort_values(by='date', ascending=False).head(5)

    # 필요한 컬럼만 선택
    history = df_history_filtered[['first_main_category', 'meta_name', 'date', 'rating', 'text', 'hash_tag']].to_dict(orient='records')
    return history
# pydeck을 사용해 지도 생성 함수
def create_pydeck_map(lat, lon):
    view_state = pdk.ViewState(
        latitude=lat,
        longitude=lon,
        zoom=15,
        pitch=0,
        height=300
    )
    #파이덱 지도 옵션
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=[{"position": [lon, lat]}],
        get_position="position",
        get_radius=75,
        get_fill_color=[0, 0, 255, 200],  # 파란색 마커
        pickable=True,
    )
    #지도 시각화
    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style='mapbox://styles/mapbox/streets-v11',  # 밝은 테마
    )
    return r

# 메인 코드 시작
    
with st.sidebar:
    st.header("추천 요청")
    #아이디 입력
    selected_user_id = st.sidebar.text_input("아이디 입력", st.session_state.selected_user_id, key="_selected_user_id")

    st.session_state.selected_user_id = selected_user_id
    selected_user_id = selected_user_id

    
    if st.button("랜덤 유저 선택"):
        user_data = get_user_data()
        selected_user_id, nickname = random_select_user(user_data)
        st.session_state.selected_user_id = selected_user_id
        selected_user_id = selected_user_id

#선택된 유저의 아이디,이름 가져오기
if selected_user_id:
    try:    
        # user_id와 name 필터링
        selected_user_info = df_users[df_users['user_id'] == selected_user_id][['user_id', 'name']].drop_duplicates().head(1).squeeze()

        if not selected_user_info.empty:
            selected_user_id = selected_user_info['user_id']
            nickname = selected_user_info['name']

            # Streamlit session state에 저장
            st.session_state.selected_user_id = selected_user_id
            st.session_state.nickname = nickname
        try:
            st.sidebar.write_stream(stream_data(f"{nickname}님을 위한 추천 결과"))
            # 화면을 업데이트하기 위해 query_params를 설정합니다.
            st.session_state.query_params = {"user": nickname}
        except:
            pass

        # 유저이름 출력
        if 'selected_user_id' in st.session_state:
            st.title(f"{st.session_state.nickname}님을 위한 추천 아이템들✨")
            
            # 추천 아이템 가져오기
            recommendations = get_recommendations(st.session_state.selected_user_id)
            
    
            #탭창 생성
            tabs = st.tabs(["🏆Rank 1", "🏆Rank 2", "🏆Rank 3", "🏆Rank 4", "🏆Rank 5"])
            
            for idx, tab in enumerate(tabs):
                with tab:
                    col1, col2 = st.columns([3, 2])  # 왼쪽에 지도, 오른쪽에 내용 표시
                    with col1:
                        # pydeck 지도 생성 및 표시
                        map_deck = create_pydeck_map(recommendations[idx]['latitude'], recommendations[idx]['longitude'])
                        # 지도 크기 조정: 세로를 텍스트와 같은 크기로 설정
                        st.pydeck_chart(map_deck, use_container_width=False)
                        
                    with col2:
                        st.subheader(f"🏅Rank {idx+1}")
                        # 카테고리
                        category_emoji = get_category_emoji(recommendations[idx]['first_main_category'])
                        category_html = f"<span style='background-color:#f0f2f6; display:inline-block; padding:0.2em; border-radius:0.25em;'>{category_emoji} {recommendations[idx]['first_main_category']}</span>"
                        st.markdown(f"**카테고리 :**&nbsp;{category_html}", unsafe_allow_html=True)

                        # 장소명
                        title_html = f"<span style='background-color:#f0f2f6; display:inline-block; padding:0.2em; border-radius:0.25em;'>{recommendations[idx]['meta_name']}</span>"
                        st.markdown(f"**장소명 :**&nbsp;{title_html}", unsafe_allow_html=True)

                        # 예상 별점
                        rating_html = f"<span style='background-color:#f0f2f6; display:inline-block; padding:0.2em; border-radius:0.25em;'>⭐ {recommendations[idx]['avg_rating']}</span>"
                        st.markdown(f"**예상 별점 :**&nbsp;{rating_html}", unsafe_allow_html=True)

                        # 해시태그 (색상 변경)
                        
                        cleaned_string = recommendations[idx]['hash_tag'].strip("[]")
                        result_list = re.findall(r"'([^']+)'", cleaned_string)
                        hashtags = ' '.join([f'#{tag}' for tag in result_list])
                        hashtag_html = f"<p style='color:orange;'>{hashtags}</p>"
                        st.markdown(hashtag_html, unsafe_allow_html=True)
                        
            
            st.write("---")
            
            # 유저 히스토리 섹션
            st.subheader(f"{st.session_state.nickname}님이 방문했던 장소")
            
            # 히스토리 컬럼명
            col1, col2, col3, col4, col5, col6 = st.columns([1, 2, 1, 1, 2, 3])
            with col1:
                st.write_stream(stream_data("**카테고리**"))
            with col2:
                st.write_stream(stream_data("**장소명**"))
            with col3:
                st.write_stream(stream_data("**방문일자**"))
            with col4:
                st.write_stream(stream_data("**평점**"))
            with col5:
                st.write_stream(stream_data("**해시태그**"))
            with col6:
                st.write_stream(stream_data("**리뷰**"))
            
            # 히스토리 데이터
            history = get_user_history(st.session_state.selected_user_id)
            for record in history:
                col1, col2, col3, col4, col5, col6 = st.columns([1, 2, 1, 1, 2, 3])
                with col1:
                    # 카테고리에 이모지 추가
                    category_emoji = get_category_emoji(record["first_main_category"])
                    st.write(f"{category_emoji} {record['first_main_category']}")
                with col2:
                    st.write(record["meta_name"])
                with col3:
                    st.write(record["date"])
                with col4:
                    rating_html = f"<div style='background-color:#f0f2f6; display:inline-block; padding:0.2em; border-radius:0.25em;'>⭐ {record['rating']}</div>"
                    st.markdown(rating_html, unsafe_allow_html=True)
                with col5:
                    cleaned_string =record['hash_tag'].strip("[]")
                    result_list = re.findall(r"'([^']+)'", cleaned_string)
                    hashtags = ' '.join([f'#{tag}' for tag in result_list])
                    hashtag_html = f"<p style='color:orange;'>{hashtags}</p>"
                    st.markdown(hashtag_html, unsafe_allow_html=True)
                
                with col6:
                    st.write(record["text"])
                st.write("---")
    except:
        st.title('유저 기반 장소 추천✨')
        st.sidebar.write('해당되는 장소 정보가 없습니다')
else:
    st.title('유저 기반 장소 추천✨')

                
