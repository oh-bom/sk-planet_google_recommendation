from function import *
import streamlit as st
from streamlit_folium import st_folium
import folium
import streamlit.components.v1 as components
from secrets_1 import HOST, HTTP_PATH, PERSONAL_ACCESS_TOKEN

# 4.chatbot과 연결
query_params = st.query_params
chat_gmap_id = query_params.get("gmap_id", None)  # 'None'은 'gmap_id'가 없을 때 반환됩니다.
print("gmap_id", chat_gmap_id)

st.set_page_config(page_title="아이템 기반 Gmap 추천시스템", page_icon="🗺️", layout="wide")


# Databricks 연결
with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=PERSONAL_ACCESS_TOKEN) as conn:
    with conn.cursor() as cursor:
        # 페이지 제목
        st.title("아이템 기반 Gmap 추천시스템🌎")
        
        # 세션 상태 초기화
        if "page" not in st.session_state:
            st.session_state.page = "main"
        if "gmap_id1" not in st.session_state:
            st.session_state.gmap_id1 = ""
        if "recommendations" not in st.session_state:
            st.session_state.recommendations = []
            st.session_state.item_recommend_list = []
            st.session_state.review_recommend_list = []
            st.session_state.hybrid_recommend_list = []
        if "selected_gmap_id" not in st.session_state:
            st.session_state.selected_gmap_id = ""

        # 메인 페이지
        if st.session_state.page == "main":
            # 사이드바 설정
            st.sidebar.title("장소 입력")
            
            #챗봇에서 gmap_id가 연동 될때
            if chat_gmap_id:
                # chat_gmap_id가 존재할 때
                gmap_id1 = chat_gmap_id
                query = f"""
                SELECT address1, gmap_id1, avg_rating1, description1, latitude1, longitude1, name1, num_of_reviews1, price1, state1, url1, main_category1, first_main_category1, region1, city1, hash_tag1 
                FROM `hive_metastore`.`streamlit`.`gmap_id1_info`
                WHERE gmap_id1 = '{gmap_id1}'
                """
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    # 결과가 있을 때
                    address1, gmap_id1, avg_rating1, description1, latitude1, longitude1, name1, num_of_reviews1, price1, state1, url1, main_category1, first_main_category1, region1, city1, hash_tag1 = result
                    st.session_state.gmap_id1 = gmap_id1
                    chat_gmap_id=None
                else:
                    # 결과가 없을 때
                    st.session_state.gmap_id1 = ""  # gmap_id 초기화
                    st.sidebar.write("해당 데이터는 아직 추천 결과가 존재하지 않습니다.")
                    gmap_id1 = None  # 이후 코드에서 쿼리를 수행하지 않도록 설정
            else:
                gmap_id1 = st.sidebar.text_input("장소 입력", st.session_state.gmap_id1, key="_gmap_id1")
                st.session_state.gmap_id1 = gmap_id1
            chat_gmap_id=None


            # 랜덤 gmap_id1 선택 버튼 추가
            if st.sidebar.button("🎲랜덤 선택"):
                query = """SELECT gmap_id1 FROM `hive_metastore`.`streamlit`.`gbdt_sample` ORDER BY RAND() LIMIT 1"""
                cursor.execute(query)
                gmap_id1 = cursor.fetchone()[0]
                st.session_state.gmap_id1 = gmap_id1
                gmap_id1 = gmap_id1

            if gmap_id1:
                try:
                    # 입력한 Gmap1에 대한 정보 조회
                    query = f"""
                    SELECT address1, gmap_id1, avg_rating1, description1, latitude1, longitude1, name1, num_of_reviews1, price1, state1, url1, main_category1, first_main_category1, region1, city1,hash_tag1 
                    FROM `hive_metastore`.`streamlit`.`gmap_id1_info`
                    WHERE gmap_id1 = '{gmap_id1}'
                    """
                    cursor.execute(query)
                    address1, gmap_id1, avg_rating1, description1, latitude1, longitude1, name1, num_of_reviews1, price1, state1, url1, main_category1, first_main_category1, region1,city1,hash_tag1  = cursor.fetchone()
                    st.sidebar.write(f'선택된 장소: {name1}')
                    # 추천 결과 생성
                    item_recommend_list, hybrid_recommend_list, review_recommend_list, recommendations = [], [], [], []
                    
                    # GBDT 쿼리
                    query = f"""
                    SELECT gmap_id2, prob, rank
                    FROM `hive_metastore`.`streamlit`.`gbdt_sample`
                    WHERE gmap_id1 = '{gmap_id1}'
                    ORDER BY rank DESC
                    LIMIT 5
                    """
                    cursor.execute(query)
                    gbdt_gmap2_list = cursor.fetchall()
                    gmap_id2_values = [t[0] for t in gbdt_gmap2_list]
                    gbdt_prob = [t[1] for t in gbdt_gmap2_list]
                    gbdt_rank = [str(t) for t in range(len(gbdt_gmap2_list),0,-1)]
                    gbdt_prob_dict,gbdt_rank_dict = create_gmap_id_prob_dict(gmap_id2_values, gbdt_prob,gbdt_rank)
                    
                    gmap_id2_tuple = tuple(gmap_id2_values)

                    query = f"""
                    SELECT address2,gmap_id2,avg_rating2,description2,latitude2,longitude2,name2,num_of_reviews2,price2,state2,url2,main_category2,first_main_category2,region2,city2,hash_tag2 
                    FROM `hive_metastore`.`streamlit`.`gmap_id2_info`
                    WHERE gmap_id2 in {gmap_id2_tuple}
                    LIMIT 5
                    """
                    cursor.execute(query)
                    similar_items = cursor.fetchall()
                    item_recommend_list.extend(similar_items)

                    # 하이브리드 쿼리
                    query = f"""
                    SELECT gmap_id2, prob,rank
                    FROM `hive_metastore`.`streamlit`.`hybrid_sample`
                    WHERE gmap_id1 = '{gmap_id1}'
                    ORDER BY rank DESC
                    LIMIT 5
                    """
                    cursor.execute(query)
                    gbdt_gmap2_list = cursor.fetchall()
                    gmap_id2_values = [t[0] for t in gbdt_gmap2_list]
                    hybrid_prob = [t[1] for t in gbdt_gmap2_list]
                    hybrid_rank = [str(t) for t in range(len(gbdt_gmap2_list),0,-1)]
                    hybrid_prob_dict,hybrid_rank_dict = create_gmap_id_prob_dict(gmap_id2_values, hybrid_prob,hybrid_rank)
                    gmap_id2_tuple = tuple(gmap_id2_values)

                    query = f"""
                    SELECT address2,gmap_id2,avg_rating2,description2,latitude2,longitude2,name2,num_of_reviews2,price2,state2,url2,main_category2,first_main_category2,region2,city2,hash_tag2 
                    FROM `hive_metastore`.`streamlit`.`gmap_id2_info`
                    WHERE gmap_id2 in {gmap_id2_tuple}
                    LIMIT 5
                    """
                    cursor.execute(query)
                    similar_items = cursor.fetchall()
                    
                    hybrid_recommend_list.extend(similar_items)

                    # 리뷰 텍스트 쿼리
                    query = f"""
                    SELECT gmap_id2, cosine_top4,rank
                    FROM `hive_metastore`.`streamlit`.`text_sample`
                    WHERE gmap_id1 = '{gmap_id1}'
                    ORDER BY rank DESC
                    LIMIT 5
                    """
                    cursor.execute(query)
                    gbdt_gmap2_list = cursor.fetchall()
                    gmap_id2_values = [t[0] for t in gbdt_gmap2_list]
                    review_prob = [t[1] for t in gbdt_gmap2_list]
                    review_rank = [str(t) for t in range(len(gbdt_gmap2_list),0,-1)]
                    review_prob_dict,review_rank_dict = create_gmap_id_prob_dict(gmap_id2_values, review_prob,review_rank)
                    
                    gmap_id2_tuple = tuple(gmap_id2_values)

                    query = f"""
                    SELECT address2,gmap_id2,avg_rating2,description2,latitude2,longitude2,name2,num_of_reviews2,price2,state2,url2,main_category2,first_main_category2,region2,city2,hash_tag2 
                    FROM `hive_metastore`.`streamlit`.`gmap_id2_info`
                    WHERE gmap_id2 in {gmap_id2_tuple}
                    LIMIT 5
                    """
                    cursor.execute(query)
                    similar_items = cursor.fetchall()
                    review_recommend_list.extend(similar_items)
                    
                    #데이터 종합
                    total_prob = [gbdt_prob, hybrid_prob, review_prob]
                    st.session_state.item_recommend_list = item_recommend_list
                    st.session_state.review_recommend_list = review_recommend_list
                    st.session_state.hybrid_recommend_list = hybrid_recommend_list
                    recommendations = [st.session_state.item_recommend_list, st.session_state.review_recommend_list, st.session_state.hybrid_recommend_list]
                    st.session_state.recommendations = recommendations

                    
                    merged_dict = {**gbdt_prob_dict, **hybrid_prob_dict,**review_prob_dict}
                    merged_rank_dict = {**gbdt_rank_dict, **hybrid_rank_dict,**review_rank_dict}

                    first_value = list(merged_dict.keys())[0]

                    

                    # 레이아웃
                    col1, col2 = st.columns([7, 3])
                    con_size = 500

                    with col1:
                        with st.container(height=con_size):
                            #비율 조정
                            col_dummy, col_main, col_dummy2 = st.columns([0.5, 8, 0.2])
                            #지도 시각화
                            with col_main:
                                m = folium.Map(location=[latitude1, longitude1], zoom_start=12)
                                #각 그룹에대한 색깔 설정
                                group1 = folium.FeatureGroup(name="🟩GBDT")
                                group2 = folium.FeatureGroup(name="🟧Hybrid")
                                group3 = folium.FeatureGroup(name="🟦Review")

                                #지도의 마커 찍는 함수
                                create_emoji_marker(latitude1, longitude1, name1, address1, gmap_id1, first_main_category1,'red','',url1).add_to(m)
                                #각 그룹별로 모델별로 예측 추천 결과 시각화 및 그룹,랭킹 지정
                                for j, session_select in enumerate(recommendations):
                                    group = group1 if j == 0 else group2 if j == 1 else group3
                                    color = 'green' if j == 0 else 'orange' if j == 1 else 'blue'
                                    for i, (address, gmap_id, avg_rating, description, latitude, longitude, name, num_of_reviews, price, state, url, main_category, first_main_category, region,city,hash_tag ) in enumerate(session_select):
                                        #create_marker(latitude, longitude, name, address, gmap_id, color).add_to(group)
                                        g_rank = f'{merged_rank_dict.get(str(gmap_id), None)}️⃣'
                                        create_emoji_marker(latitude, longitude, name, address, gmap_id, first_main_category,color,g_rank,url).add_to(group)

                                m.add_child(group1)
                                m.add_child(group2)
                                m.add_child(group3)
                                folium.LayerControl(collapsed=False).add_to(m)
                                
                                map_data = st_folium(m, width=600, height=480)
                            
                        
                    # gmap_id1은 빨간색 설정
                    all_places = [(gmap_id1, f'🟥{name1}')]

                    # 각 모델별 결과를 담을 리스트
                    green_items = []
                    orange_items = []
                    blue_items = []

                    # 해당 추천 결과의 정보를 볼 수 있는 사이드 바 구현
                    # 색깔 할당
                    for index, sublist in enumerate(recommendations):
                        for r_c,item in enumerate(sublist):
                            if index < 1:  # 첫 5개 아이템 (0~4)
                                emoji = '🟩'
                                green_items.append((item[1], f'{emoji}/{merged_rank_dict.get(str(item[1]), None)}️⃣{item[6]}'))
                            elif index < 2:  # 다음 5개 아이템 (5~9)
                                emoji = '🟧'
                                orange_items.append((item[1], f'{emoji}/{merged_rank_dict.get(str(item[1]), None)}️⃣{item[6]}'))
                            else:  # 그 외 아이템
                                emoji = '🟦'
                                blue_items.append((item[1], f'{emoji}/{merged_rank_dict.get(str(item[1]), None)}️⃣{item[6]}'))

                    # 색깔별로 예측값에 따른 랭킹으로 정렬
                    green_items.sort(key=lambda x: merged_rank_dict.get(str(x[0]), float('inf')))
                    orange_items.sort(key=lambda x: merged_rank_dict.get(str(x[0]), float('inf')))
                    blue_items.sort(key=lambda x: merged_rank_dict.get(str(x[0]), float('inf')))

                    # 정렬후 합침
                    all_places.extend(green_items + orange_items + blue_items)

                    
                    with col2:
                        with st.container(height=con_size):
                            # 선택 박스를 추가하여 사용자가 장소를 선택
                            
                            selected_place = st.selectbox("장소 선택", all_places, format_func=lambda x: x[1])
                            
                            if selected_place:
                                st.session_state.selected_gmap_id = selected_place[0]
                            update_info_container(st.session_state.selected_gmap_id,merged_dict)
               

                    #각 모델별로 익스펜더 박스 생성후 이모지,예측값,랭킹,상세정보를 비교
                    title_list = ['**GBDT Model**', '**Hybrid Model**', '**Review Similarity**']
                    for idx, recommend_session in enumerate(recommendations):
                        # 예측값이 높은순으로 정렬
                        sorted_items = sorted(enumerate(recommend_session), key=lambda x: total_prob[idx][x[0]], reverse=True)
                        if title_list[idx] =='**Review Similarity**':
                            p_name = '유사도'
                        else:
                            p_name = 'Prob'
                        with st.expander(title_list[idx]):
                            cols = st.columns(5)
                            #모델에 대한 결과 5개를 카테고리 이모지와 함께 나열
                            for i, (index, (address, gmap_id, avg_rating, description, latitude, longitude, name, num_of_reviews, price, state, url, main_category, first_main_category, region, city, hash_tag)) in enumerate(sorted_items):
                                category_emoji = get_category_emoji(first_main_category)
                                #이모지 크기 조정
                                emoji_code = resize_emoji(category_emoji, font_size=80)

                                with cols[i % 5]:
                                    #랭킹
                                    st.write(f'{i+1}️⃣')
                                    #이모지
                                    st.markdown(emoji_code, unsafe_allow_html=True)
                                    html_code = f"""
                                    <div style='text-align: center; color: gray; font-size: 18px;'>
                                        {p_name} : {round(float(total_prob[idx][index])* 100, 2)}%<br>
                                    </div>
                                    """
                                    st.markdown(html_code, unsafe_allow_html=True)
                                    #나머지 상세 정보
                                    st.write_stream(stream_data(f'*Name* : [{name}]({url})'))
                                    st.write_stream(stream_data(f'*Category* : {category_emoji}{main_category}'))
                                    st.write_stream(stream_data(f'*City* : 🏙️{city}'))
                                    st.write_stream(stream_data(f'*Rating* : ⭐{avg_rating}'))
                                    st.write_stream(stream_data(f'*Adress* : 🏡{address}'))
                except:
                    st.sidebar.write('해당되는 장소 정보가 없습니다')
                



