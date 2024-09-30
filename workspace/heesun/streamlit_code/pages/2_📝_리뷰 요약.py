
from function import *
import streamlit as st
from streamlit_folium import st_folium
import folium
import streamlit.components.v1 as components
from secrets_1 import HOST, HTTP_PATH, PERSONAL_ACCESS_TOKEN,REVIEW_API_KEY
import anthropic


st.set_page_config(page_title="Gmap 리뷰 요약 시스템", page_icon="📝", layout="wide")

#클로드 api input_prompt입력 및 사용
def get_summary_and_keywords(review_text):
    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        #최대 토큰값 지정
        max_tokens=1000,
        temperature=0,
        system="한국어로 텍스트를 3문장으로 요약해 주세요. 각 문장은 10단어 이내로 해 주세요. 또한, 내용에서 중요한 키워드 10개를 추출해 주세요, 키워드 10개에 대한 제목은 주요 키워드로 해주세요.  \n" ,
        messages=[
            {"role": "user", "content": [{"type": "text", "text": review_text}]}
        ]
    )
    content = message.content[0].text
    content = content.replace('\\n', '\n')
    content = content.replace('[TextBlock(text=\'', '')
    content = content.replace(', type=\'text\')]', '')
    return content
#클로드 api 결과값 반환 및 전처리               
def truncate_review_text(review_text, max_tokens=2000):
    # 텍스트를 공백 기준으로 토큰화
    tokens = review_text.split()
    
    # 토큰 수가 최대 토큰 수를 초과하는 경우
    if len(tokens) > max_tokens:
        # 최대 토큰 수까지의 텍스트를 재구성
        truncated_text = ' '.join(tokens[:max_tokens])
        
        # 마지막 문장 구분자('.')를 기준으로 잘라냄
        last_period_index = truncated_text.rfind('.')
        if last_period_index != -1:
            truncated_text = truncated_text[:last_period_index+1]
        
        return truncated_text
    
    # 토큰 수가 초과하지 않으면 원래 텍스트 반환
    return review_text
                
# Databricks 연결
with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=PERSONAL_ACCESS_TOKEN) as conn:
    with conn.cursor() as cursor:
        # Streamlit 앱
        st.title("Gmap 리뷰 요약 시스템📝")

        # 세션 상태 초기화
        if "page" not in st.session_state:
            st.session_state.page = "main"
        if "gmap_id1" not in st.session_state:
            st.session_state.gmap_id1 = ""
        if "recommendations" not in st.session_state:
            st.session_state.recommendations = []
            st.session_state.review_recommend_list = []
        if "selected_gmap_id" not in st.session_state:
            st.session_state.selected_gmap_id = ""

        # 메인 페이지
        if st.session_state.page == "main":
            # 사이드바 설정
            st.sidebar.title("장소 입력")
            gmap_id1 = st.sidebar.text_input("장소 입력", st.session_state.gmap_id1, key="_gmap_id1")
            
            st.session_state.gmap_id1 = gmap_id1
            with st.sidebar:
            # 랜덤 gmap_id1 선택 버튼 추가
                if st.button("🎲랜덤 선택"):
                    query = """SELECT gmap_id1 FROM `hive_metastore`.`streamlit`.`gmap_id1_info` ORDER BY RAND() LIMIT 1"""
                    cursor.execute(query)
                    gmap_id1 = cursor.fetchone()[0]
                    st.session_state.gmap_id1 = gmap_id1
                    gmap_id1 = gmap_id1


                        

            if gmap_id1:
                try:
                    # 입력한 Gmap1에 대한 정보 조회
                    query = f"""
                    SELECT address1, gmap_id1, avg_rating1, description1, latitude1, longitude1, name1, num_of_reviews1, price1, state1, url1, main_category1, first_main_category1, region1 
                    FROM `hive_metastore`.`streamlit`.`gmap_id1_info`
                    WHERE gmap_id1 = '{gmap_id1}'
                    """
                    cursor.execute(query)
                    address1, gmap_id1, avg_rating1, description1, latitude1, longitude1, name1, num_of_reviews1, price1, state1, url1, main_category1, first_main_category1, region1 = cursor.fetchone()
                    # 추천 결과 생성
                    item_recommend_list, hybrid_recommend_list, review_recommend_list, recommendations = [], [], [], []
                    

                    # 리뷰 텍스트 쿼리
                    query = f"""
                    SELECT gmap_id2, cosine_top4
                    FROM `hive_metastore`.`streamlit`.`text_sample`
                    WHERE gmap_id1 = '{gmap_id1}'
                    ORDER BY rank DESC
                    LIMIT 1
                    """
                    cursor.execute(query)
                    review_gmap2_list = cursor.fetchone()
                    
                    #유사한 장소
                    query = f"""
                    SELECT address2,gmap_id2,avg_rating2,description2,latitude2,longitude2,name2,num_of_reviews2,price2,state2,url2,main_category2,first_main_category2,region2
                    FROM `hive_metastore`.`streamlit`.`gmap_id2_info`
                    WHERE gmap_id2 ='{review_gmap2_list[0]}' and name2 != '{name1}'
                    """
                    cursor.execute(query)
                    address2,gmap_id2,avg_rating2,description2,latitude2,longitude2,name2,num_of_reviews2,price2,state2,url2,main_category2,first_main_category2,region2 = cursor.fetchone()
                    
                    #리뷰 불러오기
                    query = f"""
                    SELECT document1, document2
                    FROM `hive_metastore`.`streamlit`.`text_sample_document`
                    WHERE gmap_id2 ='{review_gmap2_list[0]}' and gmap_id1 ='{gmap_id1}'
                    """
                    cursor.execute(query)
                    document1, document2= cursor.fetchone()
                    

                    # 레이아웃
                    col1, col2 = st.columns([5,5])
                    con_size = 400
                    width, height= 420,380
                    with col1:
                        st.subheader(f'**{name1}**')
                        with st.container(height=con_size):
                            col_dummy, col_main, col_dummy2 = st.columns([0.5, 8, 0.2])
                            #gmap_id1 지도 정보
                            with col_main:
                                m = folium.Map(location=[latitude1, longitude1], zoom_start=16)

                                marker = create_emoji_marker(latitude1, longitude1, name1, address1, gmap_id1,first_main_category1, 'red','',url1)
                                marker.add_to(m)

                                
                                map_data = st_folium(m, width=width, height=height)
                    with col2:
                        st.subheader(f'**{name2}**')
                        with st.container(height=con_size):
                            col_dummy, col_main, col_dummy2 = st.columns([0.5, 8, 0.2])
                            #gmap_id2 지도 정보
                            with col_main:
                                m = folium.Map(location=[latitude2, longitude2], zoom_start=16)

                                marker = create_emoji_marker(latitude2, longitude2, name2, address2, gmap_id2,first_main_category2, 'blue','',url2).add_to(m)
                                marker.add_to(m)

                                
                                map_data = st_folium(m, width=width, height=height)
                                
                    st.write('--------')            
                    # Anthropic API 클라이언트 설정
                    client = anthropic.Anthropic(api_key=REVIEW_API_KEY)

                    
                    #review_text = 'laura and jenny are very polite and helpful the technicians worked fast and efficiently i appreciated this service experience. ive had to work with exterminators before and had decent experiences but after working with macropestexterminators the bar has definitely been set much highermy experience with macropestexterminatorswas filled with excellent communication fantastic customer service and honest  great quality work every message was returned every question was answered in detail and with patience  i asked the same questions over  over againto the entire team at macropestexterminators thank you for the excellent experience if i ever need such services ill definitely be calling you guys keep up the great work. very helpful and knowledgeable staff thank you. i have to give 5 stars for 5 star service we have used them a few times with our warranty and after canceling our warranty we decided to continue using them for service they are very professional and courteous i like how when there was a slight bit of confusion the technician took the initiative to fix the problem instead of blowing us off that takes character and we appreciate it we will continue to use their services. this is the company that my home warranty uses for pest control i have used them many times and they are difficult to work with1 most recently they refused to spray the perimeter of my home which is a part of what they are supposed to do under contract i spoke with the technician and explained that he has to do it and he refused anyway which made me have to spend the time making an additional claim with the home warranty company2 ive also had problems with their rodent exclusion work less than 2 months after paying a large contract for exclusion there were rat droppings inside the house again3 they provide very little advanced notice for the home visit they also try to change the times and dates at the last second which is extremely difficult to manage when there are multiple tenantsoverall i cannot recommend this company if you have other options go with your other options. great company to work with they sprayed my house during the covid19 pandemic  the technician wore a mask stayed 6ft away and really respected our space as well as took our heath into consideration we have a dog and i was concerned about the chemicals and he took the time to go over everything and answer any questions he also provided additional services they could offer in the future but didnt pressure us to sign up or anything i would recommend them and interested to see how my home does in the next 30 days. eliseo was very professional and took care of my ant problem he currently is following up with my mice dropping problem which they have been very persistent on their follow through i would definitely recommend them again. i really appreciate the due diligence and attention my house gets every time i recommend the bimonthly prevention service it keeps everything out and no pestsi always can rely on dependable service very thorough  i dont see bugs and gives me peace of mindthis is very specialized and targeted service i recommend. highly recommend very reliable and great service will continue to use. i highly recommend macropest to anyone seeking the services of an exterminatorthey are responsive courteous professional and most important they do the job. they were very proactive efficient reliable and friendly they were able to get the job done within 3 day notice and at reasonable rates. great company to work with they were super quick responsive and knowledgeable will definitely recommend and call in the future. i have been incredibly impressed by this service my home warranty sent them out and ended up not covering the service they assessed our needs and offered an appropriate plan to take care of the rats in our attic starting with buttoning up the entry points they found theyre even making an appointment to stop by on a weekend day when they dont usually work to clear our traps and make sure we dont have to live with the smell from them getting caughteliseo is the absolute best hes on time and extremely friendly cleans up after himself which may not seem like a big thing but weve had people work on our place that leave it an utter mess and this wasnt the situation herethey text you for appointment updates if you like and weve even just setup for a bimonthly outdoor service to keep the pests away overall just great service between the people weve met in person and both zuri and laura who weve texted withspoken to on the phone i highly recommend them also no hidden fees. ive been using them for years regular spraying of nontoxic oils keep the bugs out of the house grateful for them. as a realtor for many years i have worked with so many exterminators but macropest exterminators has been the finest as a realtor who also does some property management we had to get a company out fast for my owners tenants  the warranty company referred macropest and zuri who was our contact was fantastic  they found there were rodents and immediately arranged getting this taken care of professional and courteous and my client was so impressed has signed them on as her only maintenance company to ensure the home is sealed tight and no little critters of any kind will be on the run  i highly recommend them. this company was a pleasure to work with  zuri and eliseo were both professional friendly helpful and accommodating we plan to use this company again and highly recommend them. i would highly recommend macropest extermibators to anyone without any reservations they were very professional courteous and reliable eliseo and his entire team were very knowledgeable and willing to go the extra mile to provide excellent customer service they are a true gem. the gentleman who comes to our house is incredible he does a great job and he sprays not too close to our fountains because our dog likes to drink from our water fountainshe clears away the cobwebsluis is 100 professional and my husband and i appreciate the work he does around our home. i got in contact with this company from my home warranty  it is awful do not use  i told them i had trouble under my home with ants  when the man arrived i told him i had sprayed around the home  it cost me 75 and he did exactly as i had done only in maybe a minute flat  he told me they do not go under  he promised i would maybe see more in the couple days following then i should see nothing after that  within a week i had ants almost every day  when i called them back they said it would be about 3 weeks and i should take the appointment and then call my home warranty to have them send maybe another company and i could pay yet again  if you want to thow your money away and get nothing in return go to vegas you stand a better chance of winning. experienced professionals that get the job done. we have been very pleased with them. my service person was attentive and eager to accommodate. really great and professional service weve had their services for some time and its pretty good. very good job with our rental property  all bugs are gone with a great warranty thank you. great overall service. good food good prices. professionals and effective'

                    # result = get_summary_and_keywords(review_text)
                    # print(result)
                    # 가상의 리뷰 데이터
            
                    review_test="""
                        Summary in 3 sentences (10 words or fewer each):

                        1. MacroPest Exterminators provides excellent pest control services with professional staff.
                        2. Customers appreciate their communication, efficiency, and thorough pest elimination.
                        3. Some negative experiences reported, but mostly positive reviews overall.

                        5 key keywords:
                        1. Exterminators
                        2. Pest control
                        3. Customer service
                        4. Professional
                        5. Rodent exclusion
                        6. Rodent exclusion
                        7. Rodent exclusion
                        8. Rodent exclusion
                        9. Rodent exclusion
                        10. Rodent exclusion
                    """
                    
                    #if st.button('리뷰요약'):
                    # 레이아웃 구성
                    
                    
                    #테스트용 데이터
                    review_text1_result,review_text2_result = review_test,review_test
                    
                    # 클로드 api 실행
                    review_text1,review_text2= document1,document2
                    review_text1_2000, review_text2_2000 = truncate_review_text(review_text1, max_tokens=2000),truncate_review_text(review_text2, max_tokens=2000)
                    review_text1_result,review_text2_result = get_summary_and_keywords(review_text1_2000),get_summary_and_keywords(review_text2_2000)
                    
                    #줄정렬 적용
                    review_summary2(review_text1_result,review_text2_result)
                    
                    
                except:
                    st.sidebar.write('')
                