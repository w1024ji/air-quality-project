import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# EC2 내부 도커 네트워크용 주소
DB_URL = "postgresql://airflow:airflow@airflow-postgres-1:5432/airflow"

def load_data():
    engine = create_engine(DB_URL)
    query = 'SELECT * FROM air_quality_processed ORDER BY "dataTime" DESC'
    return pd.read_sql(query, engine)

# 등급별 색상 매핑 함수 
def get_grade_info(val, target):
    if target == 'pm10':
        bins = [0, 15, 30, 80, 150, 999] # 아주좋음~아주나쁨 기준
    elif target == 'pm25':
        bins = [0, 8, 15, 35, 75, 999]
    else: # o3 (오존)
        bins = [0, 0.015, 0.030, 0.090, 0.150, 999]
    
    labels = ['아주 좋음', '좋음', '보통', '나쁨', '아주 나쁨']
    colors = {
        '아주 좋음': '#2375C7', # 짙은 파랑
        '좋음': '#33CCFF',      # 하늘색
        '보통': '#2CB606',      # 초록색
        '나쁨': '#F29269',      # 주황색
        '아주 나쁨': '#EF2F2F'   # 빨간색
    }
    
    for i in range(len(bins)-1):
        if bins[i] <= val < bins[i+1]:
            label = labels[i]
            return label, colors[label]
    return labels[-1], colors[labels[-1]]

st.set_page_config(page_title="서울 실시간 대기질", layout="wide")
st.title("🌬️ 서울시 실시간 대기오염 3대 지표 (미세먼지, 초미세먼지, 오존)")

try:
    df = load_data()

    if not df.empty:
        # 가장 최근 수집된 시점의 데이터만 필터링
        latest_time = df['dataTime'].iloc[0]
        st.subheader(f"최근 업데이트 시간: {latest_time}")
        latest_df = df[df['dataTime'] == latest_time].copy()
        
        # 각 지표별 등급/색상 컬럼 추가
        for col, target in [('pm10Value', 'pm10'), ('pm25Value', 'pm25'), ('o3Value', 'o3')]:
            latest_df[f'{target}_label'] = latest_df[col].apply(lambda x: get_grade_info(x, target)[0])

        # 3열 레이아웃 생성
        c1, c2, c3 = st.columns(3)
        
        color_map = {'아주 좋음': "#2375C7", '좋음': '#33CCFF', '보통': "#2CB606", '나쁨': "#F29269", '아주 나쁨': "#EF2F2F"}
        category_order = ["아주 좋음", "좋음", "보통", "나쁨", "아주 나쁨"]

        with c1:
            st.markdown("### 🔵 미세먼지 (PM10)")

            sorted_df1 = latest_df.sort_values('pm10Value', ascending=False)
            fig1 = px.bar(sorted_df1, x='pm10Value', y='stationName', orientation='h', color='pm10_label',
                        color_discrete_map=color_map, category_orders={"pm10_label": category_order})
            fig1.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
            st.plotly_chart(fig1, use_container_width=True)

        with c2:
            st.markdown("### 🟢 초미세먼지 (PM2.5)")
            sorted_df2 = latest_df.sort_values('pm25Value', ascending=False)
            fig2 = px.bar(sorted_df2, x='pm25Value', y='stationName', orientation='h', color='pm25_label',
                          color_discrete_map=color_map, category_orders={"pm25_label": category_order})
            fig2.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
            st.plotly_chart(fig2, use_container_width=True)

        with c3:
            st.markdown("### 🟡 오존 (O3)")
            sorted_df3 = latest_df.sort_values('o3Value', ascending=False)
            fig3 = px.bar(sorted_df3, x='o3Value', y='stationName', orientation='h', color='o3_label',
                          color_discrete_map=color_map, category_orders={"o3_label": category_order})
            fig3.update_layout(yaxis={'categoryorder':'total ascending'}, height=600)
            st.plotly_chart(fig3, use_container_width=True)

        st.write("---")
        st.write("### 📋 상세 데이터 테이블")
        st.dataframe(latest_df[['dataTime', 'stationName', 'pm10Value', 'pm25Value', 'o3Value']], use_container_width=True)

    else:
        st.warning("DB에 데이터가 없습니다.")
except Exception as e:
    st.error(f"데이터를 불러오는 중 오류 발생: {e}")

