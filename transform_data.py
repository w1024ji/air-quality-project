import boto3
import json
import pandas as pd
from sqlalchemy import create_engine
import requests
import os

BUCKET_NAME = 'my-air-quality-project'
DB_URL = "postgresql://airflow:airflow@airflow-postgres-1:5432/airflow"
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

def send_slack_alert(district, pm10_val, status):
    payload = {
        "attachments": [
            {
                "fallback": "대기질 비상 알림!",
                "color": "#990000",  
                "pretext": "*서울시 대기질 비상 알림*",
                "fields": [
                    {
                        "title": "지역",
                        "value": district,
                        "short": True
                    },
                    {
                        "title": "미세먼지 농도",
                        "value": f"{pm10_val} µg/m³",
                        "short": True
                    },
                    {
                        "title": "상태",
                        "value": f"*{status}*",
                        "short": False
                    }
                ],
                "footer": "Air Quality Monitor",
                "ts": int(pd.Timestamp.now().timestamp())
            }
        ]
    }
    # 슬랙으로 데이터 전송
    response = requests.post(
        SLACK_WEBHOOK_URL, 
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )
    return response

def transform_and_load():
    # S3에서 파일 목록 가져오기
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='raw/')
    # 가장 마지막에 생성된 파일 찾기
    last_file = sorted(objects['Contents'], key=lambda x: x['LastModified'])[-1]['Key']
    
    print(f"가져올 파일: {last_file}")
    
    # 파일 내용 읽기
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=last_file)
    data = json.loads(obj['Body'].read().decode('utf-8'))
    items = data['response']['body']['items']
    
    # 데이터 정제 
    df = pd.DataFrame(items)
    
    # 필요한 컬럼만 선택
    df = df[df['stationName'].str.endswith('구')]
    df = df[['dataTime', 'stationName', 'pm10Value', 'pm25Value', 'o3Value']]
    
    # 숫자형 변환 및 에러값(-) 처리
    df['pm10Value'] = pd.to_numeric(df['pm10Value'], errors='coerce').fillna(0)
    df['pm25Value'] = pd.to_numeric(df['pm25Value'], errors='coerce').fillna(0)
    df['o3Value'] = pd.to_numeric(df['o3Value'], errors='coerce').fillna(0)
    
    print(f"필터링 후 총 {len(df)}건의 '구' 데이터 정제 완료")

    # 데이터 정제가 끝난 df를 한 줄씩 검사
    for _, row in df.iterrows():
        if row['pm10Value'] > 150: 
            send_slack_alert(row['stationName'], row['pm10Value'], "아주 나쁨")

    # DB 적재
    engine = create_engine(DB_URL)
    df.to_sql('air_quality_processed', engine, if_exists='append', index=False)
    print("DB 적재 성공!")


if __name__ == "__main__":
    transform_and_load()

