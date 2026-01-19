import requests
import boto3
import json
from datetime import datetime
import os

BUCKET_NAME = 'my-air-quality-project'
API_KEY = os.getenv('AIR_API_KEY')

def get_air_data():
    url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"

    params = {
        'serviceKey': requests.utils.unquote(API_KEY),
        'returnType': 'json',
        'numOfRows': '100',
        'pageNo': '1',
        'sidoName': '서울', 
        'ver': '1.0'
    }

    response = requests.get(url, params=params)
    print(f"응답 상태: {response.status_code}")
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"호출 실패: {response.text}")

def upload_to_s3(data):
    s3 = boto3.client(
        's3',
        region_name='ap-northeast-2'
    )
    
    # 파일명에 타임스탬프 추가 (중복 방지)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'raw/air_data_{timestamp}.json'
    
    # S3 업로드
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data, ensure_ascii=False)
    )
    print(f"S3 적재 완료: {file_name}")

if __name__ == "__main__":
    try:
        data = get_air_data()
        upload_to_s3(data)
    except Exception as e:
        print(f"오류 발생: {e}")

