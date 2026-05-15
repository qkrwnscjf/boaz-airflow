from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json
import requests
import os

# 실습 시 주의사항: S3_BUCKET_NAME을 본인이 생성한 버킷 이름으로 수정해야 합니다.
S3_BUCKET_NAME = "boaz-user-lab-yourname"

default_args = {
    'owner': 'boaz',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='user_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['BOAZ', 'Hands-on', 'Glue'],
) as dag:

    @task
    def extract_from_api():
        """무료 공개 API(JSONPlaceholder)에서 유저 데이터 10개를 수집합니다."""
        print("Fetching data from JSONPlaceholder API...")
        url = "https://jsonplaceholder.typicode.com/users"
        response = requests.get(url)
        raw_data = response.json()
        
        # 정확히 10개의 데이터만 추출
        collected_data = []
        for item in raw_data[:10]:
            collected_data.append({
                "id": item["id"],
                "name": item["name"],
                "email": item["email"],
                "company_name": item["company"]["name"],
                "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
        local_path = '/tmp/user_data.json'
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(collected_data, f, indent=4, ensure_ascii=False)
            
        return local_path

    @task
    def upload_to_s3(file_path):
        """로컬 파일을 S3에 파티션 없이 업로드 (Glue Crawler 실습 편의성)"""
        hook = S3Hook(aws_conn_id='aws_default')
        
        # Glue 실습의 직관성을 위해 파티션 폴더를 제거하고 단일 폴더에 업로드
        s3_key = "raw/user_data/data.json"
        
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        print(f"Successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")

    # 파이프라인 흐름 정의 (AthenaOperator 제거 -> Glue Crawler가 대신함)
    raw_file = extract_from_api()
    upload_to_s3(raw_file)
