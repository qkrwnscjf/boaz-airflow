from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os

# 실습 시 주의사항: S3_BUCKET_NAME을 본인이 생성한 버킷 이름으로 수정해야 합니다.
S3_BUCKET_NAME = "boaz-user-lab-yourname"

default_args = {
    'owner': 'boaz',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='user_data_pipeline_manual',
    default_args=default_args,
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['BOAZ', 'Hands-on', 'Manual'],
) as dag:

    @task
    def check_local_data():
        """1단계: 로컬 폴더(data/)에 준비된 데이터가 있는지 확인합니다."""
        local_path = '/opt/airflow/data/data.json'
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Data file not found at {local_path}")
        print(f"Successfully located local data: {local_path}")
        return local_path

    @task
    def upload_to_s3(file_path):
        """2단계: 로컬 파일을 S3의 raw/ 폴더로 업로드합니다."""
        hook = S3Hook(aws_conn_id='aws_default')
        s3_key = "raw/user_data/data.json"
        
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        print(f"Successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")

    # 파이프라인 흐름 정의
    data_file = check_local_data()
    upload_to_s3(data_file)
