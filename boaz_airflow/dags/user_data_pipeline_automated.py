from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import os

# 실습 시 주의사항: S3_BUCKET_NAME을 본인이 생성한 버킷 이름으로 수정해야 합니다.
S3_BUCKET_NAME = "boaz-week17-lab"

default_args = {
    'owner': 'boaz',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='user_data_pipeline_automated',
    default_args=default_args,
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['BOAZ', 'Hands-on', 'Automated'],
) as dag:

    @task
    def check_local_data():
        """1단계: 로컬 폴더(data/)에 준비된 데이터가 있는지 확인합니다."""
        local_path = '/opt/airflow/data/data.json'
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Data file not found at {local_path}")
        return local_path

    @task
    def upload_to_s3(file_path, **context):
        """2단계: 로컬 파일을 S3에 dt=YYYY-MM-DD 파티션 구조로 자동 업로드"""
        hook = S3Hook(aws_conn_id='aws_default')
        dt = context['ds']
        s3_key = f"raw/user_data_auto/dt={dt}/data.json"
        
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        print(f"Successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")

    # 3단계: Athena 테이블 파티션 자동 갱신
    repair_athena_table = AthenaOperator(
        task_id='repair_athena_table',
        query="MSCK REPAIR TABLE user_data_auto_lake;",
        database='default',
        output_location=f"s3://{S3_BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    # 파이프라인 흐름 정의
    data_file = check_local_data()
    upload_to_s3(data_file) >> repair_athena_table
