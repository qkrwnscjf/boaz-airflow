from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from datetime import datetime, timedelta
import os

# [실습 설정] 
# 1. S3_BUCKET_NAME: 본인이 생성한 S3 버킷 이름으로 수정하세요.
# 2. NEW_DATABASE_NAME: Track 2를 위해 Airflow가 자동으로 생성할 DB 이름입니다.
# 3. TABLE_NAME: Athena에 생성될 테이블 이름입니다.
S3_BUCKET_NAME = "boaz-lab"
NEW_DATABASE_NAME = "boaz_automated_db"
TABLE_NAME = "user_data_auto_lake"

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
    tags=['BOAZ', 'Hands-on', 'Automated-Infra'],
) as dag:

    # [1단계] Track 2 전용 데이터베이스 자동 생성
    # IF NOT EXISTS를 통해 이미 존재할 경우 에러 없이 넘어갑니다.
    create_db = AthenaOperator(
        task_id='create_new_database',
        query=f"CREATE DATABASE IF NOT EXISTS {NEW_DATABASE_NAME};",
        database='default', 
        output_location=f"s3://{S3_BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    # [2단계] Athena 테이블 및 스키마 자동 생성 (DDL)
    # LOCATION은 반드시 파일명이 아닌 '폴더 경로'여야 합니다.
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {NEW_DATABASE_NAME}.{TABLE_NAME} (
        id int,
        name string,
        email string,
        company_name string,
        collected_at string
    )
    PARTITIONED BY (dt string)
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION 's3://{S3_BUCKET_NAME}/raw/user_data_auto/';
    """

    create_table = AthenaOperator(
        task_id='create_athena_table',
        query=create_table_query,
        database=NEW_DATABASE_NAME,
        output_location=f"s3://{S3_BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    @task
    def check_local_data():
        """3단계: 로컬 데이터 존재 여부 확인"""
        local_path = '/opt/airflow/data/data.json'
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Data file not found at {local_path}")
        return local_path

    @task
    def upload_to_s3(file_path, **context):
        """4단계: S3 파티션 경로(dt=YYYY-MM-DD)로 데이터 업로드"""
        hook = S3Hook(aws_conn_id='aws_default')
        dt = context['ds'] # Airflow 실행 날짜 변수
        s3_key = f"raw/user_data_auto/dt={dt}/data.json"
        
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        return s3_key

    # [5단계] Athena 파티션 갱신 (MSCK REPAIR)
    # S3에 새로 올라온 폴더(파티션)를 Athena 테이블에 등록합니다.
    repair_athena_table = AthenaOperator(
        task_id='repair_athena_table',
        query=f"MSCK REPAIR TABLE {TABLE_NAME};",
        database=NEW_DATABASE_NAME,
        output_location=f"s3://{S3_BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    # --- 파이프라인 의존성 정의 ---
    # DB생성 -> 테이블생성 -> 데이터확인 -> S3업로드 -> 파티션갱신
    local_data_path = check_local_data()
    create_db >> create_table >> local_data_path >> upload_to_s3(local_data_path) >> repair_athena_table
