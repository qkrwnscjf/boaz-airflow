from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from datetime import datetime, timedelta
import os

# [실습 설정] 
S3_BUCKET_NAME = "버킷명을 작성해주세요."
NEW_DATABASE_NAME = "boaz_automated_db"

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
    tags=['BOAZ', 'Hands-on', 'Automated-Infra', 'Glue-Crawler'],
) as dag:

    # [1단계] 데이터베이스 생성 (Glue 크롤러가 바라볼 DB)
    create_db = AthenaOperator(
        task_id='create_new_database',
        query=f"CREATE DATABASE IF NOT EXISTS {NEW_DATABASE_NAME};",
        database='default', 
        output_location=f"s3://{S3_BUCKET_NAME}/athena-results/",
        aws_conn_id='aws_default'
    )

    @task
    def check_local_data():
        #[2단계] 로컬 데이터 존재 여부 확인
        local_path = '/opt/airflow/data/data.json'
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Data file not found at {local_path}")
        return local_path

    @task
    def upload_to_s3(file_path, **context):
        #[3단계]: S3 파티션 경로(dt=YYYY-MM-DD)로 데이터 업로드
        hook = S3Hook(aws_conn_id='aws_default')
        dt = context['ds'] # Airflow 실행 날짜 변수를 한번 추가 해보자.
        # 크롤러가 테이블 이름을 인식하기 좋게 폴더 구조 설정
        s3_key = f"raw/user_data_auto_lake/dt={dt}/data.json"
        
        hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        return f"s3://{S3_BUCKET_NAME}/raw/user_data_auto_lake/"

    # [4단계] Glue 크롤러 실행 (동적 스키마 추론 및 테이블/파티션 자동 생성)
    run_glue_crawler = GlueCrawlerOperator(
        task_id='run_glue_crawler',
        config={
            "Name": "boaz_automated_crawler",
            "Role": "GlueS3AccessRole", # 사전에 IAM Role이 생성되어 있어야 함 (AWS 관리형 정책 AWSGlueServiceRole 연결 권장)
            "DatabaseName": NEW_DATABASE_NAME,
            "Targets": {
                "S3Targets": [
                    {
                        "Path": f"s3://{S3_BUCKET_NAME}/raw/user_data_auto_lake/",
                        # 필요에 따라 크롤러가 제외할 패턴 지정 가능
                    }
                ]
            },
            "SchemaChangePolicy": {
                "UpdateBehavior": "UPDATE_IN_DATABASE", # 컬럼이 추가되면 테이블 업데이트
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            }
        },
        aws_conn_id='aws_default'
    )

    # --- 파이프라인 의존성 정의 ---
    local_data_path = check_local_data()
    s3_path_info = upload_to_s3(local_data_path)
    
    # 순서: DB생성 & S3업로드 완료 -> Glue 크롤러 실행
    [create_db, s3_path_info] >> run_glue_crawler
