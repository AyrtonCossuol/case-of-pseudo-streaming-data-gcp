from os import getenv
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator


GCP_PROJECT_ID = getenv('GCP_PROJECT_ID', 'project-student-361518')
GCP_CONECTION = r'C:\Users\ayrto\OneDrive\Área de Trabalho\students\project-student-account.json'
REGION = getenv('REGION', 'us-east1')
LOCATION = getenv('LOCATION', 'us-east1')
PROCESSING_BUCKET_ZONE = getenv('LANDING_BUCKET_ZONE', 'processing-zone')
# PROCESSING_BUCKET_ZONE = getenv('PROCESSING_BUCKET_ZONE', 'airflow-processing')
# CURATED_BUCKET_ZONE = getenv('CURATED_BUCKET_ZONE', 'airflow-curated-zone')
# DATAPROC_CLUSTER_NAME = getenv('DATAPROC_CLUSTER_NAME', 'spark-prc-content')
# PYSPARK_URI = getenv('PYSPARK_URI', 'gs://temp-test-datasets/etl.py')
# BQ_DATASET_NAME = getenv('BQ_DATASET_NAME', 'Result_Final')
# BQ_TABLE_NAME = getenv('BQ_TABLE_NAME', 'content_reviews')

default_args = {
    'owner': 'Ayrton Cossuol',
    'depends_on_past': False, #depende de uma execucao anterior
    'email': ['ayrton.cossuol@live.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1 #quantidade de tentativas
}

with DAG(
    dag_id = 'fluxo-inicial',
    tags = ['development', 'cloud storage'],
    default_args = default_args,
    start_date = datetime(year=2022, month=9, day=18),
    schedule_interval = None,
    catchup = False
) as dag:

    create_gcs_processing_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_processing_bucket",
        project_id = GCP_PROJECT_ID,
        bucket_name = PROCESSING_BUCKET_ZONE,
        storage_class = "STANDARD",
        location = LOCATION,
        labels = {"env": "dev", "team": "processing"},
        gcp_conn_id = GCP_CONECTION
    )

    delete_bucket_processing_zone = GCSDeleteBucketOperator(
        task_id = 'delete_bucket_processing_zone',
        bucket_name = PROCESSING_BUCKET_ZONE,
        gcp_conn_id = GCP_CONECTION
    )

    create_gcs_processing_bucket >> delete_bucket_processing_zone