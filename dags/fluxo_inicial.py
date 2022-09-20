from os import getenv
from datetime import datetime
import json
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.operators.dummy import DummyOperator
from google.cloud.pubsub import PublisherClient

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

def write_log_execution(
                    name_dag: str, 
                    time_execution: str,
                    success_task: str
                ) -> None:
    """
    Funcao que ira gravar o log da Execucao da Dag em uma tabela de logs
    """

    publisher_client = PublisherClient()

    topic_path = r'projects/project-student-361518/topics/reception-register-streaming'
    

    data = {
        'name_dag'      : name_dag,
        'time_execution': time_execution,
        'response_task' : success_task
    }

    data_str = json.dumps(data, default = str)
    data = data_str.encode("utf-8")

    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")
    return None

def custom_failure_function(context):
	"Define custom failure notification behavior"
	dag_run = context.get('dag_run')
	task_instances = dag_run.get_task_instances()
	print("These task instances failed:", task_instances)

def custom_success_function(context):
    "Define custom success notification behavior"
    name_dag = context['task_instance_key_str'].split('__')[0]
    time_execution = f'{datetime.now().strftime("%Y%m%d%H%M%S")}'
    success_task = context['task_instance_key_str']
    
    write_log_execution(name_dag, time_execution, success_task)

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
    on_failure_callback=custom_failure_function,
    catchup = False
) as dag:

    

    success_task = DummyOperator(
		task_id='success_task',
		on_success_callback=custom_success_function
	)

    
    success_task