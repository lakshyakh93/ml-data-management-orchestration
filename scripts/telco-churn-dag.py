from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.bash_operator import BashOperator
import os
from feature_engineer import add_new_features
from airflow.decorators import task

DATA_REPO_NAME = os.environ['DATA_REPO_NAME']

RAW_DATA_LOCAL_PATH = f'/opt/airflow/{DATA_REPO_NAME}/Telco-Customer-Churn.csv'
TRANSFORMED_DATA_LOCAL_PATH = f'/opt/airflow/{DATA_REPO_NAME}/Telco-Customer-Churn-Transformed.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'max_active_runs': 1,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'telco-churn-dag',
    schedule_interval=None,
    default_args=default_args,
    catchup=False
)

# Task 1: Download file from Kaggle
download_data_from_kaggle = BashOperator(
    task_id='download_data_from_kaggle',
    bash_command='cd /opt/airflow/$DATA_REPO_NAME && kaggle datasets download -d blastchar/telco-customer-churn && unzip telco-customer-churn.zip && mv WA_Fn-UseC_-Telco-Customer-Churn.csv Telco-Customer-Churn.csv && rm telco-customer-churn.zip',
    dag=dag,
)

# Task 2: Run feature engineering
engineer_features = PythonVirtualenvOperator(
    task_id='engineer_features',
    python_callable=add_new_features,
    requirements=["pandas", "tqdm"],
    op_kwargs={"RAW_DATA_LOCAL_PATH": RAW_DATA_LOCAL_PATH, "TRANSFORMED_DATA_LOCAL_PATH": TRANSFORMED_DATA_LOCAL_PATH},
    dag=dag,
)

# Task 3: Convert to Parquet
@task.virtualenv(
    task_id="convert_to_parquet", requirements=["pandas"], dag=dag
)
def convert_csv_to_parquet():
    import pandas as pd
    import os
    DATA_REPO_NAME = os.environ["DATA_REPO_NAME"]
    TRANSFORMED_DATA_LOCAL_PATH = f'/opt/airflow/{DATA_REPO_NAME}/Telco-Customer-Churn-Transformed.csv'
    pd.read_csv(TRANSFORMED_DATA_LOCAL_PATH).to_parquet(TRANSFORMED_DATA_LOCAL_PATH.split('.')[0]+".parquet")

parquet_conversion = convert_csv_to_parquet()

# Task 4: DVC and Git Checkin
dvc_git_checkin = BashOperator(
    task_id='dvc_git_checkin',
    bash_command='cd /opt/airflow/$DATA_REPO_NAME && dvc init -f && dvc remote add -f -d myremote s3://$S3_BUCKET_NAME/dvc-store && git config --global user.email "test@ickn.com" && git config --global user.name "Demo User" && dvc add *.csv *.parquet && git add *.dvc .gitignore && git commit -m "added transformed file" && git push -f origin && dvc commit && dvc push',
    dag=dag,
)

download_data_from_kaggle >> engineer_features >> parquet_conversion >> dvc_git_checkin
