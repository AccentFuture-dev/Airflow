from airflow.models import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonVirtualenvOperator,is_venv_installed
from airflow.utils.dates import days_ago
import logging
import sys
from airflow.decorators import task

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

default_args = {
    'owner':'venkat',
    'start_date' : days_ago(0),
    'email':['test123@gmail.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'pandas_analysis',
    default_args = default_args,
    description='this is pandas analysis ',
    schedule_interval = timedelta(days=1),
    catchup=False 
) as dag:
    if not is_venv_installed():
        log.warning("the virtualenv_python this is required virtualenv package, please install it")
    else:
        @task.virtualenv(
            task_id="virutalenv_python",requirements=["pandas"],system_site_packages=False)
        def pandas_read():
            import pandas as pd 
            csv_url = "https://raw.githubusercontent.com/paiml/wine-ratings/main/wine-ratings.csv"
            df = pd.read_csv(csv_url)
            head = df.head(10)
            return head.to_csv('/opt/airflow/df_head.csv')
        pandas_task = pandas_read()