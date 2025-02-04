from airflow.models import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonVirtualenvOperator,is_venv_installed
from airflow.utils.dates import days_ago
import logging
import sys
from airflow.decorators import task

log = logging.getLogger(__name__)

default_args = {
    'owner':'venkat',
    'start_date' : days_ago(0),
    'email':['test123@gmail.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'pandas_to_sqlite',
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
            df = pd.read_csv("/opt/airflow/data/wine-ratings.csv", index_col = 0)
            df = df.replace({"\r": ""}, regex=True)
            df = df.replace({"\n": " "}, regex=True)
            df.drop(['grape'], axis=1, inplace=True)
            df.to_csv("/opt/airflow/cleaned_data.csv")
        @task.virtualenv(
            task_id="sqlite_persist_wine_data",requirements=["pandas", "sqlalchemy"],system_site_packages=False)
        def pandas_to_sqllite():
            import pandas as pd 
            from sqlalchemy import create_engine
            engine = create_engine('sqlite:////opt/airflow/wine_database.db', echo=True)
            df = pd.read_csv("/opt/airflow/cleaned_data.csv", index_col = 0)
            #df.to_sql('wine_database', engine)
            df.notes.to_sql('wine_notes', engine)

        pandas_read() >> pandas_to_sqllite()
            
            
