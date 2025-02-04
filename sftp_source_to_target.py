from airflow.models import DAG
from datetime import timedelta,datetime
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.dates import days_ago


default_args = {
    'owner':'venkat',
    'start_date' : days_ago(0),
    'email':['test123@gmail.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('sftp_source_to_target', default_args = default_args, schedule_interval=None) as dag:
   source_file_check = SFTPSensor(task_id='source_file_check',
                                  sftp_conn_id='sftp_default',
                                  path = '/Users/venkata/Desktop/desktop/Usecases/Airflow-docker/data/Source/input.csv',
                                  poke_interval=10,
                                  timeout=100
                                 )
   get_data = SFTPOperator(task_id='get_data',
                                  ssh_conn_id='sftp_default',
                                  remote_filepath = "/Users/venkata/Desktop/desktop/Usecases/Airflow-docker/data/Source/input.csv",
                                  local_filepath = "/opt/airflow/{{run_id}}/input.csv",
                                  operation="get",
                                  create_intermediate_dirs=True
                                 )
   source_file_check >> get_data