from airflow.models import DAG
from datetime import timedelta,datetime
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner':'venkat',
    'start_date' : days_ago(0),
    'email':['test123@gmail.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('ssh_connectivity', default_args = default_args, schedule_interval=None) as dag:
    SSHOperator(
        task_id = 'test_ssh_remotly',
        ssh_conn_id = 'ssh_test',
        command='echo "Testing ssh connecitivity"',
    )