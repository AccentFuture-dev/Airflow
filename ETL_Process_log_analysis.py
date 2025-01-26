from airflow.models import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonOperator

input_file = 'input/web-server-access-log.txt'
extracted_file = 'extracted-data.txt'
transformed_file='transformed.txt'
output_file='output/result.txt'

def extract():
    global input_file
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as extfile:
        for line in infile:
          fields = line.split('#')
          if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                extfile.write(field_1 + "#" +field_4 + "\n")
    
def transform():
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as trnfile:
        for line in infile:
            processed_line = line.upper()
            trnfile.write(processed_line + '\n')

def load():
        with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as trnfile:
            for line in infile:
                trnfile.write(line + '\n')


def check():
    global output_file
    with open(output_file, 'r') as outfile:
        for line in outfile:
            print(line)


default_args = {
    'owner':'venkat',
    'start_date' : datetime(2024,1,1),
    'email':['test123@gmail.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_Process_log_analysis',
    default_args = default_args,
    description='this is Log ETL dag ',
    schedule_interval = timedelta(days=1),
)


execute_extract = PythonOperator(
    task_id = 'execute_extract',
    python_callable = extract,
    dag = dag
)

execute_transform = PythonOperator(
    task_id = 'execute_transform',
    python_callable = transform,
    dag = dag
)
execute_load = PythonOperator(
    task_id = 'execute_load',
    python_callable = load,
    dag = dag
)

execute_check = PythonOperator(
    task_id = 'execute_check',
    python_callable = check,
    dag = dag
)

execute_extract >> execute_transform >> execute_load >> execute_check