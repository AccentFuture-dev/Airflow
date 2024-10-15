from airflow import DAG
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine,MetaData,Integer,String,Column,Table
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner':'venkat',
    'start_date' : days_ago(0),
    'email':['test123@gmail.com'],
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

def create_insert_data_into_sqlite():
    engine = create_engine('sqlite:////opt/airflow/sqliteserver.db')
    metadata = MetaData()
    users_table = Table(
        'users',metadata,
        Column('id',Integer,primary_key=True,autoincrement=True),
        Column('name',String),
        Column('age',String),
    )
    metadata.create_all(engine)

    with engine.connect() as connection:
        connection.execute(users_table.insert().values(name='Venkata', age=35))
        connection.execute(users_table.insert().values(name='Raj', age=40))


def extract_data_from_sqlite(**kwargs):
    engine = create_engine('sqlite:////opt/airflow/sqliteserver.db')
    query = "select * from users"
    df = pd.read_sql(query,engine)
    records = df.to_dict(orient='records')
    kwargs['ti'].xcom_push(key='sqlite_data',value=records)


def prepare_snowflake_sql(**kwargs):
    records = kwargs['ti'].xcom_pull(task_ids='extract_task',key='sqlite_data')
    sql_statements = ""
    for record in records:
        sql_statements += f"INSERT INTO users (id,name,age) VALUES({record['id']}, '{record['name']}',{record['age']});\n"
    
    kwargs['ti'].xcom_push(key='snowflake_sql',value=sql_statements)


with DAG(
    'sqllite_to_snowflake_migration',
    default_args = default_args,
    description='this task is for create sqllite and load data',
    schedule_interval='@once',
) as dag:
    insert_task = PythonOperator(
        task_id='insert_task',
        python_callable = create_insert_data_into_sqlite,
    )
    extract_task = PythonOperator(
        task_id='extract_task',
        default_args = default_args,
        python_callable = extract_data_from_sqlite,
        provide_context=True,
    )
    prepare_sql_task = PythonOperator(
        task_id='prepare_sql_task',
        default_args = default_args,
        python_callable = prepare_snowflake_sql,
        provide_context=True,
    )
    create_table_task = SnowflakeOperator(
        task_id='create_table_task',
        snowflake_conn_id = 'snowflake_conn',
        sql="""
        CREATE OR REPLACE TABLE users(
        id INTEGER,
        name STRING,
        age INTEGER
        );
        """,
    )
    load_task = SnowflakeOperator(
        task_id='load_task',
        snowflake_conn_id = 'snowflake_conn',
        sql="{{ ti.xcom_pull(task_ids='prepare_sql_task',key='snowflake_sql')}}",
    )
    insert_task >> extract_task >> prepare_sql_task >> create_table_task >> load_task