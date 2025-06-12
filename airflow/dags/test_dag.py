from airflow.decorators import dag, task
from datetime import timedelta, datetime

default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
}

@dag(dag_id='test_dag', description='This DAG is for testing webserver & scheduler', default_args=default_args, start_date=datetime(2025, 6, 12), schedule='@daily', catchup=False)
def test_dag():
    @task
    def say_hello():
        print("Hello, Airflow is working!")

    say_hello()

dag_test = test_dag()