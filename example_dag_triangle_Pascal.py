from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

@task(task_id="print_triangle")
def triangle_Pacsal(n=10):
    triangle = []
    for i in range(n):
        triangle.append([1]+[0]*n)
        for j in range(1, i+1): 
            triangle[i][j] = triangle[i-1][j]+triangle[i-1][j-1]
    for i in range(n):
        for j in range(i+1):
            print(triangle[i][j], end=' ')
        print()


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 11, 26), 
    'schedule_interval' : '44 11 * * *
    'email_on_failure' : False,
    'email_on_retry': False
}

with DAG(
    dag_id='example_dag_triangle_Pascal',
    default_args=default_args,
    description='Run triangle Pascal',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    run_this = triangle_Pacsal()

