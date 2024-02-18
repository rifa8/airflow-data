from datetime import datetime
from airflow import DAG
from airflow.decorators import task

# 1. Create DAG that run in every 5 hours.
with DAG(
        'task1_rifa', 
        description='a DAG that runs every 5 hours',
        schedule_interval='0 */5 * * *',
        start_date=datetime(2023, 11, 11), 
        catchup=False
    ) as dag:

# 2. Suppose we define a new task that push a variable to xcom.
    @task
    def push_1(ti=None):
        ti.xcom_push(key='key_1', value='Pull')

# 3. How to pull multiple values at once?
# Suppose we have 3 additional tasks that also push additional values as well

    @task
    def push_2(ti=None):
        ti.xcom_push(key='key_2', value='multiple')
    
    @task
    def push_3(ti=None):
        ti.xcom_push(key='key_3', value='values')
    
    @task
    def push_4(ti=None):
        ti.xcom_push(key='key_4', value='at once.')

#Then we define another task to retrieve all those values at once

    @task
    def pull_from_xcom(ti=None):
        multiple_values = ti.xcom_pull(
            task_ids=[
                'push_1',
                'push_2',
                'push_3',
                'push_4'
            ],
            key=None   
        )

        print(f"These are multiple values from XComs: {' '.join(multiple_values)}")

# Finally, specify the order of the tasks

    push_1() >> push_2() >> push_3() >> push_4() >> pull_from_xcom()

