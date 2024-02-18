from airflow import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task
import json

with DAG(
    dag_id = 'task2_rifa',
    schedule=None,
    start_date=datetime(2023, 11, 11),
    catchup=False
) as dag:

    predict_multiple_names = SimpleHttpOperator(
        task_id="predict_multiple_names",
        endpoint="/gender/by-first-name-multiple",
        method="POST",
        data='[{"first_name":"Galih","country":"ID"},{"first_name":"Ratna","country":"ID"}]',
        http_conn_id="gender_api",
        log_response=True,
        dag=dag
    )

    create_table_in_pg = PostgresOperator(
        task_id = 'create_table_in_pg',
        sql = ('CREATE TABLE IF NOT EXISTS gender_name_prediction ' +
        '(' +
            'input TEXT, ' +
            'details TEXT, ' +
            'result_found BOOL, ' +
            'first_name TEXT, ' +
            'probability FLOAT(53), ' +
            'gender TEXT, ' +
            'timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp ' +
        ')'),
        postgres_conn_id='pg_conn_id', 
        autocommit=True,
        dag=dag
    )

    @task
    def load_to_pg(ti=None):
        predicted_names = ti.xcom_pull(task_ids='predict_multiple_names', key=None)
        json_data = json.loads(predicted_names)
        insert = '''
            INSERT INTO gender_name_prediction (input, details, result_found, first_name, probability, gender)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        
        pg_hook = PostgresHook(postgres_conn_id='pg_conn_id').get_conn()
        with pg_hook.cursor() as cursor:
            for data in json_data:
                input = json.dumps(data['input'])
                details = json.dumps(data['details'])
                result_found = data['result_found']
                first_name = data['first_name']
                probability = data['probability']
                gender = data['gender']

                cursor.execute(insert, (input, details, result_found, first_name, probability, gender))
            
        pg_hook.commit()
        pg_hook.close()

    predict_multiple_names >> create_table_in_pg >> load_to_pg()
    
