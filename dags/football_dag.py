#imports
import sys
sys.path.append('/sources')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta,datetime
from scripts.extract import fetch_fixtures,fetch_team_info,extract_teams_id

#DAG's argumnets

default_args={
    'owner' : 'Zeyad',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),     
}



# DAGs Definition 

with DAG(
   dag_id="football-etl"
   ,
    default_args=default_args,
    description='Football stats DAG',
    schedule_interval='@daily',
    start_date=datetime(2025,6,8),
   
) as dag:


    fetch_fixtures_task = PythonOperator(
            task_id='extract_fixtures',
            python_callable=fetch_fixtures,
            op_kwargs={'api_key': 'your_api_key'}
        )

    extract_teams_task = PythonOperator(
        task_id='extract_teams_id',
        python_callable=extract_teams_id,
        op_kwargs={'fixtures': "{{ ti.xcom_pull(task_ids='fetch_fixtures') }}"}
    )

    extract_team_info_task = PythonOperator(
        task_id='extract_team_info',
        python_callable=fetch_team_info,
        op_kwargs={
            'team_ids': "{{ ti.xcom_pull(task_ids='extract_teams_id')['team_ids'] }}",
            'api_key': 'your_api_key'
        }
    )

    fetch_fixtures_task >> extract_teams_task >> extract_team_info_task    
    
    
