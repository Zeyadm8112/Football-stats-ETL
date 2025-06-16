#imports
import sys
sys.path.append('/sources')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta,datetime
from scripts.extract import fetch_fixtures,extract_teams_id,fetch_teams_players_squads,fetch_teams_info
from scripts.transfer import transform_matches,transform_teams,transform_squads
from scripts.load import load_fixtures,load_squads,load_teams
#DAG's argumnets

default_args={
    'owner' : 'Zeyad',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



# DAGs Definition 

with DAG(
   dag_id="football-etl"
   ,
    default_args=default_args,
    description='Football stats DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
   
) as dag:



    #-----------------------------------------------------
    #Extract tasks
    #-----------------------------------------------------
    
    fetch_fixtures_task = PythonOperator(
            task_id='extract_fixtures',
            python_callable=fetch_fixtures,
            op_kwargs={'api_key': '3a3bd8764d180fafd01e23bafc751a09'}
        )

    extract_teams_task = PythonOperator(
        task_id='extract_teams_id',
        python_callable=extract_teams_id,
        provide_context=True,
    )

    extract_team_info_task = PythonOperator(
        task_id='extract_team_info',
        python_callable=fetch_teams_info,
        op_kwargs={
            'team_ids': "{{ ti.xcom_pull(task_ids='extract_teams_id')['team_ids'] }}",
            'api_key': '3a3bd8764d180fafd01e23bafc751a09'
        }
    )
    extract_team_squad_task = PythonOperator(
    task_id='extract_team_squad',
    python_callable=fetch_teams_players_squads,
    op_kwargs={
        'team_ids': "{{ ti.xcom_pull(task_ids='extract_teams_id')['team_ids'] }}",
        'api_key': '3a3bd8764d180fafd01e23bafc751a09'
    }
    )

    
    #-----------------------------------------------------
    #Transform tasks
    #-----------------------------------------------------
    transform_matches_task = PythonOperator(
        task_id='transform_matches',
        python_callable=transform_matches,
        provide_context=True,
    )


    transform_teams_task = PythonOperator(
        task_id='transform_teams',
        python_callable=transform_teams,
        provide_context=True,
    )

    transform_squads_task = PythonOperator(
        task_id='transform_squads',
        python_callable=transform_squads,
        provide_context=True,
    )

    #-------------------------------------------------------------
    # Load Tasks
    #-------------------------------------------------------------
    load_fixtures_task = PythonOperator(
    task_id='load_fixtures',
    python_callable=load_fixtures,
    provide_context=True,
    )

    load_teams_task = PythonOperator(
        task_id='load_teams',
        python_callable=load_teams,
        provide_context=True,
    )

    load_squads_task = PythonOperator(
        task_id='load_squads',
        python_callable=load_squads,
        provide_context=True,
    )
    
#-----------------------------------------------------
#Tasks Worflow
#-----------------------------------------------------
    

fetch_fixtures_task >> extract_teams_task
extract_teams_task >> extract_team_info_task
extract_team_info_task >> [extract_team_squad_task, transform_teams_task]
transform_teams_task >> load_teams_task
extract_team_squad_task >> transform_squads_task >> load_squads_task
fetch_fixtures_task >> transform_matches_task >> load_fixtures_task
# extract_team_squad_task>>transform_squads_task
    
    
    
    
