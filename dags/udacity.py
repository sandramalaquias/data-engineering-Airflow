
## udac_example_dag.py

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from operators import StageToRedshiftOperator, HasRowsOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator

from helpers import SqlQueries, SqlCreateTables

import pendulum
local_tz = pendulum.timezone("UTC")

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1, 0, 0, 0, tzinfo=local_tz),
    'end_date': datetime(2018, 11, 1, 0, 0, 0, tzinfo=local_tz),
    'schedule_interval': '@hourly', 
    'depends_on_past': False,
    'catchup':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udacity_project5',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          tags=["smm"]
        ) 
         
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='End_execution',  dag=dag)
       
####  create staging tables

group_staging_task = TaskGroup(
    "create_staging_task", 
    tooltip="Tasks for create staging",
    dag=dag
) 

with group_staging_task:
    create_staging_events_table= PostgresOperator(
        task_id="create_staging_events",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_STAGING_EVENTS_TABLE
        )
    create_staging_songs_table= PostgresOperator(
        task_id="create_staging_songs",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_STAGING_SONGS_TABLE
        )

    create_staging_events_table
    create_staging_songs_table
    
####  create star schema tables

group_schema_task = TaskGroup(
    "create_schema_task", 
    tooltip="Tasks for create star schema tables",
    dag=dag
) 

with group_schema_task:
    create_artist_table= PostgresOperator(
        task_id="create_artist_table",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_ARTISTS_TABLE
        )
    create_songplays_table= PostgresOperator(
        task_id="create_songplays_table",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_SONGPLAYS_TABLE
        )
    create_songs_table= PostgresOperator(
        task_id="create_songs_table",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_SONGS_TABLE
        )
    create_time_table= PostgresOperator(
        task_id="create_time_table",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_TIME_TABLE
        )
    create_users_table= PostgresOperator(
        task_id="create_users_table",
        dag=dag,
        postgres_conn_id="udt_redshift",
        sql=SqlCreateTables.CREATE_USERS_TABLE
        )
    create_artist_table
    create_songplays_table
    create_songs_table
    create_time_table
    create_users_table
      
## load data from S3 to Redshift staging

group_load_stage_task = TaskGroup(
    "load_stage_task", 
    tooltip="Tasks for load stage tables",
    dag=dag
) 

with group_load_stage_task:
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id="udt_redshift",
        aws_credentials="udt_credentials",
        table="public.staging_events",
        bucket_name="s3_udacity",
        bucket_file="log_data",
        json_data=True
)
    check_stage_events_table = HasRowsOperator(
        task_id='check_stage_events',
        dag=dag,
        table="public.staging_events",
        redshift_conn_id="udt_redshift"
        )
        
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        redshift_conn_id="udt_redshift",
        aws_credentials="udt_credentials",
        table="public.staging_songs",
        bucket_name="s3_udacity",
        bucket_file="song_data",
        json_data=False
)
    check_stage_songs_table = HasRowsOperator(
        task_id='check_stage_songs',
        dag=dag,
        table="public.staging_songs",
        redshift_conn_id="udt_redshift"
        )
    stage_events_to_redshift >> check_stage_events_table
    stage_songs_to_redshift >> check_stage_songs_table

## load FACT table
group_fact_task = TaskGroup(
    "load_songplays_task", 
    tooltip="Tasks for songsplays",
    dag=dag
) 

with group_fact_task:    
    load_songplays_task = LoadFactOperator(
       task_id='load_songplays',
       dag=dag,
       redshift_conn_id="udt_redshift",
       aws_credentials="udt_credentials",
       table="public.songplays",
       sql_load_table=SqlQueries.songplay_table_insert
    )

    check_songplays_task = HasRowsOperator(
       task_id='check_songplays',
       dag=dag,
       table="public.songplays",
       redshift_conn_id="udt_redshift"
       )
    load_songplays_task >> check_songplays_task
    
## load dimension tables

group_dim_task = TaskGroup(
    "load_dimension_task", 
    tooltip="Tasks for dimension tables",
    dag=dag
) 

with group_dim_task: 
# load song   
    load_song_task = LoadDimensionOperator(
       task_id='load_song',
       dag=dag,
       redshift_conn_id="udt_redshift",
       aws_credentials="udt_credentials",
       table="public.songs",
       sql_load_table=SqlQueries.song_table_insert,
       delete_table=False
    )

    check_song_task = HasRowsOperator(
       task_id='check_songs',
       dag=dag,
       table="public.songs",
       redshift_conn_id="udt_redshift"
       )
       
# load artists      
    load_artist_task = LoadDimensionOperator(
       task_id='load_artists',
       dag=dag,
       redshift_conn_id="udt_redshift",
       aws_credentials="udt_credentials",
       table="public.artists",
       sql_load_table=SqlQueries.artist_table_insert,
       delete_table=False
    )

    check_artist_task = HasRowsOperator(
       task_id='check_artists',
       dag=dag,
       table="public.artists",
       redshift_conn_id="udt_redshift"
       )
    
# load user
    load_user_task = LoadDimensionOperator(
       task_id='load_users',
       dag=dag,
       redshift_conn_id="udt_redshift",
       aws_credentials="udt_credentials",
       table="public.users",
       sql_load_table=SqlQueries.user_table_insert,
       delete_table=False
    )

    check_user_task = HasRowsOperator(
       task_id='check_users',
       dag=dag,
       table="public.users",
       redshift_conn_id="udt_redshift"
       )   
       
# load time
    load_time_task = LoadDimensionOperator(
       task_id='load_time',
       dag=dag,
       redshift_conn_id="udt_redshift",
       aws_credentials="udt_credentials",
       table="public.time",
       sql_load_table=SqlQueries.time_table_insert,
       delete_table=True
    )

    check_time_task = HasRowsOperator(
       task_id='check_time',
       dag=dag,
       table="public.time",
       redshift_conn_id="udt_redshift"
       ) 
       
    load_song_task >> check_song_task
    load_artist_task >> check_artist_task
    load_user_task >> check_user_task
    load_time_task >> check_time_task
    
# quality test

quality_task = DataQualityOperator(
       task_id='quality_test',
       dag=dag,
       redshift_conn_id="udt_redshift",
       aws_credentials="udt_credentials",
       tables=SqlQueries.tables, 
       sql_count=SqlQueries.sql_count,
       sql_result=SqlQueries.sql_result 
    )

[group_staging_task, group_schema_task] >> start_operator >> group_load_stage_task >> group_fact_task >> group_dim_task >> quality_task >> end_operator


 
