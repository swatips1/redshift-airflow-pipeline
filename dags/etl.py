from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# Setup default arguments for the dag
default_args = {
    'owner': 'sskelly',
#     'start_date': datetime(2019, 1, 12),
    'start_date' :  datetime(2018, 11, 1),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False,
    'email_on_retry': False
}

# ETL Dag
dag = DAG('10_sparkify_airflow_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Post Event data into redshit
stage_events_to_redshift = StageToRedshiftOperator(  
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_prefix="log_data/2018/11/2018-11-01-events.json",
    extra_params=" IGNOREHEADER 1 FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_prefix='song-data/A/A/A/TRAAAAK128F9318786.json',
    extra_params="FORMAT AS JSON 'auto'"
)

create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_create
)

create_staging_songs_table = PostgresOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_songs_table_create
)

create_songplays_table = PostgresOperator(
    task_id='Create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songplays_table_create
)

create_artists_table = PostgresOperator(
    task_id='Create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.artists_table_create
)

create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.songs_table_create
)

create_users_table = PostgresOperator(
    task_id='Create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.users_table_create
)

create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.time_table_create
)

schema_created = DummyOperator(task_id='Schema_created', dag=dag)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    select_sql=SqlQueries.songplays_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    select_sql=SqlQueries.users_table_insert,
    mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag, 
    table='songs',
    redshift_conn_id="redshift",
    select_sql=SqlQueries.songs_table_insert,
    mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    select_sql=SqlQueries.artists_table_insert,
    mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    select_sql=SqlQueries.time_table_insert,
    mode='truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_stmts=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# DAG dependencies
# Run this one time and then enable the test flow below.

start_operator >> create_staging_songs_table
start_operator >> create_staging_events_table
start_operator >> create_songplays_table
start_operator >> create_artists_table
start_operator >> create_songs_table
start_operator >> create_users_table
start_operator >> create_time_table

create_staging_events_table >> schema_created
create_staging_songs_table >> schema_created
create_songplays_table >> schema_created
create_artists_table >> schema_created
create_songs_table >> schema_created
create_users_table >> schema_created
create_time_table >> schema_created

schema_created >> stage_events_to_redshift
schema_created >> stage_songs_to_redshift


stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator 


