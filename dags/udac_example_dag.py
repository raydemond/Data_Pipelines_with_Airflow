from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)
from helpers import SqlQueries



default_args = {
    'owner': 'Tingrui_Feng', 
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes = 5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_tables = PostgresOperator(
#     task_id="create_tables",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql= 'create_tables.sql'
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id="aws_credentials", 
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    table = 'staging_events',
    region = 'us-west-2',
    json_format = 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id="aws_credentials",
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    table = 'staging_songs',
    region = 'us-west-2',
    json_format = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    query = SqlQueries.user_table_insert,
    truncate_insert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    query = SqlQueries.song_table_insert,
    truncate_insert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    query = SqlQueries.artist_table_insert,
    truncate_insert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'time',
    query = SqlQueries.time_table_insert,
    truncate_insert = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL', 'expected_result': 0, 'comparison':'='},
        { 'check_sql': 'SELECT COUNT(*) FROM songplays WHERE start_time IS NULL', 'expected_result': 0, 'comparison':'='},
        { 'check_sql': 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL', 'expected_result': 0, 'comparison':'='},
        { 'check_sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL', 'expected_result': 0, 'comparison':'=' },
        { 'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL', 'expected_result': 0, 'comparison':'=' },
        { 'check_sql': 'SELECT COUNT(*) FROM users WHERE gender IS NULL', 'expected_result': 0, 'comparison':'=' },
        { 'check_sql': 'SELECT COUNT(*) FROM time WHERE day IS NULL', 'expected_result': 0, 'comparison':'=' },  
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]


[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table,
                         load_user_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

[load_song_dimension_table,
 load_user_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
