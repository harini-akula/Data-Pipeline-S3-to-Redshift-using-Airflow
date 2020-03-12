from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, Tests
from airflow.operators.postres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_tables = PostgresOperator(
    task_id='Drop_tables',
    dag=dag,
    sql='drop_tables.sql',
    autocommit=True,
    postgres_conn_id='redshift_conn'
)

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    sql='create_tables.sql',
    autocommit=True,
    postgres_conn_id='redshift_conn'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id='aws_conn',
    redshift_conn_id='redshift_conn',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}/',
    region='US-WEST-2',
    json_format='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id='aws_conn',
    redshift_conn_id='redshift_conn',
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song-data/A/A/A/',
    region='US-WEST-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_conn',
    insert_facts_query=SqlQueries.songplay_table_insert,
    facts_table_name='songplays'   
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.user_table_insert,
    dim_table_name='public.users',
    append_only_flag=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.song_table_insert,
    dim_table_name='public.songs',
    append_only_flag=False    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.artist_table_insert,
    dim_table_name='public.artists',
    append_only_flag=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.time_table_insert,
    dim_table_name='public.time',
    append_only_flag=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift_conn',
    table_rowcounts_tests=Tests.tests_results
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
