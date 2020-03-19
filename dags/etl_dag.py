from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, Tests
from airflow.operators.postgres_operator import PostgresOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'Harini Akula',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': 'False'
}

# Create DAG object
dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          template_searchpath='/home/workspace/airflow',
          schedule_interval=None
        )

# Dummy operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Drop  and create any existing staging and analytics tables. 
create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    sql='/plugins/helpers/create_tables.sql',
    autocommit=True,
    postgres_conn_id='redshift_conn'
)

# Load stage_events data from S3 to redshift.
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id='aws_conn',
    redshift_conn_id='redshift_conn',
    table='public.staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/',
    region='US-WEST-2',
    json_format='s3://udacity-dend/log_json_path.json'
)

# Load stage_songs data from S3 to redshift.
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id='aws_conn',
    redshift_conn_id='redshift_conn',
    table='public.staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song-data/A/B/G/',
    region='US-WEST-2'
)

# Load songplays fact table in redshift.
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift_conn',
    insert_facts_query=SqlQueries.songplay_table_insert,
    facts_table_name='public.songplays'   
)

# Load users dimension table in redshift.
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.user_table_insert,
    dim_table_name='public.users',
    append_only_flag=False
)

# Load songs dimension table in redshift.
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.song_table_insert,
    dim_table_name='public.songs',
    append_only_flag=False    
)

# Load artists dimension table in redshift.
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.artist_table_insert,
    dim_table_name='public.artists',
    append_only_flag=False
)

# Load time dimension table in redshift.
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift_conn',  
    select_dim_query=SqlQueries.time_table_insert,
    dim_table_name='public.time',
    append_only_flag=False
)

# Perform data quality checks on tables in redshift.
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift_conn',
    table_rowcounts_tests=Tests.tests_results
)

# Dummy operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define task dependencies
start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
