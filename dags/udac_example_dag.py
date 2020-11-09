from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

### Create DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018, 11, 1), # we only have data from 2018/11
    'catchup': True,
    'end_date': datetime(2018,11,2)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
)

### Start operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

### Create tables only if not exist
create_staging_events = CreateTableOperator(
    task_id='create_staging_events',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_events',
    sql_command=SqlQueries.create_staging_events
)

create_staging_songs = CreateTableOperator(
    task_id='create_staging_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    sql_command=SqlQueries.create_staging_songs
)

create_songplays = CreateTableOperator(
    task_id='create_songplays',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songplays',
    sql_command=SqlQueries.create_songplays
)

create_artists = CreateTableOperator(
    task_id='create_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='artists',
    sql_command=SqlQueries.create_artists
)

create_songs = CreateTableOperator(
    task_id='create_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='songs',
    sql_command=SqlQueries.create_songs
)

create_time = CreateTableOperator(
    task_id='create_time',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='time',
    sql_command=SqlQueries.create_time
)

create_users = CreateTableOperator(
    task_id='create_users',
    dag=dag,
    redshift_conn_id='redshift',
    table_name='users',
    sql_command=SqlQueries.create_users
)

### syn operator to make sure all tables are created
sync_operator_1 = DummyOperator(task_id='sync_operator_1',  dag=dag)

### Staging from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_buckt='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}',
    s3_format='json',
    s3_format_mode='s3://udacity-dend/log_json_path.json',
    region='us-west-2',
    target_table='staging_events',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    render_s3_key=True,
    truncate=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_buckt='udacity-dend',
    s3_key='song_data',
    s3_format='json',
    s3_format_mode='auto',
    region='us-west-2',
    target_table='staging_songs',
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    render_s3_key=False,
    truncate=True
)

### Load fact table, do not truncate
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table='songplays',
    target_columns='(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.songplay_table_insert,
    truncate=False
)

### Load dim tables, do truncate
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table='users',
    target_columns='(userid, first_name, last_name, gender, level)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table='songs',
    target_columns='(songid, title, artistid, year, duration)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table='artists',
    target_columns='(artistid, name, location, lattitude, longitude)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table='time',
    target_columns='(start_time, hour, day, week, month, year, weekday)',
    redshift_conn_id='redshift',
    sql_transform_command=SqlQueries.time_table_insert,
    truncate=True
)

### The following dict defines data quality metrics
# - metric_dict['not_null'] defines which columns of which table should not be null;
# - metric_dict['row_count'] defines the expected range of row counts for which table, 'None' means no limit;
# - Additional metrics can be defined by adding additional key-value pairs to the metric_dict 
#   and by adding codes in DataQualityOperator;
metric_dict = {'not_null': [{'table': 'songplays', 'columns': ['playid', 'start_time', 'userid']},
                           {'table': 'artists', 'columns': ['artistid']},
                           {'table': 'songs', 'columns': ['songid']},
                           {'table': 'time', 'columns': ['start_time']},
                           {'table': 'users', 'columns': ['userid']}],

              'row_count': [{'table': 'songplays', 'min': 1, 'max': None},
                            {'table': 'artists', 'min': 1, 'max': None},
                            {'table': 'songs', 'min': 1, 'max': None},
                            {'table': 'time', 'min': 1, 'max': None},
                            {'table': 'users', 'min': 1, 'max': None}]
            }

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    metric_dict=metric_dict
)

### End operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

### Define dependencies for DAG
start_operator >> [create_staging_events, create_staging_songs, create_songplays, \
                    create_artists, create_songs, create_time, create_users] >> sync_operator_1

sync_operator_1 >>[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, \
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
