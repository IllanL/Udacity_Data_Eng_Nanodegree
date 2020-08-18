from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# Defining some variables for the script

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 8, 17),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_retry': False
    
    
data_quality_checks = list("""SELECT a.name AS name, COUNT(*) AS song_count 
                                                FROM artists AS a 
                                                JOIN songs AS b 
                                                ON a.artist_id=b.artist_id 
                                                GROUP BY a.name""")
    
# We start by creating the dag
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# A dummy operator starts the flow

start_operator = DummyOperator(task_id='Begin_execution',  
                               dag=dag)

# We firstly define two tasks for staging both jason files to Reshift

stage_events_to_redshift = StageToRedshiftOperator(task_id='Stage_events',
                                                   dag=dag,
                                                   table_name="public.staging_events",
                                                   redshift_id="redshift",
                                                   aws_cred="aws_credentials",
                                                   aws_s3_bucket="udacity-dend",
                                                   aws_s3_key="log_data",
                                                   json="s3://udacity-dend/log_json_path.json")

stage_songs_to_redshift = StageToRedshiftOperator(task_id='Stage_songs',
                                                  dag=dag,
                                                  table_name="public.staging_songs",
                                                  redshift_id="redshift",
                                                  aws_cred="aws_credentials",
                                                  aws_s3_bucket="udacity-dend",
                                                  aws_s3_key="song_data/A/A/A",
                                                  json="auto")

# Now we define five tasks for creating our tables

load_songplays_table = LoadFactOperator(task_id='Load_songplays_fact_table',
                                        dag=dag,
                                        redshift_id="redshift",
                                        table_name="public.songplays",
                                        sql_query=SqlQueries.songplay_table_insert)

load_user_dimension_table = LoadDimensionOperator(task_id='Load_user_dim_table',
                                                  dag=dag,
                                                  redshift_id="redshift",
                                                  table_name="public.users",
                                                  sql_query=SqlQueries.user_table_insert)

load_song_dimension_table = LoadDimensionOperator(task_id='Load_song_dim_table', 
                                                  dag=dag,
                                                  redshift_id="redshift",
                                                  table_name="public.songs",
                                                  sql_query=SqlQueries.song_table_insert)

load_artist_dimension_table = LoadDimensionOperator(task_id='Load_artist_dim_table',  
                                                    dag=dag,
                                                    redshift_id="redshift",
                                                    table_name="public.artists",
                                                    sql_query=SqlQueries.artist_table_insert)

load_time_dimension_table = LoadDimensionOperator(task_id='Load_time_dim_table', 
                                                  dag=dag,
                                                  redshift_id="redshift",
                                                  table_name="public.time",
                                                  sql_query=SqlQueries.time_table_insert)

# We create another task to run some checks in order to ensure that everything has gone as expected

run_quality_checks = DataQualityOperator(task_id='Run_data_quality_checks', 
                                        dag=dag,
                                        redshift_id="redshift",
                                        sql_queries=SqlQueries.data_quality_checks)

# And lastly, we close the whole process with a final task based on another dummy operator

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Finally, we assign priorities in the execution of the created tasks

stage_01 = start_operator
stage_02 = [stage_events_to_redshift, stage_songs_to_redshift]
stage_03 = load_songplays_table
stage_04 = [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
stage_05 = run_quality_checks
stage_06 = end_operator

stage_01 >> stage_02 >> stage_03 >> stage_04 >> stage_05 >> stage_06