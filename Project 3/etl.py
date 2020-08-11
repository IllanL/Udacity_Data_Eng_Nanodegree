import configparser
import os
from pyspark.sql import SparkSession

import datetime as dt

from pyspark.sql.functions import udf, col, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date, dayofweek

from pyspark.sql.types import TimestampType, DateType, IntegerType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """Creates a Spark session, or returns the previously created one."""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """Downloads data from S3 and creates the following tables: songs and artists.    
    Requires as input: the spark session, URL of the S3 bucket and the output path"""
    
    
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    extracted_songs_df = song_df['song_id', 'title', 'artist_id','artist_name', 'year', 'duration'].drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    extracted_songs_df.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = song_df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    
      """Downloads data from S3 and creates the following tables: users, time and songplays.    
    Requires as input: the spark session, URL of the S3 bucket and the output path"""  
    
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.where(col("page")=="NextSong")

    # extract columns for users table    
    artists_table = log_df['userId', 'firstName', 'lastName', 'gender', 'level','ts'].dropDuplicates().drop('ts')
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: dt.datetime.fromtimestamp(x/1000), TimestampType())
    log_df = log_df.withColumn('ts_to_timestamp', get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:  x, DateType())
    log_df = log_df.withColumn('ts_to_date', get_datetime(log_df.ts_to_timestamp))
    
    # definining function for extracting the fields (year, month, etc...) from the timestamp
    get_date_col = udf(lambda x, y: x.timetuple()[y], IntegerType())
    
    # extracting columns and creating time table
    time_table = log_df['ts_to_timestamp','ts_to_date']\
        .withColumn('year', get_date_col(log_df.ts_to_timestamp, lit(0)))\
        .withColumn('month',  get_date_col(log_df.ts_to_timestamp, lit(1)))\
        .withColumn('day',  get_date_col(log_df.ts_to_timestamp, lit(2)))\
        .withColumn('hour',  get_date_col(log_df.ts_to_timestamp, lit(3)))\
        .withColumn('week', weekofyear(log_df.ts_to_timestamp))\
        .withColumn('weekday', dayofweek(log_df.ts_to_timestamp))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet("results/songs.parquet")
    
    # creating views for join:
    log_df.createOrReplaceTempView("logs")
    songs_df.createOrReplaceTempView("songs")
    
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id, 
                                        a.ts_to_timestamp AS start_time, 
                                        a.userId AS userId, 
                                        a.level AS level, 
                                        b.song_id AS song_id, 
                                        b.artist_id AS artist_id, 
                                        a.sessionId AS sessionId, 
                                        a.location AS location, 
                                        a.userAgent AS userAgent
                                    FROM logs as a
                                    JOIN songs as b
                                    ON a.artist = b.artist_name
                                        AND a.song = b.title""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "/results"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
