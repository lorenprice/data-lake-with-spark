import configparser
from datetime import datetime
import os
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS_KEYS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS_KEYS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates and returns a SparkSession object
    Returns: SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Gets the directory path for song files and writes songs_table and artist_table in parquet files.
    Args:
        spark: Instance of SparkSession
        input_data: Path of the parent directory where the song files are present
        output_data: Path of the parent directory where the output tables are to be written
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(f'{output_data}songs.pq')

    # extract columns to create artists table
    artists_table = df.select('artist_id', col('artist_name').alias('name'), col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude'))

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(f'{output_data}artists.pq')


def process_log_data(spark, input_data, output_data):
    """
    Gets the directory path for log files and writes users_table, time_table, songplays table in parquet files.
    Args:
        spark: Instance of SparkSession
        input_data: Path of the parent directory where the song files are present
        output_data: Path of the parent directory where the output tables are to be written
    """
    # get file path to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.coalesce(1).write.mode('overwrite').parquet(f'{output_data}users.pq')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).isoformat())
    week_day = udf(lambda x: datetime.strptime(x.split('T')[0].strip(), '%Y-%m-%d').strftime('%w'))

    # extract columns to create time table
    time_table = df.select('ts')
    time_table = time_table.withColumn('start_time', get_timestamp('ts'))
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', week_day('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}time.pq')

    # read in song data to use for songplays table
    log_df = df.withColumn('songplay_id', monotonically_increasing_id()). \
        withColumn('start_time', get_timestamp('ts')). \
        withColumn('month', month('start_time')). \
        select(col('userId').alias('user_id'), col('sessionId').alias('session_id'),
               col('userAgent').alias('user_agent'), 'length', 'artist', 'location', 'song', 'level', 'songplay_id',
               'start_time', 'month')

    songs_df = spark.read.parquet('data/output_data/songs.pq')
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_df.join(songs_df,
                                  (log_df.song == songs_df.title) & (log_df.length == songs_df.duration)).select(
        'songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent',
        'year', 'month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}songplays.pq')


def main():
    """
    Create spark session and run all functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-lprice/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()