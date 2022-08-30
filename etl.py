import configparser
import os
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, DateType as Date, TimestampType as Ts, LongType as LInt
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql import SparkSession
from datetime import datetime


def create_spark_session():
    """
    Create Spark session
    :return: SparkSession object
    """
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processing JSON files in S3 Bucket input_data path and transform them to songs and artists tables.
    Finally, load them into S3 Bucket output_data in columnar format which is Parquet

    :param spark: SparkSession object
    :param input_data: S3 bucket path to input data
    :param output_data: S3 Bucket path to load dimensional and fact tables
    :return: None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data/*/*/*/*.json')

    # Fix the schema
    song_data_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Int()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Int())
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_data_schema)

    # Persist df to reuse in songs and artists tables (with default configuration is MEMORY_AND_DISK)
    df.persist()

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct() \
        .where(col('song_id').isNotNull()).dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']) \
        .distinct().where(col('artist_id').isNotNull()).dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))

    # Remove all bocks in memory and disk
    df.unpersist()


def process_log_data(spark, input_data, output_data):
    """
    Processing JSON files in S3 Bucket input_data path and transform them to users, time, and songplays tables.
    Finally, load them into S3 Bucket output_data in columnar format which is Parquet

    :param spark: SparkSession object
    :param input_data: S3 bucket path to input data
    :param output_data: S3 Bucket path to load dimensional and fact tables
    :return: None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # Fix the schema
    log_data_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Int()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Int()),
        Fld("song", Str()),
        Fld("status", Int()),
        Fld("ts", LInt()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_data_schema)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # Persist df to reuse in users, time, and songplays tables (with default configuration is MEMORY_AND_DISK)
    df.persist()

    # extract columns for users table
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).distinct() \
        .where(col('userId').isNotNull())

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), Ts())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), Date())
    df = df.withColumn('date_time', get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(['start_time',
                            hour('date_time').alias('hour'),
                            dayofmonth('date_time').alias('day'),
                            weekofyear('date_time').alias('week'),
                            month('date_time').alias('month'),
                            year('date_time').alias('year'),
                            dayofweek('date_time').alias('weekday')]).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))
    artist_df = spark.read.parquet(os.path.join(output_data, 'artists'))

    # create songplay_id column with monotonically_increasing_id()
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    songplays_df = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration))\
                        .join(artist_df, df.artist == artist_df.artist_name)\
                        .select(df.songplay_id,
                                df.start_time,
                                month(df.start_time).alias('month'),
                                year(df.start_time).alias('year'),
                                df.userId,
                                df.level,
                                song_df.song_id,
                                artist_df.artist_id,
                                df.sessionId,
                                df.location,
                                df.userAgent
                                )

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'songplays'))

    # Remove all bocks in memory and disk
    df.unpersist()


def main():
    """
    - Set up AWS credential
    - Process JSON files in input_data and load them into output_data
    :return: None
    """
    aws_credentials = configparser.ConfigParser()
    aws_credentials.read_file(open('dl.cfg'))

    os.environ['AWS_ACCESS_KEY_ID'] = aws_credentials.get('default', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = aws_credentials.get('default', 'AWS_SECRET_ACCESS_KEY')

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = aws_credentials.get('default', 'S3_BUCKET')

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

