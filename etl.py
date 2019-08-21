"""Read, Process and Load data from and into AWS S3 storage in parquet format.
 This script is used to process and load data into AWS S3 store.
 This file can also be imported as a module and contains the following
 functions:
 * create_spark_session - Initialising and creating Spark session.
 * process_song_data - Read song data JSON file from S3, processing the same
   and loading data into AWS S3 structures.
 * process_log_data - Read log data JSON file from S3, processing the same
   and loading data into AWS S3 structures.
 * main - the main function of the script
"""

# Importing system libraries
import configparser
import datetime
import os
from pyspark.sql import SparkSession

# Loading and Reading configuration settings.
CONFIG = configparser.ConfigParser()
CONFIG.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = CONFIG['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = CONFIG['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Initialising and creating spark session.
    Parameters
    ___________
        None
    Returns
    ___________
        Spark - Spark connection object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Loading and processing song JSON file.
    Parameters
    ___________
        Spark - Spark connection object
        input_data - Path for S3 Input data directory
        output_data - Path for S3 output data directory
    Returns
    ___________
        None
    """

    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"

    # read song data file
    data_frame = spark.read.format("json").load(song_data)

    # Create temporary view on songs data file
    data_frame.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = spark.sql('''select distinct song_id, title, artist_id,\
     year, duration FROM songs_data''')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year",\
                                                    "artist_id"\
                                                    ).parquet(output_data + "songs")
    # extract columns to create artists table
    artists_table = spark.sql('''select distinct artist_id, artist_name, \
    artist_location, artist_latitude,artist_longitude from songs_data''')

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")

def process_log_data(spark, input_data, output_data):
    """Loading and processing log JSON file.
    Parameters
    ___________
        Spark - Spark connection object
        input_data - Path for S3 Input data directory
        output_data - Path for S3 output data directory
    Returns
    ___________
        None
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    data_frame = spark.read.format("json").load(log_data)

    # filter by actions for song plays
    data_frame = data_frame[data_frame['page'] == "NextSong"]

    # Create temporary view on log data file
    data_frame.createOrReplaceTempView("log_data")

    # extract columns for users table
    users_table = spark.sql('''select distinct userId, firstName, lastName, gender,\
    level from log_data''')

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # Spark's UDF for extrating various time values from EPOCH time
    spark.udf.register("get_timestamp", lambda x: datetime.datetime.fromtimestamp(x / 1000.0).\
                       strftime('%Y-%m-%d %H:%M:%S'))
    spark.udf.register("get_month", lambda x: datetime.datetime.fromtimestamp(x / 1000.0).month)
    spark.udf.register("get_year", lambda x: datetime.datetime.fromtimestamp(x / 1000.0).year)
    spark.udf.register("get_hour", lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour)
    spark.udf.register("get_day", lambda x: datetime.datetime.fromtimestamp(x / 1000.0).day)
    spark.udf.register("get_weekday", lambda x: datetime.datetime.fromtimestamp(x/1000).\
                       strftime("%A"))
    spark.udf.register("get_week", lambda x: datetime.datetime.fromtimestamp(x/1000).strftime("%V"))

    # extract columns to create time table
    time_table = spark.sql('''select distinct get_timestamp(ts) as start_time, \
    get_hour(ts) as hour, get_day(ts) as day, get_week(ts) as week, get_month(ts) as month,\
    get_year(ts) as year, get_weekday(ts) as weekday from log_data''')

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql('''select distinct get_timestamp(ts) as start_time, userId,\
     level, song_id,artist_id, sessionId, location, userAgent, get_month(ts) as month, \
     get_year(ts) as year from log_data join songs_data on log_data.artist = \
     songs_data.artist_name and log_data.song = songs_data.title and log_data.length = \
     songs_data.duration''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").\
        parquet(output_data + "songplays")

def main():
    """
        Main Function
    """
    # Creating Spark session
    spark = create_spark_session()

    # Input & output S3 object directory
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-spark-output/"

    # Processing Song JSON file
    process_song_data(spark, input_data, output_data)

    # Processing log JSON file
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
