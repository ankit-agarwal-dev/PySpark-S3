"""Load data into AWS S3 storage in parquet format.
This script is used to process and load data into AWS S3 store. 
This file can also be imported as a module and contains the following
functions:
    * create_spark_session - Initialising and creating Spark session. 
    * process_song_data - Processing song data JSON file and loading data into
      different S3 structures.
    * process_log_data - Processing log data JSON file and loading data into
      different S3 structures.
    * main - the main function of the script
"""

# Importing system libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Loading and Reading configuration settings. 
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


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
    song_data = input_data + "song-data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.format("json").load(song_data)
    
    # Create temporary view on songs data file
    df.createOrReplaceTempView("songs_data")
    
    # extract columns to create songs table
    songs_table = spark.sql('''select distinct song_id, title, artist_id, year, duration FROM songs_data''') 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet\
    (output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql('''select distinct artist_id, artist_name, artist_location, artist_latitude,\
    artist_longitude from songs_data''')
    
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
    log_data = input_data + "log-data/2018/11/*.json"

    # read log data file
    df = spark.read.format("json").load(log_data)
    
    # filter by actions for song plays
    df = df[df['page']=="NextSong"]
    
    # Create temporary view on log data file
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql('''select distinct userId, firstName, lastName, gender, level from log_data''') 
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn("datetime", get_timestamp(df.ts)) 
    
    # extract columns to create time table
    time_table = df.select('ts',\
                           hour('datetime').alias('hour'),\
                           dayofmonth('datetime').alias('day'),\
                           weekofyear('datetime').alias('week'),\
                           month('datetime').alias('month'),\
                           year('datetime').alias('year'),\
                           date_format('datetime', 'F').alias('weekday')\
                           ) 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/*/*/*.parquet"+)
    song_df.createOrReplaceTempView("songs_data")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''select distinct ts, userId, level, song_id, artist_id, sessionId, location, \
    userAgent from log_data join songs_data on log_data.artist = songs_data.artist_name and \
    log_data.song = songs_data.title and log_data.length = songs_data.duration''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').parquet(output_data + "songplays")

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