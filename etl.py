import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create an Apache Spark Session
    
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
     Get song data from S3 input folder, process and
     store them in parquet formated file in S3 output folder

     Parameters:
     - spark : spark session
     - input_data: path to song data in S3
     - output_data: path to output folder in S3 
     '''
    
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    song_schema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("year",Int())
    ])
    
    df = spark.read.json(song_data,schema=song_schema)

    songs_table = df.selectExpr(['song_id', 'title', 'artist_id',\
                                 'year', 'duration']).dropDuplicates()
    
    songs_table.write.mode("overwrite").partitionBy\
    ("year", "artist_id").parquet(output_data+'songs')

    artists_table = df.selectExpr(['artist_id', 'artist_name', 'artist_location',\
                                   'artist_latitude', 'artist_longitude']).dropDuplicates()
    
    artists_table.write.mode("overwrite").parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    """Get log data from S3 input folder, process and
    store them in parquet formated file in S3 output folder
    
     Parameters:
                    spark : spark session
                    input_data: path to log data in S3
                    output_data: path to output folder in S3"""
    
    #Read log data
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    
    log_schema = R([
        Fld("artist",Str()),
        Fld("auth",Str()),
        Fld("firstName",Str()),
        Fld("gender",Str()),
        Fld("itemInSession",Int()),
        Fld("lastName",Str()),
        Fld("length",Dbl()),
        Fld("level",Str()),
        Fld("location",Str()),
        Fld("method",Str()),
        Fld("page",Str()),
        Fld("registration",Dbl()),
        Fld("sessionId",Int()),
        Fld("song",Str()),
        Fld("status",Int()),
        Fld("ts",Int()),
        Fld("userAgent",Str()),
        Fld("userId",Int())
    ])
    
    df = spark.read.json(log_data,schema=log_schema)
    
    df = df.filter(df.page=='NextSong')

    users_table = df.selectExpr([
        "userId as user_id", "fistName as fisrt_name",
        "lastName as last_name", "gender", "level"
    ]).dropDuplicates()
    
    users_table.write.mode("overwrite").parquet(output_data+"users")

    get_timestamp = udf(lambda x: int(int(x)/1000))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn("date_time", get_datetime(df.timestamp))
    
    time_table = df.select(col("timestamp").alias("start_time"),
                          hour("date_time").alias("hour"),
                          dayofmonth("date_time").alias("day"),
                           weekofyear("date_time").alias("week"),
                           month("date_time").alias("month"),
                           year("date_time").alias("year"),
                           date_format("date_time").alias("weekday"))
    
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"time")

    song_df = spark.read.parquet(os.path.join(output_data,"songs/*/*/*.parquet"))

    songplays_table = df.join(song_df, (df.song == song_df.title)
                             & (df.artist == song_df.artist_name)
                             & (df.lenght == song_df.duration), 
                              'left_outer').select(df.timestamp,
                                                   col("userId").alias("user_id"),
                                                   df.level,
                                                   song_df.song_id,
                                                   song_df.artist_id,
                                                   col("sessionId").alias("session_id"),
                                                   df.location,
                                                   col("useragent").alias("user_agent"),
                                                   year("datetime").alias("year"),
                                                   month("datime").alias("month"))
                               
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"songplays")


def main():
    '''
    Run ETL process using process_song_data and process_log_data functions
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-project-mar15/data_lake_project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

    
    
