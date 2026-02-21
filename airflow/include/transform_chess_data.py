import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import pyspark.sql.functions as sf
from pyspark.sql.types import DateType, TimestampType

load_dotenv('/usr/local/airflow/.env')

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

spark = SparkSession.builder \
    .appName("ChessSilverTransformation") \
    .config("spark.jars", "/usr/local/airflow/jars/hadoop-aws-3.3.4.jar,/usr/local/airflow/jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
hadoop_conf.set("fs.s3a.path.style.access", "true")

print("SparkSession lista")

path_raw_detalles = "s3a://s3-bucket-chess-proyecto-raw/raw/detalles/*/*.json"
path_raw_stats = "s3a://s3-bucket-chess-proyecto-raw/raw/stats/*/*.json"

df_detalles = spark.read \
    .option("multiline", "true") \
    .json(path_raw_detalles)

df_stats = spark.read \
    .option("multiline", "true") \
    .json(path_raw_stats)


df_detalles_clean = df_detalles.select('player_id','country','followers','username','title','status','joined','last_online','is_streamer')

df_stats_clean = df_stats.select('username',
                'chess_blitz.best.rating','chess_blitz.last.rating','chess_blitz.record.win','chess_blitz.record.loss','chess_blitz.record.draw',
                'chess_rapid.best.rating','chess_rapid.last.rating','chess_rapid.record.win','chess_rapid.record.loss','chess_rapid.record.draw',
                )

df_stats_clean = df_stats.select(
        sf.col('username'),
        sf.col('chess_blitz.best.rating').alias('blitz_BestRating'),
        sf.col('chess_blitz.last.rating').alias('blitz_lastRating'),
        sf.col('chess_blitz.record.win').alias('blitz_Wins'),
        sf.col('chess_blitz.record.loss').alias('blitz_Losses'),
        sf.col('chess_blitz.record.draw').alias('blitz_Draws'),
        sf.col('chess_rapid.best.rating').alias('rapid_BestRating'),
        sf.col('chess_rapid.last.rating').alias('rapid_LastRating'),
        sf.col('chess_rapid.record.win').alias('rapid_Wins'),
        sf.col('chess_rapid.record.loss').alias('rapid_Losses'),
        sf.col('chess_rapid.record.draw').alias('rapid_Draws')
        )

df_detalles_clean = df_detalles_clean.withColumn("country", sf.element_at(sf.split(sf.col("country"), "/"), -1))
df_detalles_clean = df_detalles_clean.withColumn("joined", sf.col("joined").cast(TimestampType()).cast(DateType()))
df_detalles_clean = df_detalles_clean.withColumn("last_online", sf.col("last_online").cast(TimestampType()).cast(DateType()))

df_final = df_detalles_clean.join(df_stats_clean, ['username'], 'inner')

df_final = df_final.dropDuplicates(['player_id'])

path_silver = "s3a://s3-bucket-chess-proyecto-cleaned/silver/players_stats_unified/"
df_final.write.mode("overwrite").parquet(path_silver)
print(f"Datos limpios guardados en: {path_silver}")