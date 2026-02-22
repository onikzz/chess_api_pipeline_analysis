import os
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as sf
from dotenv import load_dotenv


def gold_data():
    load_dotenv('/usr/local/airflow/.env')

    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    spark = SparkSession.builder \
        .appName("ChessGoldTransformation") \
        .config("spark.jars", "/usr/local/airflow/jars/hadoop-aws-3.3.4.jar,/usr/local/airflow/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_access_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("SparkSession lista")

    df_silver = spark.read.parquet("s3a://s3-bucket-chess-proyecto-cleaned/silver/players_stats_unified/")

    df_pivot_titles = df_silver.groupBy("country").pivot("title").count().fillna(0)

    for col_name in df_pivot_titles.columns:
        if col_name != "country":
            df_pivot_titles = df_pivot_titles.withColumnRenamed(col_name, f"count_{col_name}")
    window_blitz = Window.partitionBy("country").orderBy(sf.col("blitz_lastRating").desc())

    df_ranked = df_silver.withColumn("rank", sf.row_number().over(window_blitz))

    df_top_players = df_ranked.filter(sf.col("rank") == 1).select(
        sf.col("country").alias("country_ref"),
        sf.col("username").alias("top_player_name"),
        sf.col("blitz_lastRating").alias("top_player_elo")
    )

    df_averages = df_silver.groupBy("country").agg(
        sf.round(sf.avg("blitz_lastRating"), 1).alias("avg_blitz_rating"),
        sf.round(sf.avg("rapid_LastRating"), 1).alias("avg_rapid_rating"),
        sf.count("player_id").alias("total_players")
    )

    df_gold = df_averages.join(df_pivot_titles, "country", "inner") \
                        .join(df_top_players, df_averages.country == df_top_players.country_ref, "inner") \
                        .drop("country_ref")

    path_gold = "s3a://s3-bucket-chess-proyecto-cleaned/gold/stats_by_country/"
    df_gold.write.mode("overwrite").parquet(path_gold)

    print(f"Gold generada en: {path_gold}")

    spark.stop()

if __name__ == "__main__":
    gold_data()