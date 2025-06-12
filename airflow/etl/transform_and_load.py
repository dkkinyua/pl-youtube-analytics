import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
DB_URL = os.getenv("DB_URL")
DB_PWD = os.getenv("DB_PWD")

spark = SparkSession.builder.appName('PL YouTube Analytics').config("spark.jars", os.path.expanduser("~/jars/postgresql-42.7.3.jar")).getOrCreate()
filepath = "/home/deecodes/pl-youtube-analytics/docs/youtube_data.json"
properties = {
    'user': 'avnadmin',
    'password': DB_PWD,
    "driver": "org.postgresql.Driver"
}

def transform_load(spark, filepath, properties):
    df = spark.read.option("multiLine", True).json(filepath)

    videos = df.createOrReplaceTempView("videos") 
    new_df = spark.sql("""
        SELECT 
            title,
            videoId,
            CAST(viewCount as INT) as viewCount,
            CASE
                WHEN CAST(viewCount AS INT) >= 100000 THEN 'very viral'
                WHEN CAST(viewCount AS INT) BETWEEN 10000 AND 99999 THEN 'viral'
                ELSE 'normal viewing'
            END AS video_virality,
            CAST(commentCount AS INT) AS commentCount,
            CAST(likeCount AS INT) AS likeCount,
            CAST(publishedAt AS DATE) AS date
        FROM videos
    """)
    try:
        new_df.write.jdbc(url=DB_URL, table='pl_analytics', mode='overwrite', properties=properties)
        print("Data loaded into Postgres DB successfully!")
    except Exception as e:
        print(f"Loaded Spark Dataframe into db error: {e}")

# wrapper function for Airflow task
def transform_task():
    print("Transforming and loading data now...")
    transform_load(spark, filepath, properties)

