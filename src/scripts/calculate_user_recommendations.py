import sys

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

namenode_url = sys.argv[1]
base_input_path = sys.argv[2]
base_output_path = sys.argv[3]

conf = SparkConf().setAppName(f"SaveReportRecommendations")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

def calculate_user_recommendations(namenode_url, base_input_path, base_output_path):
    mes_geo = sql.read.parquet(f"{namenode_url}{base_input_path}")

    channels_users = mes_geo\
        .where("event_type == 'subscription' and event.user is not null")\
        .selectExpr("event.subscription_channel as subc", "event.user")\
        .withColumn("user", F.col("user").cast('bigint'))

    right = channels_users.select(F.col("user").alias("user_right"), "subc")
    left = channels_users.select(F.col("user").alias("user_left"), "subc")

    pairs = left\
        .join(right, on=['subc'], how='inner')\
        .where("user_left < user_right")

    sql.write.mode("overwrite").partitionBy("week").parquet(f"{namenode_url}{base_output_path}")

def main():
    calculate_user_recommendations(namenode_url, base_input_path, base_output_path)

if __name__ == "__main__":
    main()