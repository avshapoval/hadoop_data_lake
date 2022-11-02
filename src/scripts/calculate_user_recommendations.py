import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

namenode_url = sys.argv[1]
base_input_path = sys.argv[2]
base_output_path = sys.argv[3]


def calculate_user_recommendations(namenode_url: str, base_input_path: str, base_output_path: str, spark: SparkSession):
    mes_geo = spark.read.parquet(f"{namenode_url}{base_input_path}")

    channels_users = mes_geo\
        .where("event_type == 'subscription' and event.user is not null")\
        .selectExpr("event.subscription_channel as subc", "event.user")\
        .withColumn("user", F.col("user").cast('bigint'))

    right = channels_users.select(F.col("user").alias("user_right"), "subc", "city_id", F.col("lat").alias("lat_r"), F.col("lon").alias("lon_r"), "lat_c", "lon_c")
    left = channels_users.select(F.col("user").alias("user_left"), "subc", "city_id", F.col("lat").alias("lat_l"), F.col("lon").alias("lon_l"))

    r = 6371

    #Для того, чтобы не создавать экземпляр класса каждый раз при вызове udf, можно закэшировать этот экземпляр
    @cached(cache={})
    def get_tf():
        return TimezoneFinder()

    #udf для получения временной зоны по координатам (просто подставить город в from_utc_timestamp,
    #к сожалению, не вышло - функция не принимает сравнительно небольшие города, к которым не привязаны часовые пояса)
    @F.udf(returnType = T.StringType())
    def get_tz(lng, lat):
        tf = get_tf()
        tz_str = tf.timezone_at(lng=lng, lat=lat)
        return tz_str

    pairs = left\
        .join(right, on=['subc', 'city_id'], how='inner')\
        .where("user_left < user_right")\
        .withColumn("diff", 2 * r * F.abs(F.asin(F.sqrt(F.sin(("lat_r" - "lat_l") / 2) ** 2 + F.cos("lat_r") * F.cos("lat_l") * (F.sin(("lon_r" - "lon_l") / 2) ** 2)))))\
        .where("diff <= 1")\
        .withColumn("local_time", F.from_utc_timestamp("dt", get_tz("lon_c", "lat_c")))\
        .withColumn("processed_dttm", F.current_date())\
        .selectExpr("user_left", "user_right", "processed_dttm", "city_id as zone_id", "local_time")

    pairs.write.mode("overwrite").partitionBy("week").parquet(f"{namenode_url}{base_output_path}")

def main():
    spark = SparkSession.builder.master("yarn").appName(f"calculate_user_recs").getOrCreate()
    calculate_user_recommendations(namenode_url, base_input_path, base_output_path, spark)

if __name__ == "__main__":
    main()