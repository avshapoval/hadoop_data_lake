import sys
from timezonefinder import TimezoneFinder
from pyspark.sql import types as T
from cachetools import cached

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

namenode_url = sys.argv[1]
base_input_path = sys.argv[2]
base_output_path = sys.argv[3]

conf = SparkConf().setAppName(f"SaveReportUsersByCities")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

def get_user_geo(namenode_url, base_input_path):
    mes_geo = sql.read.parquet(f"{namenode_url}{base_input_path}")

    user_geo = mes_geo\
        .where("event_type == 'message'")\
        .selectExpr("event.message_from as user_id", "event.datetime as dt", "city_id", "city as city_name", "lat_c", "lon_c")\
        .withColumn("dt", F.to_timestamp("dt","yyyy-MM-dd HH:mm:ss"))

    return user_geo


def save_report(namenode_url, base_input_path, base_output_path):
    
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

    user_geo = get_user_geo(namenode_url, base_input_path)

    window_act_city = Window().partitionBy("user_id").orderBy(F.desc("dt"))

    user_city = user_geo\
        .where("dt is not null")\
        .withColumn("rn", F.row_number().over(window_act_city))

    #get act_city
    user_act_city = user_city\
        .where("rn == 1")\
        .withColumn("local_time", F.from_utc_timestamp("dt", get_tz(user_city.lon_c, user_city.lat_c)))\
        .select("user_id", F.expr("city_name as act_city"), "local_time")

    #get home_city
    user_home_city = user_city\
        .withColumn("days_stayed", (F.col("dt") - F.lag("dt", 1).over(window_act_city)).cast('long')/3600/24 * (-1))\
        .where("days_stayed > 27")\
        .groupBy("user_id", "city_id", F.expr("city_name as home_city"))\
        .agg(F.max("rn").alias("rn"))\
        .drop("rn", "city_id")\

    #get travel attributes
    user_cities_visited = user_geo\
        .where("dt is not null")\
        .groupBy("user_id")\
        .agg(F.sort_array(F.collect_list(F.struct("dt", "city_name")), asc=False).alias("collected"))\
        .withColumn("travel_array", F.col("collected.city_name"))\
        .drop("collected")\
        .withColumn("travel_count", F.size("travel_array"))

    #get result report
    user_city_info = user_act_city\
        .join(user_home_city, on = "user_id", how="left")\
        .join(user_cities_visited, on="user_id", how="left")

    #write it
    sql.write.mode("overwrite").parquet(f"{namenode_url}{base_output_path}")

def main():
    save_report(namenode_url, base_input_path, base_output_path)


if __name__ == "__main__":
    main()