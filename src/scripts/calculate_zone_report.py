import sys

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


namenode_url = sys.argv[1]
base_input_path = sys.argv[2]
base_output_path = sys.argv[3]

conf = SparkConf().setAppName(f"SaveReportZones")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)

def calculate_zone_report(namenode_url, base_input_path, base_output_path):
    mes_geo = sql.read.parquet(f"{namenode_url}{base_input_path}")

    events = ['subscription', 'message', 'reaction']

    zone_report = mes_geo\
        .withColumn("dt", F.to_timestamp("event.datetime", "yyyy-MM-dd HH:mm:ss"))\
        .where("dt is not null")\
        .withColumn("month", F.month("dt"))\
        .withColumn("week", F.weekofyear("dt"))\
        .withColumn("zone_id", F.col("city_id"))\
        .groupBy("week", "month", "zone_id")\
        .pivot("event_type", events)\
        .count()

    #Создаем отчет за месяц
    zone_month_report = zone_report\
        .groupBy("month", "zone_id")\
        .agg(F.expr("sum(subscription) as month_subscription"), F.expr("sum(message) as month_message"), F.expr("sum(reaction) as month_reaction"))

    #За неделю
    zone_week_report = zone_report\
        .groupBy("week", "zone_id")\
        .agg(F.expr("sum(subscription) as week_subscription"), F.expr("sum(message) as week_message"), F.expr("sum(reaction) as week_reaction"))


    #Соединяем и оставляем только требуемые аттрибуты
    zone_report = zone_report\
        .join(zone_week_report, on = ['week', 'zone_id'], how='inner')\
        .withColumn("week_user", F.lit(None))\
        .join(zone_month_report, on = ['month', 'zone_id'], how='inner')\
        .withColumn("month_user", F.lit(None))\
        .drop("message", "reaction", "subscription")
    
    # При записи партиционируем по номеру недели для удобства
    sql.write.mode("overwrite").partitionBy("week").parquet(f"{namenode_url}{base_output_path}")


def main():
    calculate_zone_report(namenode_url, base_input_path, base_output_path)

if __name__ == "__main__":
    main()