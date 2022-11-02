import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

namenode_url = sys.argv[1]
base_input_path = sys.argv[2]
base_output_path = sys.argv[3]


def calculate_zone_report(namenode_url: str, base_input_path: str, base_output_path: str, spark: SparkSession):
    mes_geo = spark.read.parquet(f"{namenode_url}{base_input_path}")

    events = ['subscription', 'message', 'reaction']

    zone_prep = mes_geo\
        .withColumn("dt", F.to_timestamp("event.datetime", "yyyy-MM-dd HH:mm:ss"))\
        .where("dt is not null")\
        .withColumn("month", F.month("dt"))\
        .withColumn("week", F.weekofyear("dt"))\
        .withColumn("zone_id", F.col("city_id"))\
        .groupBy("week", "month", "zone_id")\
        .pivot("event_type", events)\
        .count()

    #Вычисляем количество регистраций
    window = Window().partitionBy(["event.message_from"]).orderBy([F.col('dt').asc()]) 
    regs = mes_geo\
        .withColumn("dt", F.to_timestamp("event.datetime", "yyyy-MM-dd HH:mm:ss"))\
        .where("dt is not null")\
        .withColumn("month", F.month("dt"))\
        .withColumn("week", F.weekofyear("dt"))\
        .withColumn("zone_id", F.col("city_id"))\
        .withColumn('rn', F.row_number().over(window)).where(F.col('rn')==1)
    
    reg_m = regs.groupBy(["zone_id", "month"]).agg(F.count("event.message_id").alias('month_user')) 
    reg_w = regs.groupBy(["zone_id", "week"]).agg(F.count("event.message_id").alias('week_user'))

    #Создаем отчет за месяц
    zone_month_report = zone_prep\
        .groupBy("month", "zone_id")\
        .agg(F.expr("sum(subscription) as month_subscription"), F.expr("sum(message) as month_message"), F.expr("sum(reaction) as month_reaction"))

    #За неделю
    zone_week_report = zone_prep\
        .groupBy("week", "zone_id")\
        .agg(F.expr("sum(subscription) as week_subscription"), F.expr("sum(message) as week_message"), F.expr("sum(reaction) as week_reaction"))


    #Соединяем и оставляем только требуемые аттрибуты
    zone_report = zone_prep\
        .join(zone_week_report, on = ['week', 'zone_id'], how='inner')\
        .join(zone_month_report, on = ['month', 'zone_id'], how='inner')\
        .join(reg_m, on = ['month', 'zone_id'], how='inner')\
        .join(reg_w, on = ['week', 'zone_id'], how='inner')\
        .drop("message", "reaction", "subscription")
    
    # При записи партиционируем по номеру недели для удобства
    zone_report.write.mode("overwrite").partitionBy("week").parquet(f"{namenode_url}{base_output_path}")


def main():
    spark = SparkSession.builder.master("yarn").appName(f"calculate_zone_report").getOrCreate()
    calculate_zone_report(namenode_url, base_input_path, base_output_path, spark)

if __name__ == "__main__":
    main()