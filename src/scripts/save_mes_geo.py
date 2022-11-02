import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

date = sys.argv[1]
depth = sys.argv[2]
namenode_url = sys.argv[3]
base_input_path = sys.argv[4]
base_output_path = sys.argv[5]
base_geo_path = sys.argv[6]


def input_paths(date: str, depth: int, namenode_url:str, input_path: str):
    depth = int(depth)
    return [
        f'{namenode_url}{input_path}/' \
            f"date={datetime.strftime((datetime.strptime(date, '%Y-%m-%d') - timedelta(days=day_amount)), '%Y-%m-%d')}"
        for day_amount in range(depth)
    ]

def events_with_city(namenode_url: str, base_output_path: str, spark: SparkSession):

    #Получение путей
    paths = input_paths(date, depth, namenode_url, base_input_path)

    #Чтение
    geo = spark.read.parquet(f'{namenode_url}{base_geo_path}')
    messages = spark.read.parquet(*paths).withColumn("event_id", F.monotonically_increasing_id())

    #Радиус Земли
    r = 6371 

    #Вычисление геолокации события
    mes_geo = messages\
        .crossJoin(geo)\
        .withColumn("diff", 2 * r * F.abs(F.asin(F.sqrt(F.sin((messages.lat - geo.lat_c) / 2) ** 2 + F.cos(messages.lat) * F.cos(geo.lat_c) * (F.sin((messages.lon - geo.lon_c) / 2) ** 2)))))

    window = Window().partitionBy("event_id").orderBy("diff")
    mes_geo = mes_geo\
        .withColumn("rn", F.row_number().over(window))\
        .where("rn == 1")\
        .withColumn("city_id", F.col("id"))\
        .drop("id")   

    #Сохраняем данные
    interim_path = f"{namenode_url}{base_output_path}"
    mes_geo.write.mode("overwrite").parquet(interim_path)


def main():
    spark = SparkSession.builder.master("yarn").appName(f"save_interim_data-{date}-d{depth}").getOrCreate()
    events_with_city(namenode_url, base_output_path, spark)

if __name__ == "__main__":
    main()