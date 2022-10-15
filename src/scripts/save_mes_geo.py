import sys
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.window import Window

date = sys.argv[1]
depth = int(sys.argv[2])
namenode_url = sys.argv[3]
base_input_path = sys.argv[4]
base_output_path = sys.argv[5]

conf = SparkConf().setAppName(f"SaveMessagesGeoData-{date}-d{depth}")
sc = SparkContext(conf=conf)
sql = SQLContext(sc)


def input_paths(date: str, depth: int, namenode_url:str, input_path: str):
    return [
        f'{namenode_url}{input_path}/' \
            f"date={datetime.strftime((datetime.strptime(date, '%Y-%m-%d') - timedelta(days=day_amount)), '%Y-%m-%d')}"
        for day_amount in range(depth)
    ]

def save_mes_geo(namenode_url, base_output_path):

    #get paths
    paths = input_paths(date, depth, namenode_url, base_input_path)

    #read data
    geo = sql.read.parquet(f'{namenode_url}{base_input_path}')
    messages = sql.read.parquet(*paths).withColumn("event_id", F.monotonically_increasing_id())

    window = Window().partitionBy(messages.event_id).orderBy(F.asc(F.abs(F.asin(F.sqrt(F.sin((messages.lat - geo.lat_c) / 2) ** 2 \
    + F.cos(messages.lat) * F.cos(geo.lat_c) * \
    (F.sin((messages.lon - geo.lon_c) / 2) ** 2))))))

    #compute distance and save
    mes_geo = messages.join(geo, how='cross').withColumn("rn", F.row_number().over(window)).where("rn == 1").withColumn("city_id", F.col("id")).drop("id")                                                                                                                    
    mes_geo.write.mode("overwrite").parquet(f"{namenode_url}{base_output_path}")

def main():
    save_mes_geo(namenode_url, base_output_path)

if __name__ == "__main__":
    main()