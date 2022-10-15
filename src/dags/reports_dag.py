from datetime import datetime, timedelta
import sys
from timezonefinder import TimezoneFinder
from pyspark.sql import types as T
from cachetools import cached

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator


import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

with DAG(
    dag_id = "report_dag",
    start_date=datetime(2022, 10, 11),
    schedule_interval="@weekly"
) as dag:
    start = EmptyOperator(task_id="start")

    calculate_mes_geo_03_31_10 = SparkSubmitOperator(
        task_id = "calculate_mes_geo_03_31_10",
        application ='/lessons/scripts/save_mes_geo.py' ,
        conn_id= 'yarn_spark',
        application_args = ["2022-03-31", '10', 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020', '/user/master/data/geo/events', '/user/enpassan/tmp/mes_geo_03_31_10'],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 2,
        executor_memory = '2g'
    )

    calculate_user_city = SparkSubmitOperator(
        task_id = "calculate_user_by_city",
        application ='/lessons/scripts/calculate_user_city.py' ,
        conn_id= 'yarn_spark',
        application_args = ['hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020', '/user/enpassan/tmp/mes_geo_03_31_10', '/user/enpassan/analytics/user_city_report_03_31_10'],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 2,
        executor_memory = '2g'
    )

    calculate_zone = SparkSubmitOperator(
        task_id = "calculate_zone",
        application ='/lessons/scripts/calculate_zone_report.py' ,
        conn_id= 'yarn_spark',
        application_args = ['hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020', '/user/enpassan/tmp/mes_geo_03_31_10', '/user/enpassan/analytics/zone_report_03_31_10'],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 2,
        executor_memory = '2g'
    )

    calculate_user_recs = SparkSubmitOperator(
        task_id = "calculate_recommendations",
        application ='/lessons/scripts/calculate_user_recommandations.py' ,
        conn_id= 'yarn_spark',
        application_args = ['hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020', '/user/enpassan/tmp/mes_geo_03_31_10', '/user/enpassan/analytics/recs_report_03_31_10'],
        conf={"spark.driver.maxResultSize": "20g"},
        executor_cores = 2,
        executor_memory = '2g'
    )

    


    start >> calculate_mes_geo_03_31_10 >> calculate_user_city >> calculate_zone >> calculate_user_recs