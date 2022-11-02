from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

START_DATE = "2022-03-31"
DEPTH = '10'
NAMENODE_URL = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'
SOURCE_PATH = '/user/master/data/geo/events'
DM_BASE_PATH = '/user/enpassan/analytics/'
GEO_PATH = '/user/enpassan/dev/geo_data_correct'

INTERIM_DATA_PATH = f"/user/enpassan/tmp/mes_geo_{START_DATE.split('-')[1]}_{START_DATE.split('-')[2]}_{DEPTH}"
USER_CITY_DM_PATH = f"{DM_BASE_PATH}user_city_report_{START_DATE.split('-')[1]}_{START_DATE.split('-')[2]}_{DEPTH}"
ZONE_DM_PATH = f"{DM_BASE_PATH}zone_report_{START_DATE.split('-')[1]}_{START_DATE.split('-')[2]}_{DEPTH}"
RECS_DM_PATH = f"{DM_BASE_PATH}recs_report_{START_DATE.split('-')[1]}_{START_DATE.split('-')[2]}_{DEPTH}"

SPARK_KWARGS = {
    'conn_id': 'yarn_spark',
    'conf': {"spark.driver.maxResultSize": "20g"},
    'executor_cores': 2,
    'executor_memory': '2g'
}


with DAG(
    dag_id = "report_dag",
    start_date=datetime(2022, 10, 11),
    schedule_interval="@weekly"
) as dag:

    calculate_mes_geo_03_31_10 = SparkSubmitOperator(
        task_id = f"calculate_mes_geo_{START_DATE.split('-')[1]}_{START_DATE.split('-')[2]}_{DEPTH}",
        application ='/lessons/scripts/save_mes_geo.py' ,
        application_args = [START_DATE, DEPTH, NAMENODE_URL, SOURCE_PATH, INTERIM_DATA_PATH, GEO_PATH],
        **SPARK_KWARGS
    )

    calculate_user_city = SparkSubmitOperator(
        task_id = "calculate_user_by_city",
        application ='/lessons/scripts/calculate_user_city.py' ,
        application_args = [NAMENODE_URL, INTERIM_DATA_PATH, USER_CITY_DM_PATH],
        **SPARK_KWARGS
    )

    calculate_zone = SparkSubmitOperator(
        task_id = "calculate_zone",
        application ='/lessons/scripts/calculate_zone_report.py' ,
        application_args = [NAMENODE_URL, INTERIM_DATA_PATH, ZONE_DM_PATH],
        **SPARK_KWARGS
    )

    calculate_user_recs = SparkSubmitOperator(
        task_id = "calculate_recommendations",
        application ='/lessons/scripts/calculate_user_recommandations.py' ,
        application_args = [NAMENODE_URL, INTERIM_DATA_PATH, RECS_DM_PATH],
        **SPARK_KWARGS
    )

    
    calculate_mes_geo_03_31_10 >> calculate_user_city >> calculate_zone >> calculate_user_recs