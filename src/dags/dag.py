import airflow
import os

from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
}

dag_spark = DAG(
    dag_id="project",
    default_args=default_args,
    schedule_interval=None,
)

events_dm = SparkSubmitOperator(
    task_id='events_dm',
    dag=dag_spark,
    application='/lessons/events_dm.py',
    conn_id='yarn_spark',
    application_args=["2022-01-02",
                      "/user/master/data/geo/events",
                      "/user/nicpopov/data/geo/geo_time_zone.csv",
                      "/user/nicpopov/analytics/user_dm"]
)

geo_dm = SparkSubmitOperator(
    task_id='geo_dm',
    dag=dag_spark,
    application='/lessons/geo_dm.py',
    conn_id='yarn_spark',
    application_args=["2022-01-02",
                      "/user/master/data/geo/events",
                      "/user/nicpopov/data/geo/geo_time_zone.csv",
                      "/user/nicpopov/analytics/geo_dm"]
)

friends_dm = SparkSubmitOperator(
    task_id='friends_dm',
    dag=dag_spark,
    application='/lessons/friends_dm.py',
    conn_id='yarn_spark',
    application_args=["2022-01-02",
                      "/user/master/data/geo/events",
                      "/user/nicpopov/data/geo/geo_time_zone.csv",
                      "/user/nicpopov/analytics/friends_dm"]

)

events_dm >> geo_dm >> friends_dm