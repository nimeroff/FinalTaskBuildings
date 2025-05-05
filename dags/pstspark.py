from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import subprocess
import requests
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'pstspark',
    default_args=default_args,
    description='A simple DAG to run my script on PySpark with Postgres',
    schedule_interval=None,
)

def RunSparkPst():
    spark = (SparkSession.builder.appName("PySpark PostgreSQL Connection").config("spark.jars.packages","org.postgresql:postgresql:42.2.23").getOrCreate())
    #spark = (SparkSession.builder.appName("PySpark PostgreSQL Connection").config("spark.executor.extraClassPath","/app/postgresql-42.2.23.jar").getOrCreate())
    url = "jdbc:postgresql://postgres_user:5432/test"
    properties = {
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    df = spark.read.jdbc(url=url, table="employees", properties=properties)
    # df = spark.read.format("jdbc").options(url=url, table="employees", properties=properties)
    df.show()
    spark.stop()

start = EmptyOperator(task_id='start')

task_runspark = PythonOperator(
    task_id='run_spark_pst',
    python_callable=RunSparkPst,
    dag=dag,
)

end = EmptyOperator(task_id='end')

start >> task_runspark >> end