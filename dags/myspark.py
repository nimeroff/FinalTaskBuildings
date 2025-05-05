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
    'myspark',
    default_args=default_args,
    description='A simple DAG to run my script on PySpark',
    schedule_interval=None,
)

def RunSpark():
    spark = SparkSession.builder.appName("Simple Spark Job").getOrCreate()
    data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    df.filter(df.Age > 30).show()
    spark.stop()

start = EmptyOperator(task_id='start')

task_runspark = PythonOperator(
    task_id='run_spark',
    python_callable=RunSpark,
    dag=dag,
)

end = EmptyOperator(task_id='end')

start >> task_runspark >> end