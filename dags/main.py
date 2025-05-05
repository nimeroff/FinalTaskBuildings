from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='Final task',
    schedule_interval=None,
)

#Загрузите файл данных в DataFrame PySpark. Обязательно выведите количество строк.
def DownloadCSV():
    spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
    # Явное определение схемы
    schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("Coordinates", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Square", FloatType(),True),
        StructField("Floors", IntegerType(), True),
        StructField("Area", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Description", StringType(), True)
    ])

    #Чтение данных из CSV
    try:
        df = spark.read.csv("/opt/airflow/dags/russian_houses.csv", header=True, schema=schema)
        count_df = df.agg({"*":"count"}).withColumnRenamed("count(1)","count")
        print(count_df.collect())
        print(count_df.show(1))
        print(df.count())
        #df_auto.printSchema()
    except Exception as err:
        print(err)
        print("Файл не найден")

start = EmptyOperator(task_id='start')

task_first = PythonOperator(
    task_id='task_first',
    python_callable=DownloadCSV,
    dag=dag,
)

end = EmptyOperator(task_id='end')

start >> task_first >> end