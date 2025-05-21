from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, regexp_replace, percentile_approx, expr,year, round, desc, asc, udf
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import subprocess
import requests
import matplotlib.pyplot as plt

client = Client(host='clickhouse', port=9000,user='default', password='')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='ETL task',
    schedule_interval=None,
)

def query_clickhouse(**kwargs):
    response = requests.get('http://clickhouse_user:8123/?query=SELECT%20version()')
    if response.status_code == 200:
        print(f"ClickHouse version: {response.text}")
    else:
        print(f"Failed to connect to ClickHouse, status code: {response.status_code}")


def query_postgres(**kwargs):
    command = [
        'psql',
        '-h', 'postgres_user',
        '-U', 'user',
        '-d', 'test',
        '-c', 'SELECT version();'
    ]
    env = {"PGPASSWORD": "password"}
    result = subprocess.run(command, env=env, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"PostgreSQL version: {result.stdout}")
    else:
        print(f"Failed to connect to PostgreSQL, error: {result.stderr}")

def Diag(xlist,ylist,sg):
    if sg==1:
        # Построение графика
        plt.bar(xlist, ylist)
        plt.title('Топ 10 регионов с наибольшим количеством объектов')
        plt.xlabel('Регионы')
        plt.ylabel('Количество объектов')
        plt.xticks(rotation=60, horizontalalignment='right', fontsize=10)
        for ind, cat in enumerate(xlist):
            plt.text(cat, ylist[ind] + 500, str(ylist[ind]), fontsize=8, ha='center')
        plt.show()


#Загрузите файл данных в DataFrame PySpark. Обязательно выведите количество строк.
def ETL_CSV():
    spark = SparkSession.builder.appName("DataFrame Example").getOrCreate()
    df = spark.read.format("csv").option("header", "true").option("encoding", "UTF-16").option("multiLine","true").load('/opt/airflow/dags/russian_houses.csv')
    print(f"Количество записей: {df.count() + 1}")

    #Преобразование типов
    df = df.withColumn('house_id', col('house_id').cast('integer'))
    df = df.withColumn('latitude', col('latitude').cast('float'))
    df = df.withColumn('longitude', col('longitude').cast('float'))
    df = df.withColumn('maintenance_year', year(col('maintenance_year')))
    df = df.withColumn('square', regexp_replace(col('square'), " ", ""))
    df = df.withColumn('square', col('square').cast('float'))
    df = df.withColumn('population', col('population').cast('integer'))
    df = df.withColumn('communal_service_id', col('communal_service_id').cast('integer'))
    #Отредактируем столбцы, в случае наличия в них не правильных значений - исключим латинские символы и спец знаки
    df = df.withColumns({'description':regexp_replace('description', '[a-zA-Z\|/_=+-—]', '')})
    df.drop_duplicates() #удаляем дубликаты
    df = df.na.drop(how='any') #удаляем пустые строки
    #df = df.na.drop(subset=['maintenance_year','square','population']) #удаляем строки, где отсутствуют значения в соотв полях

    #Выявление выбросов
    #Вычисление первого и третьего квартилей
    Q1 = df.select(percentile_approx("maintenance_year", [0.25], 1000000)).collect()[0][0][0] # Первый квартиль (25-й процентиль)
    Q3 = df.select(percentile_approx("maintenance_year", [0.75], 1000000)).collect()[0][0][0]  # Третий квартиль (75-й процентиль)
    # Вычисление IQR
    IQR = Q3 - Q1
    # Определение границ выбросов
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    #print(f"Нижняя граница выбросов: {lower_bound}")
    #print(f"Верхняя граница выбросов: {upper_bound}")
    #print(f"Данные с выбросами по году:")
    #df.filter((col('maintenance_year')<=lower_bound) | (col('maintenance_year')>=upper_bound)).show(10)
    df_filter = df.filter((col('maintenance_year')>lower_bound) & (col('maintenance_year')<upper_bound)) #датафрейм без выбросов
    df_filter.createOrReplaceTempView("tcity") #сохраняем в таблицу

    #вычисляем средний год построек
    df_avg_year = df_filter.agg({'maintenance_year':'avg'}).withColumnRenamed('avg(maintenance_year)','avg_year').withColumn('avg_year',col('avg_year').cast('integer'))
    print(f"Средний год постройки {df_avg_year.select(round('avg_year', 0)).collect()[0][0]}")
    #df_avg_year = spark.sql("select cast(avg(maintenance_year) as Int) as avg_year from tcity")
    #print(df_avg_year.select('avg_year').collect()[0][0])
    #вычисляем медианный год построек
    m_year = df_filter.select(percentile_approx("maintenance_year", [0.50], 1000000)).collect()[0][0][0]  # Третий квартиль (50-й процентиль)
    print(f"Медианный год постройки {m_year}")

    #Топ  10 регионов и городов с наибольшим количеством объектов
    print("Топ 10 регионов-областей с наибольшим количеством объектов")
    df_topten = df_filter.groupBy('region').agg({'house_id': 'count'}).withColumnRenamed('count(house_id)', 'cnt_house').sort(desc('cnt_house'))
    df_topten.show(10)

    #Подготовим датафрейм для диаграммы
    df_topten = df_topten.withColumn('region', regexp_replace(col('region'), 'Республика', 'Респ.'))
    df_topten = df_topten.withColumn('region', regexp_replace(col('region'), 'область', 'обл.'))
    # Данные для диаграммы
    categories = [r[0] for r in df_topten.select(df_topten.region).head(10)]
    values = [r[0] for r in df_topten.select(df_topten.cnt_house).head(10)]
    Diag(categories, values, 1)

    #Здания с максимальной и минимальной площадью в рамках каждой области
    print("Здания с максимальной и минимальной площадью в рамках каждой области")
    #время выполнения 124.309, 93.252
    # df_minax_square = spark.sql("select region, house_id, address,  case when square=(select min(square) from tcity where region=t.region) then 'MIN' \
    # when square=(select max(square) from tcity where region=t.region) then 'MAX' end as sign, \
    # square from tcity t where square=(select min(square) from tcity where region=t.region) or \
    # square=(select max(square) from tcity where region=t.region) order by region asc, square asc")
    # время выполнения 99.41, 92.706
    df_minax_square = spark.sql("select region, house_id, address,  sign, square  from \
      (select region, house_id, address,  'MIN' as sign, square from tcity t where square=(select min(square) from tcity where region=t.region) \
      union select region, house_id, address,  'MAX' as sign, square from tcity t where square=(select max(square) from tcity where region=t.region)) \
      order by region asc, square asc")
    df_minax_square.show()

    #Определим пользовательские функции
    def del_num(value):
      return value // 10 #откидываем последнюю цифру от года, например, 1935 -> 193, для последующей группировки по десятилетиям

    def add_zero(value):
      return value * 10  #добавляем ноль в конец числа, например, 193 -> 1930, для получения десятилетия

    # Регистрация функции в UDF
    delnum = udf(del_num, IntegerType())
    addzero = udf(add_zero, IntegerType())

    #Группируем данные по модицифированному году и подсчитываем количество зданий
    print("Количество зданий по десятилетиям")
    df_cnt_build = df_filter.withColumn('mod_year',delnum(col('maintenance_year'))).groupBy('mod_year').agg({'house_id':'count'}).withColumnRenamed('count(house_id)','cnt_build').sort(desc('cnt_build'))
    df_cnt_build = df_cnt_build.withColumn('mod_year',addzero(col('mod_year')))
    df_cnt_build.show(10)

    #Работа с Clickhouse
    #Создадим таблицу, если нет
    print("Топ 25 домов, у которых площадь больше 60")
    client.execute("""CREATE OR REPLACE TABLE default.buildings 
              (`house_id` UInt32, 
              `latitude` Float32, 
              `longiude` Float32, 
              `maintenance_year` UInt32, 
              `square` Float32,  
              `population` UInt32, 
              `region` String, 
              `locality_name` String, 
              `address` String, 
              `full_address` String, 
              `communal_service_id` UInt32, 
              `description` String)  
          ENGINE = MergeTree
          ORDER BY house_id
          SETTINGS index_granularity = 8192""")
    #Подготовим данные для вставки
    data = [(r['house_id'], r['latitude'], r['longitude'], r['maintenance_year'], r['square'],
                 r['population'], r['region'], r['locality_name'], r['address'], r['full_address'],
                 r['communal_service_id'], r['description']) for r in df_filter.collect()]
    client.execute('insert into default.buildings values ', data)  #выполним скрипт вставки

    #Топ 25 домов, площадь которых больше 60
    lst = client.execute('select house_id, address, square from default.buildings where square>60 order by square desc limit 25')
    df_t = spark.createDataFrame(lst, ["house_id","address","square"]) #сохраним в датафрейм
    df_t.show(25) #выводим
    spark.stop()


task_load_csvfile = BashOperator(
    task_id='task_load_csvfile',
    #bash_command='curl -X GET https://disk.yandex.ru/d/HZ08UBRlxoCJTg -o /opt/airflow/dags/russian_houses.csv',
    bash_command='echo "Hello"',
    dag=dag,
)

task_csvetl = PythonOperator(
    task_id='task_csvetl',
    python_callable=ETL_CSV,
    dag=dag,
)

task_query_clickhouse = PythonOperator(
    task_id='query_clickhouse',
    python_callable=query_clickhouse,
    dag=dag,
)

task_query_postgres = PythonOperator(
    task_id='query_postgres',
    python_callable=query_postgres,
    dag=dag,
)

task_query_clickhouse >> task_query_postgres >> task_load_csvfile >> task_csvetl