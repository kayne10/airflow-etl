from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os
from airflow.hooks.postgres_hook import PostgresHook
import json
import numpy as np

def load_data():

    pg_hook = PostgresHook(postgres_conn_id='weather_id')

    file_name = str(datetime.now().date()) + '.json'
    tot_name =  os.path.join(os.path.dirname(__file__), 'data', file_name)

    with open(tot_name, 'r') as input_file:
        doc = json.load(input_file)

    city = str(doc['name'])
    country = str(doc['sys']['country'])
    lat = float(doc['coord']['lat'])
    lon = float(doc['coord']['lon'])
    humid = float(doc['main']['humidity'])
    press = float(doc['main']['pressure'])
    min_temp = float(doc['main']['temp_min']) - 273.15
    max_temp = float(doc['main']['temp_max']) - 273.15
    temp = float(doc['main']['temp']) - 273.15
    weather = str(doc['weather'][0]['description'])
    todays_date = datetime.now().date()

    valid_data = True
    for valid in np.isnan([lat,lon,humid,press,min_temp,max_temp,temp]):
        if valid is False:
            valid_data = False
            break;

    row = (city, country, lat, lon, todays_date, humid, press, min_temp,
            max_temp, temp, weather)

    insert_cmd = """INSERT INTO weather_table
                    (city, country, latitude, longitude, todays_date,
                    humidity, pressure, min_temp, max_temp, temp, weather)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    if valid_data is True:
        pg_hook.run(insert_cmd, parameters=row)
    else:
        print("Invalid Data: NaN values from API")


#define the default dag arguments
default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'email':['troy.kayne@colorado.edu'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

with DAG(
    dag_id='weatherDag',
    default_args=default_args,
    start_date=datetime(2019,2,28),
    schedule_interval=timedelta(minutes=1440)
    ) as dag:

    task1 = BashOperator(
        task_id='get_weather',
        bash_command='python ~/Github/workspace/ETL/get_weather.py',
        dag=dag
    )

    task2 = PythonOperator(
        task_id='transform_load',
        provide_context=True,
        python_callable=load_data,
        dag=dag
    )
#Set task1 upstream of task2
task1 >> task2
