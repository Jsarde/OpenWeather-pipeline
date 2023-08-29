from dotenv import load_dotenv
import os
from datetime import datetime,timedelta
import json
import pandas as pd
import boto3

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator



def kelvin_to_fahrenheit(kelvin):
        fahrenheit = (kelvin - 273.15) * (9/5) + 32
        return fahrenheit

def transform_load_weather(ti):
    data = ti.xcom_pull(task_ids="extract_data")
    info = {
            "city" : data["name"],
            "country" : data["sys"]["country"],
            "lat" : data["coord"]["lat"],
            "lng" : data["coord"]["lon"],
            "weather" : data["weather"][0]["description"],
            "temperature (F°)" : kelvin_to_fahrenheit(data["main"]["temp"]),
            "humidity" : data["main"]["humidity"],
            "wind_speed" : data["wind"]["speed"],
            "cloudiness (%)" : data["clouds"]["all"],
            "time" : datetime.utcfromtimestamp(data["dt"] + data["timezone"])
        }
    df = pd.DataFrame([info])
    
    output_name = "milan_weather_" + datetime.now().strftime("%d%m%Y%H%M")
    bucket = "openweather"
    
    csv_buffer = df.to_csv(index=False)
    response = s3.put_object(Bucket=bucket, Key=output_name, Body=csv_buffer)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f"File '{output_name}' caricato correttamente nel bucket S3 '{bucket}'")
    else:
        print("Si è verificato un errore durante il caricamento del file CSV")


load_dotenv()
api_key = os.getenv("OPENWEATHER_KEY")
aws_access_key=os.getenv("AWS_ACCESS_KEY")
aws_secret_key=os.getenv("AWS_SECRET_KEY")

s3 = boto3.client(
    "s3",
    region_name="eu-south-1",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

args = {
    "owner":"SjA",
    "retries":3,
    "retry_delay":timedelta(minutes=15),
    "depends_on_past":False
}

dag = DAG(
    dag_id="openweather_etl_pipeline",
    description="ETL pipeline with Airflow and AWS using OpenWeather API",
    default_args=args,
    start_date=datetime(2023,8,29),
    catchup=False,
    schedule_interval="@daily"
)

with dag:
    openweather_api_sensor = HttpSensor(
        task_id = "is_api_available",
        http_conn_id="openweather_api",
        endpoint=f"/data/2.5/weather?q=Milan&appid={api_key}&units=metrics"
    )

    extract_weather_data = SimpleHttpOperator(
        task_id="extract_data",
        http_conn_id="openweather_api",
        endpoint=f"/data/2.5/weather?q=Milan&appid={api_key}&units=metrics",
        method="GET",
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_data = PythonOperator(
        task_id="transform_load_data",
        python_callable=transform_load_weather
    )

    openweather_api_sensor >> extract_weather_data >> transform_load_data