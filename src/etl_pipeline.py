from dotenv import load_dotenv
import os
import json
from datetime import datetime,timedelta

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


load_dotenv()
api_key = os.getenv("API_KEY")


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

    openweather_api_sensor >> extract_weather_data