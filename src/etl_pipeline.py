from datetime import datetime,timedelta
from airflow import DAG


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
    pass