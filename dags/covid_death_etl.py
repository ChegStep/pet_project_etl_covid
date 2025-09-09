from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import json
import pandas as pd
from psycopg2.extras import execute_values
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def _process_and_store_covid_data(ti, **kwargs):
    data_json  = ti.xcom_pull(task_ids='extract_covid_data')

    if not data_json or 'deaths' not in data_json:
        raise ValueError("Invalid data structure received from API")

    df = pd.DataFrame.from_dict(data_json['deaths'], orient='index', columns=['deaths'])
    df.index.name = 'date'
    df.reset_index(inplace=True)
    df['date'] = pd.to_datetime(df['date']).dt.date

    df = df.sort_values('date')
    latest_date_in_pull = df['date'].max().isoformat()

    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(date) FROM raw.covid_historical_deaths;")
    result = cursor.fetchone()
    latest_date_in_db = result[0] if result else None

    if latest_date_in_db is None:
        execute_values(cursor, """
                INSERT INTO raw.covid_historical_deaths (date, deaths)
                VALUES %s;
            """, [tuple(x) for x in df[['date', 'deaths']].to_numpy()])
        print("Loaded all historical data")
    else:
        new_data_df = df[df['date'] > latest_date_in_db]
        if not new_data_df.empty:
            execute_values(cursor, """
                    INSERT INTO raw.covid_historical_deaths (date, deaths)
                    VALUES %s;
                """, [tuple(x) for x in new_data_df[['date', 'deaths']].to_numpy()])
            print(f"Loaded {len(new_data_df)} new records")
        else:
            print("No new data to load.")

    conn.commit()
    cursor.close()
    conn.close()

    return f"Successfully processed {len(df)} records"

with DAG('covid_etl_pipeline_deaths',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    create_raw_schema = SQLExecuteQueryOperator(
        task_id='create_raw_schema',
        conn_id='postgres_conn',
        sql="CREATE SCHEMA IF NOT EXISTS raw;"
    )

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='http_disease_sh',
        endpoint='/v3/covid-19/historical/all?lastdays=all',
        response_check=lambda response: response.status_code == 200,
    )

    extract_covid_data = HttpOperator(
        task_id='extract_covid_data',
        http_conn_id='http_disease_sh',
        endpoint='/v3/covid-19/historical/all?lastdays=all',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    create_raw_table = SQLExecuteQueryOperator(
        task_id='create_raw_table',
        conn_id='postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.covid_historical_deaths (
                date DATE PRIMARY KEY,
                deaths INTEGER NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )
    process_and_load_data = PythonOperator(
        task_id='process_and_load_data',
        python_callable=_process_and_store_covid_data
    )

    create_raw_schema >> is_api_available >> extract_covid_data >> create_raw_table >> process_and_load_data