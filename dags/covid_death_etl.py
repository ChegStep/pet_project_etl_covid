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
import logging

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def _process_and_store_covid_data(ti, **kwargs):
    try:
        logger.info("Начало обработки данных по COVID-19")

        # Получаем данные из предыдущей задачи
        data_json = ti.xcom_pull(task_ids='extract_covid_data')
        logger.info(f"Получены данные из XCom. Ключи: {list(data_json.keys())}")

        if not data_json or 'deaths' not in data_json:
            error_msg = f"Неверная структура данных. Ожидался ключ 'deaths'. Получены ключи {list(data_json.keys())}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Логируем sample данных для отладки
        deaths_sample = dict(list(data_json['deaths'].items())[:3])
        logger.info(f"Sample данных deaths: {deaths_sample}")

        # Преобразование в DataFrame
        df = pd.DataFrame.from_dict(data_json['deaths'], orient='index', columns=['deaths'])
        df.index.name = 'date'
        df.reset_index(inplace=True)
        df['date'] = pd.to_datetime(df['date']).dt.date

        df = df.sort_values('date')
        logger.info(f"Создан DataFrame с {len(df)} записями")

        # Подключение к БД
        hook = PostgresHook(postgres_conn_id='postgres_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()
        logger.info("Подключение к PostgreSQL установлено")

        # Проверяем на существование таблицы в БД
        cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'raw' 
                        AND table_name = 'covid_historical_deaths'
                    );
                """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            cursor.execute("SELECT MAX(date) FROM raw.covid_historical_deaths;")
            result = cursor.fetchone()
            latest_date_in_db = result[0] if result else None
            logger.info(f"Таблица существует в БД. Последняя дата: {latest_date_in_db}")
        else:
            latest_date_in_db = None
            logger.warning("Таблица не существует в БД")

        # Логика инкрементальной загрузки
        if latest_date_in_db is None:
            # Полная загрузка
            execute_values(cursor, """
                    INSERT INTO raw.covid_historical_deaths (date, deaths)
                    VALUES %s;
                """, [tuple(x) for x in df[['date', 'deaths']].to_numpy()])
            logger.info(f"Выполнена полная загрузка: {len(df)} записей")
        else:
            # Инкрементальная загрузка
            new_data_df = df[df['date'] > latest_date_in_db]
            if not new_data_df.empty:
                execute_values(cursor, """
                        INSERT INTO raw.covid_historical_deaths (date, deaths)
                        VALUES %s;
                    """, [tuple(x) for x in new_data_df[['date', 'deaths']].to_numpy()])
                logger.info(f"Добавлено {len(df)} новых записей")
            else:
                logger.info("Новых записей не обнаружено")

        # Фиксируем изменения
        conn.commit()

        # Проверка результата
        cursor.execute("SELECT COUNT(*) FROM raw.covid_historical_deaths;")
        final_count = cursor.fetchone()[0]
        logger.info(f"Итоговое кол-во записей в таблице: {final_count}")

        cursor.close()
        conn.close()
        logger.info("Обработка данных завершена")

        return f"Успешно обработано {len(df)} записей"
    except Exception as e:
        logger.error(f"Ошибка при обработке данных: {e}", exc_info=True)
        raise

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