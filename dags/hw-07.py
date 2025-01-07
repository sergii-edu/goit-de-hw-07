from airflow import DAG
from datetime import datetime
import time
import random
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

GLOBAL_NAME = "public"
connection_name = "postgres_default"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 8, 4, 0, 0),
}


def create_table():
    hook = PostgresHook(postgres_conn_id=connection_name)
    conn = hook.get_conn()
    conn.set_session(autocommit=True)
    with conn.cursor() as cursor:
        hook = PostgresHook(postgres_conn_id=connection_name, schema=GLOBAL_NAME)
        conn = hook.get_conn()
        conn.set_session(autocommit=True)

        with conn.cursor() as cursor:
            cursor.execute(
                f"""
            CREATE TABLE IF NOT EXISTS {GLOBAL_NAME}.medals (
                id SERIAL PRIMARY KEY,
                medal_type VARCHAR(50),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            )
    conn.close()


def pick_medal(ti):
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Generated medal: {medal}")
    return medal


def pick_medal_task(ti):
    medal = ti.xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"


def generate_delay():
    delay = random.randint(1, 30)
    print(f"Sleeping for {delay} seconds...")
    time.sleep(delay)


with DAG(
    "hw-07_new",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["sergiik"],
) as dag:
    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    pick_medal = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    pick_medal_task = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=pick_medal_task,
    )

    calc_Bronze = SQLExecuteQueryOperator(
        task_id="calc_Bronze",
        conn_id=connection_name,
        sql=f"""
        INSERT INTO {GLOBAL_NAME}.medals (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM {GLOBAL_NAME}.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = SQLExecuteQueryOperator(
        task_id="calc_Silver",
        conn_id=connection_name,
        sql=f"""
        INSERT INTO {GLOBAL_NAME}.medals (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM {GLOBAL_NAME}.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    calc_Gold = SQLExecuteQueryOperator(
        task_id="calc_Gold",
        conn_id=connection_name,
        sql=f"""
        INSERT INTO {GLOBAL_NAME}.medals (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM {GLOBAL_NAME}.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule="one_success",
    )

    check_for_data = SqlSensor(
        task_id="check_for_correctness",
        conn_id=connection_name,
        sql=f"""
        SELECT 1
        FROM {GLOBAL_NAME}.medals
        WHERE EXTRACT(EPOCH FROM (NOW() - created_at)) <= 30
        LIMIT 1;
        """,
        mode="poke",
        poke_interval=5,
        timeout=60,
    )

    (
        create_table_task
        >> pick_medal
        >> pick_medal_task
        >> [calc_Bronze, calc_Silver, calc_Gold]
        >> generate_delay
        >> check_for_data
    )
