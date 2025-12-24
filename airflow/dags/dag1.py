from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLE_NAMES = [
    'veiculos',
    'estados',
    'cidades',
    'concessionarias',
    'vendedores',
    'clientes',
    'vendas'
]

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='postgres_to_snowflake',
    description='Incremental load from Postgres to Snowflake',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['postgres', 'snowflake', 'etl']
)
def postgres_to_snowflake_etl():

    @task
    def get_max_primary_key(table_name: str) -> int:
        with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COALESCE(MAX(ID_{table_name}), 0) FROM {table_name}")
                return cursor.fetchone()[0]

    @task
    def load_incremental_data(table_name: str, max_id: int):
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        sf_hook = SnowflakeHook(snowflake_conn_id='snowflake')

        with pg_hook.get_conn() as pg_conn, pg_conn.cursor() as pg_cursor:
            primary_key = f'ID_{table_name}'

            pg_cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            columns = [row[0] for row in pg_cursor.fetchall()]
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))

            pg_cursor.execute(
                f"SELECT {columns_str} FROM {table_name} WHERE {primary_key} > %s",
                (max_id,)
            )
            rows = pg_cursor.fetchall()

        if not rows:
            return

        with sf_hook.get_conn() as sf_conn, sf_conn.cursor() as sf_cursor:
            insert_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
            """
            sf_cursor.executemany(insert_sql, rows)

    @task_group(group_id='load_tables')
    def load_all_tables():
        for table in TABLE_NAMES:
            max_id = get_max_primary_key(table)
            load_incremental_data(table, max_id)

    load_all_tables()

postgres_to_snowflake_etl()
