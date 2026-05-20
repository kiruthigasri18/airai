from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd


# -----------------------------------
# EXTRACT DATA
# -----------------------------------

def extract_data():

    data = {

        "id": [1, 2, 3],

        "name": ["Kiruthiga", "Arun", "John"],

        "department": [
            "Data Engineering",
            "Analytics",
            "HR"
        ],

        "salary": [5000, 6000, 7000]
    }

    df = pd.DataFrame(data)

    df.to_csv("/tmp/employee_raw.csv", index=False)

    print(df)


# -----------------------------------
# TRANSFORM DATA
# -----------------------------------

def transform_data():

    df = pd.read_csv("/tmp/employee_raw.csv")


    
    

    df.to_csv("/tmp/employee_transformed.csv", index=False)

    print(df)


# -----------------------------------
# LOAD TO POSTGRES
# -----------------------------------

def load_data():

    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_conn"
    )

    conn = postgres_hook.get_conn()

    cursor = conn.cursor()

    df = pd.read_csv("/tmp/employee_transformed.csv")

    for _, row in df.iterrows():

        query = f"""
        INSERT INTO employees
        (id, name, department, salary)

        VALUES (
            {row['id']},
            '{row['name']}',
            '{row['department']}',
            {row['salary']}
        );
        """

        cursor.execute(query)

    conn.commit()

    cursor.close()
    conn.close()

    print("Data loaded successfully")


# -----------------------------------
# VALIDATION STEP
# -----------------------------------

def validate_data():

    df = pd.read_csv("/tmp/employee_transformed.csv")

    invalid_count = (
        df["salary"] < 1000
    ).sum()

    print("INVALID SALARY COUNT:", invalid_count)

    # IF ISSUE FOUND
    if invalid_count > 0:

        postgres_hook = PostgresHook(
            postgres_conn_id="postgres_conn"
        )

        conn = postgres_hook.get_conn()

        cursor = conn.cursor()

        query = """
        INSERT INTO workflow_validation (

            dag_name,
            suspicion_type,
            suspicion_details,
            status

        )

        VALUES (

            'employee_etl_dag',
            'KPI_MISMATCH',
            'salary validation failed',
            'NOT_RESOLVED'
        );
        """

        cursor.execute(query)

        conn.commit()

        cursor.close()
        conn.close()

        print("Suspicion record inserted")


# -----------------------------------
# DAG
# -----------------------------------

with DAG(

    dag_id="employee_etl_dag",

    start_date=datetime(2025, 1, 1),

    schedule="@daily",

    catchup=False,

    tags=["employee", "etl"]

) as dag:

    extract_task = PythonOperator(

        task_id="extract_task",

        python_callable=extract_data
    )

    transform_task = PythonOperator(

        task_id="transform_task",

        python_callable=transform_data
    )

    load_task = PythonOperator(

        task_id="load_task",

        python_callable=load_data
    )

    validate_task = PythonOperator(

        task_id="validate_task",

        python_callable=validate_data
    )

    (
        extract_task
        >> transform_task
        >> load_task
        >> validate_task
    )