from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


default_args = {
    'retries':2
}
with DAG(
    dag_id='papermill_operator',
    default_args=default_args,
<<<<<<< HEAD
    start_date=pendulum.datetime(2022, 9, 27, tz="UTC"),
=======
    start_date=pendulum.datetime(2022, 9, 26, tz="UTC"),
>>>>>>> 76442f508fd7e793714220472a2563188ca822fa
    schedule_interval=None,
    catchup=False,
    template_searchpath='/opt/scripts',
    #template_searchpath='/dags_airflow-dags',
) as dag:
    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="notebook.ipynb",
        output_nb="out-{{ execution_date }}.ipynb",
        #parameters={"execution_date": "{{ execution_date }}"},
    )


notebook_task
