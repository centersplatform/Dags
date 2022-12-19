from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

# Define default_args dictionary to specify default parameters of the DAG
default_args = {
    'owner': 'hive',
    #'depends_on_past': False
    'start_date': datetime(2022, 12, 19),
}

# Instantiate a DAG with the default_args dictionary
dag = DAG(
    'spark_on_kubernetes_example',
    default_args=default_args,
    #schedule_interval=timedelta(minutes=30)
)

# Define a SparkSubmitOperator task
task = SparkSubmitOperator(
    conn_id='spark_default',
    task_id='spark_job',
    dag=dag,
    application='/opt/bitnami/spark/tmp/NTTData-1.0-SNAPSHOT.jar',
    java_class = "org.data_training.App",
    application_args=["LoadDataToDW"],
    in_cluster=True,
    kubernetes_namespace='spark',
    executor_memory='8G',
    executor_cores='4'
)

task

# Set the dependencies for the task

