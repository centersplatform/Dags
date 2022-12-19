from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator



default_args = {
    'retries':2
}
spark_master = ("spark://"
                "spark-master-svc"
                ":7077")
                
command = ("spark-submit "
            "--master {master} "
            "/tmp/test.py").format(master=spark_master)
cmd2='ls'
cmd3='kubectl exec -it spark-master-0 -n spark -- bash ./bin/spark-submit --name spark-pi --class org/data_training/App.scala tmp/NTTData-1.0-SNAPSHOT.jar'

with DAG(
    dag_id='bash_operator',
    default_args=default_args,
    start_date=datetime(2022, 12, 19),
) as dag:
    t2 = BashOperator(task_id='test_bash_operator',bash_command=command, dag=dag)

t2
