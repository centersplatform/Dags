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
cmd2='pwd'
cmd3='kubectl exec -it spark-master-0 -n spark  -- spark-submit --master spark://spark-master-svc:7077 /tmp/test.py'

with DAG(
    dag_id='bash_operator',
    default_args=default_args,
    start_date=datetime(2022, 12, 19),
) as dag:
    t2 = BashOperator(task_id='test_bash_operator',bash_command=cmd2, dag=dag)

t2
