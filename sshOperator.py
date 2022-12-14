from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator




default_args = {
    'retries':2,
    'owner': 'hive',
}

cmd2='pwd'
cmd3='kubectl get pods -n airflow'
cmd4='kubectl exec -it spark-master-0 -n spark  -- '
cmd5="""spark-submit --master spark://spark-master-svc:7077 --class org.data_training.App \
tmp/NTTData-1.0-SNAPSHOT.jar LoadDataToDW --executor-memory 10g --driver-memory 10g
"""
with DAG(
    dag_id='ssh_operator',
    default_args=default_args,
    start_date=datetime(2022, 12, 19),

) as dag:
    ssh_local = SSHOperator(
		        ssh_conn_id= 'ssh_default', 
		        task_id='ssh_submit_task', 
                command=cmd2,
		        dag=dag
    )


ssh_local 