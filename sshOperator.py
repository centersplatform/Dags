from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator




default_args = {
    'retries':2
}
bash_spark = """
        /spark/bin/spark-submit 
        --master spark://spark-master-svc:7077 
        --class org.data_training.App
        NTTData-1.0-SNAPSHOT.jar
        Customers hdfs://192.168.182.17:8020/hive/warehouse/hive/warehouse/ecom.db/customers_dataset/customers_dataset.csv
 """
with DAG(
    dag_id='ssh_operator',
    default_args=default_args,
    start_date=datetime(2022, 11, 9),

) as dag:
    ssh_local = SSHOperator(
		        ssh_conn_id= 'ssh_default', 
		        task_id='sshsubmit_task', 
                command=bash_spark,
		        dag=dag
    )

	
ssh_local
    
