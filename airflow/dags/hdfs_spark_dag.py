from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': 0,
}

dag = DAG('hdfs_spark_dag', default_args=default_args, schedule_interval=None)

upload_data = BashOperator(
    task_id='upload_metadata',
    bash_command='''
        cp /home/manu/airflow/dags/data/metadata.txt /home/manu/data/ &&
        sudo docker exec -it docker-hadoop_namenode_1 sh -c "hadoop fs -put -f /hadoop/data/metadata.txt /metadata/" &&
        rm /home/manu/data/metadata.txt 
    ''',
    dag=dag
)

run_spark = BashOperator(
    task_id='spark_submit',
    bash_command='''
        sudo docker exec -it spark-spark-1 spark-submit /opt/bitnami/spark/volume/scripts/person_transformations.py
    ''',
    dag=dag
)

download_data = BashOperator(
    task_id='download_data',
    bash_command='''
        sudo docker exec -it docker-hadoop_namenode_1 sh -c "hadoop fs -get /output/output.txt /hadoop/data/output.txt"
    ''',
    dag=dag
)

upload_data >> run_spark >> download_data
