import airflow

from  airflow import  DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta


every_5_min_dag =  DAG(
    dag_id = "every_5_min",
    start_date=datetime(2024,12,23),
    end_date=datetime(2024,12,27),
    schedule_interval = timedelta(minutes=5),
    catchup=False # this is necessary to turn off airflow repeateadly running jobs to try to catch up with current schedule
)

path = "/Users/gandalf/Documents/airflow/test_folder"
time = str(datetime.now()).replace(" ", "_").replace(":","s")


write_file = BashOperator(
    task_id = "write_file",
    bash_command=f"echo 'Time now is {time}' >> {path}/every_5_min.txt",
    dag=every_5_min_dag

)
