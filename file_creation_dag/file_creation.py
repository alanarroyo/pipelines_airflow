import pathlib
import airflow
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

my_dag = DAG(
    dag_id="file_creation",
    start_date=datetime(2024,12,23),
    schedule_interval=None
)

date = str(datetime.today())[0:10]
time = str(datetime.now()).replace(" ", "_").replace(":","s")
path = "/Users/gandalf/Documents/airflow/test_folder"
print(date)
print(f"touch {path}/file_{date}.txt")

create_file = BashOperator(
    task_id = "create_file",
    bash_command=f"touch {path}/file_{date}.txt",
    dag=my_dag
)

append_text = BashOperator(
    task_id = "append_text",
    bash_command=f"echo 'Hello right now is {time}' >> {path}/file_{date}.txt",
        dag=my_dag

)
create_file >> append_text