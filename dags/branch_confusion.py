from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import random


def decide():
    if random.randint(0, 1):
        return "thing_one"
    else:
        return "thing_two"


with DAG(
    "branch_confusion",
    schedule_interval=None,
    start_date=days_ago(1),
    default_args={"owner": "airflow"},
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    with TaskGroup("banching_logic"):

        thing_one = DummyOperator(task_id="thing_one")
        thing_two = DummyOperator(task_id="thing_two")

        # airflow.exceptions.TaskNotFound: Task thing_two not found
        decide = BranchPythonOperator(task_id="decide", python_callable=decide)
        # works ok if I remove the task group

    start >> decide >> [thing_one, thing_two] >> end
