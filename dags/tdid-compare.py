from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.version import version
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from textwrap import dedent
import json
import re
import requests

git_inputs = {
    # show me what broke and whatngot fixed in this repo
    "project": "https://github.com/MatrixManAtYrService/dummyproject",
    # between these revisions
    "project_reference_rev": "a443c6f",
    "project_candidate_rev": "latest",
    # according to these tests
    "tests": "https://github.com/MatrixManAtYrService/dummyproject-tests",
    "tests_rev": "latest",
}


def parse_dag_run_conf():

    # surely there's a better way to get dag_run.conf as a dict
    # jinja doesn't print it in an easily parsable format so...
    jstr = dedent(
        """
        {
        {% for k,v in dag_run.conf.items() %}
        "{{ k }}" : "{{ v }}"
        {% if not loop.last %}
        , 
        {% endif %}
        {% endfor %}
        }
        """
    )
    return jstr.replace("\n", "").replace(" ", "")


def find_nonhash_refs(sources):
    """
    Find cases where user specified "latest" so they can be resolved to a SHA1
    """
    print(sources)
    # print(json.loads(sources))

    latests = {}
    for prefix in ["project", "tests"]:
        repo_url = sources[prefix]
        for key, val in sources.items():
            if re.match(f"^{prefix}.*rev$", key):
                if val.lower().strip() == "latest":
                    latests[key] = repo_url

    return latests


# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    "tdid_compare",
    schedule_interval=None,
    start_date=days_ago(2),
    default_args=default_args,
    params=git_inputs,
) as dag:

    sources = parse_dag_run_conf()
    t1 = PythonOperator(
        task_id="find_nonhash_refs",
        python_callable=find_nonhash_refs,
        op_args=[sources],
    )
