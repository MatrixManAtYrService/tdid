from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from textwrap import dedent
import json
import re
import requests

# default dag parameters
git_inputs = {
    # show me what broke and what got fixed in this repo
    "project_repo": "https://github.com/MatrixManAtYrService/dummyproject",
    # between these revisions
    "project_reference_rev": "a443c6f",
    "project_candidate_rev": "latest",
    # according to these tests
    "tests_repo": "https://github.com/MatrixManAtYrService/dummyproject-tests",
    "tests_rev": "latest",
}

# keys by type
revisions = [x[:-4] for x in git_inputs if re.match(r".*rev$", x)]
repositories = [x[:-5] for x in git_inputs if re.match(r".*repo$", x)]


def gen_github_apis(user_inputs_json):
    "Generate github api endpoints for each repo"

    user_inputs = json.loads(user_inputs_json)

    for repo in repositories:

        repo_key = f"{repo}_repo"
        repo_url = user_inputs[repo_key]

        # parse url
        expr = r".*github.com/([^/]*)/([^/]*)$"
        match = re.match(expr, repo_url)
        owner = match.group(1)
        name = match.group(2)

        # store api endpoint
        ti = get_current_context()["ti"]
        ti.xcom_push(
            key=f"{repo}_api", value=f"https://api.github.com/repos/{owner}/{name}"
        )


def maybe_dereference_rev(user_inputs_json, revision):
    "Skip api call if SHA1 is explicitly given"

    user_inputs = json.loads(user_inputs_json)
    rev_key = f"{revision}_rev"
    rev_reference = user_inputs[rev_key]
    if rev_reference.lower().strip() == "latest":
        return f"fix_{revision}"
    else:
        return "solidify_params"


def solidify_params(user_inputs_json):
    "Write nonambiguous params"

    user_inputs = json.loads(user_inputs_json)
    ti = get_current_context()["ti"]

    for revision in revisions:

        fix_task_id = f"fix_{revision}"
        rev_key = f"{revision}_rev"

        the_fix = ti.xcom_pull(task_id=fix_task_id)
        if the_fix:
            user_inputs[rev_key] = the_fix

    return user_inputs


def parse_dag_run_conf():

    # surely there's a better way to get dag_run.conf as json...
    return dedent(
        """
        {
        {% for k,v in dag_run.conf.items() %}
        "{{ k }}" : "{{ v }}"
        {% if not loop.last %} , {% endif %}
        {% endfor %}
        }
        """
    )


with DAG(
    "tdid_compare",
    schedule_interval=None,
    start_date=days_ago(2),
    default_args={"owner": "airflow"},
    params=git_inputs,
) as dag:

    sources = parse_dag_run_conf()

    parse_input = PythonOperator(
        task_id="gen_github_apis",
        python_callable=gen_github_apis,
        op_args=[sources],
    )

    solidify_params = PythonOperator(
        task_id="solidify_params",
        python_callable=solidify_params,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    with TaskGroup("get_branch_info") as get_branch_info:

        for revision in revisions:
            repo = revision.split("_")[0]

            # "latest" -> "a6ff88b"
            fix = SimpleHttpOperator(
                task_id=f"fix_{revision}",
                method="GET",
                http_conn_id="project_branch_conn",
                endpoint=f"{{{{ ti.xcom_pull(key='{repo}_api') }}}}",
                headers={"Authorization": "bearer {{ var.value.GITHUB_ACCESS_TOKEN }}"},
                do_xcom_push=True,
            )

            dont_fix = DummyOperator(task_id=f"leave_{revision}")

            # "a6ff88b" -> do nothing
            # "latest" -> execute fix
            maybe_fix = BranchPythonOperator(
                task_id=f"maybe_fix_{revision}",
                python_callable=maybe_dereference_rev,
                op_args=[sources, revision],
            )

            parse_input >> maybe_fix >> [fix, dont_fix] >> solidify_params
