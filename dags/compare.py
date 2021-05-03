from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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
    "project_candidate_rev": "master",
    # according to these tests
    "tests_repo": "https://github.com/MatrixManAtYrService/dummyproject-test",
    "tests_rev": "master",
}

# keys by type
revisions = [x for x in git_inputs if re.match(r".*rev$", x)]
repositories = [x[:-5] for x in git_inputs if re.match(r".*repo$", x)]


def gen_github_endpoints(user_inputs_json):
    "Generate github api endpoints for each repo"

    user_inputs = json.loads(user_inputs_json)

    # build endpoints
    xcom = {}
    for repo in repositories:

        repo_key = f"{repo}_repo"
        repo_url = user_inputs[repo_key]

        # parse url
        expr = r".*github.com/([^/]*)/([^/]*)$"
        match = re.match(expr, repo_url)
        owner = match.group(1)
        name = match.group(2)

        for revision in revisions:
            rev_reference = user_inputs[revision]
            if repo in revision:
                xcom[revision] = f"/repos/{owner}/{name}/commits/{rev_reference}"

    return xcom


def decide_dereference_rev(revision_endpoint, decision):
    "Skip api call if SHA1 is explicitly given"

    yes = decision["branch"]
    no = decision["sha1"]

    "None comes in as empty string"
    if "/" not in revision_endpoint:
        return no
    else:
        return yes


def branch_to_sha1(revision, response, ti):
    "Given a github '/repos/foo/bar/commits' response, extract the SHA1"

    sha = json.loads(response)["sha"]
    ti.xcom_push(key=f"{revision}_sha", value=sha)


def solidify_params(user_inputs_json, ti):
    "Write nonambiguous params"

    user_inputs = json.loads(user_inputs_json)

    # update revision references with nontemporal ones
    for revision in revisions:
        sha = ti.xcom_pull(key=f"{revision}_sha")
        if sha:
            user_inputs[revision] = sha

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

    repo_endpoints = PythonOperator(
        task_id="gen_github_endpoints",
        python_callable=gen_github_endpoints,
        op_args=[sources],
    )

    solidify_params = PythonOperator(
        task_id="solidify_params",
        python_callable=solidify_params,
        trigger_rule=TriggerRule.NONE_FAILED,
        op_args=[sources],
    )

    with TaskGroup("get_branch_info") as get_branch_info:

        for revision in revisions:

            # requires HTTP connection: api.github.com
            # called: http_default
            ask_github = SimpleHttpOperator(
                task_id=f"ask_github_{revision}",
                method="GET",
                endpoint=f"{{{{ ti.xcom_pull(task_ids='{repo_endpoints.task_id}')['{revision}'] }}}}",
                headers={"Authorization": "bearer {{ var.value.GITHUB_ACCESS_TOKEN }}"},
                do_xcom_push=True,
            )

            to_sha1 = PythonOperator(
                task_id=f"getsha1_{revision}",
                python_callable=branch_to_sha1,
                op_args=[
                    revision,
                    f"{{{{ ti.xcom_pull(task_ids='{ask_github.task_id}') }}}}",
                ],
            )

            repo_endpoints >> ask_github >> to_sha1 >> solidify_params
