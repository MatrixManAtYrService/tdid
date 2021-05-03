from tdid.dags import compare
import json

# run pytest -m pytest -s .
# from tdid dir


def test_endpoint_gen():
    j = json.dumps(
        {
            "project_repo": "https://github.com/MatrixManAtYrService/dummyproject",
            "project_reference_rev": "a443c6f",
            "project_candidate_rev": "master",
            "tests_repo": "https://github.com/MatrixManAtYrService/dummyproject-test",
            "tests_rev": "master",
        }
    )
    endpoints = compare.gen_github_endpoints(j)
    assert (
        endpoints["project_reference_rev"]
        == "/repos/MatrixManAtYrService/dummyproject/commits/a443c6f"
    )
    assert (
        endpoints["project_candidate_rev"]
        == "/repos/MatrixManAtYrService/dummyproject/commits/master"
    )
    assert (
        endpoints["tests_rev"]
        == "/repos/MatrixManAtYrService/dummyproject-test/commits/master"
    )
