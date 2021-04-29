This is a sandbox project.  I'll remove this message when it's actually useful.

# TDID (test data is data)

Here's a workflow:

```
((proj-git-sha1:abc123, proj-git-sha1:def789), testsuite-git-sha1:112233) ==>
    build two artifacts ==>
    set up two environments ==>
    run tests against both ==>
    a report showing which tests went from:
        - pass->fail
        - fail->pass
        - pass->pass
        - fail->fail
      and which fingerprints:
        - changed
        - stayed the same

```

The purpose of this project is to conceive of this workflow as a *data* pipeline.
We start with a tuple containing some git revisions and "transform" it into test results.

It's a proof-of-concept.

This lets dev's pick a best-known-version in their project's past and ask:
 - What has broken since then?
 - What has been fixed since then?
 - What fingerprints have changed since then?
 - What fingerprints have not changed same since then?

I belive that this will be a powerful tool because it accomodates "fingerprint testing" (a term that I made up).
Fingerprint testing is hardly testing.
Rather than saying "this passed" or "this failed" fingerprint tests mearly say: "I derived this value".

On their own, they're pretty useless.
How is a developer supposed to know if the derived value is the _right_ value?
In conjunction with reliable test automation, however, fingerprint testing helps developers reason about which features are sensitive to changes in which files.  That is, it causes you to have moments like this:

> I changed *this* file.  It shouldn't have impacted *that* feature.

Often, merely dumping your logs into a fingerprint (with timestamps and other unique values masked) can be useful because if you make a code change that introduces a warning into STDERR, it shows up in the "which fingerprints changed" category.

### Ok, but how?

dags/tdid.py defines an airflow DAG which accepts some parameters:
 - project repo (default: https://github.com/MatrixManAtYrService/dummyproject )
 - project best-known-revision (default: latest~)
 - project revision-under-test (default: latest)
 - tests repo (default: https://github.com/MatrixManAtYrService/dummyproject-tests)
 - tests tests-revision (default: latest)

It generates the report described above in the local filesystem

