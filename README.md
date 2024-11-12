# metaplane_utils

## About
metaplane_utils is a collection of macros that we at [Metaplane](https://www.metaplane.dev/) have written
to solve common issues we have run into with dbt.



## Installation
We currently support git based installation:
```yaml
packages:
  - git: https://github.com/metaplane/dbt-utils.git
```


### Macros

`publish_test_failures`: This macro can be used to automatically ingest the result of test failures captured by dbt via [store_failures](https://docs.getdbt.com/reference/resource-configs/store_failures).
Ingestion will only happen if you manually opt a test in like so:
```yaml
- accepted_values:
    meta:
      metaplane:
        publish_failures: true
    values:
      - returned
      - completed
```
Metaplane will then upload the failed test results into a snowflake stage, generate a pre-signed URL for that stage, 
and write the URL into the run_results.json file on the adapter response of each run tests.


