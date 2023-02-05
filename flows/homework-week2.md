
1. 
`python flows/03_deployments/parameterized_flow.py`

```
01:16:59.021 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
01:16:59.035 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
```

2. Cron Scheduling
* * * * * == min, hour, day, month, year
first of every month at 5am UTC
0 5 1 * *

0 0 5 1 * - midnight at 5th of january

3. 
`gcsbq_homework.py`
then in bigquery
`SELECT count(1) FROM alien-handler-376020.dezoomhomework.rides`
14851920

4. 
` prefect deployment build ./flows/04_homeworks/webgcs_homework.py:etl_web_to_gcs_hw -n "GitHub Storage Flow" -sb github/github-block -o etl_web_to_gcs_hw_github-deployment.yaml --apply`
rows: 88605

5.
rows: 514392

6. 8