
building image:
`docker image build -t deebee504/prefect:zoom .`

running image locally:
`docker run -it deebee504/prefect:zoom`

pushing to docker hub:
`docker image push deebee504/prefect:zoom`

creating deployment thru command line
this will create a yaml file
`prefect deployment build ./parameterized_flow.py:etl_parent_flow -n etl2 --cron "0 0 * * *" -a`


running prefect deployment:
`prefect deployment run etl-parent-flow/docker-flow -p "months=[4, 5]"`

start an agent: agent needed for running flows
`prefect agent start  --work-queue "default"`

how do dockerized flow know which script to run?
need to understand this error
```
load_script_as_module
    raise ScriptError(user_exc=exc, path=path) from exc
prefect.exceptions.ScriptError: Script at '03_deployments/parameterized_flow.py' encountered an
```