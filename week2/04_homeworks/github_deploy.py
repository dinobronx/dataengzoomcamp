from prefect.deployments import Deployment
from etl_web_to_gcs_Q4 import etl_web_to_gcs
from prefect.filesystems import GitHub 

# sample github deploy
storage = GitHub.load("github-de-zoomcamp-block")


deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="github-example",
     storage=storage,
     entrypoint="Week2/Homework/etl_web_to_gcs_Q4.py:etl_web_to_gcs")

if __name__ == "__main__":
    deployment.apply()