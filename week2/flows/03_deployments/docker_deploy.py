from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow

docker_block = DockerContainer.load("dockerzoom")




docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker-flow',
    infrastructure=docker_block
)

if __name__ == "__main__":
    """
        When creating deployment thru code, the path seems to be inferred
        ex. My current dir is flows/03_deployments/docker_deploy.py, when I run python docker_deploy.py under this dir
        it's gonna try to find the script on the root of docker
        But if my current dir is above flows and I run python flows/03_deployments/docker_deploy.py 
        the flow run is gonna try to find the script in flows/03_deployments/docker_deploy.py path of docker
        
        it seems like the prefect docker image should match the folder path of the local code
        
        how to run this code:
        cd into week2 directory and run python3 flows/03_deployments/docker_deploy.py
    """
    docker_dep.apply()