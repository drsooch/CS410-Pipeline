# Distributed Airflow Setup
This software can deployed in a distributed setup leveraging Docker and Docker-Compose.

## Requirements
- Docker
- Docker-compose

## Initializating Environment:
Our dag utilizes the Airflow 2.0.2 base image with additional Mongo provider packages and our CSPipeline Python modules.

1. Create a working directory to house the container's files.

2. Create directories inside this root folder to house dags, logs, plugins, and the initial-dataset:
```
mkdir ./dags ./logs ./plugins ./initial-dataset
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

3. Place the dockerfile in the root directory and run `docker build . -t airflow410:v1`.

4. Place the docker-compose yaml file into the root directory.
	- Two options exist here, you can use the `docker-compose-with-mongo.yaml` to run Airflow with the LocalExecutor or `docker-compose-with-mongo-celeryexecutor.yaml` to run Airflow with the CeleryExecutor. <br/>

5. Run database migrations and create the first user account.
`docker-compose -f {DOCKER_COMPOSE_FILE_NAME.YAML} up airflow-init`
	- After initilization you should recieve the message: `start_airflow-init_1 exited with code 0`.

	- An Airflow account was created during initialization by default with a username and password of `airflow`.

## Starting Airflow:
1. From the root directory run `docker-compose -f {DOCKER_COMPOSE_FILE_NAME.YAML} up -d`.

2. After services have started you can check the condition of the containers using `docker ps`.

## Creating Connections
1. Access the web interface at `http://localhost:8080` with default login and password `airflow:airflow`.

2. [Create an account](https://www.courtlistener.com/register/) on Court listener if you have not already.

3. Go to the connections tab and create two new connections:
```
Conn Id: mongo_default
Conn Type: MongoDB
Login: root
Password: rootpassword
Port: 27017
```
```
Conn Id: default_api
Conn Type: HTTP
Host: https://www.courtlistener.com/api/rest/v3/
Login: {COURTLISTENER_USERNAME}
Password: {COURTLISTENER_PASSWORD}
```

## Bulk Dataset

1. First, [download and extract a bulk data set](https://www.courtlistener.com/api/bulk-info/).

2. Next, place the bulk data set in the `initial-dataset` directory.

3. Open up the Airflow webserver and trigger the bulk data DAG.

4. Ensure that the Courtlistener Daily DAG is unpaused, and you have completed pipeline setup.

While running you may check the mongo database to see the data as it's transformed. After the bulk data DAG has finished running initialization is complete.

## Accessing Mongo Data
Please note, the container's name may not be mongo_1. Use `docker ps` and look for a container with `mongo` in its name.

1. Connect to the Mongo container:
`docker exec -it -u 0 {ROOT_FOLDER_NAME_}mongo_1 bash`

2. From inside the container, connect to Mongo:
`mongo -u root -p rootpassword`

3. Use Mongo commands to look at the collections:
```
use courts
show collections
db.{collectionName}.find()
db.{collectionName}.find({ "_id" : "<id>" })
```
