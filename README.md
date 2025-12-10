- create project folder
```bash
 
mkdir docker-airflow-dbt 
 

```
- download docker-compose.ymal
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

```
# add the below in .env 
"AIRFLOW_IMAGE_NAME=apache/airflow:3.1.3
AIRFLOW_UID=501"

# create the dags, config, logs and plugins folder after run "docker compose up"

```bash

docker compose up -d
```
# If you are cloning from this project
- Install uv
- then install dependencies
```bash
uv sync
```
- !set the uer id
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
- init the database
```bash
docker compose up airflow-init
```

# Usage
- start airflow
```bash

docker compose up -d
```
login with: airflow@airflow

- go to browser
localhost:8080


- stop airflow
```bash
docker compse down
```

# Source - https://stackoverflow.com/a
# Posted by mik-laj, modified by community. See post 'Timeline' for change history
# Retrieved 2025-12-04, License - CC BY-SA 4.0

- build a custom image
```
docker build . --tag my-company-airflow:2.0.0
```

# Debug
- file cannot change: echo -e "AIRFLOW_UID=$(id -u)" > .env

- clean volumes:
'''
docker-compose down --volumes --rmi all
'''