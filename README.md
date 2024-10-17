# qa4sm-airflow

This repository contains the configuration to set up the task scheduler for
[QA4SM](https://qa4sm.eu) . It is based on the [airflow docker setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

1) Make sure that the file `qa4sm.env` which contains the environment variables
   required by some of the DAGs to access different data sources etc. is available
   in the same directory as the `docker-run.sh` script
2) To start all necessary container, call the `docker-run.sh` script. Starting
   the service should pull the 'latest' available versions of DAG containers 
   (`airflow-dag-XXX` with `pull_policy: always` in `compose.yaml`).
3) After a few seconds the airflow service should run on port 8080.

## Setup
This setup will build/launch multiple images/containers. See the `compose.yml` setup.

### Airflow Common
See the `Dockerfile`. This is just the original airflow
image with the packages from `base_requirements.txt` installed and a new 
directory to mount the qa4sm data. Compose will build it if it's not yet 
available.

### Dag containers
DAGs consist of different operators. Complex operators 
should start their own containers (DockerOperator) that contain all the code
they need (to keep the task scheduler environment simple and operators 
separated). DockerOperators can use different python version, or even
different programming languages. Airflow is just the scheduler to launch them!
Of course, if the base environment is sufficient (for simple tasks like moving
files) you don't have to use the DockerOperator (that's also why we have 
the base_requirements file - but don't add any heavy dependencies there!)

# Environment variables
Most DAGs require some (secret) variables such as tokens to download data or
communicate with the service API (prod, test, dev). 
These are stored in a file `qa4sm.env` the same directory as `docker-run.sh`. 
See the template  `qa4sm.env.template` file for a list of required variables.

# Communicating with local development instance
Usually developing DAGs also needs access to a local instance of qa4sm (never
send requests to the production API!). For this
- start the qa4sm development instance with IP 0.0.0.0 (`python manage.py runserver 0.0.0.0:8000`)
- in `qa4sm.env` enter `"172.17.0.1"` for `QA4SM_IP_OR_URL` and `"8000"` for `QA4SM_PORT_OR_NONE`.
- the `QA4SM_API_TOKEN` is one that you created in your local develop instance.
