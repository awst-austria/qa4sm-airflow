# qa4sm-airflow

This repository contains the configuration to set up the task scheduler for
[QA4SM](https://qa4sm.eu) . It is based on the [airflow docker setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

1) Make sure that the file `qa4sm.env` which contains the environment variables
   required by some of the DAGs to access different data sources etc. is available
   in the same directory as the `docker-run.sh` script (don't track it with GIT).
2) To start all necessary container, call the `docker-run.sh` script (the user ID
will be used by docker, e.g., `bash docker-run.sh wpreimes`). 
The service should pull the airflow containers and DAG images as indicated in the 
environment file (`airflow-dag-XXX` with `pull_policy: always` in `compose.yaml`).
3) After a few seconds the airflow service should run on port 8085 (localhost:8085).
4) To stop and clean up the container run `bash docker-stop.sh`.

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


# DAGs

All DAGs are in `dags/` (shared helper functions in `dags/misc.py`). Paths
(`QA4SM_DATA_PATH`, `QA4SM_REPORTS_PATH`) and API access are taken from `qa4sm.env`.

## Data Update DAGs (`dags/data_update/`)

Keep the operational datasets on the service up to date. Each DAG runs weekly
in its own DockerOperator image (`*_DAG_IMAGE` in `qa4sm.env`): it downloads new
images from the data provider (FTP, CDS, etc.), appends them to the time series
and reports the new period (and fixtures) to the QA4SM API.

| DAG | Dataset / version | Image variable |
|---|---|---|
| `C3S-<version>-Processing` | C3S SM v202505, v202312 | `C3S_SM_DAG_IMAGE` |
| `ERA5-<version>-Processing` | ERA5 (latest, extended) | `ERA_DAG_IMAGE` |
| `ERA5Land-<version>-Processing` | ERA5-Land (latest, extended) | `ERA_DAG_IMAGE` |
| `ISMN-<version>-Processing` | ISMN 20260518 (extended) | `ISMN_DAG_IMAGE` |
| `SMAP_L3-<version>-Processing` | SMAP L3 v9 | `SMAP_DAG_IMAGE` |
| `SMOS_L2-<version>-Processing` | SMOS L2 v700 | `SMOS_DAG_IMAGE` |

## Autoreport DAGs (`dags/autoreport/`)

Run predefined validations on QA4SM at regular intervals and compile a PDF
report from the results, using the
[qa4sm-autoreports](https://pypi.org/project/qa4sm-autoreports/) package
(installed via `base_requirements.txt`, runs in the Airflow environment, needs
LaTeX from the `Dockerfile`). Each report series has a folder with
`report_config_templates/` (one QA4SM validation config per run) and
`report_latex_templates/src/` (LaTeX templates filled with the results).

Every DAG run goes through the same steps:
1. Stage a new report for the period (copy configs, set the time period)
2. Trigger the validation runs on QA4SM
3. Wait until all runs are finished (sensor, up to several days)
4. Download the results, track the metrics against previous reports
   (traffic light), fill the templates and compile the PDF

Results are stored in `$QA4SM_REPORTS_PATH/results/<series>`, the PDFs in
`$QA4SM_REPORTS_PATH/pdf/<series>` (both directories must exist on the host).

| DAG | Series | Schedule / period | Validation runs |
|---|---|---|---|
| `SMOS_L2-v700-Autoreport` | `SMOS_L2_v700` | monthly, last 3 months | SMOS L2 vs. ISMN, ERA5-Land, SMAP L3 |
| `C3S_SM-v202605-Autoreport` | `C3S_v202605_Quarterly` | quarterly, last complete quarter | ISMN, ERA5-Land vs. C3S combined / active / passive |
