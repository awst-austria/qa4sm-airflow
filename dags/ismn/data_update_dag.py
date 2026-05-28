"""
This is the processing pipeline to update SMOS L2 data in qa4sm.
It will download new image data from FTP, update time series in the qa4sm
datastore as far as image data is available and send the updated time series
period through the API to the DB.
"""
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from airflow.models.dag import DAG
from datetime import datetime, timedelta
import yaml
import os
import pandas as pd
import logging

from misc import (api_update_period, 
                  load_qa4sm_dotenv, api_update_fixtures, log_command)

load_qa4sm_dotenv()
IMAGE = os.environ["ISMN_DAG_IMAGE"]
QA4SM_IP_OR_URL = os.environ["QA4SM_IP_OR_URL"]
QA4SM_PORT_OR_NONE = os.environ["QA4SM_PORT_OR_NONE"]
QA4SM_API_TOKEN = os.environ["QA4SM_API_TOKEN"]
QA4SM_DATA_PATH = os.environ["QA4SM_DATA_PATH"]  # On the HOST machine
EMAIL_ON_FAILURE = bool(int(os.environ.get("EMAIL_ON_FAILURE", 0)))
ISMN_USERNAME = os.environ["ISMN_USERNAME"]
ISMN_PASSWORD = os.environ["ISMN_PASSWORD"]

# Source is on the HOST machine (not airflow container), target is in the worker image
#   see also https://stackoverflow.com/questions/31381322/docker-in-docker-cannot-mount-volume
data_mount = Mount(target="/qa4sm/data", source=QA4SM_DATA_PATH, type='bind')

logger = logging.getLogger(__name__)

"""
All versions are added to the list. The dag itself can be 
(de)activated in the GUI afterwards
"""
DAG_SETUP = {
    'ISMN_20260518_ext': {
        # Paths here refer to the worker image and should not be changed!
        'data_path': "/qa4sm/data/ISMN/ISMN_V20260518_ext/",
        'ext_start_date': "2025-05-18",
        'qa4sm_dataset_id': "80",
    },
}


def get_period_to_from_yml(yml_file: str) -> str | None:
    """Read period_to from overview.yml. Returns ISO date string or None."""
    if not os.path.isfile(yml_file):
        logging.info(f"No yml file at {yml_file}")
        return None
    with open(yml_file, 'r') as stream:
        props = yaml.safe_load(stream)
    period_to = props.get('period_to')
    if period_to is None:
        return None
    return str(pd.to_datetime(period_to).date())


def decide_ts_update_required(ext_start_date: str, **context):
    ti = context['ti']
    before = ti.xcom_pull(task_ids='get_period_before')
    after = ti.xcom_pull(task_ids='get_period_after')

    # First run ever: no prior data, treat ext_start_date as the baseline
    if before is None:
        before = ext_start_date

    if after is None:
        logging.warning("No period_to after download — skipping update.")
        return 'finish'

    if pd.to_datetime(after) > pd.to_datetime(before):
        logging.info(f"New data: {before} → {after}, updating.")
        return 'api_update_period'
    else:
        logging.info(f"No new data ({before} == {after}), skipping.")
        return 'finish'


for version, dag_settings in DAG_SETUP.items():
    data_path = dag_settings['data_path']
    ext_start_date = dag_settings['ext_start_date']
    qa4sm_id = dag_settings['qa4sm_dataset_id']

    yml_file = os.path.join(data_path, 'overview.yml')

    with DAG(
            f"ISMN-{version}-Processing",
            default_args={
                "depends_on_past": False,
                "email": ["support@qa4sm.eu"],
                "email_on_failure": EMAIL_ON_FAILURE,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(hours=1),
            },
            description="Update ISMN image data",
            schedule=timedelta(weeks=1),
            start_date=datetime(2026, 5, 1),
            catchup=False,  # don't repeat missed runs!
            tags=["ismn", "download", "update"],
    ) as dag:
        # Check data setup -----------------------------------------------------
        _task_id = "verify_dir_available"
        _doc = f"""
        Check if the data store is mounted, check if {data_path} exists.
        """
        _command = (
            f"bash -c '"
            f"echo \"=== Container view of /qa4sm/data ===\"; "
            f"ls -la /qa4sm/data/ 2>&1; "
            f"echo \"=== Looking for {data_path} ===\"; "
            f"ls -la {data_path} 2>&1; "
            f"[ -d \"{data_path}\" ] && echo OK || exit 1"
            f"'"
        )
        logger.info(f"Running Container Command in {IMAGE}: {_command}")
        # start container with sudo?
        verify_dir_available = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__ISMN__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            force_pull=True,  # make sure the image is pulled once the start of the pipeline
            auto_remove="force",
            mount_tmp_dir=False,
            docker_url='tcp://docker-proxy:2375',
            on_execute_callback=log_command,
            doc=_doc
        )

        _task_id = "verify_qa4sm_reachable"
        _command = f"(set -e; nc -z -v {QA4SM_IP_OR_URL} {QA4SM_PORT_OR_NONE if QA4SM_PORT_OR_NONE.lower() not in ['none', ''] else 443} 2>&1 | grep -q 'succeeded' && echo 'OK') || exit 1"
        _doc = f"""
        Check if qa4sm is reachable, 0 = success, 1 = fail
        """
        logger.info(f"Running Container Command in {IMAGE}: {_command}")
        # start container with sudo?
        verify_qa4sm_available = BashOperator(
            task_id=_task_id,
            bash_command=_command,
            on_execute_callback=log_command,
            doc=_doc
        )

        # BEFORE download — capture the existing period_to
        get_period_before = PythonOperator(
            task_id="get_period_before",
            python_callable=get_period_to_from_yml,
            op_kwargs={'yml_file': yml_file},
            doc="Read period_to from overview.yml before the archive is refreshed.",
        )

        # ISMN Archive Download----------------------------------------------------
        _task_id = 'download_archive'
        _command = (
            f'bash -c \''
            f'mkdir -p {data_path} && '
            f'ismn nrt-download {data_path}/ismn_archive.zip --username {ISMN_USERNAME} --password {ISMN_PASSWORD} && '
            f'ismn nrt-extract {data_path}/ismn_archive.zip {data_path}'
            f'\''
        )
        _doc = f"""
        This task will check if any image data is available in the img_path.
        Then it downloads the available archive from ismn.earth. It extracts data from nrt networks,
        if they have a different crc32 checksum then the former files. If the files are identical, 
        it leaves them as they are to save time.
        """
        logger.info(f"Running Container Command in {IMAGE}: {_command}")
        # https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html
        download_archive = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__ISMN__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600 * 2,
            mount_tmp_dir=False,
            docker_url='tcp://docker-proxy:2375',
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Get current time series coverage -------------------------------------
        _task_id = "get_img_timeranges"
        _doc = f"""
        Look at time series and image data and infer their covered period.
        """
        get_period_after = PythonOperator(
            task_id="get_period_after",
            python_callable=get_period_to_from_yml,
            op_kwargs={'yml_file': yml_file},
            doc="Read period_to from overview.yml after the archive has been refreshed.",
        )

        # Update QA4SM? --------------------------------------------------
        decide_update = BranchPythonOperator(
            task_id="decide_ts_update",
            python_callable=decide_ts_update_required,
            op_kwargs={'ext_start_date': ext_start_date},
            doc="Compare before/after period_to and decide whether to update qa4sm.",
        )        

        # Optional: Send new time range to QA4SM -------------------------------
        _doc = f"""
        Report the latest covered period to the service to update it on the 
        website.
        """
        update_period = PythonOperator(
            task_id='api_update_period',
            python_callable=api_update_period,
            op_kwargs={'QA4SM_PORT_OR_NONE': QA4SM_PORT_OR_NONE,
                        'QA4SM_IP_OR_URL': QA4SM_IP_OR_URL,
                        'QA4SM_API_TOKEN': QA4SM_API_TOKEN,
                        'ds_id': qa4sm_id,
                        'ts_task_id': 'get_period_after',
                        'ts_key': 'return_value'},
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Optional: Update and push fixtures -----------------------------------
        _task_id = "api_update_fixtures"
        _doc = f"""
        Dump the updated periods to fixtures and push to github.
        """
        update_fixtures = PythonOperator(
            task_id=_task_id,
            python_callable=api_update_fixtures,
            op_kwargs={'QA4SM_PORT_OR_NONE': QA4SM_PORT_OR_NONE,
                       'QA4SM_IP_OR_URL': QA4SM_IP_OR_URL,
                       'QA4SM_API_TOKEN': QA4SM_API_TOKEN
                       },
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Always check the current time range again ----------------------------
        finish = PythonOperator(
            task_id="finish",
            python_callable=get_period_to_from_yml,
            op_kwargs={'yml_file': yml_file},
            trigger_rule="none_failed_min_one_success",
            doc="Always run: print final coverage.",
        )

        # Task logic -----------------------------------------------------------
        verify_dir_available >> verify_qa4sm_available >> get_period_before >> download_archive >> get_period_after >> decide_update 
        decide_update >> finish
        decide_update >> update_period >> update_fixtures >> finish
