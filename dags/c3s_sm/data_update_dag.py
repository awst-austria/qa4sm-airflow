from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

import yaml
import os
from datetime import datetime
import pandas as pd
import logging

from misc import api_update_period, decide_ts_update_required, load_qa4sm_dotenv

# TODO:
# - Change schedule (not daily)
# - Configure URL, token, path etc for different instances?
# - Could split dag for separate image and TS updates?

load_qa4sm_dotenv()  # environment variables loaded from qa4sm.env file
IMAGE = os.environ["C3S_SM_DAG_IMAGE"]
QA4SM_IP_OR_URL = os.environ["QA4SM_IP_OR_URL"]
QA4SM_PORT_OR_NONE = os.environ["QA4SM_PORT_OR_NONE"]
QA4SM_API_TOKEN = os.environ["QA4SM_API_TOKEN"]
QA4SM_DATA_PATH = os.environ["QA4SM_DATA_PATH"]  # On the HOST machine

# Source is on the HOST machine (not airflow container), target is in the worker image
#   see also https://stackoverflow.com/questions/31381322/docker-in-docker-cannot-mount-volume
data_mount = Mount(target="/qa4sm/data", source=QA4SM_DATA_PATH, type='bind')

"""
All versions are added to the list. The dag itself can be 
(de)activated in the GUI afterwards
"""
DAG_SETUP = {
    'v202212': {
        'img_path': "/qa4sm/data/C3S_combined/C3S_V202212-ext/images/",
        'ts_path': "/qa4sm/data/C3S_combined/C3S_V202212-ext/timeseries/",
        'ext_start_date': "2022-01-01",
        'qa4sm_dataset_id': "45",
    },
    'v202312': {
        'img_path': "/qa4sm/data/C3S_combined/C3S_V202312-ext/images/",
        'ts_path': "/qa4sm/data/C3S_combined/C3S_V202312-ext/timeseries/",
        'ext_start_date': "2024-01-01",
        'qa4sm_dataset_id': "58",
    },
}


def get_timeranges_from_yml(
        img_yml: str = None,
        ts_yml: str = None,
        ext_start: str = None,
        do_print: bool = False
) -> dict:
    if (img_yml is None) or (not os.path.isfile(img_yml)):
        logging.info(f"No img yml file: {img_yml}")
        img_to = None
    else:
        with open(img_yml, 'r') as stream:
            img_props = yaml.safe_load(stream)
        img_to = pd.to_datetime(img_props['period_to']).to_pydatetime()

    if (ts_yml is None) or (not os.path.isfile(ts_yml)):
        logging.info(f"No ts yml file: {ts_yml}")
        ts_to = None
    else:
        with open(ts_yml, 'r') as stream:
            ts_props = yaml.safe_load(stream)
        ts_to = pd.to_datetime(ts_props['img2ts_kwargs']['enddate']).to_pydatetime()

    if ts_to is not None:
        ts_next = ts_to + timedelta(days=1)
    elif ext_start is not None:
        ts_next = pd.to_datetime(ext_start).to_pydatetime()
    else:
        ts_next = None

    logging.info(f"Images time to: {img_to}")
    logging.info(f"Ts time to: {ts_to}")
    logging.info(f"Ts next: {ts_next}")

    if do_print:
        print("Images time to: ", img_to)
        print("TS time to: ", ts_to)
        print("Ts next: ", ts_next)

    return {'img_to': str(img_to.date() if img_to is not None else None),
            'ts_to': str(ts_to.date()) if ts_to is not None else None,
            'ts_next': str(ts_next.date())}


for version, dag_settings in DAG_SETUP.items():
    img_path = dag_settings['img_path']
    ts_path = dag_settings['ts_path']
    ext_start_date = dag_settings['ext_start_date']
    qa4sm_id = dag_settings['qa4sm_dataset_id']

    img_yml_file = os.path.join(img_path, 'overview.yml')
    ts_yml_file = os.path.join(ts_path, 'overview.yml')

    with DAG(
            f"C3S-{version}-Processing",
            default_args={
                "depends_on_past": False,
                "email": ["support@qa4sm.eu"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(hours=1),
            },
            description="Update C3S SM image data",
            schedule=timedelta(weeks=1),
            start_date=datetime(2024, 12, 2),
            catchup=False,  # avoid duplicate processing
            tags=["c3s_sm", "download"],
    ) as dag:
        # Check data setup -----------------------------------------------------
        _task_id = "verify_dir_available"
        _command = bash_command = f"bash -c '[ -d \"{img_path}\" ] && [ -d \"{ts_path}\" ] || exit 1'"
        _doc = f"""
        Check if the data store is mounted, chck if {img_path} and {ts_path} exist.
        """
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        # start container with sudo?
        verify_dir_available = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__c3s_sm__{_task_id}_{version}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            force_pull=True,  # make sure the image is pulled once the start of the pipeline
            auto_remove="force",
            mount_tmp_dir=False,
            doc=_doc
        )

        _task_id = "verify_qa4sm_reachable"
        _command = f"(set -e; nc -z -v {QA4SM_IP_OR_URL} {QA4SM_PORT_OR_NONE if QA4SM_PORT_OR_NONE.lower() not in ['none', ''] else 443} 2>&1 | grep -q 'succeeded' && echo 'OK') || exit 1"
        _doc = f"""
        Check if qa4sm is reachable, 0 = success, 1 = fail
        """
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        # start container with sudo?
        verify_qa4sm_available = BashOperator(
            task_id=_task_id,
            bash_command=_command,
            doc=_doc
        )

        # CDS Image Download----------------------------------------------------
        _task_id = f'update_images'
        _command = f"""bash -c '[ "$(ls -A {img_path})" ] && """ \
                   f"""c3s_sm update_img {img_path} --cds_token {os.environ['CDS_TOKEN']} || """ \
                   f"""c3s_sm download {img_path} -s {ext_start_date} -v {version} -p combined --freq daily --cds_token {os.environ['CDS_TOKEN']}'"""
        _doc = f"""
        This task will check if there is new data available online, and if 
        there is any data that matches to the one found locally, download new 
        files.
        This will not replace any existing files locally. This finds the LATEST 
        local file and checks if any new data AFTER this date is available.
        """
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        # https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html
        update_images = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__c3s_sm__{_task_id}_{version}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600 * 2,
            mount_tmp_dir=False,
            doc=_doc,
        )

        # Get current time series coverage -------------------------------------
        _task_id = "get_img_timeranges"
        _doc = f"""
        Look at time series and image data and infer their covered period.
        """
        get_img_timeranges = PythonOperator(
            task_id=_task_id,
            python_callable=get_timeranges_from_yml,
            op_kwargs={'img_yml': img_yml_file,
                       'ts_yml': ts_yml_file,
                       'ext_start': ext_start_date,
                       'do_print': True},
            multiple_outputs=True,
            do_xcom_push=True,
            doc=_doc,
        )

        # Update time series? --------------------------------------------------
        _task_id = "decide_ts_update"
        _doc = f"""
        Compare image period and time series period and decide if update needed.
        """
        decide_ts_update = BranchPythonOperator(
            task_id=_task_id,
            python_callable=decide_ts_update_required,
            doc=_doc,
        )

        # Optional: Update TS --------------------------------------------------
        _task_id = "extend_ts"
        _command = f"""bash -c '[ "$(ls -A {os.path.join(ts_path, '*.nc')})" ] && """ \
                   f"""c3s_sm update_ts {img_path} {ts_path} || """ \
                   f"""c3s_sm reshuffle {img_path} {ts_path} --land True --imgbuffer 100'"""
        _doc = f"""
        Only runs when a update is required.
        Get the current time range, the date of the first new image, 
        the last available image date and APPEND new data to the existing time 
        series. If a file is currently being used, repurpose will try until it 
        can append to it.
        """
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        extend_ts = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__c3s_sm__{_task_id}_{version}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600 * 2,
            mount_tmp_dir=False,
            doc=_doc,
        )

        # Optional: Get updated time range -------------------------------------
        _task_id = "get_ts_timerange"
        _doc = f"""
        Get the current temporal coverage of the time series data
        """
        get_ts_timerange = PythonOperator(
            task_id=_task_id,
            python_callable=get_timeranges_from_yml,
            op_kwargs={'img_yml': None,
                       'ts_yml': ts_yml_file,
                       'ext_start': ext_start_date,
                       'do_print': False},
            multiple_outputs=True,
            do_xcom_push=True,
            trigger_rule="none_failed_min_one_success",
            doc=_doc,
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
                       'ds_id': qa4sm_id},
            doc=_doc,
        )

        # Always check the current time range again ----------------------------
        _task_id = "finish"
        _doc = f""" 
        Print the current coverages again. This is just a wrap-up task that 
        ALWAYS runs.
        """
        finish = PythonOperator(
            task_id=_task_id,
            python_callable=get_timeranges_from_yml,
            op_kwargs={'img_yml': img_yml_file,
                       'ts_yml': ts_yml_file,
                       'ext_start': ext_start_date,
                       'do_print': True},
        )

        # Task Logic
        verify_dir_available >> verify_qa4sm_available >> update_images >> get_img_timeranges >> decide_ts_update
        decide_ts_update >> extend_ts >> get_ts_timerange >> update_period >> finish
        decide_ts_update >> get_ts_timerange >> update_period >> finish

