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
from datetime import datetime
import pandas as pd
import requests
import logging

# TODO:
# - Change schedule (not daily)

IMAGE = os.environ["SMOS_DAG_IMAGE"]
QA4SM_IP_OR_URL = os.environ["QA4SM_IP_OR_URL"]
QA4SM_PORT_OR_NONE = os.environ["QA4SM_PORT_OR_NONE"]
QA4SM_API_TOKEN = os.environ["QA4SM_API_TOKEN"]
QA4SM_DATA_PATH = os.environ["QA4SM_DATA_PATH"]   # On the HOST machine

# Source is on the HOST machine (not airflow container), target is in the worker image
#   see also https://stackoverflow.com/questions/31381322/docker-in-docker-cannot-mount-volume
data_mount = Mount(target="/qa4sm/data", source=QA4SM_DATA_PATH, type='bind')

"""
All versions are added to the list. The dag itself can be 
(de)activated in the GUI afterwards
"""
DAG_SETUP = {
    'v700': {
        # Paths here refer to the worker image and should not be changed!
        'img_path': "/qa4sm/data/SMOS_L2/SMOSL2_V700/images/",
        'ts_path': "/qa4sm/data/SMOS_L2/SMOSL2_V700/ts_extension/",
        #'ext_start': "2024-02-21",
        'ext_start_date': "2024-10-01",  # TODO: CHANGE THIS BACK!!!
        'qa4sm_dataset_id': "39",
    },
}

def get_timerange_from_yml(
        img_yml: str = None,
        ts_yml: str = None,
        ext_start_date: str = None,
        do_print: bool = False
) -> dict:

    if (img_yml is None) or (not os.path.isfile(img_yml)):
        logging.info(f"No img yml file: {img_yml}")
        img_to = None
    else:
        with open(img_yml, 'r') as stream:
            img_props = yaml.safe_load(stream)
        img_to = pd.to_datetime(img_props['last_day']).to_pydatetime()

    if (ts_yml is None) or (not os.path.isfile(ts_yml)):
        logging.info(f"No ts yml file: {ts_yml}")
        ts_to = None
    else:
        with open(ts_yml, 'r') as stream:
            ts_props = yaml.safe_load(stream)
        ts_to = pd.to_datetime(ts_props['last_day']).to_pydatetime()

    if ts_to is not None:
        ts_next = ts_to + timedelta(days=1)
    elif ext_start_date is not None:
        ts_next = pd.to_datetime(ext_start_date).to_pydatetime()
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


def decide_ts_update_required(ti=None) -> str:
    img_to = ti.xcom_pull(task_ids="get_timeranges", key="img_to")
    ts_to = ti.xcom_pull(task_ids="get_timeranges", key="ts_to")
    ts_next = ti.xcom_pull(task_ids="get_timeranges", key="ts_next")

    img_to = pd.to_datetime(img_to).to_pydatetime()
    ts_to = pd.to_datetime(ts_to).to_pydatetime() if ts_to is not None else None
    ts_next = pd.to_datetime(ts_next).to_pydatetime()

    logging.info(f"Image to: {img_to}")
    logging.info(f"Ts to: {ts_to}")
    logging.info(f"Ts Next to: {ts_next}")

    if ts_to is None:
        if img_to >= ts_next:
            next = "update_ts"
        else:
            next = "finish"
    else:
        ts_to = pd.to_datetime(ts_to).to_pydatetime()
        if img_to > ts_to:
            next = "update_ts"
        else:
            next = "finish"

    logging.info(f"Decision: {next}")

    return next

def _api_update_period(_ds_id, ti=None) -> str:
    new_ts_to_date: str = ti.xcom_pull("get_new_ts_timerange", key="ts_to")
    if QA4SM_PORT_OR_NONE.lower() not in ['none', '']:
        url = f"http://{QA4SM_IP_OR_URL}:{QA4SM_PORT_OR_NONE}/api/update-dataset-version"
    else:
        url = QA4SM_IP_OR_URL

    headers = {
        "Authorization": f"Token {QA4SM_API_TOKEN}",
        "Content-Type": "application/json"
    }
    data = [
        {
            "id": str(_ds_id),
            "time_range_end": str(new_ts_to_date)
        }
    ]
    response = requests.post(url, headers=headers, json=data)

    return str(response)


for version, dag_settings in DAG_SETUP.items():
    img_path = dag_settings['img_path']
    ts_path = dag_settings['ts_path']
    ext_start_date = dag_settings['ext_start_date']
    qa4sm_id = dag_settings['qa4sm_dataset_id']

    ts_yml = os.path.join(ts_path, 'overview.yml')
    img_yml = os.path.join(img_path, 'overview.yml')

    with DAG(
            f"SMOS_L2-{version}-Processing",
            default_args={
                "depends_on_past": False,
                "email": ["support@qa4sm.eu"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(minutes=1),
            },
            description="Update SMOS L2 image data",
            schedule=timedelta(days=1),
            start_date=datetime(2024, 10, 9),
            catchup=False,   # don't repeat missed runs!
            tags=["smos_l2", "download", "reshuffle", "update"],
    ) as dag:

        # Check data setup -----------------------------------------------------
        _task_id = "verify_dir_available"
        _doc = f"""
        Check if the data store is mounted, chck if {img_path} and {ts_path} exist.
        """
        _command = bash_command = f"bash -c '[ -d \"{img_path}\" ] && [ -d \"{ts_path}\" ] || exit 1'"
        # start container with sudo?
        verify_dir_available = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__smosl2__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            mount_tmp_dir=False,
            doc=_doc
        )

        _task_id = "verify_qa4sm_reachable"
        _doc = f"""
        Check if qa4sm is reachable, 0 = success, 1 = fail
        """
        # start container with sudo?
        verify_qa4sm_available = BashOperator(
            task_id=_task_id,
            bash_command=f"(set -e; nc -z -v {QA4SM_IP_OR_URL} {QA4SM_PORT_OR_NONE} 2>&1 | grep -q 'succeeded' && echo 'OK') || exit 1",
            doc=_doc
        )

        # CDS Image Download----------------------------------------------------
        _task_id = f'update_images'
        _doc = f"""
        This task will check if any image data is available in the img_path.
        If not it download all available data after the ext_start_date (first
        time only). Otherwise, it will download new data on the server, after
        the data that is already available.
        This will not replace any existing files locally. This finds the LATEST 
        local file and checks if any new data AFTER this date is available.
        """
        # https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html
        update_images = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__smosl2__{version}__{_task_id}',
            privileged=True,
            command=f"""bash -c '[ "$(ls -A {img_path})" ] && """ \
            f"""smos_l2 update_img {img_path} --username {os.environ['DISSEO_USERNAME']} --password {os.environ['DISSEO_PASSWORD']} || """ \
            f"""smos_l2 download {img_path} -s {ext_start_date} --username {os.environ['DISSEO_USERNAME']} --password {os.environ['DISSEO_PASSWORD']}'""",
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600*2,
            mount_tmp_dir=False,
            doc=_doc,
        )

        # Get current time series coverage -------------------------------------
        _task_id = "get_timeranges"
        _doc = f"""
        Look at time series and image data and infer their covered period.
        """
        get_timeranges = PythonOperator(
            task_id=_task_id,
            python_callable=get_timerange_from_yml,
            op_kwargs={'img_yml': img_yml,
                       'ts_yml': ts_yml,
                       'ext_start_date': ext_start_date,
                       'do_print': True},
            multiple_outputs=True,
            do_xcom_push=True,
            doc=_doc,
        )

        # Update time series? --------------------------------------------------
        _task_id = "decide_ts_update"
        _doc = f"""
        Compare image period and time series period and decide if update needed.
        This can result in
        - 'finish': No processing required
        - 'update_ts': If extensions exist, update them
        """
        decide_reshuffle = BranchPythonOperator(
            task_id=_task_id,
            python_callable=decide_ts_update_required,
            doc=_doc,
        )

        # Optional: Initial extension TS ---------------------------------------
        _task_id = "update_ts"
        _command = f"""bash -c '[ "$(ls -A {os.path.join(ts_path, '*.nc')})" ] && smos_l2 update_ts {img_path} {ts_path} || """ \
                   f"""smos_l2 reshuffle {img_path} {ts_path} -s {ext_start_date} -m 4'"""
        _doc = f"""
        Creates new time series, or appends new data in time series format.
        """
        extend_ts = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__smosl2__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600*2,
            mount_tmp_dir=False,
            doc=_doc,
        )

        # Optional: Get updated time range -------------------------------------
        _task_id = "get_new_ts_timerange"
        _doc = f"""
        Only runs when an update was performed. Get the new time range of the 
        time series after they were updated.
        """
        get_new_ts_timerange = PythonOperator(
            task_id=_task_id,
            python_callable=get_timerange_from_yml,
            op_kwargs={'img_yml': None, 'ts_yml': ts_yml,
                       'ext_start_date': None, 'do_print': False},
            multiple_outputs=True,
            do_xcom_push=True,
            doc=_doc,
        )

        # Optional: Send new time range to QA4SM -------------------------------
        _doc = f"""
        After the time series is updated, use the NEW time range information
        to update the qa4sm database for the dataset.
        """
        update_period = PythonOperator(
            task_id='api_update_period',
            python_callable=_api_update_period,
            op_kwargs={'_ds_id': qa4sm_id},
            doc=_doc,
        )

        # Always check the current time range again ----------------------------
        _task_id = "finish"
        _doc = f""" 
        Print the current coverages again (either after update or not).
        This is just a wrap-up task that ALWAYS runs.
        """
        finish = PythonOperator(
            task_id=_task_id,
            python_callable=get_timerange_from_yml,
            # trigger_rule='none_failed_min_one_success',
            op_kwargs={'img_yml': img_yml,
                       'ts_yml': ts_yml,
                       'ext_start_date': ext_start_date,
                       'do_print': True},
            doc=_doc,
        )

        # Task logic -----------------------------------------------------------
        verify_dir_available >> verify_qa4sm_available >> update_images >> get_timeranges >> decide_reshuffle
        decide_reshuffle >> extend_ts >> get_new_ts_timerange >> update_period >> finish
        decide_reshuffle >> finish
